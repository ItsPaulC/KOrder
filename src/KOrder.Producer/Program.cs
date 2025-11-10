using KOrder;
using KOrder.Producer;

// Configuration
const string bootstrapServers = "localhost:9092";
const string topic = "keyed-messages-topic";

// Create the producer
using var producer = new KafkaProducer(new KafkaProducerConfig{BootstrapServers = bootstrapServers, Topic = topic});

Console.WriteLine("Kafka Producer started. Press Ctrl+C to exit.");
Console.WriteLine($"Connected to: {bootstrapServers}");
Console.WriteLine($"Publishing to topic: {topic}");
Console.WriteLine();

var running = true;
while (running)
{
    Console.WriteLine("Menu Options:");
    Console.WriteLine("1. Send a single message with key");
    Console.WriteLine("2. Send batch of messages with the same key");
    Console.WriteLine("3. Send messages with multiple keys (simulate concurrent processing)");
    Console.WriteLine("4. Run demo with predefined messages");
    Console.WriteLine("5. Exit");
    Console.WriteLine();
    Console.Write("Select an option (1-5): ");
    var option = Console.ReadLine();

    switch (option)
    {
        case "1":
            await SendSingleMessage(producer);
            break;
        case "2":
            await SendBatchWithSameKey(producer);
            break;
        case "3":
            await SendMultiKeyBatch(producer);
            break;
        case "4":
            await RunDemo(producer);
            break;
        case "5":
            running = false;
            break;
        default:
            Console.WriteLine("Invalid option. Please try again.");
            break;
    }
}

Console.WriteLine("Producer shutting down...");

// Helper methods
async Task SendSingleMessage(KafkaProducer kafkaProducer)
{
    Console.Write("Enter key: ");
    var key = Console.ReadLine() ?? "default-key";

    Console.Write("Enter status: ");
    var status = Console.ReadLine() ?? "default-status";
    var order = new Order { Status = status };

    var result = await kafkaProducer.ProduceAsync(key, order);
    Console.WriteLine($"Message delivered to: {result.TopicPartitionOffset}");
}

async Task SendBatchWithSameKey(KafkaProducer kafkaProducer)
{
    Console.Write("Enter key: ");
    var key = Console.ReadLine() ?? "default-key";

    Console.Write("Enter number of messages to send: ");
    if (!int.TryParse(Console.ReadLine(), out var count))
    {
        count = 5; // Default
    }

    var messages = new List<Order>();
    for (var i = 0; i < count; i++)
    {
        Console.Write($"Enter status {i + 1}: ");
        var status = Console.ReadLine() ?? $"Order {i + 1} for key {key}";
        messages.Add(new Order { Status = status });
    }

    await kafkaProducer.ProduceBatchAsync(key, messages);
    Console.WriteLine($"{count} messages sent with key: {key}");
}

async Task SendMultiKeyBatch(IKafkaProducer kafkaProducer)
{
    Console.Write("Enter number of keys: ");
    if (!int.TryParse(Console.ReadLine(), out var keyCount))
    {
        keyCount = 3; // Default
    }

    var keyedMessages = new Dictionary<string, List<Order>>();

    for (var k = 0; k < keyCount; k++)
    {
        Console.Write($"Enter key {k + 1}: ");
        var key = Console.ReadLine() ?? $"key-{k + 1}";

        Console.Write($"Enter number of messages for key {key}: ");
        if (!int.TryParse(Console.ReadLine(), out var messageCount))
        {
            messageCount = 3; // Default
        }

        var messages = new List<Order>();
        for (var i = 0; i < messageCount; i++)
        {
            Console.Write($"Enter status {i + 1} for key {key}: ");
            var status = Console.ReadLine() ?? $"Order {i + 1} for key {key}";
            messages.Add(new Order { Status = status });
        }

        keyedMessages[key] = messages;
    }

    await kafkaProducer.ProduceMultiKeyBatchAsync(keyedMessages);
    Console.WriteLine("All messages sent successfully.");
}

async Task RunDemo(IKafkaProducer kafkaProducer)
{
    Console.WriteLine("Running demo with predefined messages...");
    Console.WriteLine("This demo will send messages for 3 customers concurrently to test ordering preservation per key.");

    // Create sample data with multiple keys
    var keyedMessages = new Dictionary<string, List<Order>>
    {
        ["customer-1"] =
        [
            new Order { Status = "Order placed for customer 1" },
            new Order { Status = "Payment processed for customer 1" },
            new Order { Status = "Order shipped for customer 1" },
            new Order { Status = "Order delivered for customer 1" }
        ],
        ["customer-2"] =
        [
            new Order { Status = "Order placed for customer 2" },
            new Order { Status = "Payment failed for customer 2" },
            new Order { Status = "Payment retry for customer 2" },
            new Order { Status = "Payment processed for customer 2" },
            new Order { Status = "Order shipped for customer 2" }
        ],
        ["customer-3"] =
        [
            new Order { Status = "Order placed for customer 3" },
            new Order { Status = "Payment processed for customer 3" },
            new Order { Status = "Order cancelled for customer 3" }
        ]
    };

    // Send messages concurrently to test the ordering preservation
    // Each key's messages should be processed in order, but different keys can be interleaved
    await kafkaProducer.ProduceMultiKeyBatchAsync(keyedMessages);
    Console.WriteLine("Demo completed - sent messages for 3 different customers.");
    Console.WriteLine("Check the consumer output - messages for each customer should be in order.");
}