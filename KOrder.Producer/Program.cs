
using KOrder.Producer;

// Configuration
const string bootstrapServers = "localhost:9092";
const string topic = "keyed-messages-topic";

// Create the producer
using var producer = new KafkaProducer(bootstrapServers, topic);

Console.WriteLine("Kafka Producer started. Press Ctrl+C to exit.");
Console.WriteLine($"Connected to: {bootstrapServers}");
Console.WriteLine($"Publishing to topic: {topic}");
Console.WriteLine();
Console.WriteLine("Menu Options:");
Console.WriteLine("1. Send a single message with key");
Console.WriteLine("2. Send batch of messages with the same key");
Console.WriteLine("3. Send messages with multiple keys (simulate concurrent processing)");
Console.WriteLine("4. Run demo with predefined messages");
Console.WriteLine("5. Exit");

var running = true;
while (running)
{
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
    
    Console.Write("Enter message: ");
    var message = Console.ReadLine() ?? "default-message";

    var result = await kafkaProducer.ProduceAsync(key, message);
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

    var messages = new List<string>();
    for (var i = 0; i < count; i++)
    {
        Console.Write($"Enter message {i + 1}: ");
        var message = Console.ReadLine() ?? $"Message {i + 1} for key {key}";
        messages.Add(message);
    }

    await kafkaProducer.ProduceBatchAsync(key, messages);
    Console.WriteLine($"{count} messages sent with key: {key}");
}

async Task SendMultiKeyBatch(KafkaProducer kafkaProducer)
{
    Console.Write("Enter number of keys: ");
    if (!int.TryParse(Console.ReadLine(), out var keyCount))
    {
        keyCount = 3; // Default
    }

    var keyedMessages = new Dictionary<string, List<string>>();
    
    for (var k = 0; k < keyCount; k++)
    {
        Console.Write($"Enter key {k + 1}: ");
        var key = Console.ReadLine() ?? $"key-{k + 1}";
        
        Console.Write($"Enter number of messages for key {key}: ");
        if (!int.TryParse(Console.ReadLine(), out var messageCount))
        {
            messageCount = 3; // Default
        }

        var messages = new List<string>();
        for (var i = 0; i < messageCount; i++)
        {
            Console.Write($"Enter message {i + 1} for key {key}: ");
            var message = Console.ReadLine() ?? $"Message {i + 1} for key {key}";
            messages.Add(message);
        }
        
        keyedMessages[key] = messages;
    }

    await kafkaProducer.ProduceMultiKeyBatchAsync(keyedMessages);
    Console.WriteLine("All messages sent successfully.");
}

async Task RunDemo(KafkaProducer kafkaProducer)
{
    Console.WriteLine("Running demo with predefined messages...");
    
    // Create sample data with multiple keys
    var keyedMessages = new Dictionary<string, List<string>>
    {
        ["customer-1"] = new List<string>
        {
            "Order placed for customer 1",
            "Payment processed for customer 1",
            "Order shipped for customer 1",
            "Order delivered for customer 1"
        },
        ["customer-2"] = new List<string>
        {
            "Order placed for customer 2",
            "Payment failed for customer 2",
            "Payment retry for customer 2",
            "Payment processed for customer 2",
            "Order shipped for customer 2"
        },
        ["customer-3"] = new List<string>
        {
            "Order placed for customer 3",
            "Payment processed for customer 3",
            "Order cancelled for customer 3"
        }
    };

    // This will simulate concurrent messages for different keys
    // The consumer should process them in order per key
    await kafkaProducer.ProduceMultiKeyBatchAsync(keyedMessages);
    Console.WriteLine("Demo completed - sent messages for 3 different customers.");
}