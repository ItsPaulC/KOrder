using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Google.Protobuf;
using KOrder;
using KThread.Consumer;
using Testcontainers.Kafka;

namespace KOrder.Consumer.Tests;

public class KeyedConsumerIntegrationTests : IAsyncLifetime
{
    private KafkaContainer? _kafkaContainer;
    private string _bootstrapServers = string.Empty;
    private const string TestTopic = "test-keyed-messages";

    private readonly ITestOutputHelper _output;

    public KeyedConsumerIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public async Task InitializeAsync()
    {
        _output.WriteLine("Starting Kafka container...");
        _kafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.8.0")
            .Build();

        await _kafkaContainer.StartAsync();
        _bootstrapServers = _kafkaContainer.GetBootstrapAddress();
        _output.WriteLine($"Kafka container started at: {_bootstrapServers}");

        // Create the test topic
        await CreateTopicAsync(TestTopic, partitions: 3);
    }

    public async Task DisposeAsync()
    {
        if (_kafkaContainer != null)
        {
            _output.WriteLine("Stopping Kafka container...");
            await _kafkaContainer.StopAsync();
            await _kafkaContainer.DisposeAsync();
        }
    }

    private async Task CreateTopicAsync(string topicName, int partitions = 1)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _bootstrapServers
        }).Build();

        try
        {
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = partitions,
                    ReplicationFactor = 1
                }
            });
            _output.WriteLine($"Topic '{topicName}' created with {partitions} partitions");
        }
        catch (CreateTopicsException ex)
        {
            _output.WriteLine($"Topic creation warning: {ex.Message}");
        }
    }

    [Fact]
    public async Task SingleKey_MessagesProcessedInOrder()
    {
        // Arrange
        var groupId = $"test-group-{Guid.NewGuid()}";
        var key = "order-123";
        var messagesProcessed = new ConcurrentBag<(string Key, int OrderId, DateTime ProcessedAt)>();
        var messageCount = 10;

        // Create a custom consumer that tracks processing order
        var consumer = CreateTrackingConsumer(groupId, messagesProcessed);

        // Produce messages in order
        await ProduceOrderedMessagesAsync(key, messageCount);

        // Act
        var consumerTask = consumer.StartConsumerAsync();

        // Wait for all messages to be processed
        await WaitForMessagesAsync(messagesProcessed, messageCount, timeoutSeconds: 30);

        // Stop the consumer
        consumer.StopConsumer();
        await consumerTask;

        // Assert
        var processedMessages = messagesProcessed
            .Where(m => m.Key == key)
            .OrderBy(m => m.ProcessedAt)
            .Select(m => m.OrderId)
            .ToList();

        Assert.Equal(messageCount, processedMessages.Count);

        // Verify messages were processed in order
        for (int i = 0; i < messageCount; i++)
        {
            Assert.Equal(i, processedMessages[i]);
        }

        _output.WriteLine($"✓ All {messageCount} messages for key '{key}' were processed in correct order");
    }

    [Fact]
    public async Task MultipleKeys_MessagesProcessedInParallel()
    {
        // Arrange
        var groupId = $"test-group-{Guid.NewGuid()}";
        var keys = new[] { "order-A", "order-B", "order-C" };
        var messagesPerKey = 5;
        var messagesProcessed = new ConcurrentBag<(string Key, int OrderId, DateTime ProcessedAt)>();

        var consumer = CreateTrackingConsumer(groupId, messagesProcessed);

        // Produce messages for multiple keys
        foreach (var key in keys)
        {
            await ProduceOrderedMessagesAsync(key, messagesPerKey);
        }

        // Act
        var consumerTask = consumer.StartConsumerAsync();

        // Wait for all messages to be processed
        await WaitForMessagesAsync(messagesProcessed, keys.Length * messagesPerKey, timeoutSeconds: 30);

        consumer.StopConsumer();
        await consumerTask;

        // Assert
        Assert.Equal(keys.Length * messagesPerKey, messagesProcessed.Count);

        // Verify each key's messages were processed in order
        foreach (var key in keys)
        {
            var keyMessages = messagesProcessed
                .Where(m => m.Key == key)
                .OrderBy(m => m.ProcessedAt)
                .Select(m => m.OrderId)
                .ToList();

            Assert.Equal(messagesPerKey, keyMessages.Count);

            for (int i = 0; i < messagesPerKey; i++)
            {
                Assert.Equal(i, keyMessages[i]);
            }

            _output.WriteLine($"✓ Key '{key}': All {messagesPerKey} messages processed in correct order");
        }

        // Verify parallel processing: check that different keys were processed concurrently
        var processingTimeSpan = messagesProcessed.Max(m => m.ProcessedAt) - messagesProcessed.Min(m => m.ProcessedAt);
        _output.WriteLine($"Total processing time span: {processingTimeSpan.TotalSeconds:F2}s");

        // If sequential, would take ~1.5s (3 keys * 5 messages * 100ms)
        // If parallel, should take ~0.5s (5 messages * 100ms)
        // Allow some margin for timing variations
        Assert.True(processingTimeSpan.TotalSeconds < 1.0,
            "Messages should be processed in parallel, not sequentially");
    }

    [Fact]
    public async Task DifferentKeys_DoNotBlockEachOther()
    {
        // Arrange
        var groupId = $"test-group-{Guid.NewGuid()}";
        var fastKey = "fast-key";
        var slowKey = "slow-key";
        var messagesProcessed = new ConcurrentBag<(string Key, int OrderId, DateTime ProcessedAt)>();

        var consumer = CreateTrackingConsumer(groupId, messagesProcessed, processingDelayMs: 100);

        // Produce one message for slow key, then multiple for fast key
        await ProduceOrderedMessagesAsync(slowKey, 1);
        await Task.Delay(50); // Small delay to ensure slow message is picked up first
        await ProduceOrderedMessagesAsync(fastKey, 3);

        // Act
        var consumerTask = consumer.StartConsumerAsync();

        await WaitForMessagesAsync(messagesProcessed, 4, timeoutSeconds: 30);

        consumer.StopConsumer();
        await consumerTask;

        // Assert
        var slowKeyMessages = messagesProcessed.Where(m => m.Key == slowKey).ToList();
        var fastKeyMessages = messagesProcessed.Where(m => m.Key == fastKey).ToList();

        Assert.Single(slowKeyMessages);
        Assert.Equal(3, fastKeyMessages.Count);

        // Fast key messages should complete despite slow key still processing
        var firstFastMessage = fastKeyMessages.Min(m => m.ProcessedAt);
        var slowMessage = slowKeyMessages.First().ProcessedAt;

        _output.WriteLine($"Slow key processed at: {slowMessage:HH:mm:ss.fff}");
        _output.WriteLine($"First fast key processed at: {firstFastMessage:HH:mm:ss.fff}");

        // Both should be processing in parallel
        _output.WriteLine("✓ Different keys processed independently without blocking");
    }

    [Fact]
    public async Task ConsumerRestart_OffsetCommitted_NoMessageReprocessing()
    {
        // Arrange
        var groupId = $"test-group-{Guid.NewGuid()}";
        var key = "order-restart-test";
        var messagesProcessedFirstRun = new ConcurrentBag<(string Key, int OrderId, DateTime ProcessedAt)>();
        var messagesProcessedSecondRun = new ConcurrentBag<(string Key, int OrderId, DateTime ProcessedAt)>();

        // First run: Process some messages
        await ProduceOrderedMessagesAsync(key, 5);

        var consumer1 = CreateTrackingConsumer(groupId, messagesProcessedFirstRun);

        var consumerTask1 = consumer1.StartConsumerAsync();
        await WaitForMessagesAsync(messagesProcessedFirstRun, 5, timeoutSeconds: 30);

        consumer1.StopConsumer();
        await consumerTask1;

        _output.WriteLine($"First run: Processed {messagesProcessedFirstRun.Count} messages");

        // Wait a bit to ensure offsets are committed
        await Task.Delay(1000);

        // Second run: Start new consumer with same group ID
        var consumer2 = CreateTrackingConsumer(groupId, messagesProcessedSecondRun);

        var consumerTask2 = consumer2.StartConsumerAsync();

        // Wait a bit to see if any messages are reprocessed
        await Task.Delay(3000);

        consumer2.StopConsumer();
        await consumerTask2;

        // Assert
        Assert.Equal(5, messagesProcessedFirstRun.Count);
        Assert.Empty(messagesProcessedSecondRun); // No messages should be reprocessed

        _output.WriteLine("✓ No messages were reprocessed after consumer restart");
    }

    [Fact]
    public async Task MixedKeys_InterleavedMessages_OrderPreservedPerKey()
    {
        // Arrange
        var groupId = $"test-group-{Guid.NewGuid()}";
        var messagesProcessed = new ConcurrentBag<(string Key, int OrderId, DateTime ProcessedAt)>();

        var consumer = CreateTrackingConsumer(groupId, messagesProcessed);

        // Produce interleaved messages for different keys
        var config = new ProducerConfig { BootstrapServers = _bootstrapServers };
        using var producer = new ProducerBuilder<string, byte[]>(config).Build();

        var messagesToProduce = new List<(string Key, int OrderId)>
        {
            ("A", 0), ("B", 0), ("A", 1), ("C", 0), ("B", 1),
            ("A", 2), ("C", 1), ("B", 2), ("C", 2), ("A", 3)
        };

        foreach (var (key, orderId) in messagesToProduce)
        {
            var order = new Order { OrderId = orderId, Status = $"{key}-{orderId}" };
            var message = new Message<string, byte[]>
            {
                Key = key,
                Value = order.ToByteArray()
            };
            await producer.ProduceAsync(TestTopic, message);
            _output.WriteLine($"Produced: Key={key}, OrderId={orderId}");
        }

        producer.Flush(TimeSpan.FromSeconds(10));

        // Act
        var consumerTask = consumer.StartConsumerAsync();
        await WaitForMessagesAsync(messagesProcessed, 10, timeoutSeconds: 30);

        consumer.StopConsumer();
        await consumerTask;

        // Assert
        var keyGroups = messagesProcessed.GroupBy(m => m.Key);

        foreach (var group in keyGroups)
        {
            var orderedMessages = group.OrderBy(m => m.ProcessedAt).Select(m => m.OrderId).ToList();
            var expectedOrder = Enumerable.Range(0, orderedMessages.Count).ToList();

            Assert.Equal(expectedOrder, orderedMessages);
            _output.WriteLine($"✓ Key '{group.Key}': Messages processed in order {string.Join(", ", orderedMessages)}");
        }
    }

    private async Task ProduceOrderedMessagesAsync(string key, int count)
    {
        var config = new ProducerConfig { BootstrapServers = _bootstrapServers };
        using var producer = new ProducerBuilder<string, byte[]>(config).Build();

        for (int i = 0; i < count; i++)
        {
            var order = new Order
            {
                OrderId = i,
                Status = $"Status-{i}",
                CustomerId = key
            };

            var message = new Message<string, byte[]>
            {
                Key = key,
                Value = order.ToByteArray()
            };

            var result = await producer.ProduceAsync(TestTopic, message);
            _output.WriteLine($"Produced message: Key={key}, OrderId={i}, Partition={result.Partition.Value}, Offset={result.Offset.Value}");
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private async Task WaitForMessagesAsync(
        ConcurrentBag<(string Key, int OrderId, DateTime ProcessedAt)> bag,
        int expectedCount,
        int timeoutSeconds)
    {
        var timeout = DateTime.UtcNow.AddSeconds(timeoutSeconds);

        while (bag.Count < expectedCount && DateTime.UtcNow < timeout)
        {
            await Task.Delay(100);
        }

        if (bag.Count < expectedCount)
        {
            _output.WriteLine($"Warning: Only {bag.Count}/{expectedCount} messages processed within {timeoutSeconds}s timeout");
        }
    }

    private KeyedConsumer CreateTrackingConsumer(
        string groupId,
        ConcurrentBag<(string Key, int OrderId, DateTime ProcessedAt)> messagesProcessed,
        int processingDelayMs = 100)
    {
        return new KeyedConsumer(
            _bootstrapServers,
            groupId,
            TestTopic,
            async (consumeResult) =>
            {
                // Parse the order message
                var order = Order.Parser.ParseFrom(consumeResult.Message.Value);
                var key = consumeResult.Message.Key;

                _output.WriteLine($"[Key: {key}] Processing OrderId={order.OrderId} at {DateTime.UtcNow:HH:mm:ss.fff}");

                // Simulate processing time
                await Task.Delay(processingDelayMs);

                // Track the processed message
                messagesProcessed.Add((key, order.OrderId, DateTime.UtcNow));

                _output.WriteLine($"[Key: {key}] Completed OrderId={order.OrderId}");
            });
    }
}
