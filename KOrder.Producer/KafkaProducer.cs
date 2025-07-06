using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Google.Protobuf;

namespace KOrder.Producer;

public class KafkaProducer : IDisposable
{
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly IProducer<string, byte[]> _producer;

    public KafkaProducer(string bootstrapServers, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;

        // Create topic if it doesn't exist
        EnsureTopicExistsAsync(bootstrapServers, topic).GetAwaiter().GetResult();

        ProducerConfig config = new()
        {
            BootstrapServers = _bootstrapServers,
            // Idempotence ensures that messages are delivered exactly once
            EnableIdempotence = true,
            // Set acknowledgment level to wait for the leader and all replicas
            Acks = Acks.All,
            // Retry configuration
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000
        };

        _producer = new ProducerBuilder<string, byte[]>(config).Build();
    }

    /// <summary>
    /// Produces a message to Kafka with the specified key and value
    /// </summary>
    /// <param name="key">The message key (used for partitioning)</param>
    /// <param name="value">The message content</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    /// <returns>A task representing the asynchronous operation</returns>
    public async Task<DeliveryResult<string, byte[]>> ProduceAsync(string key, Order value, CancellationToken cancellationToken = default)
    {
        try
        {
            Message<string, byte[]> message = new()
            {
                Key = key,
                Value = value.ToByteArray()
            };

            return await _producer.ProduceAsync(_topic, message, cancellationToken);
        }
        catch (ProduceException<string, byte[]> ex)
        {
            await Console.Error.WriteLineAsync($"Failed to deliver message: {ex.Error.Reason}");
            throw;
        }
    }

    /// <summary>
    /// Produces a batch of messages with the same key to maintain ordering
    /// </summary>
    /// <param name="key">The message key (used for partitioning)</param>
    /// <param name="messages">The message values to send</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    public async Task ProduceBatchAsync(string key, IEnumerable<Order> messages, CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
        {
            await ProduceAsync(key, message, cancellationToken);
        }
    }

    /// <summary>
    /// Produces messages for multiple keys, useful for testing the KeyedConsumer
    /// </summary>
    /// <param name="keyedMessages">Dictionary where keys are message keys and values are lists of message content</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    public async Task ProduceMultiKeyBatchAsync(
        Dictionary<string, List<Order>> keyedMessages,
        CancellationToken cancellationToken = default)
    {
        List<Task> tasks = [];

        foreach (KeyValuePair<string, List<Order>> kvp in keyedMessages)
        {
            string key = kvp.Key;
            List<Order> messages = kvp.Value;

            tasks.Add(ProduceBatchAsync(key, messages, cancellationToken));
        }

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Ensures that the specified topic exists, creating it if necessary
    /// </summary>
    /// <param name="bootstrapServers">Kafka bootstrap servers</param>
    /// <param name="topicName">Name of the topic to create</param>
    /// <param name="numPartitions">Number of partitions for the topic (default: 3)</param>
    /// <param name="replicationFactor">Replication factor for the topic (default: 1)</param>
    /// <returns>A task representing the asynchronous operation</returns>
    private static async Task EnsureTopicExistsAsync(
        string bootstrapServers,
        string topicName,
        int numPartitions = 3,
        short replicationFactor = 1)
    {
        AdminClientConfig adminConfig = new()
        {
            BootstrapServers = bootstrapServers
        };

        using IAdminClient adminClient = new AdminClientBuilder(adminConfig).Build();

        try
        {
            // Check if topic already exists
            Metadata metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            bool topicExists = metadata.Topics.Any(t => t.Topic == topicName);

            if (!topicExists)
            {
                Console.WriteLine($"Topic '{topicName}' does not exist. Creating it...");

                TopicSpecification topicSpec = new()
                {
                    Name = topicName,
                    NumPartitions = numPartitions,
                    ReplicationFactor = replicationFactor
                };

                await adminClient.CreateTopicsAsync(new[] { topicSpec });
                Console.WriteLine($"Topic '{topicName}' created successfully with {numPartitions} partitions and replication factor {replicationFactor}.");
            }
            else
            {
                Console.WriteLine($"Topic '{topicName}' already exists.");
            }
        }
        catch (CreateTopicsException ex)
        {
            // Check if the error is because topic already exists
            if (ex.Results[0].Error.Code == ErrorCode.TopicAlreadyExists)
            {
                Console.WriteLine($"Topic '{topicName}' already exists.");
            }
            else
            {
                Console.WriteLine($"Failed to create topic '{topicName}': {ex.Results[0].Error.Reason}");
                throw;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error checking/creating topic '{topicName}': {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Flush any pending messages and clean up resources
    /// </summary>
    public void Dispose()
    {
        // Ensure all messages are delivered before disposing
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
