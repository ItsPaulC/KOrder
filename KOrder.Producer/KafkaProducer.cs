using Confluent.Kafka;

namespace KOrder.Producer;

public class KafkaProducer : IDisposable
{
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly IProducer<string, string> _producer;
    
    public KafkaProducer(string bootstrapServers, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
        
        var config = new ProducerConfig
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
        
        _producer = new ProducerBuilder<string, string>(config).Build();
    }
    
    /// <summary>
    /// Produces a message to Kafka with the specified key and value
    /// </summary>
    /// <param name="key">The message key (used for partitioning)</param>
    /// <param name="value">The message content</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    /// <returns>A task representing the asynchronous operation</returns>
    public async Task<DeliveryResult<string, string>> ProduceAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        try
        {
            var message = new Message<string, string>
            {
                Key = key,
                Value = value
            };
            
            return await _producer.ProduceAsync(_topic, message, cancellationToken);
        }
        catch (ProduceException<string, string> ex)
        {
            Console.Error.WriteLine($"Failed to deliver message: {ex.Error.Reason}");
            throw;
        }
    }
    
    /// <summary>
    /// Produces a batch of messages with the same key to maintain ordering
    /// </summary>
    /// <param name="key">The message key (used for partitioning)</param>
    /// <param name="messages">The message values to send</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    public async Task ProduceBatchAsync(string key, IEnumerable<string> messages, CancellationToken cancellationToken = default)
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
        Dictionary<string, List<string>> keyedMessages, 
        CancellationToken cancellationToken = default)
    {
        var tasks = new List<Task>();
        
        foreach (var kvp in keyedMessages)
        {
            var key = kvp.Key;
            var messages = kvp.Value;
            
            tasks.Add(ProduceBatchAsync(key, messages, cancellationToken));
        }
        
        await Task.WhenAll(tasks);
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
