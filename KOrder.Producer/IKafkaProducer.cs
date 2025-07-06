using Confluent.Kafka;

namespace KOrder.Producer;

public interface IKafkaProducer
{
    /// <summary>
    /// Produces a message to Kafka with the specified key and value
    /// </summary>
    /// <param name="key">The message key (used for partitioning)</param>
    /// <param name="value">The message content</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    /// <returns>A task representing the asynchronous operation</returns>
    Task<DeliveryResult<string, byte[]>> ProduceAsync(string key, Order value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Produces a batch of messages with the same key to maintain ordering
    /// </summary>
    /// <param name="key">The message key (used for partitioning)</param>
    /// <param name="messages">The message values to send</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    Task ProduceBatchAsync(string key, IEnumerable<Order> messages, CancellationToken cancellationToken = default);

    /// <summary>
    /// Produces messages for multiple keys, useful for testing the KeyedConsumer
    /// </summary>
    /// <param name="keyedMessages">Dictionary where keys are message keys and values are lists of message content</param>
    /// <param name="cancellationToken">Optional cancellation token</param>
    Task ProduceMultiKeyBatchAsync(
        Dictionary<string, List<Order>> keyedMessages,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Flush any pending messages and clean up resources
    /// </summary>
    void Dispose();
}