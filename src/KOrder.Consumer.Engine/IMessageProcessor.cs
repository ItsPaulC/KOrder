using Confluent.Kafka;
using Google.Protobuf;

namespace KOrder.Consumer.Engine;

/// <summary>
/// Interface for processing Kafka messages
/// </summary>
/// <typeparam name="TMessage">Protobuf message type</typeparam>
public interface IMessageProcessor<TMessage> where TMessage : IMessage<TMessage>, new()
{
    /// <summary>
    /// Process a Kafka message
    /// </summary>
    /// <param name="consumeResult">The consumed message</param>
    /// <returns>Task representing the async operation</returns>
    Task ProcessAsync(ConsumeResult<string, TMessage> consumeResult);
}
