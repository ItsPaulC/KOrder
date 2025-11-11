using Confluent.Kafka;
using KOrder;
using KOrder.Consumer.Engine;
using Microsoft.Extensions.Logging;

namespace KThread.Consumer;

/// <summary>
/// Message processor for Order messages
/// </summary>
public class OrderMessageProcessor : IMessageProcessor<Order>
{
    private readonly ILogger<OrderMessageProcessor> _logger;

    public OrderMessageProcessor(ILogger<OrderMessageProcessor> logger)
    {
        _logger = logger;
    }

    public async Task ProcessAsync(ConsumeResult<string, Order> consumeResult)
    {
        Order order = consumeResult.Message.Value;
        string key = consumeResult.Message.Key;

        _logger.LogDebug("Processing message for Key={Key}, Status={Status}, Partition={Partition}, Offset={Offset}",
            key, order.Status, consumeResult.Partition.Value, consumeResult.Offset.Value);

        // Simulate some work - this ensures messages are processed one at a time per key
        await Task.Delay(100);

        // Add your custom processing logic here
        // For example: save to database, call external API, etc.
    }
}
