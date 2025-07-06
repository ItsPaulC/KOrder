using System.Collections.Concurrent;
using Confluent.Kafka;
using KOrder;

namespace KThread.Consumer;

public class KeyedConsumer
{
    private readonly string _bootstrapServers;
    private readonly string _groupId;
    private readonly string _topic;
    private readonly ConcurrentDictionary<string, Task> _processingTasks = new();
    private readonly ConcurrentDictionary<string, ConcurrentQueue<ConsumeResult<string, byte[]>>> _messageQueues = new();
    private readonly CancellationTokenSource _cts = new();

    public KeyedConsumer(string bootstrapServers, string groupId, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _groupId = groupId;
        _topic = topic;
    }

    public Task StartConsumerAsync()
    {
        return Task.Run(() =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
            consumer.Subscribe(_topic);

            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(_cts.Token);

                    if (consumeResult?.Message != null)
                    {
                        string? key = consumeResult.Message.Key;
                        if (!string.IsNullOrEmpty(key))
                        {
                            _messageQueues.TryGetValue(key, out var queue);
                            if (queue == null)
                            {
                                queue = new ConcurrentQueue<ConsumeResult<string, byte[]>>();
                                _messageQueues.TryAdd(key, queue);
                                // Start processing for this key if not already started
                                _processingTasks.TryAdd(key, Task.Run(() => ProcessMessagesForKeyAsync(key, queue)));
                            }
                            queue.Enqueue(consumeResult);
                        }
                        else
                        {
                            // Handle messages without a key (processing order not guaranteed)
                            Console.WriteLine($"Received message without key: {consumeResult.Message?.Value}");
                            // You might want a separate processing mechanism for these
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Consumer stopped
            }
            finally
            {
                consumer.Close();
            }
        });
    }

    private async Task ProcessMessagesForKeyAsync(string key, ConcurrentQueue<ConsumeResult<string, byte[]>> queue)
    {
        while (!_cts.Token.IsCancellationRequested || !queue.IsEmpty)
        {
            if (queue.TryDequeue(out var message) && message.Message?.Value != null)
            {
                try
                {
                    // Simulate processing the message
                    var order = Order.Parser.ParseFrom(message.Message.Value);
                    Console.WriteLine($"Processing message with key '{key}': {order.Status} (Partition: {message.Partition.Value}, Offset: {message.Offset.Value})");
                    
                    // Simulate some work - this ensures messages are processed one at a time per key
                    await Task.Delay(100, _cts.Token);
                }
                catch (OperationCanceledException)
                {
                    // Processing was cancelled
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message for key '{key}': {ex.Message}");
                }
            }
            else
            {
                // If the queue is empty and the consumer is still running, wait for new messages
                try
                {
                    await Task.Delay(50, _cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        // Clean up the processing task when done
        _processingTasks.TryRemove(key, out _);
        _messageQueues.TryRemove(key, out _);
        Console.WriteLine($"Stopped processing for key: {key}");
    }

    public void StopConsumer()
    {
        _cts.Cancel();
        Task.WaitAll(_processingTasks.Values.ToArray()); // Wait for all processing tasks to complete
    }
}