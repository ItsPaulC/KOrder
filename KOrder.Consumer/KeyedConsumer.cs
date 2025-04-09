using System.Collections.Concurrent;
using Confluent.Kafka;

namespace KThread.Consumer;

public class KeyedConsumer
{
    private readonly string _bootstrapServers;
    private readonly string _groupId;
    private readonly string _topic;
    private readonly ConcurrentDictionary<string, Task> _processingTasks = new();
    private readonly ConcurrentDictionary<string, ConcurrentQueue<ConsumeResult<string, string>>> _messageQueues = new();
    private readonly CancellationTokenSource _cts = new();

    public KeyedConsumer(string bootstrapServers, string groupId, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _groupId = groupId;
        _topic = topic;
    }

    public async Task StartConsumerAsync()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_topic);

        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(_cts.Token);

                if (consumeResult != null)
                {
                    string key = consumeResult.Message?.Key;
                    if (key != null)
                    {
                        _messageQueues.TryGetValue(key, out var queue);
                        if (queue == null)
                        {
                            queue = new ConcurrentQueue<ConsumeResult<string, string>>();
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
    }

    private async Task ProcessMessagesForKeyAsync(string key, ConcurrentQueue<ConsumeResult<string, string>> queue)
    {
        while (!_cts.Token.IsCancellationRequested || !queue.IsEmpty)
        {
            if (queue.TryDequeue(out var message))
            {
                // Simulate processing the message
                Console.WriteLine($"Processing message with key '{key}': {message.Message?.Value} (Partition: {message.Partition.Value}, Offset: {message.Offset.Value})");
                await Task.Delay(100); // Simulate some work
            }
            else
            {
                // If the queue is empty and the consumer is still running, wait for new messages
                await Task.Delay(100);
            }
        }
        // Clean up the processing task when done
        _processingTasks.TryRemove(key, out _);
        _messageQueues.TryRemove(key, out _);
    }

    public void StopConsumer()
    {
        _cts.Cancel();
        Task.WaitAll(_processingTasks.Values.ToArray()); // Wait for all processing tasks to complete
    }
}