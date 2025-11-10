using System.Collections.Concurrent;
using System.Threading.Channels;
using Confluent.Kafka;
using KOrder;

namespace KThread.Consumer;

public class KeyedConsumer
{
    private readonly string _bootstrapServers;
    private readonly string _groupId;
    private readonly string _topic;
    private readonly ConcurrentDictionary<string, Task> _processingTasks = new();
    private readonly ConcurrentDictionary<string, ChannelWriter<ConsumeResult<string, Order>>> _messageChannels = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastActivityTime = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly TimeSpan _idleTimeout = TimeSpan.FromMinutes(5);
    private IConsumer<string, Order>? _consumer;
    private readonly Func<ConsumeResult<string, Order>, Task>? _messageProcessor;

    public KeyedConsumer(string bootstrapServers, string groupId, string topic, Func<ConsumeResult<string, Order>, Task>? messageProcessor = null)
    {
        _bootstrapServers = bootstrapServers;
        _groupId = groupId;
        _topic = topic;
        _messageProcessor = messageProcessor;
    }

    public Task StartConsumerAsync()
    {
        return Task.Run(async () =>
        {
            ConsumerConfig config = new()
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false, // Manual commit for better control
                EnableAutoOffsetStore = false, // Store offsets manually after processing
                SessionTimeoutMs = 10000,
                MaxPollIntervalMs = 300000
            };

            _consumer = new ConsumerBuilder<string, Order>(config)
                .SetValueDeserializer(new ProtobufDeserializer<Order>(Order.Parser))
                .Build();
            _consumer.Subscribe(_topic);

            // Start idle key cleanup task
            Task cleanupTask = Task.Run(() => CleanupIdleKeysAsync());

            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    ConsumeResult<string, Order>? consumeResult = _consumer.Consume(_cts.Token);

                    if (consumeResult?.Message != null)
                    {
                        string? key = consumeResult.Message.Key;
                        if (!string.IsNullOrEmpty(key))
                        {
                            // Get or create channel for this key
                            if (!_messageChannels.TryGetValue(key, out ChannelWriter<ConsumeResult<string, Order>>? channelWriter))
                            {
                                Channel<ConsumeResult<string, Order>> channel = Channel.CreateUnbounded<ConsumeResult<string, Order>>(new UnboundedChannelOptions
                                {
                                    SingleReader = true,
                                    SingleWriter = false
                                });

                                channelWriter = channel.Writer;
                                _messageChannels.TryAdd(key, channelWriter);

                                // Start processing for this key
                                _processingTasks.TryAdd(key, Task.Run(() => ProcessMessagesForKeyAsync(key, channel.Reader)));
                            }

                            // Update last activity time
                            _lastActivityTime.AddOrUpdate(key, DateTime.UtcNow, (_, _) => DateTime.UtcNow);

                            // Write to channel (non-blocking)
                            await channelWriter.WriteAsync(consumeResult, _cts.Token);
                        }
                        else
                        {
                            // Handle messages without a key
                            Console.WriteLine($"Received message without key at Partition: {consumeResult.Partition.Value}, Offset: {consumeResult.Offset.Value}");
                            // Commit immediately since we're not processing
                            _consumer.StoreOffset(consumeResult);
                            _consumer.Commit();
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Consumer stopped
                Console.WriteLine("Consumer cancellation requested");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Consumer error: {ex.Message}");
            }
            finally
            {
                // Signal all channels to complete
                foreach (ChannelWriter<ConsumeResult<string, Order>> channel in _messageChannels.Values)
                {
                    channel.Complete();
                }

                _consumer?.Close();
                _consumer?.Dispose();

                await cleanupTask;
            }
        });
    }

    private async Task ProcessMessagesForKeyAsync(string key, ChannelReader<ConsumeResult<string, Order>> channelReader)
    {
        try
        {
            // Process messages until the channel is completed
            await foreach (ConsumeResult<string, Order> message in channelReader.ReadAllAsync(_cts.Token))
            {
                if (message.Message?.Value != null)
                {
                    bool processingSucceeded = false;
                    int retryCount = 0;
                    const int maxRetries = 3;

                    while (!processingSucceeded && retryCount < maxRetries)
                    {
                        try
                        {
                            // Use custom processor if provided, otherwise use default logic
                            if (_messageProcessor != null)
                            {
                                await _messageProcessor(message);
                            }
                            else
                            {
                                // Default processing logic
                                Order order = message.Message.Value; // Already deserialized by ProtobufDeserializer
                                Console.WriteLine($"[Key: {key}] Processing: {order.Status} (Partition: {message.Partition.Value}, Offset: {message.Offset.Value}, Attempt: {retryCount + 1})");

                                // Simulate some work - this ensures messages are processed one at a time per key
                                await Task.Delay(100, _cts.Token);
                            }

                            processingSucceeded = true;

                            // Store offset after successful processing
                            _consumer?.StoreOffset(message);

                            // Commit periodically (every message in this case, but could batch)
                            _consumer?.Commit();
                        }
                        catch (OperationCanceledException)
                        {
                            // Processing was cancelled, exit without retry
                            Console.WriteLine($"[Key: {key}] Processing cancelled at Offset: {message.Offset.Value}");
                            return;
                        }
                        catch (Exception ex)
                        {
                            retryCount++;
                            Console.WriteLine($"[Key: {key}] Error processing message (Attempt {retryCount}/{maxRetries}): {ex.Message}");

                            if (retryCount >= maxRetries)
                            {
                                // After max retries, log to dead letter queue (simulated here)
                                Console.WriteLine($"[Key: {key}] DEAD LETTER: Failed to process message at Partition: {message.Partition.Value}, Offset: {message.Offset.Value}");
                                // In production, send to DLQ topic or database

                                // Store offset to move past this message
                                _consumer?.StoreOffset(message);
                                _consumer?.Commit();
                            }
                            else
                            {
                                // Exponential backoff before retry
                                await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, retryCount)), _cts.Token);
                            }
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"[Key: {key}] Processing task cancelled");
        }
        finally
        {
            Console.WriteLine($"[Key: {key}] Stopped processing");
        }
    }

    private async Task CleanupIdleKeysAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromMinutes(1), _cts.Token);

                DateTime now = DateTime.UtcNow;
                List<string> keysToRemove = new();

                foreach (KeyValuePair<string, DateTime> kvp in _lastActivityTime)
                {
                    if (now - kvp.Value > _idleTimeout)
                    {
                        keysToRemove.Add(kvp.Key);
                    }
                }

                foreach (string key in keysToRemove)
                {
                    // Mark channel as complete
                    if (_messageChannels.TryRemove(key, out ChannelWriter<ConsumeResult<string, Order>>? channelWriter))
                    {
                        channelWriter.Complete();
                        Console.WriteLine($"[Key: {key}] Marked idle channel for cleanup");
                    }

                    // Wait for processing task to complete
                    if (_processingTasks.TryRemove(key, out Task? task))
                    {
                        await task;
                    }

                    _lastActivityTime.TryRemove(key, out _);
                    Console.WriteLine($"[Key: {key}] Cleaned up idle key after {_idleTimeout.TotalMinutes} minutes of inactivity");
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public void StopConsumer()
    {
        Console.WriteLine("Stopping consumer...");
        _cts.Cancel();

        // Wait for all processing tasks to complete with timeout
        TimeSpan timeout = TimeSpan.FromSeconds(30);
        Task allTasks = Task.WhenAll(_processingTasks.Values);

        if (!allTasks.Wait(timeout))
        {
            Console.WriteLine($"Warning: Not all processing tasks completed within {timeout.TotalSeconds} seconds");
        }
        else
        {
            Console.WriteLine("All processing tasks completed successfully");
        }

        _consumer?.Dispose();
    }
}