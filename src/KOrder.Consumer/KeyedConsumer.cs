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
    private readonly ConcurrentDictionary<string, Channel<ConsumeResult<string, Order>>> _channels = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastActivityTime = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly TimeSpan _idleTimeout = TimeSpan.FromMinutes(5);
    private IConsumer<string, Order>? _consumer;
    private readonly Func<ConsumeResult<string, Order>, Task>? _messageProcessor;

    // Pause/Resume configuration (global backpressure)
    private readonly int _maxQueuedMessages;
    private readonly int _resumeThreshold;
    private bool _isPaused = false;
    private readonly object _pauseLock = new();

    // Bounded channel configuration (per-key backpressure)
    private readonly int _perKeyChannelCapacity;

    // Health monitoring
    private readonly ConsumerHealthMonitor _healthMonitor;
    private readonly bool _enableHealthMonitoring;
    private readonly TimeSpan _lagCheckInterval;

    public ConsumerHealthMonitor HealthMonitor => _healthMonitor;

    public KeyedConsumer(
        string bootstrapServers,
        string groupId,
        string topic,
        Func<ConsumeResult<string, Order>, Task>? messageProcessor = null,
        int maxQueuedMessages = 10000,
        int resumeThreshold = 5000,
        int perKeyChannelCapacity = 1000,
        bool enableHealthMonitoring = true,
        long maxAcceptableLag = 10000,
        int lagCheckIntervalSeconds = 10)
    {
        _bootstrapServers = bootstrapServers;
        _groupId = groupId;
        _topic = topic;
        _messageProcessor = messageProcessor;
        _maxQueuedMessages = maxQueuedMessages;
        _resumeThreshold = resumeThreshold;
        _perKeyChannelCapacity = perKeyChannelCapacity;
        _enableHealthMonitoring = enableHealthMonitoring;
        _lagCheckInterval = TimeSpan.FromSeconds(lagCheckIntervalSeconds);
        _healthMonitor = new ConsumerHealthMonitor(
            lagCheckWindowSize: 5,
            maxAcceptableLag: maxAcceptableLag,
            consecutiveIncreaseThreshold: 3);
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

            Console.WriteLine($"Consumer started. Subscribed to topic '{_topic}' with group ID '{_groupId}'. Waiting for messages...");
            Console.WriteLine($"Backpressure Configuration:");
            Console.WriteLine($"  - Per-key channel capacity: {_perKeyChannelCapacity} messages (bounded channels)");
            Console.WriteLine($"  - Global max queued messages: {_maxQueuedMessages} (pause threshold)");
            Console.WriteLine($"  - Global resume threshold: {_resumeThreshold} messages");

            if (_enableHealthMonitoring)
            {
                Console.WriteLine($"Health Monitoring: ENABLED (lag check interval: {_lagCheckInterval.TotalSeconds}s)");
            }

            // Start idle key cleanup task
            Task cleanupTask = Task.Run(() => CleanupIdleKeysAsync());

            // Start periodic status logging
            Task statusTask = Task.Run(() => LogStatusPeriodicallyAsync());

            // Start lag monitoring task
            Task? lagMonitorTask = null;
            if (_enableHealthMonitoring)
            {
                lagMonitorTask = Task.Run(() => MonitorLagAsync());
            }

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
                                Channel<ConsumeResult<string, Order>> channel = Channel.CreateBounded<ConsumeResult<string, Order>>(new BoundedChannelOptions(_perKeyChannelCapacity)
                                {
                                    SingleReader = true,
                                    SingleWriter = false,
                                    FullMode = BoundedChannelFullMode.Wait // Backpressure: blocks writer when full
                                });

                                channelWriter = channel.Writer;
                                _messageChannels.TryAdd(key, channelWriter);
                                _channels.TryAdd(key, channel);

                                // Start processing for this key
                                _processingTasks.TryAdd(key, Task.Run(() => ProcessMessagesForKeyAsync(key, channel.Reader)));
                            }

                            // Update last activity time
                            _lastActivityTime.AddOrUpdate(key, DateTime.UtcNow, (_, _) => DateTime.UtcNow);

                            // Write to channel (non-blocking)
                            await channelWriter.WriteAsync(consumeResult, _cts.Token);

                            // Check if we need to pause consumption due to backpressure
                            CheckAndPauseIfNeeded();
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
                await statusTask;
                if (lagMonitorTask != null)
                {
                    await lagMonitorTask;
                }
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

                    // Remove from channels dictionary
                    _channels.TryRemove(key, out _);

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

    private int GetTotalQueuedMessages()
    {
        int total = 0;
        foreach (var channel in _channels.Values)
        {
            total += channel.Reader.Count;
        }
        return total;
    }

    private async Task LogStatusPeriodicallyAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(30), _cts.Token);

                int queuedMessages = GetTotalQueuedMessages();
                int activeKeys = _channels.Count;
                string status = _isPaused ? "PAUSED" : "RUNNING";

                Console.WriteLine($"[STATUS] Consumer: {status} | Active Keys: {activeKeys} | Queued Messages: {queuedMessages}");
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task MonitorLagAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_lagCheckInterval, _cts.Token);

                if (_consumer == null) continue;

                // Get assigned partitions
                var assignment = _consumer.Assignment;
                if (assignment == null || assignment.Count == 0) continue;

                // Calculate lag for each partition
                foreach (var partition in assignment)
                {
                    try
                    {
                        // Get committed offset (where we are)
                        var committed = _consumer.Committed(new[] { partition }, TimeSpan.FromSeconds(5));
                        var committedOffset = committed?.FirstOrDefault()?.Offset ?? Offset.Unset;

                        if (committedOffset == Offset.Unset)
                            continue;

                        // Get high water mark (latest offset in partition)
                        var watermarkOffsets = _consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(5));
                        var highWaterMark = watermarkOffsets.High.Value;

                        // Calculate lag
                        long lag = highWaterMark - committedOffset.Value;

                        // Record lag in health monitor
                        _healthMonitor.RecordLag(partition, lag);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[LAG-MONITOR] Error checking lag for partition {partition.Partition}: {ex.Message}");
                    }
                }

                // Check health after recording all partition lags
                var healthStatus = _healthMonitor.CheckHealth();

                if (!healthStatus.IsHealthy)
                {
                    Console.WriteLine($"[HEALTH] ⚠️ UNHEALTHY: {healthStatus.Reason}");
                    Console.WriteLine($"[HEALTH] Partition Lags: {string.Join(", ", healthStatus.PartitionLags.Select(kvp => $"P{kvp.Key}={kvp.Value}"))}");
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[LAG-MONITOR] Error in lag monitoring: {ex.Message}");
            }
        }
    }

    private void CheckAndPauseIfNeeded()
    {
        int queuedMessages = GetTotalQueuedMessages();

        if (!_isPaused && queuedMessages >= _maxQueuedMessages)
        {
            lock (_pauseLock)
            {
                if (!_isPaused)
                {
                    _isPaused = true;
                    _consumer?.Pause(_consumer.Assignment);
                    Console.WriteLine($"[BACKPRESSURE] Consumer PAUSED - Queued messages: {queuedMessages}/{_maxQueuedMessages}");
                }
            }
        }
        else if (_isPaused && queuedMessages <= _resumeThreshold)
        {
            lock (_pauseLock)
            {
                if (_isPaused)
                {
                    _isPaused = false;
                    _consumer?.Resume(_consumer.Assignment);
                    Console.WriteLine($"[BACKPRESSURE] Consumer RESUMED - Queued messages: {queuedMessages}/{_resumeThreshold}");
                }
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