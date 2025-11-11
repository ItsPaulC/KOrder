using System.Collections.Concurrent;
using System.Threading.Channels;
using Confluent.Kafka;
using Google.Protobuf;
using KOrder.Consumer.Engine.HealthMonitoring;
using Microsoft.Extensions.Logging;

namespace KOrder.Consumer.Engine;

/// <summary>
/// Kafka consumer that guarantees message ordering per key while processing different keys in parallel
/// </summary>
/// <typeparam name="TMessage">Protobuf message type</typeparam>
public class KeyedConsumer<TMessage> where TMessage : IMessage<TMessage>, new()
{
    private readonly string _bootstrapServers;
    private readonly string _groupId;
    private readonly string _topic;
    private readonly ConcurrentDictionary<string, Task> _processingTasks = new();
    private readonly ConcurrentDictionary<string, ChannelWriter<ConsumeResult<string, TMessage>>> _messageChannels = new();
    private readonly ConcurrentDictionary<string, Channel<ConsumeResult<string, TMessage>>> _channels = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastActivityTime = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly TimeSpan _idleTimeout = TimeSpan.FromMinutes(5);
    private IConsumer<string, TMessage>? _consumer;
    private readonly Func<ConsumeResult<string, TMessage>, Task> _messageProcessor;
    private readonly MessageParser<TMessage> _parser;

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

    // Logging
    private readonly ILogger<KeyedConsumer<TMessage>> _logger;

    public ConsumerHealthMonitor HealthMonitor => _healthMonitor;

    public KeyedConsumer(
        string bootstrapServers,
        string groupId,
        string topic,
        MessageParser<TMessage> parser,
        Func<ConsumeResult<string, TMessage>, Task> messageProcessor,
        ILogger<KeyedConsumer<TMessage>> logger,
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
        _parser = parser;
        _messageProcessor = messageProcessor;
        _logger = logger;
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

            _consumer = new ConsumerBuilder<string, TMessage>(config)
                .SetValueDeserializer(new ProtobufDeserializer<TMessage>(_parser))
                .Build();
            _consumer.Subscribe(_topic);

            _logger.LogInformation("Consumer started. Subscribed to topic '{Topic}' with group ID '{GroupId}'", _topic, _groupId);
            _logger.LogInformation("Backpressure: per-key capacity={PerKeyCapacity}, global max={GlobalMax}, resume={Resume}",
                _perKeyChannelCapacity, _maxQueuedMessages, _resumeThreshold);

            if (_enableHealthMonitoring)
            {
                _logger.LogInformation("Health monitoring enabled with {LagCheckInterval}s lag check interval", _lagCheckInterval.TotalSeconds);
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
                    ConsumeResult<string, TMessage>? consumeResult = _consumer.Consume(_cts.Token);

                    if (consumeResult?.Message != null)
                    {
                        string? key = consumeResult.Message.Key;
                        if (!string.IsNullOrEmpty(key))
                        {
                            // Get or create channel for this key
                            if (!_messageChannels.TryGetValue(key, out ChannelWriter<ConsumeResult<string, TMessage>>? channelWriter))
                            {
                                Channel<ConsumeResult<string, TMessage>> channel = Channel.CreateBounded<ConsumeResult<string, TMessage>>(new BoundedChannelOptions(_perKeyChannelCapacity)
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
                            _logger.LogWarning("Received message without key at Partition={Partition}, Offset={Offset}",
                                consumeResult.Partition.Value, consumeResult.Offset.Value);
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
                _logger.LogInformation("Consumer cancellation requested");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Consumer error: {Message}", ex.Message);
            }
            finally
            {
                // Signal all channels to complete
                foreach (ChannelWriter<ConsumeResult<string, TMessage>> channel in _messageChannels.Values)
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

    private async Task ProcessMessagesForKeyAsync(string key, ChannelReader<ConsumeResult<string, TMessage>> channelReader)
    {
        try
        {
            // Process messages until the channel is completed
            await foreach (ConsumeResult<string, TMessage> message in channelReader.ReadAllAsync(_cts.Token))
            {
                if (message.Message != null)
                {
                    bool processingSucceeded = false;
                    int retryCount = 0;
                    const int maxRetries = 3;

                    while (!processingSucceeded && retryCount < maxRetries)
                    {
                        try
                        {
                            // Use custom processor
                            await _messageProcessor(message);

                            processingSucceeded = true;

                            // Store offset after successful processing
                            _consumer?.StoreOffset(message);

                            // Commit periodically (every message in this case, but could batch)
                            _consumer?.Commit();
                        }
                        catch (OperationCanceledException)
                        {
                            // Processing was cancelled, exit without retry
                            _logger.LogInformation("Processing cancelled for Key={Key} at Offset={Offset}", key, message.Offset.Value);
                            return;
                        }
                        catch (Exception ex)
                        {
                            retryCount++;
                            _logger.LogWarning(ex, "Error processing message for Key={Key} (Attempt {Attempt}/{MaxRetries})",
                                key, retryCount, maxRetries);

                            if (retryCount >= maxRetries)
                            {
                                // After max retries, log to dead letter queue (simulated here)
                                _logger.LogError("DEAD LETTER: Failed to process message for Key={Key} at Partition={Partition}, Offset={Offset}",
                                    key, message.Partition.Value, message.Offset.Value);
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
            _logger.LogInformation("Processing task cancelled for Key={Key}", key);
        }
        finally
        {
            _logger.LogInformation("Stopped processing for Key={Key}", key);
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
                List<string> keysToRemove = [];

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
                    if (_messageChannels.TryRemove(key, out ChannelWriter<ConsumeResult<string, TMessage>>? channelWriter))
                    {
                        channelWriter.Complete();
                        _logger.LogDebug("Marked idle channel for cleanup for Key={Key}", key);
                    }

                    // Remove from channels dictionary
                    _channels.TryRemove(key, out _);

                    // Wait for processing task to complete
                    if (_processingTasks.TryRemove(key, out Task? task))
                    {
                        await task;
                    }

                    _lastActivityTime.TryRemove(key, out _);
                    _logger.LogInformation("Cleaned up idle key Key={Key} after {IdleMinutes} minutes of inactivity",
                        key, _idleTimeout.TotalMinutes);
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
        foreach (Channel<ConsumeResult<string, TMessage>> channel in _channels.Values)
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

                _logger.LogInformation("Consumer status: {Status}, Active keys: {ActiveKeys}, Queued messages: {QueuedMessages}",
                    status, activeKeys, queuedMessages);
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
                List<TopicPartition>? assignment = _consumer.Assignment;
                if (assignment == null || assignment.Count == 0) continue;

                // Calculate lag for each partition
                foreach (TopicPartition partition in assignment)
                {
                    try
                    {
                        // Get committed offset (where we are)
                        List<TopicPartitionOffset>? committed = _consumer.Committed(new[] { partition }, TimeSpan.FromSeconds(5));
                        Offset committedOffset = committed?.FirstOrDefault()?.Offset ?? Offset.Unset;

                        if (committedOffset == Offset.Unset)
                            continue;

                        // Get high water mark (latest offset in partition)
                        WatermarkOffsets? watermarkOffsets = _consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(5));
                        long highWaterMark = watermarkOffsets.High.Value;

                        // Calculate lag
                        long lag = highWaterMark - committedOffset.Value;

                        // Record lag in health monitor
                        _healthMonitor.RecordLag(partition, lag);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Error checking lag for partition {Partition}", partition.Partition);
                    }
                }

                // Check health after recording all partition lags
                HealthStatus healthStatus = _healthMonitor.CheckHealth();

                if (!healthStatus.IsHealthy)
                {
                    string lagsSummary = string.Join(", ", healthStatus.PartitionLags.Select(kvp => $"P{kvp.Key}={kvp.Value}"));
                    _logger.LogWarning("Consumer UNHEALTHY: {Reason}. Partition lags: {Lags}",
                        healthStatus.Reason, lagsSummary);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in lag monitoring");
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
                    _logger.LogWarning("BACKPRESSURE: Consumer PAUSED - Queued messages: {QueuedMessages}/{MaxQueuedMessages}",
                        queuedMessages, _maxQueuedMessages);
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
                    _logger.LogInformation("BACKPRESSURE: Consumer RESUMED - Queued messages: {QueuedMessages}/{ResumeThreshold}",
                        queuedMessages, _resumeThreshold);
                }
            }
        }
    }

    public void StopConsumer()
    {
        _logger.LogInformation("Stopping consumer...");
        _cts.Cancel();

        // Wait for all processing tasks to complete with timeout
        TimeSpan timeout = TimeSpan.FromSeconds(30);
        Task allTasks = Task.WhenAll(_processingTasks.Values);

        if (!allTasks.Wait(timeout))
        {
            _logger.LogWarning("Not all processing tasks completed within {TimeoutSeconds} seconds", timeout.TotalSeconds);
        }
        else
        {
            _logger.LogInformation("All processing tasks completed successfully");
        }

        _consumer?.Dispose();
    }
}
