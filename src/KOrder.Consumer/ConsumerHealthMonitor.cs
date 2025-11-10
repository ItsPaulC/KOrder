using System.Collections.Concurrent;
using Confluent.Kafka;

namespace KThread.Consumer;

/// <summary>
/// Monitors consumer health by tracking lag and detecting stuck consumers
/// </summary>
public class ConsumerHealthMonitor
{
    private readonly ConcurrentDictionary<TopicPartition, PartitionLagHistory> _lagHistory = new();
    private readonly int _lagCheckWindowSize;
    private readonly long _maxAcceptableLag;
    private readonly int _consecutiveIncreaseThreshold;
    private DateTime _lastHealthCheck = DateTime.UtcNow;
    private bool _isHealthy = true;
    private string _unhealthyReason = string.Empty;

    public ConsumerHealthMonitor(
        int lagCheckWindowSize = 5,
        long maxAcceptableLag = 10000,
        int consecutiveIncreaseThreshold = 3)
    {
        _lagCheckWindowSize = lagCheckWindowSize;
        _maxAcceptableLag = maxAcceptableLag;
        _consecutiveIncreaseThreshold = consecutiveIncreaseThreshold;
    }

    /// <summary>
    /// Records current lag for a partition
    /// </summary>
    public void RecordLag(TopicPartition partition, long currentLag)
    {
        var history = _lagHistory.GetOrAdd(partition, _ => new PartitionLagHistory(_lagCheckWindowSize));
        history.AddLag(currentLag);
    }

    /// <summary>
    /// Checks if consumer is healthy based on lag patterns
    /// </summary>
    public HealthStatus CheckHealth()
    {
        _lastHealthCheck = DateTime.UtcNow;

        foreach (var kvp in _lagHistory)
        {
            var partition = kvp.Key;
            var history = kvp.Value;

            // Check 1: Lag exceeds maximum acceptable threshold
            if (history.CurrentLag > _maxAcceptableLag)
            {
                _isHealthy = false;
                _unhealthyReason = $"Partition {partition.Partition} lag ({history.CurrentLag}) exceeds maximum ({_maxAcceptableLag})";
                return new HealthStatus
                {
                    IsHealthy = false,
                    Reason = _unhealthyReason,
                    PartitionLags = GetPartitionLags(),
                    LastCheckTime = _lastHealthCheck
                };
            }

            // Check 2: Lag is consistently increasing (stuck consumer)
            if (history.IsConsistentlyIncreasing(_consecutiveIncreaseThreshold))
            {
                _isHealthy = false;
                _unhealthyReason = $"Partition {partition.Partition} lag is consistently increasing ({history.GetIncreasingCount()} consecutive increases) - consumer may be stuck";
                return new HealthStatus
                {
                    IsHealthy = false,
                    Reason = _unhealthyReason,
                    PartitionLags = GetPartitionLags(),
                    LastCheckTime = _lastHealthCheck
                };
            }
        }

        _isHealthy = true;
        _unhealthyReason = string.Empty;
        return new HealthStatus
        {
            IsHealthy = true,
            Reason = "All partitions healthy",
            PartitionLags = GetPartitionLags(),
            LastCheckTime = _lastHealthCheck
        };
    }

    /// <summary>
    /// Gets current lag for all partitions
    /// </summary>
    public Dictionary<int, long> GetPartitionLags()
    {
        return _lagHistory.ToDictionary(
            kvp => kvp.Key.Partition.Value,
            kvp => kvp.Value.CurrentLag
        );
    }

    /// <summary>
    /// Gets total lag across all partitions
    /// </summary>
    public long GetTotalLag()
    {
        return _lagHistory.Values.Sum(h => h.CurrentLag);
    }

    public bool IsHealthy => _isHealthy;
    public string UnhealthyReason => _unhealthyReason;
    public DateTime LastHealthCheck => _lastHealthCheck;
}

/// <summary>
/// Tracks lag history for a single partition
/// </summary>
internal class PartitionLagHistory
{
    private readonly Queue<long> _lagHistory;
    private readonly int _maxHistorySize;

    public PartitionLagHistory(int maxHistorySize)
    {
        _maxHistorySize = maxHistorySize;
        _lagHistory = new Queue<long>(maxHistorySize);
    }

    public long CurrentLag { get; private set; }

    public void AddLag(long lag)
    {
        CurrentLag = lag;
        _lagHistory.Enqueue(lag);

        if (_lagHistory.Count > _maxHistorySize)
        {
            _lagHistory.Dequeue();
        }
    }

    /// <summary>
    /// Checks if lag has been increasing consistently
    /// </summary>
    public bool IsConsistentlyIncreasing(int threshold)
    {
        if (_lagHistory.Count < threshold)
            return false;

        var increases = GetIncreasingCount();
        return increases >= threshold;
    }

    /// <summary>
    /// Counts consecutive lag increases
    /// </summary>
    public int GetIncreasingCount()
    {
        if (_lagHistory.Count < 2)
            return 0;

        var history = _lagHistory.ToArray();
        int consecutiveIncreases = 0;

        for (int i = 1; i < history.Length; i++)
        {
            if (history[i] > history[i - 1])
            {
                consecutiveIncreases++;
            }
            else
            {
                consecutiveIncreases = 0; // Reset on any decrease or plateau
            }
        }

        return consecutiveIncreases;
    }
}

/// <summary>
/// Represents consumer health status
/// </summary>
public class HealthStatus
{
    public bool IsHealthy { get; set; }
    public string Reason { get; set; } = string.Empty;
    public Dictionary<int, long> PartitionLags { get; set; } = new();
    public DateTime LastCheckTime { get; set; }
}
