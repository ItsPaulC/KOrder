using System.Collections.Concurrent;
using Confluent.Kafka;

namespace KOrder.Consumer.Engine.HealthMonitoring;

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
        PartitionLagHistory history = _lagHistory.GetOrAdd(partition, _ => new PartitionLagHistory(_lagCheckWindowSize));
        history.AddLag(currentLag);
    }

    /// <summary>
    /// Checks if consumer is healthy based on lag patterns
    /// </summary>
    public HealthStatus CheckHealth()
    {
        _lastHealthCheck = DateTime.UtcNow;

        foreach (KeyValuePair<TopicPartition, PartitionLagHistory> kvp in _lagHistory)
        {
            TopicPartition partition = kvp.Key;
            PartitionLagHistory history = kvp.Value;

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
    /// Gets total lag across all partitions
    /// </summary>
    public long GetTotalLag()
    {
        return _lagHistory.Values.Sum(h => h.CurrentLag);
    }

    /// <summary>
    /// Gets current lag for all partitions
    /// </summary>
    private Dictionary<int, long> GetPartitionLags()
    {
        return _lagHistory.ToDictionary(
            kvp => kvp.Key.Partition.Value,
            kvp => kvp.Value.CurrentLag
        );
    }

    public bool IsHealthy => _isHealthy;
    public string UnhealthyReason => _unhealthyReason;
    public DateTime LastHealthCheck => _lastHealthCheck;
}
