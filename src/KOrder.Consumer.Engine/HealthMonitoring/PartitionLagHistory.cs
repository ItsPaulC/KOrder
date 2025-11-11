namespace KOrder.Consumer.Engine.HealthMonitoring;

/// <summary>
/// Tracks lag history for a single partition
/// </summary>
internal class PartitionLagHistory(int maxHistorySize)
{
    private readonly Queue<long> _lagHistory = new(maxHistorySize);

    public void AddLag(long lag)
    {
        CurrentLag = lag;
        _lagHistory.Enqueue(lag);

        if (_lagHistory.Count > maxHistorySize)
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

        int increases = GetIncreasingCount();
        return increases >= threshold;
    }

    /// <summary>
    /// Counts consecutive lag increases
    /// </summary>
    public int GetIncreasingCount()
    {
        if (_lagHistory.Count < 2)
            return 0;

        long[] history = _lagHistory.ToArray();
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

    public long CurrentLag { get; private set; }
}
