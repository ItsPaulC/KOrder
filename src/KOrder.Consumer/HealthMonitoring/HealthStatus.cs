namespace KThread.Consumer.HealthMonitoring;

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
