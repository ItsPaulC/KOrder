namespace KOrder.Consumer.Engine.HealthMonitoring;

/// <summary>
/// Represents consumer health status
/// </summary>
public class HealthStatus
{
    public bool IsHealthy { get; set; }
    public string Reason { get; set; } = string.Empty;
    public Dictionary<int, long> PartitionLags { get; set; } = [];
    public DateTime LastCheckTime { get; set; }
}
