namespace KOrder.Consumer.Engine;

/// <summary>
/// Configuration settings for the Kafka keyed consumer
/// </summary>
public class KafkaConsumerSettings
{
    /// <summary>
    /// Kafka bootstrap servers (e.g., "localhost:9092")
    /// </summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>
    /// Kafka consumer group ID
    /// </summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>
    /// Kafka topic to consume from
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Maximum number of messages that can be queued across all keys before pausing consumption
    /// </summary>
    public int MaxQueuedMessages { get; set; } = 10000;

    /// <summary>
    /// Threshold at which to resume consumption after pausing
    /// </summary>
    public int ResumeThreshold { get; set; } = 5000;

    /// <summary>
    /// Maximum capacity for each per-key channel
    /// </summary>
    public int PerKeyChannelCapacity { get; set; } = 1000;

    /// <summary>
    /// Enable health monitoring for lag detection
    /// </summary>
    public bool EnableHealthMonitoring { get; set; } = true;

    /// <summary>
    /// Maximum acceptable lag before consumer is considered unhealthy
    /// </summary>
    public long MaxAcceptableLag { get; set; } = 10000;

    /// <summary>
    /// Interval in seconds between lag checks
    /// </summary>
    public int LagCheckIntervalSeconds { get; set; } = 10;

    /// <summary>
    /// Port for the health check server (for K8s probes)
    /// </summary>
    public int HealthCheckPort { get; set; } = 8080;
}
