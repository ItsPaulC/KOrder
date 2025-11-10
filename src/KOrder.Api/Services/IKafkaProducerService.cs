using KOrder.Producer;

namespace KOrder.Api.Services;

/// <summary>
/// Interface for Kafka producer service operations
/// </summary>
public interface IKafkaProducerService
{
    /// <summary>
    /// Runs a demo by producing predefined messages to Kafka
    /// </summary>
    /// <returns>A task representing the asynchronous operation</returns>
    Task RunDemoAsync();
}
