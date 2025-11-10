using KOrder.Producer;
using KOrder;

namespace KOrder.Api.Services;

/// <summary>
/// Service for managing Kafka message production operations
/// </summary>
public class KafkaProducerService : IKafkaProducerService, IDisposable
{
    private readonly KafkaProducer _kafkaProducer;
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly IConfiguration _configuration;

    /// <summary>
    /// Initializes a new instance of the KafkaProducerService
    /// </summary>
    /// <param name="logger">The logger instance</param>
    /// <param name="configuration">The configuration instance</param>
    public KafkaProducerService(ILogger<KafkaProducerService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;

        // Get configuration from appsettings.json or use defaults
        string bootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
        string topic = _configuration["Kafka:Topic"] ?? "keyed-messages-topic";

        KafkaProducerConfig config = new()
        {
            BootstrapServers = bootstrapServers,
            Topic = topic
        };

        _kafkaProducer = new KafkaProducer(config);
        
        _logger.LogInformation("KafkaProducerService initialized with servers: {BootstrapServers}, topic: {Topic}", 
            bootstrapServers, topic);
    }

    /// <summary>
    /// Runs a demo by producing predefined messages to Kafka with different keys
    /// This method replicates the functionality from the RunDemo method in the console application
    /// </summary>
    /// <returns>A task representing the asynchronous operation</returns>
    public async Task RunDemoAsync()
    {
        _logger.LogInformation("Running demo with predefined messages...");
        _logger.LogInformation("This demo will send messages for 3 customers concurrently to test ordering preservation per key.");
        
        // Create sample data with multiple keys - same as the original RunDemo method
        Dictionary<string, List<Order>> keyedMessages = new()
        {
            ["customer-1"] = 
            [
                new Order { Status = "Order placed for customer 1" },
                new Order { Status = "Payment processed for customer 1" },
                new Order { Status = "Order shipped for customer 1" },
                new Order { Status = "Order delivered for customer 1" }
            ],
            ["customer-2"] = 
            [
                new Order { Status = "Order placed for customer 2" },
                new Order { Status = "Payment failed for customer 2" },
                new Order { Status = "Payment retry for customer 2" },
                new Order { Status = "Payment processed for customer 2" },
                new Order { Status = "Order shipped for customer 2" }
            ],
            ["customer-3"] = 
            [
                new Order { Status = "Order placed for customer 3" },
                new Order { Status = "Payment processed for customer 3" },
                new Order { Status = "Order cancelled for customer 3" }
            ]
        };

        // Send messages concurrently to test the ordering preservation
        // Each key's messages should be processed in order, but different keys can be interleaved
        await _kafkaProducer.ProduceMultiKeyBatchAsync(keyedMessages);
        
        _logger.LogInformation("Demo completed - sent messages for 3 different customers.");
        _logger.LogInformation("Check the consumer output - messages for each customer should be in order.");
    }

    /// <summary>
    /// Disposes the Kafka producer resources
    /// </summary>
    public void Dispose()
    {
        _kafkaProducer.Dispose();
        GC.SuppressFinalize(this);
    }
}
