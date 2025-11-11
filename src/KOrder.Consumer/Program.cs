using dotenv.net;
using KOrder;
using KOrder.Consumer.Engine;
using KOrder.Consumer.Engine.HealthMonitoring;
using KThread.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

public class Program
{
    public static async Task Main(string[] args)
    {
        // Load environment variables from .env file
        DotEnv.Load();

        // Configuration from environment variables
        string bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
        string groupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "my-keyed-consumer-group";
        string topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "keyed-messages-topic";
        int healthPort = int.TryParse(Environment.GetEnvironmentVariable("HEALTH_CHECK_PORT"), out int port) ? port : 8080;
        long maxLag = long.TryParse(Environment.GetEnvironmentVariable("MAX_ACCEPTABLE_LAG"), out long lag) ? lag : 10000;

        // Create Kafka consumer settings
        KafkaConsumerSettings settings = new()
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            Topic = topic,
            MaxAcceptableLag = maxLag,
            HealthCheckPort = healthPort,
            EnableHealthMonitoring = false  // Set to false for local development to avoid HTTP listener permissions
        };

        // Create logger factory to get loggers for the consumer
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Information);
            // Can be configured via environment variable: DOTNET_LogLevel__Default=Debug
            // Or via appsettings.json in production
        });

        ILogger<KeyedConsumer<Order>> consumerLogger = loggerFactory.CreateLogger<KeyedConsumer<Order>>();
        ILogger<HealthCheckServer> healthLogger = loggerFactory.CreateLogger<HealthCheckServer>();

        // Set up DI container with the Kafka consumer engine
        ServiceProvider serviceProvider = new ServiceCollection()
            // Add logging services - this enables ILogger<T> resolution for OrderMessageProcessor
            .AddLogging(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Information);
            })
            // Register the message processor
            .AddSingleton<IMessageProcessor<Order>, OrderMessageProcessor>()
            // Register the Kafka consumer with settings and loggers
            .AddKafkaKeyedConsumer<Order>(
                parser: Order.Parser,
                settings: settings,
                consumerLogger: consumerLogger,
                healthLogger: healthLogger)
            .BuildServiceProvider();

        // Get services from DI container
        var consumer = serviceProvider.GetRequiredService<KeyedConsumer<Order>>();

        // Start health check server only if health monitoring is enabled
        HealthCheckServer? healthServer = null;
        if (settings.EnableHealthMonitoring)
        {
            healthServer = serviceProvider.GetRequiredService<HealthCheckServer>();
            healthServer.Start();
        }

        // Start the consumer
        await consumer.StartConsumerAsync();

        Console.WriteLine("Press any key to stop the consumer...");
        Console.ReadKey();

        // Cleanup
        consumer.StopConsumer();
        healthServer?.Dispose();
    }
}