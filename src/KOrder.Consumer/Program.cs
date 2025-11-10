using KThread.Consumer;
using KThread.Consumer.HealthMonitoring;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

public class Program
{
    public static async Task Main(string[] args)
    {
        // Set up logging and DI container
        var serviceProvider = new ServiceCollection()
            .AddLogging(builder =>
            {
                builder.AddConsole();
                builder.SetMinimumLevel(LogLevel.Information);

                // Can be configured via environment variable: DOTNET_LogLevel__Default=Debug
                // Or via appsettings.json in production
            })
            .BuildServiceProvider();

        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
        var consumerLogger = loggerFactory.CreateLogger<KeyedConsumer>();
        var healthServerLogger = loggerFactory.CreateLogger<HealthCheckServer>();

        // Configuration
        string bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
        string groupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "my-keyed-consumer-group";
        string topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "keyed-messages-topic";
        int healthPort = int.TryParse(Environment.GetEnvironmentVariable("HEALTH_CHECK_PORT"), out int port) ? port : 8080;
        long maxLag = long.TryParse(Environment.GetEnvironmentVariable("MAX_ACCEPTABLE_LAG"), out long lag) ? lag : 10000;

        // Create consumer with logging
        var consumer = new KeyedConsumer(
            bootstrapServers,
            groupId,
            topic,
            consumerLogger,
            maxAcceptableLag: maxLag);

        // Start health check server for K8s probes
        using var healthServer = new HealthCheckServer(consumer.HealthMonitor, healthServerLogger, port: healthPort);
        healthServer.Start();

        await consumer.StartConsumerAsync();

        Console.WriteLine("Press any key to stop the consumer...");
        Console.ReadKey();

        consumer.StopConsumer();
    }
}