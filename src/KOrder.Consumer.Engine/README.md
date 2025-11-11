# KOrder.Consumer.Engine

A flexible, reusable Kafka consumer library that guarantees message ordering per key while processing different keys in parallel. Built with dependency injection support for easy integration into any .NET application.

## Features

- **Order Preservation**: Messages with the same key are always processed in order
- **Parallel Processing**: Messages with different keys process concurrently for optimal throughput
- **Backpressure Management**: Both per-key and global backpressure controls to prevent memory issues
- **Health Monitoring**: Built-in lag monitoring and health checks for Kubernetes deployments
- **Flexible DI Support**: Easy integration with Microsoft.Extensions.DependencyInjection
- **Generic Message Support**: Works with any Protocol Buffer message type
- **Retry Logic**: Automatic retry with exponential backoff
- **Dead Letter Queue**: Failed messages are logged (extensible for custom DLQ implementations)

## Installation

Add a reference to this project in your consumer application:

```bash
dotnet add reference path/to/KOrder.Consumer.Engine.csproj
```

## Quick Start

### 1. Define Your Message Processor

Implement the `IMessageProcessor<TMessage>` interface:

```csharp
public class OrderMessageProcessor : IMessageProcessor<Order>
{
    private readonly ILogger<OrderMessageProcessor> _logger;

    public OrderMessageProcessor(ILogger<OrderMessageProcessor> logger)
    {
        _logger = logger;
    }

    public async Task ProcessAsync(ConsumeResult<string, Order> consumeResult)
    {
        Order order = consumeResult.Message.Value;
        _logger.LogInformation("Processing order {OrderId}", order.OrderId);

        // Your processing logic here
        await Task.CompletedTask;
    }
}
```

### 2. Configure Settings

Create and populate the Kafka consumer settings:

```csharp
var settings = new KafkaConsumerSettings
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-consumer-group",
    Topic = "my-topic",
    MaxAcceptableLag = 10000,
    HealthCheckPort = 8080
};
```

### 3. Create Loggers

Set up logging and create the required loggers:

```csharp
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

var consumerLogger = loggerFactory.CreateLogger<KeyedConsumer<Order>>();
var healthLogger = loggerFactory.CreateLogger<HealthCheckServer>();
```

### 4. Register Services

In your `Program.cs` or startup configuration:

```csharp
var services = new ServiceCollection()
    .AddSingleton(loggerFactory)
    // Register your message processor
    .AddSingleton<IMessageProcessor<Order>, OrderMessageProcessor>()
    // Register the Kafka consumer with settings and loggers
    .AddKafkaKeyedConsumer<Order>(
        parser: Order.Parser,
        settings: settings,
        consumerLogger: consumerLogger,
        healthLogger: healthLogger)
    .BuildServiceProvider();
```

### 5. Start the Consumer

```csharp
var consumer = serviceProvider.GetRequiredService<KeyedConsumer<Order>>();
var healthServer = serviceProvider.GetRequiredService<HealthCheckServer>();

healthServer.Start();
await consumer.StartConsumerAsync();
```

## Configuration Settings

The `KafkaConsumerSettings` class contains all configuration options:

```csharp
public class KafkaConsumerSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public int MaxQueuedMessages { get; set; } = 10000;
    public int ResumeThreshold { get; set; } = 5000;
    public int PerKeyChannelCapacity { get; set; } = 1000;
    public bool EnableHealthMonitoring { get; set; } = true;
    public long MaxAcceptableLag { get; set; } = 10000;
    public int LagCheckIntervalSeconds { get; set; } = 10;
    public int HealthCheckPort { get; set; } = 8080;
}
```

## Alternative: Using a Custom Function

Instead of implementing `IMessageProcessor<T>`, you can provide a custom processing function:

```csharp
var settings = new KafkaConsumerSettings
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    Topic = "my-topic"
};

var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var consumerLogger = loggerFactory.CreateLogger<KeyedConsumer<Order>>();
var healthLogger = loggerFactory.CreateLogger<HealthCheckServer>();

services.AddKafkaKeyedConsumer<Order>(
    parser: Order.Parser,
    settings: settings,
    consumerLogger: consumerLogger,
    healthLogger: healthLogger,
    messageProcessorFactory: sp => async (consumeResult) =>
    {
        // Your custom processing logic
        var logger = sp.GetRequiredService<ILogger<Program>>();
        logger.LogInformation("Processing order {OrderId}",
            consumeResult.Message.Value.OrderId);
    });
```

## Health Checks for Kubernetes

The library includes a built-in HTTP server for Kubernetes health probes:

- **Liveness**: `http://localhost:8080/health/live`
- **Readiness**: `http://localhost:8080/health/ready`
- **Metrics**: `http://localhost:8080/metrics` (Prometheus format)

### Example Kubernetes Configuration

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /health/ready
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 5
```

## Architecture

### Message Flow

```
Main Consumer Loop → Routes by Key → Per-Key Channel (SingleReader) → Sequential Processing Task
                                    ↓
                           Parallel across different keys
```

### Key Components

- **KeyedConsumer<TMessage>**: Core consumer implementing per-key message ordering
- **ProtobufDeserializer<T>**: Kafka deserializer for Protocol Buffer messages
- **ConsumerHealthMonitor**: Tracks lag and detects stuck consumers
- **HealthCheckServer**: HTTP server for Kubernetes health probes

## Environment Variables

The consumer can be configured via environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses (default: localhost:9092)
- `KAFKA_GROUP_ID`: Consumer group ID
- `KAFKA_TOPIC`: Topic to consume from
- `HEALTH_CHECK_PORT`: Port for health check server (default: 8080)
- `MAX_ACCEPTABLE_LAG`: Maximum acceptable lag before unhealthy (default: 10000)

## Backpressure Strategy

The library implements two levels of backpressure:

1. **Per-Key Backpressure**: Each key has a bounded channel (default 1000 messages)
2. **Global Backpressure**: When total queued messages exceed threshold (default 10000), consumption pauses

## License

This library is part of the KOrder project.
