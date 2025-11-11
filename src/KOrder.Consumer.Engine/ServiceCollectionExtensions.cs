using Confluent.Kafka;
using Google.Protobuf;
using KOrder.Consumer.Engine.HealthMonitoring;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KOrder.Consumer.Engine;

/// <summary>
/// Extension methods for configuring Kafka consumer services
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Kafka keyed consumer to the service collection
    /// </summary>
    /// <typeparam name="TMessage">Protobuf message type</typeparam>
    /// <param name="services">The service collection</param>
    /// <param name="parser">Protobuf message parser</param>
    /// <param name="configureOptions">Optional action to configure consumer options</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddKafkaKeyedConsumer<TMessage>(
        this IServiceCollection services,
        MessageParser<TMessage> parser,
        Action<KafkaConsumerOptions>? configureOptions = null)
        where TMessage : IMessage<TMessage>, new()
    {
        // Register configuration
        if (configureOptions != null)
        {
            services.Configure(configureOptions);
        }

        // Register the consumer as singleton
        services.AddSingleton(sp =>
        {
            KafkaConsumerOptions options = sp.GetRequiredService<IOptions<KafkaConsumerOptions>>().Value;
            ILogger<KeyedConsumer<TMessage>> logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<KeyedConsumer<TMessage>>>();
            IMessageProcessor<TMessage> messageProcessor = sp.GetRequiredService<IMessageProcessor<TMessage>>();

            return new KeyedConsumer<TMessage>(
                bootstrapServers: options.BootstrapServers,
                groupId: options.GroupId,
                topic: options.Topic,
                parser: parser,
                messageProcessor: messageProcessor.ProcessAsync,
                logger: logger,
                maxQueuedMessages: options.MaxQueuedMessages,
                resumeThreshold: options.ResumeThreshold,
                perKeyChannelCapacity: options.PerKeyChannelCapacity,
                enableHealthMonitoring: options.EnableHealthMonitoring,
                maxAcceptableLag: options.MaxAcceptableLag,
                lagCheckIntervalSeconds: options.LagCheckIntervalSeconds);
        });

        // Register health check server
        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaConsumerOptions>>().Value;
            var consumer = sp.GetRequiredService<KeyedConsumer<TMessage>>();
            var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<HealthCheckServer>>();

            return new HealthCheckServer(
                consumer.HealthMonitor,
                logger,
                options.HealthCheckPort);
        });

        return services;
    }

    /// <summary>
    /// Adds the Kafka keyed consumer with a custom message processor function
    /// </summary>
    /// <typeparam name="TMessage">Protobuf message type</typeparam>
    /// <param name="services">The service collection</param>
    /// <param name="parser">Protobuf message parser</param>
    /// <param name="messageProcessorFactory">Factory function to create the message processor</param>
    /// <param name="configureOptions">Optional action to configure consumer options</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddKafkaKeyedConsumer<TMessage>(
        this IServiceCollection services,
        MessageParser<TMessage> parser,
        Func<IServiceProvider, Func<Confluent.Kafka.ConsumeResult<string, TMessage>, Task>> messageProcessorFactory,
        Action<KafkaConsumerOptions>? configureOptions = null)
        where TMessage : IMessage<TMessage>, new()
    {
        // Register configuration
        if (configureOptions != null)
        {
            services.Configure(configureOptions);
        }

        // Register the consumer as singleton
        services.AddSingleton(sp =>
        {
            KafkaConsumerOptions options = sp.GetRequiredService<IOptions<KafkaConsumerOptions>>().Value;
            ILogger<KeyedConsumer<TMessage>> logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<KeyedConsumer<TMessage>>>();
            Func<ConsumeResult<string, TMessage>, Task> messageProcessor = messageProcessorFactory(sp);

            return new KeyedConsumer<TMessage>(
                bootstrapServers: options.BootstrapServers,
                groupId: options.GroupId,
                topic: options.Topic,
                parser: parser,
                messageProcessor: messageProcessor,
                logger: logger,
                maxQueuedMessages: options.MaxQueuedMessages,
                resumeThreshold: options.ResumeThreshold,
                perKeyChannelCapacity: options.PerKeyChannelCapacity,
                enableHealthMonitoring: options.EnableHealthMonitoring,
                maxAcceptableLag: options.MaxAcceptableLag,
                lagCheckIntervalSeconds: options.LagCheckIntervalSeconds);
        });

        // Register health check server
        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaConsumerOptions>>().Value;
            var consumer = sp.GetRequiredService<KeyedConsumer<TMessage>>();
            var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<HealthCheckServer>>();

            return new HealthCheckServer(
                consumer.HealthMonitor,
                logger,
                options.HealthCheckPort);
        });

        return services;
    }
}
