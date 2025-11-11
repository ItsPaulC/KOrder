using Confluent.Kafka;
using Google.Protobuf;
using KOrder.Consumer.Engine.HealthMonitoring;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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
    /// <param name="settings">Consumer configuration settings</param>
    /// <param name="consumerLogger">Logger for the consumer</param>
    /// <param name="healthLogger">Logger for the health check server</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddKafkaKeyedConsumer<TMessage>(
        this IServiceCollection services,
        MessageParser<TMessage> parser,
        KafkaConsumerSettings settings,
        ILogger<KeyedConsumer<TMessage>> consumerLogger,
        ILogger<HealthCheckServer> healthLogger)
        where TMessage : IMessage<TMessage>, new()
    {
        // Register the consumer as singleton
        services.AddSingleton(sp =>
        {
            IMessageProcessor<TMessage> messageProcessor = sp.GetRequiredService<IMessageProcessor<TMessage>>();

            return new KeyedConsumer<TMessage>(
                bootstrapServers: settings.BootstrapServers,
                groupId: settings.GroupId,
                topic: settings.Topic,
                parser: parser,
                messageProcessor: messageProcessor.ProcessAsync,
                logger: consumerLogger,
                maxQueuedMessages: settings.MaxQueuedMessages,
                resumeThreshold: settings.ResumeThreshold,
                perKeyChannelCapacity: settings.PerKeyChannelCapacity,
                enableHealthMonitoring: settings.EnableHealthMonitoring,
                maxAcceptableLag: settings.MaxAcceptableLag,
                lagCheckIntervalSeconds: settings.LagCheckIntervalSeconds);
        });

        // Register health check server
        services.AddSingleton(sp =>
        {
            KeyedConsumer<TMessage> consumer = sp.GetRequiredService<KeyedConsumer<TMessage>>();

            return new HealthCheckServer(
                consumer.HealthMonitor,
                healthLogger,
                settings.HealthCheckPort);
        });

        return services;
    }

    /// <summary>
    /// Adds the Kafka keyed consumer with a custom message processor function
    /// </summary>
    /// <typeparam name="TMessage">Protobuf message type</typeparam>
    /// <param name="services">The service collection</param>
    /// <param name="parser">Protobuf message parser</param>
    /// <param name="settings">Consumer configuration settings</param>
    /// <param name="consumerLogger">Logger for the consumer</param>
    /// <param name="healthLogger">Logger for the health check server</param>
    /// <param name="messageProcessorFactory">Factory function to create the message processor</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddKafkaKeyedConsumer<TMessage>(
        this IServiceCollection services,
        MessageParser<TMessage> parser,
        KafkaConsumerSettings settings,
        ILogger<KeyedConsumer<TMessage>> consumerLogger,
        ILogger<HealthCheckServer> healthLogger,
        Func<IServiceProvider, Func<ConsumeResult<string, TMessage>, Task>> messageProcessorFactory)
        where TMessage : IMessage<TMessage>, new()
    {
        // Register the consumer as singleton
        services.AddSingleton(sp =>
        {
            Func<ConsumeResult<string, TMessage>, Task> messageProcessor = messageProcessorFactory(sp);

            return new KeyedConsumer<TMessage>(
                bootstrapServers: settings.BootstrapServers,
                groupId: settings.GroupId,
                topic: settings.Topic,
                parser: parser,
                messageProcessor: messageProcessor,
                logger: consumerLogger,
                maxQueuedMessages: settings.MaxQueuedMessages,
                resumeThreshold: settings.ResumeThreshold,
                perKeyChannelCapacity: settings.PerKeyChannelCapacity,
                enableHealthMonitoring: settings.EnableHealthMonitoring,
                maxAcceptableLag: settings.MaxAcceptableLag,
                lagCheckIntervalSeconds: settings.LagCheckIntervalSeconds);
        });

        // Register health check server
        services.AddSingleton(sp =>
        {
            KeyedConsumer<TMessage> consumer = sp.GetRequiredService<KeyedConsumer<TMessage>>();

            return new HealthCheckServer(
                consumer.HealthMonitor,
                healthLogger,
                settings.HealthCheckPort);
        });

        return services;
    }
}
