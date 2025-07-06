using KThread.Consumer;


public class Program
{
    public static async Task Main(string[] args)
    {
        string bootstrapServers = "localhost:9092"; // Replace with your Kafka brokers
        string groupId = "my-keyed-consumer-group";
        string topic = "keyed-messages-topic"; // Replace with your topic name

        var consumer = new KeyedConsumer(bootstrapServers, groupId, topic);
        await consumer.StartConsumerAsync();

        Console.WriteLine("Press any key to stop the consumer...");
        Console.ReadKey();

        consumer.StopConsumer();
    }
}