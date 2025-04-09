using KThread.Consumer;


public class Program
{
    public static async Task Main(string[] args)
    {
         string bootstrapServers = "your_bootstrap_servers"; // Replace with your Kafka brokers
        string groupId = "my-keyed-consumer-group";
        string topic = "your_topic_name"; // Replace with your topic name

        var consumer = new KeyedConsumer(bootstrapServers, groupId, topic);
        await consumer.StartConsumerAsync();

        Console.WriteLine("Press any key to stop the consumer...");
        Console.ReadKey();

        consumer.StopConsumer();
    }
}