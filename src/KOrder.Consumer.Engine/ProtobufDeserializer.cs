using Confluent.Kafka;
using Google.Protobuf;

namespace KOrder.Consumer.Engine;

/// <summary>
/// Kafka deserializer for Protocol Buffer messages
/// </summary>
/// <typeparam name="T">Protobuf message type</typeparam>
public class ProtobufDeserializer<T> : IDeserializer<T> where T : IMessage<T>, new()
{
    private readonly MessageParser<T> _parser;

    public ProtobufDeserializer(MessageParser<T> parser)
    {
        _parser = parser;
    }

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
        {
            return default!;
        }

        return _parser.ParseFrom(data.ToArray());
    }
}
