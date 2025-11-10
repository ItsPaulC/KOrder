# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KOrder is a Kafka-based message processing system demonstrating ordered message consumption with parallel processing capabilities. The system uses Protocol Buffers for message serialization and implements a custom keyed consumer pattern that guarantees message ordering per key while processing different keys in parallel.

### Core Architecture

**Message Flow:**
1. **Producer** → Serializes `Order` protobuf messages → Kafka topic (with message key for partitioning)
2. **Consumer** → Deserializes at Kafka layer → Routes to per-key channels → Sequential processing per key

**Key Components:**

- **KOrder.Producer**: Kafka producer library with protobuf serialization
  - `KafkaProducer`: Handles message production, topic creation, and batch operations
  - Uses `EnableIdempotence = true` and `Acks = All` for reliability
  - Serializes `Order` protobuf messages to `byte[]`

- **KOrder.Consumer**: Keyed consumer with order preservation
  - `KeyedConsumer`: Core consumer implementing per-key message ordering
  - `ProtobufDeserializer<T>`: Custom Kafka deserializer for protobuf messages
  - Uses `System.Threading.Channels` for per-key message queues
  - Each unique message key gets dedicated `Channel<ConsumeResult<string, Order>>` with `SingleReader = true`
  - Messages processed sequentially per key via `await foreach`, parallel across keys
  - Manual offset commits after successful processing
  - Automatic cleanup of idle keys after 5 minutes

- **KOrder.Api**: ASP.NET Core Web API exposing producer functionality via HTTP endpoints

- **Protos/order.proto**: Shared protobuf schema defining `Order` message structure
  - Fields: `status` (string), `order_id` (int32), `customer_id` (string)

### Critical Design Patterns

**Order Preservation Strategy:**
```
Main Consumer Loop → Routes by Key → Per-Key Channel (SingleReader) → Sequential Processing Task
                                    ↓
                           Parallel across different keys
```

- Messages with the same key are ALWAYS processed in order
- Messages with different keys process concurrently
- Uses unbounded channels to prevent backpressure on consumer loop
- Retry logic: max 3 attempts with exponential backoff (2^n seconds)
- Dead letter queue simulation after max retries

**Protobuf Deserialization:**
- Deserialization happens once at Kafka client layer via `ProtobufDeserializer<T>`
- Consumer works with strongly-typed `Order` objects throughout
- Producer serializes using `Google.Protobuf.MessageExtensions.ToByteArray(order)`

## Development Commands

### Build
```bash
# Build entire solution
dotnet build

# Build specific project
dotnet build KOrder.Consumer/KOrder.Consumer.csproj
dotnet build KOrder.Producer/KOrder.Producer.csproj
dotnet build KOrder.Api/KOrder.Api.csproj
```

### Running Components

**Start Kafka (required for all operations):**
```bash
docker-compose -f .SolutionItems/docker-compose.yml up -d
```
- Kafka UI (Kafdrop): http://localhost:9000
- Kafka broker: localhost:9092

**Run Consumer:**
```bash
dotnet run --project KOrder.Consumer/KOrder.Consumer.csproj
```

**Run Producer:**
```bash
dotnet run --project KOrder.Producer/KOrder.Producer.csproj
```

**Run API:**
```bash
dotnet run --project KOrder.Api/KOrder.Api.csproj
```
- Swagger UI: https://localhost:<port>/swagger

### Testing

**Run all tests:**
```bash
dotnet test
```

**Run specific test project:**
```bash
dotnet test KOrder.Consumer.Tests/KOrder.Consumer.Tests.csproj
```

**Run single test:**
```bash
dotnet test KOrder.Consumer.Tests/KOrder.Consumer.Tests.csproj --filter "FullyQualifiedName~SingleKey_PreservesMessageOrder"
```

**Test framework:** xUnit v3 (2.0.0)
- Integration tests use Testcontainers.Kafka for Docker-based Kafka instances
- Tests verify order preservation and parallel processing behavior
- Collection attribute `[Collection("Kafka Tests")]` ensures sequential test execution

### Clean
```bash
dotnet clean
```

## Coding Standards

**C# Style (from copilot-codeGeneration-instructions.md):**
- Use collection expressions: `List<string> myList = [];`
- Explicit `new` keyword: `Employee employee = new();`
- Prefer `async`/`await` for I/O-bound operations
- Use `Span<T>` for performance-critical code
- Avoid magic strings/numbers - use constants
- Document new methods with XML comments

**Testing:**
- Use **xUnit v3** (NOT v2)
- Use standard xUnit assertions (NOT FluentAssertions)
- Mock dependencies using NSubstitute
- Each test gets unique topic name: `var testTopic = $"test-topic-{Guid.NewGuid()}";`
- Each test gets unique consumer group: `var consumerGroupId = $"test-group-{Guid.NewGuid()}";`

## Protobuf Updates

When modifying `Protos/order.proto`:
1. Update the `.proto` file
2. Rebuild all projects that reference it (auto-generates C# classes)
3. Projects referencing protobuf: KOrder.Producer, KOrder.Consumer, KOrder.Consumer.Tests

The build process automatically generates C# classes in `obj/Debug/net*/Order.cs`

## Important Implementation Details

**KeyedConsumer Constructor Overloads:**
```csharp
// Default constructor - uses built-in console logging
new KeyedConsumer(bootstrapServers, groupId, topic)

// Custom processor - inject your own message handler
new KeyedConsumer(bootstrapServers, groupId, topic,
    async (consumeResult) => { /* custom logic */ })
```

**Message Processing Guarantees:**
- Messages arrive as `ConsumeResult<string, Order>` (already deserialized)
- Offset committed only after successful processing
- Consumer can be stopped gracefully with 30-second timeout for in-flight messages

**Confluent.Kafka Configuration:**
- Consumer: `EnableAutoCommit = false`, `EnableAutoOffsetStore = false` (manual control)
- Producer: `EnableIdempotence = true`, `Acks = All` (reliability)
- Topics auto-created with 3 partitions, replication factor 1

## Project Structure Notes

- **Net versions:** Consumer uses .NET 9.0, Producer/Api use .NET 8.0
- **Solution file:** `KOrder.slnx` (modern XML format)
- **Docker:** `docker-compose.yml` in `.SolutionItems/` includes Zookeeper, Kafka, and Kafdrop
