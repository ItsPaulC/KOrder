✅ Defense-in-Depth Backpressure Implementation Complete!

What Was Implemented:

Layer 1: Bounded Channels (Per-Key Protection)

Channel.CreateBounded<ConsumeResult<string, Order>>(
new BoundedChannelOptions(_perKeyChannelCapacity) {
SingleReader = true,
SingleWriter = false,
FullMode = BoundedChannelFullMode.Wait  // ⭐ Blocks when full
});

Default: 1,000 messages per key
- Prevents any single hot key from consuming excessive memory
- When a key's channel fills up, WriteAsync() blocks the consumer loop
- Natural backpressure mechanism

Layer 2: Pause/Resume (Global Protection)

// When total queued > 10,000
_consumer.Pause(_consumer.Assignment);

// When total queued < 5,000
_consumer.Resume(_consumer.Assignment);

Defaults: 10,000 max, 5,000 resume
- Monitors total messages across ALL channels
- Stops Kafka consumption when global limit reached
- Resumes when processing catches up

  ---
New Constructor Signature:

public KeyedConsumer(
string bootstrapServers,
string groupId,
string topic,
Func<ConsumeResult<string, Order>, Task>? messageProcessor = null,
int maxQueuedMessages = 10000,          // Global pause threshold
int resumeThreshold = 5000,              // Global resume threshold
int perKeyChannelCapacity = 1000)        // ⭐ NEW: Per-key limit

  ---
How The Two Layers Work Together:

Scenario 1: Hot Key Protection

Key "customer-VIP" gets 1,000 messages queued
↓
Bounded channel FULL → WriteAsync() blocks
↓
Consumer loop can't add more for this key
↓
Other keys continue processing normally

Scenario 2: Global Memory Protection

10 keys × 1,000 msgs each = 10,000 total
↓
Global threshold reached → Pause entire consumer
↓
No new messages from Kafka
↓
Processing drains to 5,000 → Resume

Scenario 3: Combined Protection

Key "customer-123": 1,000 msgs (channel full, blocked)
Key "customer-456": 800 msgs
Key "customer-789": 750 msgs
... 8 more keys with 500-900 msgs each
Total: ~10,000 → Global pause kicks in

Result:
✅ No single key can exceed 1,000
✅ Total memory capped at ~10,000
✅ Predictable memory footprint

  ---
Startup Output (When You Run Consumer):

Consumer started. Subscribed to topic 'keyed-messages-topic' with group ID 'my-keyed-consumer-group'. Waiting for messages...
Backpressure Configuration:
- Per-key channel capacity: 1000 messages (bounded channels)
- Global max queued messages: 10000 (pause threshold)
- Global resume threshold: 5000 messages

  ---
Production K8s Tuning Guide:

Low-Medium Throughput (<500 msg/s):

new KeyedConsumer(
bootstrapServers, groupId, topic,
maxQueuedMessages: 5000,
resumeThreshold: 2500,
perKeyChannelCapacity: 500);

High Throughput (500-2000 msg/s):

new KeyedConsumer(
bootstrapServers, groupId, topic,
maxQueuedMessages: 20000,
resumeThreshold: 10000,
perKeyChannelCapacity: 2000);  // Default: 10K/5K/1K

Very High Throughput (>2000 msg/s):

new KeyedConsumer(
bootstrapServers, groupId, topic,
maxQueuedMessages: 50000,
resumeThreshold: 25000,
perKeyChannelCapacity: 5000);

  ---
Memory Calculation Formula:

Worst-case memory usage:
Max Memory = min(
perKeyChannelCapacity × activeKeys,
maxQueuedMessages
) × sizeof(ConsumeResult<string, Order>)

Example with defaults:
- 100 active keys × 1,000 per key = 100,000 possible
- BUT global cap at 10,000 messages
- Actual max: 10,000 messages (~few MB depending on protobuf size)

  ---
Testing the Implementation:

Run the consumer:
dotnet run --project src/KOrder.Consumer/KOrder.Consumer.csproj

You should see the new backpressure configuration printed at startup!

Send messages to test:
# Terminal 2
dotnet run --project src/KOrder.Producer/KOrder.Producer.csproj
# Select option 4 multiple times to send bursts

Watch for these log patterns:
[BACKPRESSURE] Consumer PAUSED - Queued messages: 10005/10000
[STATUS] Consumer: PAUSED | Active Keys: 12 | Queued Messages: 8500
[BACKPRESSURE] Consumer RESUMED - Queued messages: 4998/5000

  ---
Benefits for K8s Production:

✅ No OutOfMemory crashes - Hard limits at both levels✅ Predictable GC behavior - Bounded heap growth✅ Graceful degradation - Slows down vs crashing✅ Hot key protection - Single customer can't DoS the
system✅ Observable - Status logs every 30 seconds✅ Auto-recovery - Resumes automatically when processing catches up

Your KeyedConsumer is now production-grade for high-throughput K8s deployments! 🚀
