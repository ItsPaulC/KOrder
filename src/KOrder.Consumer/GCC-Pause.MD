Pause/Resume Kafka Consumer with intelligent backpressure management. Here's what was added:

Implementation Summary:

1. Backpressure Configuration:

public KeyedConsumer(
string bootstrapServers,
string groupId,
string topic,
Func<ConsumeResult<string, Order>, Task>? messageProcessor = null,
int maxQueuedMessages = 10000,      // Pause when queue hits this
int resumeThreshold = 5000)         // Resume when queue drops to this

2. Key Features Added:

Memory Monitoring:
- Tracks total queued messages across all key channels
- GetTotalQueuedMessages() - real-time count of buffered messages

Automatic Pause/Resume:
- Pauses Kafka consumer when queued messages ≥ maxQueuedMessages
- Resumes when queued messages ≤ resumeThreshold
- Uses Confluent.Kafka's Pause()/Resume() methods for backpressure

Status Logging:
- Every 30 seconds logs: Consumer status (PAUSED/RUNNING), active keys, queued messages
- Clear visibility into consumer health

Backpressure Alerts:
[BACKPRESSURE] Consumer PAUSED - Queued messages: 10005/10000
[BACKPRESSURE] Consumer RESUMED - Queued messages: 4998/5000

How It Prevents GC Issues:

1. Bounded Memory Growth: Prevents unbounded channel growth by stopping Kafka consumption
2. Self-Regulating: Automatically adapts to processing speed
3. Predictable Footprint: Max memory = maxQueuedMessages * sizeof(ConsumeResult<string, Order>)
4. No Message Loss: Messages stay in Kafka until consumer can handle them


----

Testing the Implementation:

Option A: Test with Current Defaults (10K messages):
# Terminal 1: Run consumer (defaults: max=10000, resume=5000)
dotnet run --project src/KOrder.Consumer/KOrder.Consumer.csproj

# Terminal 2: Send many messages rapidly
dotnet run --project src/KOrder.Producer/KOrder.Producer.csproj
# Select option 4 multiple times quickly to build up queued messages

Option B: Test with Lower Thresholds (easier to trigger):

Update Program.cs to use lower thresholds:
var consumer = new KeyedConsumer(
bootstrapServers,
groupId,
topic,
maxQueuedMessages: 100,   // Pause at 100 messages
resumeThreshold: 50);     // Resume at 50 messages

Production K8s Recommendations:

Tune based on your throughput:
- High throughput (1000+ msg/s): maxQueuedMessages: 50000, resumeThreshold: 25000
- Medium throughput (100-1000 msg/s): maxQueuedMessages: 10000, resumeThreshold: 5000 (default)
- Low throughput (<100 msg/s): maxQueuedMessages: 1000, resumeThreshold: 500

Monitor these metrics:
- [STATUS] logs every 30 seconds
- Watch for frequent pause/resume cycles (indicates processing bottleneck)
- Gen2 GC frequency in your monitoring tools

The consumer is now production-ready for high-throughput K8s deployments with built-in memory protection! 🚀