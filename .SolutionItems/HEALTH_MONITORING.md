# KOrder Health Monitoring System

## Overview for Non-Technical Readers

Imagine you have a restaurant with multiple kitchens (pods in Kubernetes). Each kitchen receives orders from customers (Kafka messages) and needs to process them in the order they arrive. The Health Monitoring system is like having a manager who constantly watches each kitchen to make sure:

1. **No kitchen gets stuck** - If a kitchen stops processing orders and the queue keeps growing, something is wrong
2. **No kitchen gets overwhelmed** - If orders arrive faster than the kitchen can handle, we need to slow down or get help
3. **Bad kitchens get replaced** - If a kitchen can't recover, we shut it down and start fresh

### How We Handle a Stuck Pod

**The Problem:**
A pod (kitchen) might get stuck due to:
- A bug in the code (chef got confused)
- Database connection issues (freezer door jammed)
- Network problems (phone line cut)

**How We Detect It:**
Every 10 seconds, we check:
- "How many orders are waiting to be processed?" (the lag)
- We keep the last 5 measurements
- If the queue grew 3 times in a row → **Kitchen is stuck!**

**What Happens:**
1. **Health check fails** - We mark the kitchen as unhealthy
2. **Kubernetes stops sending orders** - Readiness probe fails (503 response)
3. **Kubernetes restarts the kitchen** - Liveness probe eventually fails
4. **Fresh start** - New pod starts clean and processes the backlog

### How We Prevent Downstream Flooding

**The Problem:**
If messages arrive at 1000/second but we can only process 500/second, the queue grows forever until we run out of memory.

**Our Solution - Two Layers of Protection:**

#### Layer 1: Per-Key Bounded Channels
- Each customer key has a maximum queue size (1,000 messages)
- When a customer's queue is full, we stop accepting new messages for that customer
- Other customers continue normally

#### Layer 2: Global Pause/Resume
- Total queued messages across all customers: max 10,000
- When we hit 10,000 → **Pause** pulling from Kafka
- Messages stay safe in Kafka (not in memory)
- When queue drops to 5,000 → **Resume** pulling
- This prevents memory exhaustion and GC pauses

**Analogy:**
Think of a highway with two traffic control systems:
1. **Per-lane metering lights** - Each lane can only have so many cars
2. **Global highway closure** - If total traffic exceeds capacity, we temporarily stop letting cars enter

---

## Component Architecture

### 1. ConsumerHealthMonitor

**Purpose:** The "health inspector" that tracks lag patterns and detects problems.

**What It Does:**
- Monitors lag for each Kafka partition assigned to this pod
- Keeps a sliding window of the last 5 lag measurements
- Detects two types of problems:
  - **Stuck consumer:** Lag increasing 3+ times consecutively
  - **High lag:** Lag exceeds threshold (default: 10,000 messages)

**Key Methods:**
```csharp
RecordLag(partition, currentLag)     // Record lag measurement
CheckHealth()                         // Evaluate if consumer is healthy
GetTotalLag()                        // Get total lag across all partitions
```

**Configuration:**
- `lagCheckWindowSize: 5` - Keep last 5 measurements
- `maxAcceptableLag: 10000` - Fail health if lag > 10,000
- `consecutiveIncreaseThreshold: 3` - 3 consecutive increases = stuck

---

### 2. PartitionLagHistory

**Purpose:** Tracks lag history for a single partition to detect patterns.

**What It Does:**
- Maintains a queue of recent lag values (FIFO)
- Analyzes trends: Is lag consistently increasing?
- Counts consecutive increases

**Key Methods:**
```csharp
AddLag(lag)                          // Add new lag measurement
IsConsistentlyIncreasing(threshold)  // Check if stuck (3+ increases)
GetIncreasingCount()                 // Count consecutive increases
```

**Example Detection:**
```
Time  Lag   Status
T0    100   baseline
T1    150   +1 increase
T2    200   +2 increases
T3    280   +3 increases → STUCK!
```

---

### 3. HealthStatus

**Purpose:** Data class representing the current health state.

**Properties:**
```csharp
IsHealthy           // true/false health flag
Reason              // Human-readable explanation
PartitionLags       // Dictionary<partitionId, currentLag>
LastCheckTime       // When was this checked
```

**Used By:**
- HealthCheckServer to determine HTTP response codes
- Logging to report unhealthy states

---

### 4. HealthCheckServer

**Purpose:** HTTP server that exposes health endpoints for Kubernetes probes.

**Endpoints:**

#### `/health/live` (Liveness Probe)
- **Always returns 200 OK** (unless process is completely dead)
- Kubernetes uses this to know if pod needs restart
- Only fails if process is deadlocked or unrecoverable

#### `/health/ready` (Readiness Probe)
- **Returns 200** if consumer is healthy
- **Returns 503** if consumer is stuck or lagging
- Kubernetes uses this to route traffic
- Causes pod to stop receiving new messages when unhealthy

#### `/metrics` (Prometheus)
- Exposes lag metrics in Prometheus format
- Used by monitoring systems (Grafana, etc.)
- Metrics:
  - `kafka_consumer_lag{partition="0"}` - Per-partition lag
  - `kafka_consumer_total_lag` - Total lag
  - `kafka_consumer_healthy` - Health status (1=healthy, 0=unhealthy)

**HTTP Response Codes:**
- `200 OK` - Healthy, ready for traffic
- `503 Service Unavailable` - Unhealthy, don't send traffic

---

### 5. KeyedConsumer Integration

**MonitorLagAsync Task:**
Runs every 10 seconds (configurable) and:

1. **Gets assigned partitions** from Kafka
2. **For each partition:**
   - Query committed offset (where we are)
   - Query high water mark (latest message in Kafka)
   - Calculate lag: `lag = highWaterMark - committedOffset`
   - Record in HealthMonitor
3. **Check health** after all partitions measured
4. **Log warnings** if unhealthy

**Integration Points:**
- `HealthMonitor` property exposed to HealthCheckServer
- Lag monitoring runs as background task
- Automatic cleanup on consumer shutdown

---

## Scenarios with Sequence Diagrams

### Scenario 1: Healthy Pod Processing Messages

```mermaid
sequenceDiagram
    participant K as Kafka
    participant C as KeyedConsumer
    participant HM as HealthMonitor
    participant HS as HealthCheckServer
    participant K8s as Kubelet

    Note over C,HM: Every 10 seconds
    C->>K: Query committed offset (100)
    C->>K: Query high water mark (105)
    C->>C: Calculate lag = 105 - 100 = 5
    C->>HM: RecordLag(partition0, lag=5)

    HM->>HM: Check lag history [3, 4, 5, 4, 5]
    HM->>HM: No consistent increase
    HM->>HM: Lag < 10,000 ✓
    HM->>HM: Status = HEALTHY

    Note over HS,K8s: Every 5 seconds
    K8s->>HS: GET /health/ready
    HS->>HM: CheckHealth()
    HM-->>HS: IsHealthy=true
    HS-->>K8s: 200 OK

    Note over K8s: Pod stays healthy<br/>continues receiving traffic
```

---

### Scenario 2: Pod Gets Stuck (Increasing Lag)

```mermaid
sequenceDiagram
    participant K as Kafka
    participant C as KeyedConsumer
    participant HM as HealthMonitor
    participant HS as HealthCheckServer
    participant K8s as Kubelet

    Note over C: Pod has a bug<br/>processing stalls

    rect rgb(255, 240, 240)
        Note over C,HM: T+0s: First check
        C->>K: Query lag
        K-->>C: lag = 1000
        C->>HM: RecordLag(partition0, 1000)
        HM->>HM: History: [1000]
    end

    rect rgb(255, 230, 230)
        Note over C,HM: T+10s: Second check
        C->>K: Query lag
        K-->>C: lag = 1500 (+500)
        C->>HM: RecordLag(partition0, 1500)
        HM->>HM: History: [1000, 1500]<br/>+1 increase
    end

    rect rgb(255, 220, 220)
        Note over C,HM: T+20s: Third check
        C->>K: Query lag
        K-->>C: lag = 2100 (+600)
        C->>HM: RecordLag(partition0, 2100)
        HM->>HM: History: [1000, 1500, 2100]<br/>+2 consecutive increases
    end

    rect rgb(255, 200, 200)
        Note over C,HM: T+30s: DETECTION!
        C->>K: Query lag
        K-->>C: lag = 2800 (+700)
        C->>HM: RecordLag(partition0, 2800)
        HM->>HM: History: [1000, 1500, 2100, 2800]<br/>+3 consecutive increases
        HM->>HM: UNHEALTHY: Consumer stuck!
        C->>C: Log WARNING: Consumer UNHEALTHY
    end

    rect rgb(200, 200, 255)
        Note over HS,K8s: T+31s: Health check fails
        K8s->>HS: GET /health/ready
        HS->>HM: CheckHealth()
        HM-->>HS: IsHealthy=false<br/>Reason="Partition 0 lag consistently increasing"
        HS-->>K8s: 503 Service Unavailable
        HS->>HS: Log WARNING: Readiness check FAILED
    end

    Note over K8s: Pod marked UNREADY<br/>Stops receiving traffic

    rect rgb(200, 200, 255)
        Note over HS,K8s: T+36s: Liveness check starts failing
        K8s->>HS: GET /health/live
        HS-->>K8s: 200 OK (still alive)
        Note over K8s: After failureThreshold=3<br/>Liveness fails
    end

    rect rgb(200, 255, 200)
        Note over K8s: T+60s: Pod restart
        K8s->>K8s: Kill pod
        K8s->>K8s: Start new pod
        Note over C,HM: Fresh start<br/>Lag resets to 0
    end
```

---

### Scenario 3: High Lag (Not Stuck, Just Slow)

```mermaid
sequenceDiagram
    participant K as Kafka
    participant C as KeyedConsumer
    participant HM as HealthMonitor
    participant HS as HealthCheckServer
    participant K8s as Kubelet

    Note over K: Burst of messages<br/>10,500 messages arrive

    rect rgb(255, 255, 200)
        Note over C,HM: T+0s: Lag spike detected
        C->>K: Query lag
        K-->>C: lag = 10,500
        C->>HM: RecordLag(partition0, 10500)
        HM->>HM: Check lag history [8000, 9000, 10500]
        HM->>HM: NOT consistently increasing<br/>(9000→10500 is not consistent)
        HM->>HM: BUT lag > maxAcceptableLag (10000)
        HM->>HM: UNHEALTHY: Lag exceeds threshold!
        C->>C: Log WARNING: Lag exceeds maximum
    end

    rect rgb(255, 200, 200)
        K8s->>HS: GET /health/ready
        HS->>HM: CheckHealth()
        HM-->>HS: IsHealthy=false<br/>Reason="Partition 0 lag (10500) exceeds maximum (10000)"
        HS-->>K8s: 503 Service Unavailable
    end

    Note over K8s: Pod marked UNREADY<br/>Traffic diverted to other pods

    rect rgb(200, 255, 200)
        Note over C: T+10s: Processing catches up
        C->>K: Query lag
        K-->>C: lag = 8500
        C->>HM: RecordLag(partition0, 8500)
        HM->>HM: Lag history [9000, 10500, 8500]
        HM->>HM: Lag < 10000 ✓
        HM->>HM: HEALTHY again!
    end

    rect rgb(200, 255, 200)
        K8s->>HS: GET /health/ready
        HS->>HM: CheckHealth()
        HM-->>HS: IsHealthy=true
        HS-->>K8s: 200 OK
    end

    Note over K8s: Pod marked READY<br/>Resumes receiving traffic
```

---

### Scenario 4: Backpressure Prevents Memory Overflow

```mermaid
sequenceDiagram
    participant K as Kafka
    participant C as KeyedConsumer
    participant Ch as Channels (Memory)
    participant BP as Backpressure System

    Note over K: High message rate<br/>1000 msg/s arriving

    rect rgb(200, 255, 200)
        Note over C,Ch: Normal operation
        K->>C: Consume messages
        C->>Ch: Write to per-key channels
        Ch->>Ch: Processing: 500 msg/s<br/>Queued: 3,000 messages
        Note over Ch: Safe memory usage
    end

    rect rgb(255, 255, 200)
        Note over C,Ch: T+10s: Queue building up
        K->>C: Consume messages (fast)
        C->>Ch: Write to channels
        Ch->>Ch: Queued: 7,000 messages
        Note over Ch: Memory increasing
    end

    rect rgb(255, 230, 200)
        Note over C,Ch: T+15s: Approaching limit
        K->>C: Consume messages
        C->>Ch: Write to channels
        Ch->>Ch: Queued: 9,500 messages
        Ch->>BP: CheckAndPauseIfNeeded()
        BP->>BP: 9500 < 10000 ✓ (OK)
    end

    rect rgb(255, 200, 200)
        Note over C,Ch: T+20s: LIMIT EXCEEDED
        K->>C: Consume messages
        C->>Ch: Write to channels
        Ch->>Ch: Queued: 10,100 messages
        Ch->>BP: CheckAndPauseIfNeeded()
        BP->>BP: 10100 >= 10000 ⚠️
        BP->>C: PAUSE Kafka consumer!
        C->>K: consumer.Pause(partitions)
        C->>C: Log WARNING: Consumer PAUSED
    end

    rect rgb(255, 220, 220)
        Note over K,C: T+21s: Kafka paused
        K->>K: Messages accumulate in Kafka<br/>(durable, safe)
        C->>C: No new messages consumed
        Ch->>Ch: Processing continues<br/>Queued: 9,800
        Ch->>Ch: Processing continues<br/>Queued: 9,400
    end

    rect rgb(255, 240, 200)
        Note over C,Ch: T+30s: Processing catches up
        Ch->>Ch: Queued: 8,000
        Ch->>Ch: Queued: 6,500
        Ch->>Ch: Queued: 4,900
        Ch->>BP: CheckAndPauseIfNeeded()
        BP->>BP: 4900 <= 5000 ✓
        BP->>C: RESUME Kafka consumer!
        C->>K: consumer.Resume(partitions)
        C->>C: Log INFO: Consumer RESUMED
    end

    rect rgb(200, 255, 200)
        Note over K,C: T+31s: Normal operation restored
        K->>C: Consume messages (resumes)
        C->>Ch: Write to channels
        Ch->>Ch: Queued: stable ~5,000-7,000
        Note over Ch: Memory under control<br/>No GC pauses
    end
```

---

### Scenario 5: Per-Key Bounded Channel Protection

```mermaid
sequenceDiagram
    participant K as Kafka
    participant C as KeyedConsumer
    participant Ch1 as Channel[customer-123]
    participant Ch2 as Channel[customer-456]
    participant P as Processing Tasks

    Note over K: customer-123 having issues<br/>slow processing

    rect rgb(200, 255, 200)
        Note over C,Ch2: Normal customer
        K->>C: Message for customer-456
        C->>Ch2: WriteAsync() - non-blocking
        Ch2->>P: Process immediately
        P-->>Ch2: Done (100ms)
        Note over Ch2: Queue: 10/1000 messages
    end

    rect rgb(255, 255, 200)
        Note over C,Ch1: Hot customer (slow)
        K->>C: Messages for customer-123
        C->>Ch1: WriteAsync() messages
        Ch1->>P: Process slowly
        P-->>Ch1: Done (5000ms each!)
        Note over Ch1: Queue: 800/1000 messages
    end

    rect rgb(255, 230, 200)
        Note over Ch1: Approaching limit
        K->>C: More messages for customer-123
        C->>Ch1: WriteAsync()
        Note over Ch1: Queue: 980/1000
    end

    rect rgb(255, 200, 200)
        Note over Ch1: CHANNEL FULL
        K->>C: Message for customer-123
        C->>Ch1: WriteAsync() - BLOCKS!
        Note over Ch1: Queue: 1000/1000 (FULL)
        Note over C: Consumer loop blocked<br/>can't pull from Kafka
    end

    rect rgb(255, 220, 220)
        Note over C: Entire consumer paused
        K->>K: Messages accumulate in Kafka
        Note over Ch1: Waiting for space in channel
        P->>P: Processing customer-123 slowly
    end

    rect rgb(255, 240, 200)
        Note over Ch1: Space available
        P-->>Ch1: Message processed
        Note over Ch1: Queue: 999/1000
        Ch1-->>C: WriteAsync() completes
        C->>C: Consumer unblocked
    end

    rect rgb(200, 255, 200)
        Note over C: Resumes normal operation
        K->>C: Can consume again
        C->>Ch2: customer-456 continues normally
        Note over Ch2: Other customers unaffected<br/>by customer-123 slowness
    end

    Note over Ch1,Ch2: customer-123 limited to 1000 messages<br/>Cannot hog all memory
```

---

## Configuration Reference

### Environment Variables (K8s)

```yaml
env:
  # Kafka Configuration
  - name: KAFKA_BOOTSTRAP_SERVERS
    value: "kafka-service:9092"
  - name: KAFKA_GROUP_ID
    value: "korder-consumer-group"
  - name: KAFKA_TOPIC
    value: "keyed-messages-topic"

  # Health Monitoring
  - name: HEALTH_CHECK_PORT
    value: "8080"
  - name: MAX_ACCEPTABLE_LAG
    value: "10000"            # Fail health if lag > this
  - name: LAG_CHECK_INTERVAL_SECONDS
    value: "10"               # Check lag every N seconds

  # Backpressure
  - name: MAX_QUEUED_MESSAGES
    value: "10000"            # Global pause threshold
  - name: RESUME_THRESHOLD
    value: "5000"             # Global resume threshold
  - name: PER_KEY_CHANNEL_CAPACITY
    value: "1000"             # Max messages per key

  # Logging
  - name: Logging__LogLevel__Default
    value: "Information"      # or Debug, Warning, Error
```

### KeyedConsumer Constructor Parameters

```csharp
new KeyedConsumer(
    bootstrapServers,              // "kafka:9092"
    groupId,                       // Consumer group ID
    topic,                         // Topic name
    logger,                        // ILogger<KeyedConsumer>
    messageProcessor: null,        // Optional custom processor
    maxQueuedMessages: 10000,      // Global pause threshold
    resumeThreshold: 5000,         // Global resume threshold
    perKeyChannelCapacity: 1000,   // Per-key channel limit
    enableHealthMonitoring: true,  // Enable lag monitoring
    maxAcceptableLag: 10000,       // Health check threshold
    lagCheckIntervalSeconds: 10    // Check frequency
);
```

---

## Monitoring Recommendations

### Prometheus Queries

```promql
# High lag alert
kafka_consumer_total_lag > 50000

# Consumer unhealthy
kafka_consumer_healthy == 0

# Pod is frequently pausing (backpressure)
rate(log_messages{message=~".*PAUSED.*"}[5m]) > 0

# Per-partition lag
kafka_consumer_lag{partition="0"}
```

### Grafana Dashboard Panels

1. **Total Consumer Lag** - Line graph of `kafka_consumer_total_lag`
2. **Health Status** - Gauge of `kafka_consumer_healthy` (1=healthy, 0=unhealthy)
3. **Per-Partition Lag** - Multi-line graph with partition breakdown
4. **Backpressure Events** - Log panel filtered for PAUSED/RESUMED
5. **Pod Restarts** - K8s pod restart count

### Log Queries (Loki/CloudWatch)

```logql
# Find unhealthy events
{app="korder-consumer"} |= "UNHEALTHY"

# Track backpressure
{app="korder-consumer"} |= "BACKPRESSURE"

# Dead letter queue entries
{app="korder-consumer"} |= "DEAD LETTER"
```

---

## Troubleshooting Guide

### Symptom: Pod constantly restarting

**Possible Causes:**
- Bug causing consistent failures
- Database connection always failing
- Invalid configuration

**Investigation:**
```bash
# Check previous crash logs
kubectl logs korder-consumer-abc123 --previous

# Check health endpoint manually
kubectl port-forward korder-consumer-abc123 8080:8080
curl http://localhost:8080/health/ready

# Check lag metrics
curl http://localhost:8080/metrics | grep lag
```

---

### Symptom: Lag keeps growing but pod is "healthy"

**Possible Causes:**
- Lag threshold too high (increase from 10,000)
- Messages arriving faster than processing capacity
- Need horizontal scaling

**Solution:**
```yaml
# Lower lag threshold
env:
- name: MAX_ACCEPTABLE_LAG
  value: "5000"  # More sensitive

# Or scale horizontally
spec:
  replicas: 6  # More pods = more parallel processing
```

---

### Symptom: Frequent PAUSED/RESUMED cycles

**Possible Causes:**
- Threshold too aggressive
- Processing bottleneck
- Need performance optimization

**Solution:**
```yaml
# Adjust backpressure thresholds
env:
- name: MAX_QUEUED_MESSAGES
  value: "20000"  # Higher pause threshold
- name: RESUME_THRESHOLD
  value: "10000"  # Higher resume threshold
```

---

## Best Practices

1. **Start Conservative:** Use default thresholds and adjust based on observed behavior
2. **Monitor Prometheus Metrics:** Set up alerts for high lag and unhealthy consumers
3. **Log Aggregation:** Use ELK/Loki to centralize logs for analysis
4. **Load Testing:** Test backpressure behavior under high load before production
5. **Horizontal Scaling:** Add more pods when lag consistently high across all pods
6. **Tune GC:** Use server GC settings for high-throughput scenarios

---

## Summary

The KOrder Health Monitoring system provides **defense in depth** against common Kafka consumer problems:

| Problem | Detection | Response | Outcome |
|---------|-----------|----------|---------|
| Stuck consumer | 3+ consecutive lag increases | Health check fails → Pod restart | Fresh pod processes backlog |
| High lag | Lag > threshold | Health check fails → Load balancing | Traffic distributed to healthy pods |
| Memory overflow | Global queue > 10,000 | Pause Kafka consumption | Messages stay in Kafka (safe) |
| Hot key | Per-key queue full | Block that key's writes | Other keys unaffected |

All of this happens **automatically** with no manual intervention required. Kubernetes orchestrates the healing process based on health check responses.
