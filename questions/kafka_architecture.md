# Kafka Architecture

## What is Kafka?

A **distributed event streaming platform**. Think of it as a durable, high-throughput message pipe between systems.

**Core use cases:**

- Decouple services (producer doesn't care who reads its data)
- Stream processing (real-time analytics, fraud detection)
- Event sourcing / audit log
- Data pipeline between microservices

---

## Big Picture

```
  PRODUCERS                    KAFKA CLUSTER                    CONSUMERS
  (write data)                                                  (read data)

 ┌──────────┐                ┌─────────────────┐              ┌──────────────┐
 │ Service A│ ──────────────►│                 │─────────────►│ Service X    │
 └──────────┘                │   B R O K E R   │              └──────────────┘
                             │                 │
 ┌──────────┐                │  (holds topics) │              ┌──────────────┐
 │ Service B│ ──────────────►│                 │─────────────►│ Service Y    │
 └──────────┘                └─────────────────┘              └──────────────┘

                       producers PUSH,  consumers PULL
```

---

## Core Components

### 1. Broker (The BE server handle logics - we are building this)

A single Kafka **server process**. It:

- Receives messages from producers
- Stores them on disk
- Serves them to consumers

A **cluster** is just multiple brokers working together for fault tolerance and scale.

```
Kafka Cluster
┌──────────────────────────────────┐
│  Broker 1    Broker 2    Broker 3│
│  (leader)    (follower)  (follower)
└──────────────────────────────────┘
```

---

### 2. Topic

A **named stream of records**. Like a database table, but append-only.

- Producers write to a topic
- Consumers read from a topic
- Records are **not deleted** after being read (unlike a queue)
- Retention is time-based (default: 7 days)

```
Topic: "orders"
┌──────────────────────────────────────────────────────┐
│  msg0 │ msg1 │ msg2 │ msg3 │ msg4 │ msg5 │ ...        │
└──────────────────────────────────────────────────────┘
        ▲                               ▲
   oldest msg                      newest msg
   (may be deleted                (producer writes here)
    after retention)
```

---

### 3. Partition

A topic is split into **partitions** — ordered, immutable sequences of records.

**Why?** Parallelism. Multiple consumers can read different partitions simultaneously.

```
Topic: "orders"  (3 partitions)

Partition 0:  [msg0] → [msg3] → [msg6] → ...
Partition 1:  [msg1] → [msg4] → [msg7] → ...
Partition 2:  [msg2] → [msg5] → [msg8] → ...
```

Each message has an **offset** — its position within a partition (starts at 0).  
Offsets are per-partition, not global.

```
Partition 0:
offset →  0      1      2      3      4
        [msgA] [msgB] [msgC] [msgD] [msgE]
                              ▲
                    consumer is here (offset=3 = next to read)
```

---

### 4. Producer

Writes records to a topic. It chooses which partition via:

- **Round-robin** (default, no key)
- **Key hash** — same key always goes to same partition (ordering guarantee)
- **Custom partitioner**

```
Producer writes key="user-42"
  → hash("user-42") % num_partitions = 1
  → always goes to Partition 1
  → guarantees order for user-42's events
```

---

### 5. Consumer, Consumer Group & Group ID

A **consumer** reads from one or more partitions.

A **consumer group** is multiple consumers sharing the work — each partition is assigned to exactly one consumer in the group.

A **group ID** is just a string name you set in your consumer code. It is the only thing that controls whether services compete for messages or each get their own copy.

```go
// set in consumer code — nothing to configure on the broker
kafka.consumer({ groupId: "billing" })
```

Kafka auto-creates the group on first connect. No registration needed.

---

#### Case 1: All 3 services must each receive msg A (fan-out)

Give each service a **different group ID**:

```
Producer → Topic "orders" → msg A
                │
    ┌───────────┼───────────┐
    ▼           ▼           ▼
group           group       group
"service1"   "service2"  "service3"
    │           │           │
service1     service2    service3
gets msg A   gets msg A  gets msg A  ← all 3 get it
```

```go
service1 := kafka.consumer({ groupId: "service1" })
service2 := kafka.consumer({ groupId: "service2" })
service3 := kafka.consumer({ groupId: "service3" })
```

Use this when: billing, inventory, and analytics all need to react to the same order event.

---

#### Case 2: Only ONE of the 3 services reads msg A (load balancing)

Give all 3 the **same group ID**:

```
Producer → Topic "orders" → msg A
                │
           group "workers"
        ┌───────┼───────┐
        ▼       ▼       ▼
   service1  service2  service3
   gets msg A  ✗         ✗     ← only one gets it, Kafka decides which
```

```go
service1 := kafka.consumer({ groupId: "workers" })
service2 := kafka.consumer({ groupId: "workers" })
service3 := kafka.consumer({ groupId: "workers" })
```

Use this when: you have 3 identical worker instances and each job should only be processed once.

---

#### Summary: group ID is the only switch

| Goal | Config |
|------|--------|
| Every service gets every message | Different group ID per service |
| Only one service gets each message | Same group ID across all instances |

Same topic, same producer. Just the group ID changes.

---

### 6. Offset

A cursor. Each consumer group tracks **where it left off** per partition.

```
Partition 0:  [0] [1] [2] [3] [4] [5]
                              ▲
               group "billing" is at offset 3
               (has processed 0,1,2 — will read 3 next)
```

Kafka stores committed offsets in an internal topic `__consumer_offsets`.

---

### 7. Replication

Each partition has **1 leader + N replicas** on different brokers.

- Producers/consumers always talk to the **leader**
- Replicas silently copy from the leader
- If leader dies, a replica is **elected as new leader** (no data loss)

```
Partition 0, replication-factor=3:

  Broker 1 [LEADER]  ◄── producer writes here
  Broker 2 [replica] ◄── copies from leader
  Broker 3 [replica] ◄── copies from leader

  Broker 1 dies →  Broker 2 becomes leader instantly
```

---

## Full Picture Together

```
                        ┌─────────────────────────────────────────────┐
                        │              KAFKA CLUSTER                   │
                        │                                              │
 ┌──────────┐  key=A    │  Topic "events"                              │
 │Producer 1│──────────►│                                              │
 └──────────┘           │  Partition 0: [0][1][2][3]...  (Broker 1)   │
                        │  Partition 1: [0][1][2][3]...  (Broker 2)   │
 ┌──────────┐  key=B    │  Partition 2: [0][1][2][3]...  (Broker 3)   │
 │Producer 2│──────────►│                                              │
 └──────────┘           └─────────┬───────────────────────────────────┘
                                  │
                    ┌─────────────┴──────────────┐
                    ▼                            ▼
           Consumer Group A              Consumer Group B
           ┌──────────────┐              ┌──────────────┐
           │ Consumer 1   │◄─ Part 0     │ Consumer X   │◄─ Part 0,1,2
           │ Consumer 2   │◄─ Part 1     └──────────────┘
           │ Consumer 3   │◄─ Part 2      (1 consumer reads all)
           └──────────────┘
           (3 consumers share load)
```

---

## What Kafka is NOT

| Kafka                           | Traditional Queue (RabbitMQ)       |
| ------------------------------- | ---------------------------------- |
| Messages persist after read     | Messages deleted after ack         |
| Pull-based consumers            | Push-based consumers               |
| Multiple consumers get same msg | Each msg delivered to one consumer |
| Ordered per partition           | Ordering not guaranteed            |
| Built for high throughput       | Built for task distribution        |

---

## Relevance to This CodeCrafters Project

You're implementing the **broker** — the server that:

1. **ApiVersions** (key 18) — tells clients what APIs you support ✅ done
2. **DescribeTopicPartitions** (key 75) — returns topic/partition metadata ← current stage
3. **Fetch** (key 1) — returns actual message records from a partition
4. **Produce** (key 0) — accepts and stores messages from a producer
