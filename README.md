# 🐰 RabbitMQ — Complete Learning Guide (Beginner to Advanced)

> **Node.js-focused · Production-ready · Interview-prepared**

---

## Table of Contents

| # | Section | Level |
|---|---------|-------|
| 1 | [Introduction to RabbitMQ](#1-introduction-to-rabbitmq) | 🟢 Beginner |
| 2 | [RabbitMQ vs Other Systems](#2-rabbitmq-vs-other-messaging-systems) | 🟢 Beginner |
| 3 | [Installation & Setup](#3-installation--setup) | 🟢 Beginner |
| 4 | [First RabbitMQ Application](#4-first-rabbitmq-application-nodejs) | 🟢 Beginner |
| 5 | [Core Concepts Deep Dive](#5-core-rabbitmq-concepts) | 🟡 Intermediate |
| 6 | [Routing](#6-routing) | 🟡 Intermediate |
| 7 | [Message Acknowledgement](#7-message-acknowledgement) | 🟡 Intermediate |
| 8 | [Message Durability & Reliability](#8-message-durability--reliability) | 🟡 Intermediate |
| 9 | [Prefetch & Consumer Control](#9-prefetch--consumer-control) | 🟡 Intermediate |
| 10 | [Publisher Confirms](#10-publisher-confirms) | 🟡 Intermediate |
| 11 | [Consumer Reliability](#11-consumer-reliability) | 🟡 Intermediate |
| 12 | [Retry Mechanisms](#12-retry-mechanisms) | 🔴 Advanced |
| 13 | [Dead Letter Exchange (DLX)](#13-dead-letter-exchange-dlx) | 🔴 Advanced |
| 14 | [Error Handling](#14-error-handling) | 🔴 Advanced |
| 15 | [Message Ordering](#15-message-ordering) | 🟡 Intermediate |
| 16 | [Scaling RabbitMQ](#16-scaling-rabbitmq) | 🔴 Advanced |
| 17 | [High Availability & Clustering](#17-high-availability--clustering) | 🔴 Advanced |
| 18 | [Security](#18-security) | 🟡 Intermediate |
| 19 | [Monitoring & Management](#19-monitoring--management) | 🟡 Intermediate |
| 20 | [RabbitMQ with Microservices](#20-rabbitmq-with-microservices) | 🔴 Advanced |
| 21 | [Real-World Use Cases](#21-real-world-use-cases) | 🟡 Intermediate |
| 22 | [When to Use RabbitMQ](#22-when-should-i-use-rabbitmq) | 🟢 Beginner |
| 23 | [When NOT to Use RabbitMQ](#23-when-should-i-not-use-rabbitmq) | 🟢 Beginner |
| 24 | [Design Patterns](#24-rabbitmq-design-patterns) | 🔴 Advanced |
| 25 | [Advanced Concepts](#25-advanced-rabbitmq-concepts) | 🔴 Advanced |
| 26 | [Production Best Practices](#26-production-best-practices) | 🔴 Advanced |
| 27 | [Common Mistakes](#27-common-mistakes) | 🟡 Intermediate |
| 28 | [Complete Practical Project](#28-complete-practical-project) | 🔴 Advanced |
| 29 | [Debugging Guide](#29-debugging-guide) | 🟡 Intermediate |
| 30 | [Interview Preparation](#30-interview-preparation) | All Levels |
| 31 | [Cheat Sheet](#31-cheat-sheet) | Reference |
| 32 | [Learning Roadmap](#32-learning-roadmap) | Reference |

---

## 1. Introduction to RabbitMQ

### What is RabbitMQ?

RabbitMQ is a **message broker** — think of it as a post office for your software. When Service A wants to send data to Service B, instead of calling Service B directly, it drops a message into RabbitMQ. RabbitMQ holds it safely and delivers it to Service B when Service B is ready to receive it.

- Written in **Erlang** (built for reliability and concurrency)
- Implements the **AMQP** (Advanced Message Queuing Protocol) standard
- Open-source, battle-tested, used by companies like Instagram, Reddit, and NASA
- Supports millions of messages per second in production systems

### What Problem Does RabbitMQ Solve?

#### Problem 1: Tight Coupling

Without RabbitMQ:
```
Order Service ──HTTP──► Payment Service
                          (What if Payment Service is down?)
```

With RabbitMQ:
```
Order Service ──message──► RabbitMQ ──message──► Payment Service
                (Order stored safely even if Payment is down)
```

#### Problem 2: Speed Mismatch
When Service A processes 10,000 orders/minute but Service B (email sender) can only handle 1,000 emails/minute — RabbitMQ buffers the difference.

#### Problem 3: Broadcasting
One event (order placed) needs to trigger many actions: email, SMS, inventory update, analytics. Without RabbitMQ you'd need to call each service in sequence. With RabbitMQ you publish once and all services react independently.

### Message Queue vs Direct API Communication

| Aspect | Direct API (HTTP/REST) | Message Queue (RabbitMQ) |
|--------|----------------------|--------------------------|
| **Coupling** | Tight — caller waits | Loose — fire and forget |
| **Speed** | Synchronous (slow chain) | Asynchronous (non-blocking) |
| **Resilience** | If B is down, A fails | If B is down, message waits |
| **Scaling** | Hard — A must know B's address | Easy — add more consumers |
| **Fan-out** | A must call B, C, D separately | A publishes once |
| **Retry** | Manual, in A's code | Built-in via RabbitMQ |

**When to stay with direct API:** User login, reading data, real-time responses where the user waits.  
**When to use RabbitMQ:** Sending emails, processing orders, anything that can happen in the background.

### RabbitMQ Architecture — Complete Map

```
┌─────────────────────────────────────────────────────────────────┐
│                        RABBITMQ BROKER                          │
│                                                                 │
│   Virtual Host: /production                                     │
│   ┌───────────────────────────────────────────────────────┐     │
│   │                                                       │     │
│   │  ┌──────────┐  Routing Key   ┌─────────────────────┐ │     │
│   │  │ Exchange │───────────────►│      Queue A        │ │     │
│   │  │(orders)  │                │  [msg1][msg2][msg3] │ │     │
│   │  └──────────┘                └──────────┬──────────┘ │     │
│   │       │                                 │             │     │
│   │       │ Binding                         │ deliver     │     │
│   │       │                                 ▼             │     │
│   │       │                      ┌─────────────────────┐ │     │
│   │       └─────────────────────►│      Queue B        │ │     │
│   │                              │  [msg4][msg5]       │ │     │
│   │                              └──────────┬──────────┘ │     │
│   └───────────────────────────────────────┬─┴────────────┘     │
│                                           │                     │
└───────────────────────────────────────────┼─────────────────────┘
         ▲                                  │
         │ publish                          │ consume
         │                                  ▼
┌────────┴───────┐                 ┌─────────────────┐
│    PRODUCER    │                 │    CONSUMER     │
│ (Order Service)│                 │ (Email Service) │
└────────────────┘                 └─────────────────┘
```

### Key Terms — Explained Simply

| Term | Simple Explanation | Technical Definition |
|------|--------------------|----------------------|
| **Broker** | The post office itself | RabbitMQ server that routes messages |
| **Producer** | The person dropping a letter | Application that publishes messages |
| **Consumer** | The person receiving a letter | Application that reads messages |
| **Message** | The letter | Data payload (bytes, JSON, etc.) |
| **Queue** | The mailbox | Buffer that stores messages |
| **Exchange** | The sorting department | Receives messages, decides which queue |
| **Binding** | The sorting rule | Rule connecting exchange → queue |
| **Routing Key** | The address on the envelope | String used by exchange to route |
| **Virtual Host** | Separate post office branches | Isolated namespace (like separate databases) |
| **Connection** | The phone line to the post office | TCP connection to RabbitMQ |
| **Channel** | Separate conversations on the same line | Lightweight connection inside a TCP connection |

### Complete Message Flow

```
Step 1: Producer creates connection to RabbitMQ broker
Step 2: Producer opens a channel inside that connection
Step 3: Producer publishes message to an Exchange with a Routing Key

Step 4: Exchange receives the message
Step 5: Exchange looks at its Bindings to find matching Queues
Step 6: Exchange copies/routes message to matching Queue(s)

Step 7: Queue stores the message durably (if configured)
Step 8: Consumer pulls/receives message from Queue
Step 9: Consumer processes the message
Step 10: Consumer sends ACK (acknowledgement) to RabbitMQ
Step 11: RabbitMQ removes message from Queue
```

```
Producer
   │
   │ (1) publish("order.created", message)
   ▼
Exchange [type=topic, name="orders"]
   │
   │ (2) matches binding "order.*" → Queue A
   │ (3) matches binding "order.created" → Queue B
   ▼              ▼
Queue A        Queue B
   │              │
   ▼              ▼
Consumer 1    Consumer 2
   │
   │ (4) process message
   │ (5) channel.ack(msg)
   ▼
Message deleted from queue ✅
```

---

## 2. RabbitMQ vs Other Messaging Systems

### Quick Comparison Table

| Feature | RabbitMQ | Kafka | Amazon SQS | Amazon SNS | Redis/BullMQ | ActiveMQ |
|---------|---------|-------|-----------|-----------|-------------|---------|
| **Type** | Message Broker | Event Log | Managed Queue | Pub/Sub | In-memory Queue | Message Broker |
| **Protocol** | AMQP | Custom | HTTP/HTTPS | HTTP/HTTPS | Redis protocol | AMQP, STOMP, etc. |
| **Message Retention** | Until consumed | Configurable (days/forever) | 14 days max | No storage | Until consumed | Until consumed |
| **Ordering** | Per-queue | Per-partition | Not guaranteed | N/A | Per-queue | Per-queue |
| **Throughput** | ~50k msg/s | Millions/s | ~3,000 msg/s | Millions/s | ~100k msg/s | ~30k msg/s |
| **Routing** | Powerful (4 exchange types) | None (use Kafka Streams) | Basic | Topic-based | None | Powerful |
| **Replay** | ❌ No | ✅ Yes | ❌ No | ❌ No | ❌ No | Limited |
| **Setup Complexity** | Low-Medium | High | None (managed) | None (managed) | Low | Medium |
| **Best For** | Task queues, routing | Event streaming | Simple queuing | Fan-out alerts | Fast background jobs | Enterprise Java |

### RabbitMQ vs Kafka — The Big One

This is the most important comparison because developers often confuse when to use which.

#### Kafka Mental Model: The Newspaper
Kafka is like a newspaper. Once printed, all subscribers can read it. Even if you weren't subscribed last week, you can go back and read last week's editions. The newspaper stays for 30 days. Many different subscribers (groups) read the same newspaper simultaneously, independently.

#### RabbitMQ Mental Model: The Post Office
RabbitMQ is like a post office. You hand in a package, it's delivered to ONE recipient (or broadcast to specific named recipients). Once delivered and signed for, it's gone. The focus is on guaranteed delivery to the right person, not on keeping records.

| Criteria | Use RabbitMQ | Use Kafka |
|----------|-------------|-----------|
| **Task Distribution** | ✅ Worker queues, background jobs | ❌ Overkill |
| **Complex Routing** | ✅ Route by key, headers, patterns | ❌ Kafka has no routing |
| **Event Replay** | ❌ Not supported | ✅ Built-in |
| **Event Sourcing** | ❌ Not designed for it | ✅ Perfect |
| **Audit Logs** | ❌ Messages disappear | ✅ Permanent log |
| **Throughput > 100k/s** | ⚠️ Possible but harder | ✅ Native |
| **Microservice Commands** | ✅ Ideal | ⚠️ Possible but heavy |
| **Analytics Pipeline** | ❌ Wrong tool | ✅ Perfect |
| **Request/Reply (RPC)** | ✅ Supported | ❌ Complex |
| **Message TTL** | ✅ Built-in | ⚠️ Partition-level retention |

**Decision Rule:**
- "I want to process work tasks, send emails, decouple microservices" → **RabbitMQ**
- "I want to stream events, build data pipelines, replay history" → **Kafka**

---

## 3. Installation & Setup

### Option A: Docker (Recommended for Learning)

```bash
# Start RabbitMQ with Management UI
docker run -d \
  --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=admin \
  -e RABBITMQ_DEFAULT_PASS=password \
  rabbitmq:3-management

# Check logs
docker logs rabbitmq

# Stop
docker stop rabbitmq

# Remove
docker rm rabbitmq
```

### Option B: Linux/Ubuntu

```bash
# Install Erlang (required by RabbitMQ)
sudo apt-get update
sudo apt-get install -y erlang

# Add RabbitMQ signing key and repository
curl -fsSL https://packagecloud.io/rabbitmq/rabbitmq-server/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/rabbitmq.gpg
echo "deb [signed-by=/usr/share/keyrings/rabbitmq.gpg] https://packagecloud.io/rabbitmq/rabbitmq-server/ubuntu/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/rabbitmq.list

# Install RabbitMQ
sudo apt-get update
sudo apt-get install -y rabbitmq-server

# Enable management UI plugin
sudo rabbitmq-plugins enable rabbitmq_management

# Start service
sudo systemctl start rabbitmq-server
sudo systemctl enable rabbitmq-server   # start on boot

# Check status
sudo rabbitmq-diagnostics status
sudo systemctl status rabbitmq-server
```

### Option C: Windows

```powershell
# 1. Download and install Erlang from: https://erlang.org/download
# 2. Download RabbitMQ installer from: https://rabbitmq.com/download.html
# 3. Run the installer
# 4. Enable management plugin via RabbitMQ Command Prompt:
rabbitmq-plugins enable rabbitmq_management

# Start/Stop via Services or:
rabbitmq-service start
rabbitmq-service stop
```

### Docker Compose Setup (Best for Projects)

```yaml
# docker-compose.yml
version: '3.8'
services:
  rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq
    ports:
      - "5672:5672"    # AMQP port (your app connects here)
      - "15672:15672"  # Management UI (browser)
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: password
      RABBITMQ_DEFAULT_VHOST: /
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 30s
      timeout: 10s
      retries: 5

volumes:
  rabbitmq_data:
```

```bash
docker compose up -d
docker compose down
```

### Default Ports

| Port | Purpose |
|------|---------|
| **5672** | AMQP — your application connects here |
| **15672** | HTTP Management UI |
| **5671** | AMQP over TLS |
| **15671** | Management UI over TLS |
| **25672** | Erlang distribution (cluster communication) |
| **4369** | EPMD — Erlang Port Mapper Daemon |

### Management UI

Open browser: `http://localhost:15672`  
Default login: `guest` / `guest` (localhost only) or your configured user.

The UI lets you: create queues, view messages, monitor consumers, publish test messages, view bindings, check memory/disk usage.

### User & Permission Management

```bash
# Add new user
rabbitmqctl add_user myapp mysecretpassword

# Set role (administrator, monitoring, management, policymaker, or none)
rabbitmqctl set_user_tags myapp administrator

# Create a virtual host
rabbitmqctl add_vhost /production

# Grant permissions (configure, write, read) using regex
rabbitmqctl set_permissions -p /production myapp ".*" ".*" ".*"

# List users
rabbitmqctl list_users

# List vhosts
rabbitmqctl list_vhosts

# Delete user
rabbitmqctl delete_user olduser

# Change password
rabbitmqctl change_password myapp newpassword
```

---

## 4. First RabbitMQ Application (Node.js)

### Install the Client Library

```bash
npm init -y
npm install amqplib
```

### Simple Producer → Queue → Consumer

#### producer.js

```javascript
const amqp = require('amqplib');

async function producer() {
  // Step 1: Connect to RabbitMQ
  const connection = await amqp.connect('amqp://admin:password@localhost:5672');
  
  // Step 2: Create a channel (lightweight connection)
  const channel = await connection.createChannel();
  
  // Step 3: Declare the queue
  // This is idempotent — safe to call multiple times
  const queue = 'hello';
  await channel.assertQueue(queue, {
    durable: false,  // Queue survives RabbitMQ restart? No (for now)
  });
  
  // Step 4: Publish a message
  const message = 'Hello, RabbitMQ!';
  channel.sendToQueue(queue, Buffer.from(message));
  console.log(`[x] Sent: ${message}`);
  
  // Step 5: Close connection (after short delay to flush)
  setTimeout(() => {
    connection.close();
    process.exit(0);
  }, 500);
}

producer().catch(console.error);
```

#### consumer.js

```javascript
const amqp = require('amqplib');

async function consumer() {
  // Step 1: Connect
  const connection = await amqp.connect('amqp://admin:password@localhost:5672');
  
  // Step 2: Create channel
  const channel = await connection.createChannel();
  
  // Step 3: Assert queue (same declaration as producer)
  const queue = 'hello';
  await channel.assertQueue(queue, {
    durable: false,
  });
  
  console.log(`[*] Waiting for messages in ${queue}. CTRL+C to exit`);
  
  // Step 4: Consume messages
  channel.consume(queue, (msg) => {
    if (msg !== null) {
      const content = msg.content.toString();
      console.log(`[x] Received: ${content}`);
      
      // Step 5: Acknowledge the message
      // Tells RabbitMQ "I processed this, you can remove it"
      channel.ack(msg);
    }
  });
}

consumer().catch(console.error);
```

```bash
# Terminal 1: Start consumer first
node consumer.js

# Terminal 2: Send messages
node producer.js
```

### Understanding Each Step

```
amqp.connect()
  └─► Establishes TCP connection to RabbitMQ (expensive, do once)

connection.createChannel()
  └─► Creates a lightweight virtual channel (cheap, can have many)

channel.assertQueue(name, options)
  └─► Creates queue if not exists, or verifies existing queue matches options
  └─► If queue exists with DIFFERENT options → error (conflict)
  └─► Always safe to call — idempotent

channel.sendToQueue(queue, Buffer.from(message))
  └─► Publishes to the default exchange with queue name as routing key
  └─► Message must be a Buffer (binary)

channel.consume(queue, callback)
  └─► Registers your function to be called for each message
  └─► Non-blocking — continues executing after this line
  └─► callback receives msg object with:
        msg.content   → Buffer with message bytes
        msg.fields    → routing key, exchange, etc.
        msg.properties → content-type, headers, correlation-id, etc.

channel.ack(msg)
  └─► Tells RabbitMQ "message successfully processed"
  └─► RabbitMQ removes message from queue
  └─► If you never call ack, message stays in "unacked" state
```

---

## 5. Core RabbitMQ Concepts

### 5.1 Connection

A **Connection** is a long-lived TCP connection to the RabbitMQ broker.

```javascript
const connection = await amqp.connect('amqp://user:pass@host:port/vhost');
```

**Connection Lifecycle:**
```
App Start
   │
   ▼
amqp.connect() ──TCP Handshake──► RabbitMQ
   │
   │ (one connection per application process)
   │
   ▼
Use connection to create channels
   │
   ▼
App Shutdown → connection.close()
```

**Connection String Format:**
```
amqp://username:password@hostname:port/vhost

Examples:
amqp://admin:secret@localhost:5672/            ← default vhost
amqp://admin:secret@localhost:5672/production  ← custom vhost
amqps://admin:secret@rabbitmq.myapp.com:5671/  ← TLS
```

**Connection Failure & Reconnection:**

```javascript
const amqp = require('amqplib');

class RabbitMQConnection {
  constructor(url) {
    this.url = url;
    this.connection = null;
    this.reconnectDelay = 5000;
  }

  async connect() {
    while (true) {
      try {
        this.connection = await amqp.connect(this.url);
        
        this.connection.on('error', (err) => {
          console.error('Connection error:', err.message);
        });
        
        this.connection.on('close', () => {
          console.log('Connection closed, reconnecting in 5s...');
          setTimeout(() => this.connect(), this.reconnectDelay);
        });
        
        console.log('Connected to RabbitMQ');
        return this.connection;
      } catch (err) {
        console.error(`Connection failed: ${err.message}. Retrying in ${this.reconnectDelay}ms`);
        await new Promise(resolve => setTimeout(resolve, this.reconnectDelay));
      }
    }
  }
}
```

### 5.2 Channel

A **Channel** is a lightweight virtual connection inside a TCP connection. AMQP was designed so that opening/closing a channel is much cheaper than opening/closing a TCP connection.

**Why channels exist:** TCP connections are expensive (handshake, kernel resources). If your app needs 50 concurrent message streams, creating 50 TCP connections would be wasteful. Instead, create 1 TCP connection and 50 channels.

```
TCP Connection
├── Channel 1 → handles "orders" queue
├── Channel 2 → handles "emails" queue
├── Channel 3 → publishes events
└── Channel 4 → consumes notifications
```

**Channel vs Connection:**

| | Connection | Channel |
|-|-----------|---------|
| **Type** | TCP | Virtual (AMQP) |
| **Creation cost** | High | Very low |
| **Recommended per process** | 1 | 1 per thread/coroutine |
| **Shared across threads?** | Yes | **NO — not thread-safe** |
| **Error recovery** | Restart whole connection | Create new channel |

**Key rule:** One channel per concurrent operation. Never share a channel between multiple async operations simultaneously.

```javascript
// ✅ Correct — separate channels for publish and consume
const publishChannel = await connection.createChannel();
const consumeChannel = await connection.createChannel();

// ❌ Wrong — using same channel concurrently (race conditions)
const channel = await connection.createChannel();
Promise.all([
  channel.consume(queue1, handler1),  // concurrent use — risky
  channel.consume(queue2, handler2),
]);
```

### 5.3 Queue

A queue is a FIFO buffer that stores messages until consumers are ready.

**Queue Declaration Options:**

```javascript
await channel.assertQueue('my-queue', {
  durable: true,        // Survive RabbitMQ restart? (default: false)
  exclusive: false,     // Only this connection can use it? (default: false)
  autoDelete: false,    // Delete when last consumer leaves? (default: false)
  arguments: {
    'x-message-ttl': 60000,          // Messages expire after 60s (ms)
    'x-expires': 300000,             // Queue deletes itself after 5min of no use
    'x-max-length': 10000,           // Max 10,000 messages
    'x-max-length-bytes': 10485760,  // Max 10MB
    'x-overflow': 'reject-publish',  // When full: reject new messages (or 'drop-head')
    'x-dead-letter-exchange': 'dlx', // Send dead letters here
    'x-dead-letter-routing-key': 'dead', // With this routing key
    'x-queue-type': 'quorum',        // Use quorum queue (recommended for production)
  }
});
```

**Queue Types:**

| Type | Description | When to Use |
|------|-------------|-------------|
| **Classic** | Default, single node | Development, non-critical |
| **Durable** | Survives restarts | Production standard |
| **Temporary/Exclusive** | Tied to connection, auto-deleted | RPC reply queues |
| **Quorum** | Replicated across nodes | High availability production |
| **Lazy** | Messages stored on disk | Huge backlogs |

### 5.4 Exchange Types

An exchange receives messages from producers and routes them to queues using bindings and routing keys.

#### Direct Exchange

Routes messages to queues whose binding key **exactly matches** the routing key.

```
Producer sends: routingKey = "email"
                    │
              [Direct Exchange]
                    │
        ┌─────binds "email"──────►  Queue: email-queue  →  Email Consumer
        │
        └─────binds "sms"────────►  Queue: sms-queue    →  SMS Consumer
                    │
                (no match)──────►  Message discarded (or to alternate exchange)
```

```javascript
// Setup
await channel.assertExchange('notifications', 'direct', { durable: true });
await channel.assertQueue('email-queue', { durable: true });
await channel.assertQueue('sms-queue', { durable: true });

// Bind: queue ← exchange (with routing key)
await channel.bindQueue('email-queue', 'notifications', 'email');
await channel.bindQueue('sms-queue', 'notifications', 'sms');

// Publish
channel.publish('notifications', 'email', Buffer.from('Welcome!'));  // → email-queue only
channel.publish('notifications', 'sms', Buffer.from('Code: 1234')); // → sms-queue only
```

**Use direct exchange when:** You know exactly which service should handle a message.

#### Fanout Exchange

Ignores routing keys completely. Broadcasts message to **ALL** bound queues.

```
Producer sends: any message
                    │
             [Fanout Exchange]
            /        |         \
     Queue A      Queue B     Queue C
       │             │           │
  Consumer 1    Consumer 2   Consumer 3
  (email)       (SMS)        (analytics)
```

```javascript
await channel.assertExchange('order-events', 'fanout', { durable: true });

// Every bound queue gets EVERY message
await channel.bindQueue('email-queue', 'order-events', '');    // routing key ignored
await channel.bindQueue('sms-queue', 'order-events', '');
await channel.bindQueue('analytics-queue', 'order-events', '');

// Publish (routing key is ignored with fanout)
channel.publish('order-events', '', Buffer.from(JSON.stringify({ orderId: 123 })));
```

**Use fanout when:** You want to broadcast to all listeners (event sourcing style).

#### Topic Exchange

Routes based on **pattern matching** of the routing key using wildcards:
- `*` matches **exactly one word**
- `#` matches **zero or more words**
- Words are separated by dots

```
Routing key pattern: <service>.<entity>.<action>

Examples:
  order.created     → matches "order.*", "order.#", "#.created", "#"
  payment.refunded  → matches "payment.*", "*.refunded", "#"
  user.profile.updated → matches "user.#", "#.updated", but NOT "user.*"
```

```javascript
await channel.assertExchange('app-events', 'topic', { durable: true });

// Bind queues with patterns
await channel.bindQueue('order-all-queue',    'app-events', 'order.#');      // all order events
await channel.bindQueue('payment-queue',      'app-events', 'payment.*');    // payment.success, payment.failed
await channel.bindQueue('critical-queue',     'app-events', '#.failed');     // anything.failed
await channel.bindQueue('everything-queue',   'app-events', '#');            // literally everything

// Publish
channel.publish('app-events', 'order.created',    Buffer.from('...'));  // → order-all-queue, everything-queue
channel.publish('app-events', 'payment.failed',   Buffer.from('...'));  // → payment-queue, critical-queue, everything-queue
channel.publish('app-events', 'order.item.added', Buffer.from('...'));  // → order-all-queue, everything-queue
```

**Use topic exchange when:** You need flexible, pattern-based routing (most common in microservices).

#### Headers Exchange

Routes based on message **headers** instead of routing keys. Rarely used in practice but powerful.

```javascript
await channel.assertExchange('report-exchange', 'headers', { durable: true });

// Bind with header matching rules
await channel.bindQueue('pdf-queue', 'report-exchange', '', {
  'x-match': 'all',   // 'all' = AND logic, 'any' = OR logic
  'format': 'pdf',
  'region': 'EU'
});

// Publish with matching headers
channel.publish('report-exchange', '', Buffer.from('...'), {
  headers: { format: 'pdf', region: 'EU' }  // matches above binding
});
```

#### Default Exchange (Nameless Exchange)

Every queue is automatically bound to the **default exchange** with the queue name as the routing key. When you call `sendToQueue()`, you're actually using the default exchange.

```javascript
// These are equivalent:
channel.sendToQueue('my-queue', Buffer.from('hello'));
channel.publish('', 'my-queue', Buffer.from('hello'));  // '' = default exchange
```

**Exchange Comparison:**

| Exchange Type | Routing Logic | When to Use |
|--------------|---------------|-------------|
| **Direct** | Exact key match | Known destination, one-to-one |
| **Fanout** | Broadcast all | Notify all services of an event |
| **Topic** | Pattern matching | Complex routing, microservices |
| **Headers** | Header attributes | Content-type routing |
| **Default** | Queue name = routing key | Simple, no routing needed |

---

## 6. Routing

### Complete Topic Exchange Routing Example

Imagine an e-commerce platform with these events:

```
order.created
order.updated
order.cancelled
order.item.added
payment.success
payment.failed
payment.refunded
user.registered
user.profile.updated
inventory.low
inventory.restocked
```

Design:

```javascript
const exchange = 'ecommerce.events';
await channel.assertExchange(exchange, 'topic', { durable: true });

// Order service queue — handles all order events
await channel.assertQueue('order-processor', { durable: true });
await channel.bindQueue('order-processor', exchange, 'order.#');

// Payment service — handles payment events  
await channel.assertQueue('payment-processor', { durable: true });
await channel.bindQueue('payment-processor', exchange, 'payment.*');

// Notification service — handles user actions + failed payments
await channel.assertQueue('notification-service', { durable: true });
await channel.bindQueue('notification-service', exchange, 'user.*');
await channel.bindQueue('notification-service', exchange, 'payment.failed');
await channel.bindQueue('notification-service', exchange, 'order.created');

// Audit log — captures EVERYTHING
await channel.assertQueue('audit-log', { durable: true });
await channel.bindQueue('audit-log', exchange, '#');

// Inventory service
await channel.assertQueue('inventory-service', { durable: true });
await channel.bindQueue('inventory-service', exchange, 'inventory.*');
await channel.bindQueue('inventory-service', exchange, 'order.created');
await channel.bindQueue('inventory-service', exchange, 'order.cancelled');
```

**Routing Table:**

| Event | order-processor | payment-processor | notification-service | audit-log | inventory-service |
|-------|:--------------:|:-----------------:|:--------------------:|:---------:|:-----------------:|
| order.created | ✅ | ❌ | ✅ | ✅ | ✅ |
| order.cancelled | ✅ | ❌ | ❌ | ✅ | ✅ |
| order.item.added | ✅ | ❌ | ❌ | ✅ | ❌ |
| payment.success | ❌ | ✅ | ❌ | ✅ | ❌ |
| payment.failed | ❌ | ✅ | ✅ | ✅ | ❌ |
| user.registered | ❌ | ❌ | ✅ | ✅ | ❌ |
| inventory.low | ❌ | ❌ | ❌ | ✅ | ✅ |

---

## 7. Message Acknowledgement

### Why Acknowledgement Exists

Without acks, RabbitMQ has no way of knowing if your consumer processed a message successfully or crashed mid-way. Acks solve: "Did the message get processed, or should I deliver it again?"

### Auto Acknowledgement (autoAck: true)

```javascript
// Dangerous — message deleted the moment it's delivered to consumer
channel.consume(queue, (msg) => {
  processMessage(msg); // if this crashes, message is LOST
}, { noAck: true });
```

**Auto ack** means: the moment RabbitMQ delivers the message to your consumer, it's marked as done — even if your consumer crashes before processing it.

**When to use:** Low-importance data, logs, metrics where losing a message is acceptable.  
**Never use for:** Orders, payments, or anything critical.

### Manual Acknowledgement (autoAck: false — the default)

```javascript
channel.consume(queue, async (msg) => {
  try {
    await processMessage(msg);
    channel.ack(msg);           // ✅ Success: remove from queue
  } catch (err) {
    channel.nack(msg, false, true);  // ❌ Failed: put back in queue
  }
}, { noAck: false }); // noAck: false = manual ack mode
```

### ack vs nack vs reject

```javascript
// ✅ ACK — message processed successfully
channel.ack(msg);
// RabbitMQ: delete message permanently

// ❌ NACK — message failed, I want to (optionally) requeue it
channel.nack(msg, false, true);   // (msg, allUpTo, requeue)
// allUpTo: false = only this message, true = ack all unacked up to this one
// requeue: true = put back in original position in queue
// requeue: false = discard (or send to DLX if configured)

// ❌ REJECT — like nack but for a single message only (simpler)
channel.reject(msg, true);   // (msg, requeue)
// requeue: true = put back in queue
// requeue: false = discard/DLX

// The difference between nack and reject:
// nack(msg, allUpTo, requeue) — can nack multiple messages at once
// reject(msg, requeue)        — always just one message
```

### What Happens in Failure Scenarios

```
Scenario 1: Consumer crashes before ack
──────────────────────────────────────
RabbitMQ delivers msg → Consumer starts processing
                      → Consumer crashes (no ack sent)
                      → RabbitMQ detects connection closed
                      → Message moves from "unacked" back to "ready"
                      → Next available consumer receives it ✅

Scenario 2: Consumer processes but never acks (bug)
────────────────────────────────────────────────────
RabbitMQ delivers msg → Consumer processes (but forgets to ack)
                      → Message stays in "unacked" state
                      → Consumes memory in RabbitMQ
                      → New messages to this consumer limited by prefetch
                      → Eventually: consumer_timeout kicks in (default: 30min)
                      → Message requeued ✅ (but slow)

Scenario 3: nack with requeue:false + DLX configured
──────────────────────────────────────────────────────
Processing fails → channel.nack(msg, false, false)
                → Message removed from original queue
                → Message delivered to Dead Letter Exchange
                → DLX routes to Dead Letter Queue
                → Ops team can inspect/reprocess ✅
```

### Best Practice: Structured Message Handling

```javascript
channel.consume(queue, async (msg) => {
  if (!msg) return; // consumer cancelled

  try {
    const data = JSON.parse(msg.content.toString());
    
    // Validate message format
    if (!data.orderId) {
      console.error('Invalid message format, rejecting without requeue');
      channel.reject(msg, false);  // discard bad format messages
      return;
    }

    // Process
    await processOrder(data);
    channel.ack(msg);
    
  } catch (err) {
    if (isTransientError(err)) {
      // Temporary failure (DB down, network issue) — retry
      channel.nack(msg, false, true);
    } else {
      // Permanent failure (bad data, logic error) — send to DLQ
      channel.nack(msg, false, false);
    }
  }
});
```

---

## 8. Message Durability & Reliability

### The Durability Triangle

```
                    Full Reliability
                    (messages survive restart)
                           ✅
                      requires ALL THREE:
                     /        |        \
           Durable        Persistent    Durable
           Queue          Message       Exchange
```

### Durable Queue

A durable queue is **persisted to disk** — it survives a RabbitMQ server restart.

```javascript
await channel.assertQueue('orders', { durable: true });  // ✅ Survives restart
await channel.assertQueue('temp', { durable: false });   // ❌ Gone on restart
```

### Persistent Message

A persistent message is **written to disk** when enqueued. Non-persistent messages are kept in RAM only.

```javascript
// Persistent message (deliveryMode: 2)
channel.sendToQueue('orders', Buffer.from(JSON.stringify(order)), {
  persistent: true,     // or deliveryMode: 2
  contentType: 'application/json',
  messageId: uuid(),
  timestamp: Date.now(),
});

// Non-persistent (faster, but lost on restart)
channel.sendToQueue('logs', Buffer.from(message), {
  persistent: false,  // or deliveryMode: 1
});
```

### Why Durable Queue ≠ Persistent Message

This is a **critical and commonly misunderstood concept**:

| Queue Durable | Message Persistent | Result on Restart |
|:-------------:|:-----------------:|-------------------|
| ✅ Yes | ✅ Yes | Queue exists, messages survive ✅ |
| ✅ Yes | ❌ No | Queue exists, but messages are LOST ❌ |
| ❌ No | ✅ Yes | Doesn't matter — queue itself is gone |
| ❌ No | ❌ No | Queue and messages both gone |

**Analogy:** A durable queue is like a filing cabinet that isn't destroyed when the office closes. A persistent message is like putting a file in a locked safe inside that cabinet. If you have the cabinet (durable queue) but just leave files on the desk (non-persistent messages), they'll be gone when you come back.

### What Happens on RabbitMQ Restart

```
Scenario A (fully configured):
- Queue: durable: true
- Message: persistent: true
──────────────────────────────
RabbitMQ restarts → Queue reloaded from disk → Messages restored → Consumers reconnect → Processing resumes ✅

Scenario B (queue durable, message not persistent):
- Queue: durable: true
- Message: persistent: false
──────────────────────────────
RabbitMQ restarts → Queue reloaded → Messages GONE (were in RAM) → Queue is empty ❌

Scenario C (queue not durable):
- Queue: durable: false
──────────────────────────────
RabbitMQ restarts → Queue gone → All messages gone ❌
Producers/consumers need to redeclare the queue
```

### Production Durability Setup

```javascript
async function setupDurableInfrastructure(channel) {
  // Durable exchange
  await channel.assertExchange('orders', 'topic', {
    durable: true,  // Exchange survives restart
  });

  // Durable queue
  await channel.assertQueue('order-processor', {
    durable: true,  // Queue survives restart
    arguments: {
      'x-queue-type': 'quorum',  // Replicated across nodes (safest)
    }
  });

  await channel.bindQueue('order-processor', 'orders', 'order.#');
}

async function publishDurably(channel, event, data) {
  const message = Buffer.from(JSON.stringify(data));
  
  channel.publish('orders', event, message, {
    persistent: true,           // Message written to disk
    contentType: 'application/json',
    messageId: `${Date.now()}-${Math.random()}`,
    timestamp: Math.floor(Date.now() / 1000),
  });
}
```

### Durable Exchange

```javascript
// Exchange also needs to be durable:
await channel.assertExchange('my-exchange', 'direct', {
  durable: true,    // Exchange definition survives restart
  autoDelete: false // Don't delete when unused
});
```

---

## 9. Prefetch & Consumer Control

### What is Prefetch?

By default, RabbitMQ sends all ready messages to a consumer as fast as possible (push model). If your consumer is slow, it might receive 10,000 messages in its buffer while it's still processing message #1. **Prefetch** (Quality of Service) limits how many unacknowledged messages a consumer can hold at once.

### Without Prefetch (Unfair Dispatch)

```
Queue: [1][2][3][4][5][6][7][8][9][10]

Consumer A (fast)  ──── gets [1][2][3][4][5]   (all delivered at once)
Consumer B (slow)  ──── gets [6][7][8][9][10]  (all delivered at once)

Consumer A finishes quickly and sits IDLE
Consumer B struggles with [6] for 10 minutes, [7]-[10] waiting in B's buffer
Result: UNFAIR and INEFFICIENT ❌
```

### With Prefetch (Fair Dispatch)

```javascript
// Only allow 1 unacked message per consumer
await channel.prefetch(1);

// Consumer A and B now compete fairly:
Queue: [1][2][3][4][5][6][7][8][9][10]

Consumer A (fast)  gets [1] → acks → gets [3] → acks → gets [5] → ...
Consumer B (slow)  gets [2] → ...  (busy for 10 min)

Result: Fast consumers do more work, slow ones don't get buried ✅
```

### Setting Prefetch

```javascript
// prefetch(count, global)
// count: max unacked messages
// global: false = per consumer (default), true = per channel (all consumers)

// Most common — per consumer limit
await channel.prefetch(10);   // Consumer can have max 10 unacked messages

// Per channel (all consumers on this channel share the limit)
await channel.prefetch(100, true);
```

### Choosing Prefetch Count

| Processing Type | Recommended Prefetch | Reason |
|----------------|---------------------|--------|
| **CPU-intensive (image processing)** | 1-2 | Avoid overwhelming CPU |
| **I/O-intensive (DB writes)** | 10-50 | Multiple concurrent I/O operations |
| **Fast, lightweight processing** | 100-500 | Higher throughput |
| **Batch processing** | Batch size | Process in chunks |
| **Unknown/default** | 10 | Safe starting point |

```javascript
// Example: I/O intensive consumer with moderate parallelism
await channel.prefetch(10);

channel.consume('order-queue', async (msg) => {
  try {
    const order = JSON.parse(msg.content.toString());
    await saveOrderToDatabase(order);  // I/O operation
    await sendOrderConfirmationEmail(order);  // Another I/O operation
    channel.ack(msg);
  } catch (err) {
    channel.nack(msg, false, true);
  }
});
```

---

## 10. Publisher Confirms

### What are Publisher Confirms?

Publisher confirms answer the question: **"Did RabbitMQ actually receive and persist my message?"**

By default, `channel.publish()` or `channel.sendToQueue()` returns immediately without waiting for RabbitMQ to confirm it received the message. If the network drops after you call publish but before RabbitMQ receives it — you've lost a message.

Publisher confirms provide **at-least-once delivery** guarantees from the publisher side.

### Publisher Confirm vs Consumer Acknowledgement

| | Publisher Confirm | Consumer Acknowledgement |
|-|------------------|-------------------------|
| **Direction** | Producer → RabbitMQ | Consumer → RabbitMQ |
| **Confirms** | "Did RabbitMQ receive my message?" | "Did I process the message?" |
| **When to use** | Always in production | Always in production |
| **Without it** | Possible message loss at send | Possible message loss at process |

### Implementing Publisher Confirms

```javascript
const amqp = require('amqplib');

async function reliablePublisher() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createConfirmChannel(); // ← Use confirmChannel
  
  await channel.assertQueue('orders', { durable: true });
  
  return {
    async publish(queue, data) {
      return new Promise((resolve, reject) => {
        const success = channel.sendToQueue(
          queue,
          Buffer.from(JSON.stringify(data)),
          { persistent: true },
          (err, ok) => {
            // This callback fires when RabbitMQ confirms (or rejects)
            if (err) {
              console.error('Message NACKED by broker:', err);
              reject(err);
            } else {
              console.log('Message confirmed by broker ✅');
              resolve(ok);
            }
          }
        );
        
        if (!success) {
          // Channel's write buffer is full — need to wait for 'drain'
          reject(new Error('Channel write buffer full'));
        }
      });
    }
  };
}

// Usage
const publisher = await reliablePublisher();
try {
  await publisher.publish('orders', { orderId: 123, amount: 99.99 });
  console.log('Order queued reliably');
} catch (err) {
  console.error('Failed to queue order:', err);
  // Retry or persist to database for later
}
```

### Batch Publisher Confirms

```javascript
async function batchPublish(channel, messages) {
  // Enable confirm mode
  await channel.waitForConfirms(); // Wait for all pending confirms
  
  const promises = messages.map(msg => 
    new Promise((resolve, reject) => {
      channel.sendToQueue(
        'orders',
        Buffer.from(JSON.stringify(msg)),
        { persistent: true },
        (err, ok) => err ? reject(err) : resolve(ok)
      );
    })
  );
  
  await Promise.all(promises);
  console.log(`All ${messages.length} messages confirmed`);
}
```

---

## 11. Consumer Reliability

### Designing Reliable Consumers

```javascript
const amqp = require('amqplib');

class ReliableConsumer {
  constructor(queueName, handler) {
    this.queueName = queueName;
    this.handler = handler;
    this.connection = null;
    this.channel = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
  }

  async start() {
    await this.connect();
  }

  async connect() {
    try {
      this.connection = await amqp.connect(process.env.RABBITMQ_URL);
      
      this.connection.on('error', (err) => {
        console.error('Connection error:', err.message);
        this.scheduleReconnect();
      });
      
      this.connection.on('close', () => {
        console.log('Connection closed');
        this.scheduleReconnect();
      });
      
      await this.createChannel();
      this.reconnectAttempts = 0;
      
    } catch (err) {
      console.error('Connect failed:', err.message);
      this.scheduleReconnect();
    }
  }

  async createChannel() {
    this.channel = await this.connection.createChannel();
    
    this.channel.on('error', (err) => {
      console.error('Channel error:', err.message);
      // Channel errors usually require new channel, not new connection
    });
    
    this.channel.on('close', () => {
      console.log('Channel closed');
    });
    
    // Set QoS
    await this.channel.prefetch(10);
    
    await this.channel.assertQueue(this.queueName, { durable: true });
    
    this.channel.consume(this.queueName, async (msg) => {
      if (!msg) return; // consumer cancelled
      await this.handleMessage(msg);
    });
    
    console.log(`Consuming from ${this.queueName}`);
  }

  async handleMessage(msg) {
    try {
      const data = JSON.parse(msg.content.toString());
      await this.handler(data);
      this.channel.ack(msg);
    } catch (err) {
      console.error('Processing error:', err);
      
      // Check retry count from headers
      const retryCount = (msg.properties.headers?.['x-retry-count'] || 0);
      
      if (retryCount < 3) {
        // Requeue for retry (will go to retry queue if DLX configured)
        this.channel.nack(msg, false, false);
      } else {
        // Max retries reached — dead letter
        this.channel.nack(msg, false, false);
        console.error('Message sent to DLQ after max retries');
      }
    }
  }

  scheduleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error('Max reconnect attempts reached. Exiting.');
      process.exit(1);
    }
    
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
    this.reconnectAttempts++;
    
    console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
    setTimeout(() => this.connect(), delay);
  }
}

// Usage
const consumer = new ReliableConsumer('orders', async (order) => {
  await processOrder(order);
});
consumer.start();
```

### Idempotent Consumers — Handling Duplicates

RabbitMQ can deliver a message more than once (network issues, consumer restart). Your consumers MUST be idempotent (processing the same message twice produces the same result).

```javascript
// ❌ Not idempotent — double-charging!
async function processPayment(payment) {
  await chargeCard(payment.cardId, payment.amount);
  await db.create({ type: 'payment', ...payment });
}

// ✅ Idempotent — check first
async function processPayment(payment) {
  // Use messageId or a business key for deduplication
  const existing = await db.findOne({
    where: { paymentId: payment.paymentId }
  });
  
  if (existing) {
    console.log(`Payment ${payment.paymentId} already processed, skipping`);
    return; // Still ack the message — we "processed" it (by skipping)
  }
  
  // Use a database transaction with unique constraint
  await db.transaction(async (trx) => {
    await chargeCard(payment.cardId, payment.amount);
    await db.create({ paymentId: payment.paymentId, ...payment }, { transaction: trx });
  });
}
```

**Idempotency strategies:**
1. **Database unique constraint** on a business ID (paymentId, orderId)
2. **Redis SET NX** — store processed message IDs with expiry
3. **Conditional updates** — `UPDATE SET processed=true WHERE processed=false`
4. **Event sourcing** — append-only log, replay is safe

---

## 12. Retry Mechanisms

### Strategy 1: Immediate Requeue (Not Recommended)

```javascript
channel.nack(msg, false, true); // requeue immediately
// Problem: Creates a HOT LOOP if processing always fails
// msg bounces between consumer and queue at maximum speed
// CPU maxes out, real messages can't get through
```

**Never use immediate requeue in a loop.** It's the RabbitMQ equivalent of busy-waiting.

### Strategy 2: TTL-Based Delayed Retry (Recommended)

Architecture:
```
Main Queue ──fail──► Dead Letter Exchange ──► Retry Queue (TTL: 30s)
     ▲                                              │
     └─────────────── (after 30s, DLX routes) ──────┘
```

```javascript
async function setupRetryArchitecture(channel) {
  // Main exchange
  await channel.assertExchange('orders', 'direct', { durable: true });
  
  // Main queue — send failures to retry exchange
  await channel.assertQueue('orders.queue', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': 'orders.retry',  // Where to send dead letters
      'x-dead-letter-routing-key': 'retry',
    }
  });
  await channel.bindQueue('orders.queue', 'orders', 'process');

  // Retry exchange
  await channel.assertExchange('orders.retry', 'direct', { durable: true });
  
  // Retry queue — holds messages for 30 seconds, then sends back to main
  await channel.assertQueue('orders.retry.queue', {
    durable: true,
    arguments: {
      'x-message-ttl': 30000,                  // Messages expire after 30s
      'x-dead-letter-exchange': 'orders',       // After TTL, back to main!
      'x-dead-letter-routing-key': 'process',  // With the original routing key
    }
  });
  await channel.bindQueue('orders.retry.queue', 'orders.retry', 'retry');
  
  // DLQ for messages that exhausted retries
  await channel.assertExchange('orders.dlx', 'direct', { durable: true });
  await channel.assertQueue('orders.dlq', { durable: true });
  await channel.bindQueue('orders.dlq', 'orders.dlx', 'dead');
}
```

### Strategy 3: Exponential Backoff with Multiple Retry Queues

```
Retry 1: wait 10s  (retry.10s queue)
Retry 2: wait 30s  (retry.30s queue)
Retry 3: wait 60s  (retry.60s queue)
Retry 4: wait 300s (retry.5m queue)
Retry 5+: → DLQ
```

```javascript
async function setupExponentialRetry(channel) {
  const retryDelays = [10000, 30000, 60000, 300000]; // ms
  
  // Create retry queues with different TTLs
  for (let i = 0; i < retryDelays.length; i++) {
    await channel.assertQueue(`orders.retry.${retryDelays[i]}ms`, {
      durable: true,
      arguments: {
        'x-message-ttl': retryDelays[i],
        'x-dead-letter-exchange': 'orders',
        'x-dead-letter-routing-key': 'process',
      }
    });
  }
}

// Consumer with retry logic
channel.consume('orders.queue', async (msg) => {
  try {
    await processOrder(JSON.parse(msg.content.toString()));
    channel.ack(msg);
  } catch (err) {
    const retryCount = (msg.properties.headers?.['x-retry-count'] || 0) + 1;
    const delays = [10000, 30000, 60000, 300000];
    
    if (retryCount <= delays.length) {
      // Republish with incremented retry count to appropriate retry queue
      channel.publish('', `orders.retry.${delays[retryCount - 1]}ms`, 
        msg.content, {
          ...msg.properties,
          headers: {
            ...msg.properties.headers,
            'x-retry-count': retryCount,
            'x-original-error': err.message,
            'x-first-failed-at': msg.properties.headers?.['x-first-failed-at'] || Date.now(),
          }
        }
      );
      channel.ack(msg); // Ack original, we republished it
    } else {
      // Max retries exceeded — dead letter
      channel.publish('orders.dlx', 'dead', msg.content, {
        ...msg.properties,
        headers: {
          ...msg.properties.headers,
          'x-final-error': err.message,
          'x-died-at': Date.now(),
        }
      });
      channel.ack(msg);
    }
  }
});
```

---

## 13. Dead Letter Exchange (DLX)

### What is a Dead Letter?

A message becomes a **dead letter** when:
1. It's **rejected** (`nack` or `reject`) with `requeue: false`
2. Its **TTL expires** (message too old)
3. The **queue is full** (x-max-length exceeded with x-overflow: drop-head)

### What is DLX?

A Dead Letter Exchange (DLX) is a regular exchange designated to receive dead letters. When a message dies, RabbitMQ routes it to the DLX instead of discarding it.

```
┌─────────────┐   reject/expire   ┌─────────┐   route   ┌─────────┐
│ Main Queue  │──────────────────►│   DLX   │──────────►│   DLQ   │
└─────────────┘                   └─────────┘           └─────────┘
                                                              │
                                                    Ops team inspects
                                                    and reprocesses
```

### Complete DLX Setup in Node.js

```javascript
const amqp = require('amqplib');

async function setupDLXInfrastructure() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  // ─────────────────────────────────────────────────
  // 1. Setup DLX and DLQ
  // ─────────────────────────────────────────────────
  await channel.assertExchange('orders.dlx', 'direct', { durable: true });
  
  await channel.assertQueue('orders.dlq', {
    durable: true,
    // DLQ itself might have a DLX for archiving very old dead letters
  });
  
  await channel.bindQueue('orders.dlq', 'orders.dlx', 'dead-order');

  // ─────────────────────────────────────────────────
  // 2. Setup Main Queue with DLX configured
  // ─────────────────────────────────────────────────
  await channel.assertExchange('orders', 'direct', { durable: true });
  
  await channel.assertQueue('orders.queue', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': 'orders.dlx',         // Where dead messages go
      'x-dead-letter-routing-key': 'dead-order',       // With this routing key
      'x-message-ttl': 3600000,                        // Messages expire after 1 hour
    }
  });
  
  await channel.bindQueue('orders.queue', 'orders', 'create');

  console.log('DLX infrastructure ready ✅');
  return { connection, channel };
}

// ─────────────────────────────────────────────────
// DLQ Consumer — monitors and processes dead letters
// ─────────────────────────────────────────────────
async function startDLQMonitor(channel) {
  channel.consume('orders.dlq', async (msg) => {
    if (!msg) return;
    
    const data = JSON.parse(msg.content.toString());
    const headers = msg.properties.headers || {};
    
    // x-death header added by RabbitMQ — tells you why/when it died
    const deathInfo = headers['x-death']?.[0] || {};
    
    console.error('Dead letter received:', {
      orderId: data.orderId,
      reason: deathInfo.reason,        // 'rejected', 'expired', 'maxlen'
      count: deathInfo.count,          // How many times it died
      time: deathInfo.time,            // When it first died
      queue: deathInfo.queue,          // Which queue it died in
      exchange: deathInfo.exchange,    // Which exchange
      routingKeys: deathInfo['routing-keys'],
    });
    
    // Options:
    // 1. Alert on-call engineer
    // 2. Store in database for manual review
    // 3. Attempt manual reprocessing after investigation
    // 4. Log metrics for monitoring dashboards
    
    await saveToDeadLetterDatabase(data, deathInfo);
    await alertOpsTeam(data, deathInfo);
    
    channel.ack(msg);  // Always ack DLQ messages (prevents infinite loop)
  });
}
```

### Retry + DLQ Complete Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │             RABBITMQ INFRASTRUCTURE          │
                    │                                              │
Producer            │   orders exchange                           │
   │                │       │ route: "create"                     │
   │ publish ──────►│───────┤                                      │
                    │       ▼                                      │
                    │   ┌──────────────────────────────────┐      │
                    │   │  orders.queue (durable)           │      │
                    │   │  DLX: orders.retry               │      │
                    │   └──────────────┬───────────────────┘      │
                    │                  │                           │
                    │           [Consumer processes]               │
                    │                  │                           │
                    │          success │ fail (nack, requeue:false) │
                    │                  │                           │
                    │                  ▼ (dead-lettered)           │
                    │   ┌──────────────────────────────────┐      │
                    │   │  orders.retry.queue (TTL: 30s)   │      │
                    │   │  DLX: orders (back to main!)     │      │
                    │   └──────────────┬───────────────────┘      │
                    │                  │ (after 30s TTL)           │
                    │                  │ max retry header check    │
                    │                  ├───────────────────────────┤
                    │                  │ retry < 3   │ retry >= 3  │
                    │                  ▼             ▼             │
                    │           [back to main]  orders.dlq         │
                    │                               │              │
                    │                        DLQ Consumer          │
                    │                        (alert + store)       │
                    └─────────────────────────────────────────────┘
```

---

## 14. Error Handling

### Error Categories

```
RabbitMQ Errors
├── Connection Errors       (network, broker down)
├── Channel Errors          (bad operation, permission)
├── Consumer Errors         (your processing code failed)
├── Publisher Errors        (message rejected, buffer full)
└── Protocol Errors         (message format, version)
```

### Production Error Handling Template

```javascript
const amqp = require('amqplib');

class RabbitMQService {
  constructor(config) {
    this.config = config;
    this.connection = null;
    this.publishChannel = null;
    this.consumeChannel = null;
  }

  async initialize() {
    this.connection = await amqp.connect(this.config.url);
    
    this.connection.on('error', (err) => {
      // Connection-level errors (network drops, broker crash)
      this.handleConnectionError(err);
    });

    this.connection.on('blocked', (reason) => {
      // Broker is under memory/disk pressure — stop publishing
      console.warn('RabbitMQ blocked:', reason);
      this.isBlocked = true;
    });

    this.connection.on('unblocked', () => {
      console.log('RabbitMQ unblocked, resuming');
      this.isBlocked = false;
    });

    // Separate channels for publishing and consuming
    this.publishChannel = await this.connection.createConfirmChannel();
    this.consumeChannel = await this.connection.createChannel();
    
    this.publishChannel.on('error', (err) => {
      console.error('Publish channel error:', err);
      // Recreate publish channel
    });
    
    this.publishChannel.on('return', (msg) => {
      // Message returned because no queue matched (if mandatory: true)
      console.error('Message returned (no route):', msg.fields.routingKey);
    });
  }

  async publish(exchange, routingKey, data, options = {}) {
    if (this.isBlocked) {
      throw new Error('RabbitMQ broker is blocked, cannot publish');
    }

    // Parse/validate data
    let payload;
    try {
      payload = JSON.stringify(data);
    } catch (err) {
      throw new Error(`Cannot serialize message: ${err.message}`);
    }

    return new Promise((resolve, reject) => {
      const sent = this.publishChannel.publish(
        exchange,
        routingKey,
        Buffer.from(payload),
        {
          persistent: true,
          contentType: 'application/json',
          messageId: options.messageId || `${Date.now()}`,
          timestamp: Math.floor(Date.now() / 1000),
          mandatory: options.mandatory || false, // Return if unroutable
          ...options,
        },
        (err, ok) => {
          if (err) reject(new Error(`Broker nacked message: ${err.message}`));
          else resolve(ok);
        }
      );

      if (!sent) {
        reject(new Error('Write buffer full, apply back pressure'));
      }
    });
  }

  async consume(queue, handler, options = {}) {
    await this.consumeChannel.prefetch(options.prefetch || 10);

    this.consumeChannel.consume(queue, async (msg) => {
      if (!msg) {
        // Consumer was cancelled (queue deleted, etc.)
        console.warn(`Consumer for ${queue} cancelled`);
        return;
      }

      let data;
      try {
        data = JSON.parse(msg.content.toString());
      } catch (err) {
        // Invalid JSON — can never be processed, reject without requeue
        console.error('Invalid message format, sending to DLQ:', err);
        this.consumeChannel.reject(msg, false);
        return;
      }

      try {
        await handler(data, msg);
        this.consumeChannel.ack(msg);
      } catch (err) {
        console.error(`Handler error for ${queue}:`, err);
        
        if (this.isTransientError(err)) {
          // Temporary failure — requeue (will go to retry queue via DLX)
          this.consumeChannel.nack(msg, false, false);
        } else {
          // Permanent failure — dead letter without requeue
          this.consumeChannel.nack(msg, false, false);
        }
      }
    });
  }

  isTransientError(err) {
    // Network errors, timeouts, DB connection errors are transient
    const transientMessages = [
      'ECONNREFUSED', 'ETIMEDOUT', 'ECONNRESET',
      'Connection terminated', 'deadlock'
    ];
    return transientMessages.some(msg => err.message.includes(msg));
  }

  handleConnectionError(err) {
    console.error('Connection error:', err.message);
    // Reconnection logic...
  }

  async shutdown() {
    console.log('Gracefully shutting down...');
    try {
      await this.consumeChannel?.close();
      await this.publishChannel?.close();
      await this.connection?.close();
    } catch (err) {
      console.error('Shutdown error:', err.message);
    }
  }
}

// Graceful shutdown handlers
process.on('SIGTERM', async () => {
  await rabbitService.shutdown();
  process.exit(0);
});

process.on('SIGINT', async () => {
  await rabbitService.shutdown();
  process.exit(0);
});
```

---

## 15. Message Ordering

### Does RabbitMQ Guarantee Ordering?

**Short answer:** Yes, within a single queue with a single consumer.  
**Practical answer:** In many real setups, ordering breaks.

### When Ordering is Preserved ✅

```
Producer → Queue → Single Consumer (prefetch: 1)
Messages arrive in FIFO order and are processed in order.
```

### When Ordering Breaks ❌

**Scenario 1: Multiple Consumers**
```
Queue: [1][2][3][4][5]
Consumer A: receives [1][3][5] — processes fast
Consumer B: receives [2][4]   — processes slow

Actual processing order: 1, 3, 5, 2, 4 ← NOT IN ORDER
```

**Scenario 2: Prefetch > 1 with Retries**
```
Consumer receives: [1][2][3]
Processes [2] first (fast), acks
[1] fails, nacked (goes back to queue)
Now queue: [3][1] ← order disrupted
```

**Scenario 3: Priority Queues**
Higher priority messages jump the queue.

### Designing for Ordered Processing

**Pattern 1: Partition by entity ID**
```javascript
// All events for the same order go to the same queue
const queueIndex = orderId % numQueues; // hash-based partition
channel.sendToQueue(`orders.partition.${queueIndex}`, message);
// Each partition has ONE consumer → order guaranteed per partition
```

**Pattern 2: Sequence numbers + reorder buffer**
```javascript
// Each message has a sequence number
const message = { seq: 42, data: {...} };
// Consumer buffers out-of-order messages and processes in seq order
```

**Pattern 3: Accept eventual ordering**
In most real systems, strict ordering isn't required. Design your data model to handle out-of-order updates:
```javascript
// Instead of: "set price to $10", then "set price to $20"
// Use:        "set price to $20 if version < 5" (optimistic locking)
```

---

## 16. Scaling RabbitMQ

### Competing Consumers (Horizontal Scaling)

The simplest scaling pattern: add more consumers reading from the same queue.

```
               ┌──────────────────────────┐
               │         Queue            │
               │  [1][2][3][4][5][6]...   │
               └────┬──────┬──────┬───────┘
                    │      │      │
              Consumer1 Consumer2 Consumer3
              (Node 1)  (Node 2)  (Node 3)
```

```javascript
// Scale by running more instances of this consumer script
// No code changes needed — RabbitMQ distributes work automatically

async function startConsumer(instanceId) {
  const connection = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await connection.createChannel();
  
  await channel.prefetch(10);  // Each consumer handles 10 at a time
  
  channel.consume('orders', async (msg) => {
    console.log(`Instance ${instanceId} processing: ${msg.content}`);
    await processOrder(JSON.parse(msg.content.toString()));
    channel.ack(msg);
  });
  
  console.log(`Consumer instance ${instanceId} started`);
}

startConsumer(process.env.INSTANCE_ID || 1);
```

```bash
# Scale with Docker
docker run -d -e INSTANCE_ID=1 order-consumer
docker run -d -e INSTANCE_ID=2 order-consumer
docker run -d -e INSTANCE_ID=3 order-consumer

# Or with Docker Compose
docker compose up --scale order-consumer=5
```

### Multi-Queue Architecture

```
10 Producers         RabbitMQ               20 Consumers
──────────           ────────               ────────────
Order Svc ──┐        ┌──orders.queue──┐──── Consumer 1-5
Order Svc ──┤        │                │
Order Svc ──┤──────►Exchange──payment.queue──── Consumer 6-10
Order Svc ──┤        │                │
  ...       │        │──email.queue───┤──── Consumer 11-15
Order Svc ──┘        └──sms.queue─────┘──── Consumer 16-20
```

### Queue Sharding (Plugin)

```javascript
// RabbitMQ Sharding plugin creates N queue copies automatically
// Each consumer connects to one shard — truly parallel

// Policy setup (via management API or CLI):
// rabbitmqctl set_policy sharding "^orders" '{"shards-per-node": 4}'
// This creates 4 queues per node, distributes consumers automatically
```

### Performance Benchmarks

| Setup | Throughput |
|-------|-----------|
| 1 producer, 1 consumer, no persistence | ~50,000 msg/s |
| 1 producer, 1 consumer, persistent | ~20,000 msg/s |
| 3 producers, 10 consumers, persistent | ~100,000 msg/s |
| Quorum queues (replicated) | ~15,000–30,000 msg/s |

---

## 17. High Availability & Clustering

### RabbitMQ Cluster Architecture

A cluster is multiple RabbitMQ nodes that share metadata (exchanges, queues, bindings) but NOT queue contents by default.

```
┌──────────────────────────────────────────────────────────┐
│                     RabbitMQ Cluster                      │
│                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │   Node 1     │  │   Node 2     │  │   Node 3     │   │
│  │ (disk node)  │  │ (disk node)  │  │ (disk node)  │   │
│  │              │◄─►              │◄─►              │   │
│  │ Erlang DIST  │  │ Erlang DIST  │  │ Erlang DIST  │   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
│         │                 │                 │             │
└─────────┼─────────────────┼─────────────────┼─────────────┘
          │                 │                 │
    Load Balancer (HAProxy / nginx)
          │
    Your Application
```

### Classic vs Quorum Queues

| Feature | Classic Queue | Quorum Queue |
|---------|--------------|-------------|
| **Replication** | Optional (mirrored — deprecated) | Built-in, majority-based |
| **Availability** | Depends on single node | Survives minority node failure |
| **Data safety** | Can lose messages on failure | Strong guarantee |
| **Performance** | Higher throughput | Slightly lower, but consistent |
| **Message TTL** | Supported | Supported |
| **Priority** | Supported | Not supported |
| **Poison handling** | Manual | Built-in delivery limit |
| **Recommended for** | Development/simple cases | **All production use** |

### Quorum Queues

Quorum queues use the **Raft consensus algorithm**. Messages are replicated to a majority (quorum) of nodes before being confirmed.

```
              Quorum of 3 nodes (need 2/3 to agree)
              
Producer ──publish──► Node 1 (Leader)
                           │
                    ┌──────┴──────┐
                    │             │
                 Node 2        Node 3
               (Follower)    (Follower)
               
Message confirmed only when majority (2/3) acknowledge it.
If Node 2 crashes: Node 1 + Node 3 = majority → still works ✅
If Node 1 crashes: New leader elected from Node 2/3 → still works ✅
If 2 nodes crash: No quorum → queue unavailable ❌
```

```javascript
// Create a quorum queue
await channel.assertQueue('critical-orders', {
  durable: true,
  arguments: {
    'x-queue-type': 'quorum',           // Enable quorum queue
    'x-quorum-initial-group-size': 3,   // Start with 3 replicas
    'x-delivery-limit': 5,             // Auto DLQ after 5 delivery attempts
  }
});
```

### Cluster Setup with Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  rabbitmq1:
    image: rabbitmq:3-management
    hostname: rabbitmq1
    environment:
      RABBITMQ_ERLANG_COOKIE: "SECRETCOOKIE"
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: password
      RABBITMQ_NODENAME: rabbit@rabbitmq1
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - rabbitmq1_data:/var/lib/rabbitmq

  rabbitmq2:
    image: rabbitmq:3-management
    hostname: rabbitmq2
    environment:
      RABBITMQ_ERLANG_COOKIE: "SECRETCOOKIE"
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: password
      RABBITMQ_NODENAME: rabbit@rabbitmq2
    ports:
      - "5673:5672"
      - "15673:15672"
    volumes:
      - rabbitmq2_data:/var/lib/rabbitmq

  rabbitmq3:
    image: rabbitmq:3-management
    hostname: rabbitmq3
    environment:
      RABBITMQ_ERLANG_COOKIE: "SECRETCOOKIE"
      RABBITMQ_NODENAME: rabbit@rabbitmq3
    ports:
      - "5674:5672"
    volumes:
      - rabbitmq3_data:/var/lib/rabbitmq

volumes:
  rabbitmq1_data:
  rabbitmq2_data:
  rabbitmq3_data:
```

```bash
# After starting, join nodes into a cluster:
docker exec rabbitmq2 rabbitmqctl stop_app
docker exec rabbitmq2 rabbitmqctl join_cluster rabbit@rabbitmq1
docker exec rabbitmq2 rabbitmqctl start_app

docker exec rabbitmq3 rabbitmqctl stop_app
docker exec rabbitmq3 rabbitmqctl join_cluster rabbit@rabbitmq1
docker exec rabbitmq3 rabbitmqctl start_app

# Verify cluster
docker exec rabbitmq1 rabbitmqctl cluster_status
```

---

## 18. Security

### Users, Permissions & Virtual Hosts

```bash
# Create a dedicated app user (never use guest in production)
rabbitmqctl add_user orderapp StrongPass@123

# Set role
rabbitmqctl set_user_tags orderapp management  # read-only management UI

# Create virtual host per environment
rabbitmqctl add_vhost /production
rabbitmqctl add_vhost /staging

# Set permissions: (configure, write, read) using regex
# ".*" means all resources
rabbitmqctl set_permissions -p /production orderapp "^orderapp\." "^orderapp\." "^orderapp\."
# ^ orderapp\. means: only resources starting with "orderapp."
# This prevents the app from touching other teams' queues

# List permissions
rabbitmqctl list_permissions -p /production

# Disable guest user (it can only connect from localhost anyway, but best practice)
rabbitmqctl delete_user guest
```

### Permission Model

```
                     configure  write   read
                     ─────────  ─────   ────
Exchange declare:       ✅        ❌      ❌
Exchange delete:        ✅        ❌      ❌
Queue declare:          ✅        ❌      ❌
Queue delete:           ✅        ❌      ❌
Bind exchange/queue:    ✅        ✅      ✅
Publish message:        ❌        ✅      ❌
Consume message:        ❌        ❌      ✅
```

### TLS/SSL Configuration

```javascript
// Connect with TLS
const amqps = require('amqplib');
const fs = require('fs');

const connection = await amqp.connect({
  protocol: 'amqps',
  hostname: 'rabbitmq.production.com',
  port: 5671,
  username: 'orderapp',
  password: 'StrongPass@123',
  vhost: '/production',
  tls: {
    cert: fs.readFileSync('./client-cert.pem'),
    key: fs.readFileSync('./client-key.pem'),
    ca: [fs.readFileSync('./ca-cert.pem')],
    rejectUnauthorized: true,  // Verify broker certificate
  }
});
```

### Production Security Checklist

```
✅ Delete or disable the default 'guest' user
✅ Use strong passwords (generated, not guessable)
✅ Create one user per application/service
✅ Apply least-privilege permissions (regex restrict to own resources)
✅ Use separate virtual hosts per environment
✅ Enable TLS for all connections (no plaintext in production)
✅ Keep RabbitMQ management port (15672) behind VPN/firewall
✅ Use certificate-based authentication for critical services
✅ Rotate credentials regularly
✅ Never embed credentials in code — use environment variables/secrets manager
✅ Monitor for unusual connection patterns (many connections from unexpected IPs)
```

---

## 19. Monitoring & Management

### Management UI Key Screens

**Queues Tab:**
- **Ready** — messages waiting to be delivered to consumers
- **Unacked** — messages delivered but not yet acknowledged
- **Total** — Ready + Unacked

If **Unacked is growing** → consumers are too slow or not acking
If **Ready is growing** → not enough consumers

### Important Metrics & What They Mean

| Metric | Normal | Warning | Action |
|--------|--------|---------|--------|
| **Queue depth (Ready)** | 0–1000 | > 10,000 | Add consumers |
| **Unacked messages** | < prefetch × consumers | Growing | Consumer bug? Too slow? |
| **Publish rate** | Matches consume rate | Pub >> Consume | Add consumers |
| **Memory** | < 60% | > 80% | Increase RAM or reduce messages |
| **Disk free** | > 50GB | < 5GB | RabbitMQ will alarm and block |
| **Consumer count** | > 0 for all queues | 0 (orphaned queue) | Start consumers |
| **Connections** | Stable | Rapidly cycling | App reconnection bug |
| **Channels per connection** | < 100 | > 1000 | Connection/channel leak |

### Key RabbitMQ CLI Commands

```bash
# ─── Queue Operations ───────────────────────────────
rabbitmqctl list_queues name messages consumers
rabbitmqctl list_queues name messages_ready messages_unacknowledged
rabbitmqctl purge_queue my-queue          # Delete all messages in queue
rabbitmqctl delete_queue my-queue         # Delete queue itself

# ─── Connection/Channel Info ─────────────────────────
rabbitmqctl list_connections name peer_host state
rabbitmqctl list_channels name number messages_unacknowledged

# ─── Consumer Info ───────────────────────────────────
rabbitmqctl list_consumers

# ─── Exchange & Binding Info ──────────────────────────
rabbitmqctl list_exchanges name type durable
rabbitmqctl list_bindings

# ─── Node Health ─────────────────────────────────────
rabbitmq-diagnostics status
rabbitmq-diagnostics check_running
rabbitmq-diagnostics check_local_alarms   # Memory/disk alarms

# ─── Cluster ──────────────────────────────────────────
rabbitmqctl cluster_status
rabbitmqctl node_health_check
```

### Management HTTP API (for Dashboards)

```bash
# Base URL: http://localhost:15672/api/

# List all queues
curl -u admin:password http://localhost:15672/api/queues

# Get specific queue metrics
curl -u admin:password http://localhost:15672/api/queues/%2F/orders

# List connections
curl -u admin:password http://localhost:15672/api/connections

# Publish a test message via API
curl -u admin:password -X POST http://localhost:15672/api/exchanges/%2F/orders/publish \
  -H "Content-Type: application/json" \
  -d '{"properties":{},"routing_key":"test","payload":"hello","payload_encoding":"string"}'
```

### Monitoring with Prometheus & Grafana

```bash
# Enable Prometheus plugin
rabbitmq-plugins enable rabbitmq_prometheus

# Scrape metrics
curl http://localhost:15692/metrics

# Key Prometheus metrics:
# rabbitmq_queue_messages{queue="orders"}          — total messages
# rabbitmq_queue_messages_ready{queue="orders"}    — ready to consume
# rabbitmq_queue_messages_unacked{queue="orders"}  — unacknowledged
# rabbitmq_connections                             — total connections
# rabbitmq_queue_consumers{queue="orders"}         — consumer count
```

---

## 20. RabbitMQ with Microservices

### Why RabbitMQ in Microservices?

Direct HTTP between services creates:
- **Temporal coupling** — Service B must be up when A sends a request
- **Spatial coupling** — A must know B's address
- **Behavioral coupling** — A waits for B to respond

RabbitMQ removes all three:
```
Without RabbitMQ:                   With RabbitMQ:
A ──HTTP──► B ──HTTP──► C          A ──msg──► Queue ──msg──► B
  (B down = A fails)                           ↕ Queue ──msg──► C
  (latency adds up)                  (B can be down, messages wait)
  (tight coupling)                   (A doesn't know about B)
```

### Architecture: Order Processing System

```
                      ┌────────────────────────────────┐
                      │        Order Service            │
                      │  POST /orders → create order   │
                      └───────────────┬────────────────┘
                                      │ publish to exchange
                                      │ routing key: order.created
                                      ▼
                          ┌───────────────────┐
                          │  orders exchange  │
                          │  (type: topic)    │
                          └──┬────────┬───────┘
                             │        │
               binding:      │        │ binding:
               order.created │        │ order.*
                             ▼        ▼
                    ┌─────────────┐  ┌─────────────────┐
                    │  payment    │  │   inventory     │
                    │   queue     │  │     queue       │
                    └──────┬──────┘  └────────┬────────┘
                           │                  │
                    ┌──────┴──────┐   ┌───────┴───────┐
                    │  Payment   │   │  Inventory    │
                    │  Service   │   │   Service     │
                    └──────┬──────┘   └───────┬───────┘
                           │ (publishes       │ (publishes
                           │ payment.success) │ inv.reserved)
                           ▼                  ▼
                    ┌─────────────────────────────────┐
                    │         Notification Service     │
                    │  (listening to #.success, etc.) │
                    └─────────────────────────────────┘
```

#### Order Service (Producer)

```javascript
// order-service/publisher.js
const amqp = require('amqplib');

class OrderPublisher {
  async connect() {
    this.connection = await amqp.connect(process.env.RABBITMQ_URL);
    this.channel = await this.connection.createConfirmChannel();
    
    await this.channel.assertExchange('orders', 'topic', { durable: true });
  }

  async publishOrderCreated(order) {
    const message = {
      eventType: 'order.created',
      orderId: order.id,
      userId: order.userId,
      items: order.items,
      totalAmount: order.totalAmount,
      currency: order.currency,
      timestamp: new Date().toISOString(),
    };

    return new Promise((resolve, reject) => {
      this.channel.publish(
        'orders',
        'order.created',
        Buffer.from(JSON.stringify(message)),
        {
          persistent: true,
          messageId: `order-${order.id}-created`,
          contentType: 'application/json',
        },
        (err) => err ? reject(err) : resolve()
      );
    });
  }
}
```

#### Payment Service (Consumer + Producer)

```javascript
// payment-service/processor.js
async function startPaymentProcessor() {
  const connection = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await connection.createChannel();
  
  // Consume order events
  await channel.assertQueue('payment.queue', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': 'payments.dlx',
    }
  });
  await channel.bindQueue('payment.queue', 'orders', 'order.created');
  
  await channel.prefetch(5);  // Process 5 payments concurrently
  
  channel.consume('payment.queue', async (msg) => {
    const order = JSON.parse(msg.content.toString());
    
    try {
      // Process payment
      const result = await chargeCustomer(order.userId, order.totalAmount);
      
      // Publish payment result event
      await channel.publish(
        'orders',
        result.success ? 'payment.success' : 'payment.failed',
        Buffer.from(JSON.stringify({
          orderId: order.orderId,
          paymentId: result.paymentId,
          amount: order.totalAmount,
          status: result.success ? 'success' : 'failed',
        })),
        { persistent: true }
      );
      
      channel.ack(msg);
    } catch (err) {
      channel.nack(msg, false, false);  // Send to DLX
    }
  });
}
```

### Architecture: Email Queue

```javascript
// Separate concerns: App publishes, Email Worker consumes

// Application (any service that needs emails)
async function sendWelcomeEmail(userId, email) {
  await emailChannel.publish('notifications', 'email.welcome', Buffer.from(JSON.stringify({
    to: email,
    template: 'welcome',
    data: { userId }
  })), { persistent: true });
}

// Email Worker (dedicated process/service)
channel.consume('email.queue', async (msg) => {
  const { to, template, data } = JSON.parse(msg.content.toString());
  await sendViaMailgun(to, template, data);
  channel.ack(msg);
}, { noAck: false });
```

---

## 21. Real-World Use Cases

### 1. Order Processing

**Problem:** Checkout must complete instantly, but many things must happen after.  
**Solution:** Create order → publish event → async processing.

```
User clicks "Buy" → Order saved to DB → publish "order.created"
                                              ↓ (async)
                              ┌──────────────────────────┐
                              │ Payment Service          │
                              │ Inventory Service        │
                              │ Notification Service     │
                              │ Loyalty Points Service   │
                              │ Analytics Service        │
                              └──────────────────────────┘
```

### 2. Email/SMS Notification Queue

**Problem:** Sending 100,000 emails synchronously would take hours and block your app.  
**Solution:** Queue them, worker sends at a controlled rate.

```
App → RabbitMQ → Email Worker (limited concurrency) → Email Provider (SES/Mailgun)
                  └── Rate limit: 100 emails/second max
```

### 3. Image/Video Processing

**Problem:** Uploading a video triggers transcoding (minutes of work).  
**Solution:** Upload succeeds immediately, transcoding queued.

```
API: POST /videos → save file → publish "video.uploaded" → return 202 Accepted
                                       ↓
                           Transcoding Worker (FFmpeg)
                                       ↓
                           publish "video.transcoded"
                                       ↓
                           Notification → User
```

### 4. Webhook Processing

**Problem:** Your service receives 10,000 webhooks/minute from payment provider, processing takes 200ms each.  
**Solution:** Receive webhook → queue it → process asynchronously.

```
Stripe Webhook → Your API → Queue → Worker → DB Update → Response sent to user
(fast response to Stripe)    (async processing)
```

### 5. Report Generation

**Problem:** Generating a 50,000-row Excel report takes 30 seconds.  
**Solution:** Queue report request, notify user when done.

```
User: "Generate sales report"
API → queue "report.requested" → return "Report queued, check email in 5 min"
         ↓
  Report Worker → generate file → upload to S3 → email user link
```

### 6. Background Jobs

```javascript
// Job queue pattern — generic background processing
async function scheduleJob(jobType, data, options = {}) {
  await channel.publish('jobs', jobType, Buffer.from(JSON.stringify({
    jobId: uuid(),
    type: jobType,
    data,
    scheduledAt: new Date().toISOString(),
    priority: options.priority || 5,
  })), {
    persistent: true,
    headers: {
      'x-delay': options.delayMs || 0,  // Delayed message plugin
    }
  });
}

// Usage
await scheduleJob('send-email', { to: 'user@example.com', template: 'welcome' });
await scheduleJob('cleanup-expired-sessions', {}, { delayMs: 3600000 }); // 1 hour
await scheduleJob('generate-report', { userId: 123 }, { priority: 8 });
```

### 7. Microservice Communication (Event Bus)

```javascript
// Services communicate via events, not direct calls
// Pattern: each service publishes what happened, doesn't know who cares

// User Service publishes:
publish('user.registered', { userId, email, plan });

// These services react independently:
// Email Service → sends welcome email
// Analytics Service → records new user
// Billing Service → sets up subscription
// Onboarding Service → creates tutorial data
// Admin Service → updates dashboard counts
```

### 8. Data Sync / ETL Pipelines

```
Source System ──change event──► RabbitMQ ──► ETL Worker ──► Data Warehouse
(Postgres change data capture)              (transform)     (BigQuery/Redshift)
```

---

## 22. When Should I Use RabbitMQ?

### ✅ Good Fit

| Scenario | Why RabbitMQ Fits |
|----------|-----------------|
| **Background email/SMS sending** | Decouples send from app, handles spikes |
| **Order processing pipeline** | Multiple services react independently |
| **Task/job queue** | Distribute work across workers |
| **Retry with backoff** | Built-in DLX/TTL mechanism |
| **Complex message routing** | Topic/direct exchanges cover most cases |
| **Request/Reply (RPC)** | Supported via correlation ID + reply queue |
| **Microservice decoupling** | Services don't need to know each other |
| **Rate limiting a slow downstream** | Queue absorbs spikes |
| **Asynchronous workflows** | Fire events, let services react |

### ✅ Decision Checklist

Answer YES to these → Use RabbitMQ:
- [ ] Can this work happen after the HTTP response?
- [ ] Would I benefit from adding more workers to speed this up?
- [ ] Should this work survive if the processing service restarts?
- [ ] Do multiple services need to react to one event?
- [ ] Is retry logic needed?

---

## 23. When Should I NOT Use RabbitMQ?

### ❌ Poor Fit

| Scenario | Use Instead | Why |
|----------|-------------|-----|
| **Very high throughput (>500k/s)** | Kafka | RabbitMQ maxes out; Kafka is designed for this |
| **Event replay / audit log** | Kafka | RabbitMQ deletes messages after consumption |
| **Long-term event retention** | Kafka | RabbitMQ is not a log store |
| **Analytics / data pipelines** | Kafka + Flink/Spark | Kafka integrates with the data ecosystem |
| **Real-time user queries** | Direct HTTP/WebSocket | Don't add async latency for user-facing reads |
| **Simple cron jobs** | Node-cron, Celery beat | Overkill for scheduled tasks |
| **Simple pub/sub to browser clients** | WebSocket/SSE | RabbitMQ is server-to-server |
| **Session/cache storage** | Redis | RabbitMQ is not a key-value store |

### The Kafka Rule of Thumb

Use Kafka if you answer YES to any:
- "I need to replay events from yesterday"
- "Multiple independent teams need to consume the same stream"
- "I'm building a data pipeline, ETL, or streaming analytics"
- "I need to process > 1 million messages per second"
- "Messages must be retained for 30+ days regardless of consumption"

---

## 24. RabbitMQ Design Patterns

### Pattern 1: Work Queue (Task Queue)

Distribute time-consuming tasks among multiple workers.

```javascript
// Publisher
channel.sendToQueue('tasks', Buffer.from(JSON.stringify(task)), {
  persistent: true  // Tasks survive restart
});

// Multiple workers (run N instances)
await channel.prefetch(1);  // One task at a time per worker
channel.consume('tasks', async (msg) => {
  await doHeavyWork(JSON.parse(msg.content.toString()));
  channel.ack(msg);
});
```

### Pattern 2: Publish/Subscribe (Fanout)

Broadcast every message to all subscribers.

```javascript
// Publisher
await channel.assertExchange('updates', 'fanout', { durable: false });
channel.publish('updates', '', Buffer.from(message));

// Each subscriber has their own queue
// (new queue per consumer = independent consumption)
const { queue } = await channel.assertQueue('', { exclusive: true }); // temp, auto-named
await channel.bindQueue(queue, 'updates', '');
channel.consume(queue, handler);
```

### Pattern 3: Request/Reply (RPC)

Synchronous-style interaction over asynchronous messaging.

```javascript
// RPC Client
async function rpcCall(procedure, params) {
  const correlationId = uuid();
  const replyQueue = await channel.assertQueue('', { exclusive: true });
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('RPC timeout')), 10000);
    
    channel.consume(replyQueue.queue, (msg) => {
      if (msg.properties.correlationId === correlationId) {
        clearTimeout(timeout);
        resolve(JSON.parse(msg.content.toString()));
        channel.cancel(msg.fields.consumerTag);
      }
    }, { noAck: true });
    
    channel.sendToQueue('rpc_queue', Buffer.from(JSON.stringify(params)), {
      correlationId,
      replyTo: replyQueue.queue,
    });
  });
}

// RPC Server
channel.consume('rpc_queue', async (msg) => {
  const params = JSON.parse(msg.content.toString());
  const result = await processRPCRequest(params);
  
  channel.sendToQueue(
    msg.properties.replyTo,
    Buffer.from(JSON.stringify(result)),
    { correlationId: msg.properties.correlationId }
  );
  channel.ack(msg);
});
```

### Pattern 4: Competing Consumers

Same queue, multiple consumers — whoever is free takes the next message.

```javascript
// No code changes needed! Just run multiple consumer processes.
// RabbitMQ distributes messages automatically.
// Key: set prefetch(1) for fair distribution.

// Consumer (run multiple instances):
await channel.prefetch(1);  // Critical for fair distribution
channel.consume('jobs', handler);
```

### Pattern 5: Delayed Processing

Execute tasks after a delay without external schedulers.

```javascript
// Method 1: TTL + DLX trick
await channel.assertQueue('delayed.30s', {
  durable: true,
  arguments: {
    'x-message-ttl': 30000,          // Wait 30 seconds
    'x-dead-letter-exchange': 'tasks', // Then route to actual processor
    'x-dead-letter-routing-key': 'process',
  }
});

// Send to delay queue
channel.sendToQueue('delayed.30s', Buffer.from(JSON.stringify(task)));
// After 30s, message automatically appears in the tasks exchange

// Method 2: RabbitMQ Delayed Message Plugin
// rabbitmq-plugins enable rabbitmq_delayed_message_exchange
await channel.assertExchange('delayed', 'x-delayed-message', {
  durable: true,
  arguments: { 'x-delayed-type': 'direct' }
});

channel.publish('delayed', 'process', Buffer.from(JSON.stringify(task)), {
  headers: { 'x-delay': 60000 }  // Delay 60 seconds
});
```

### Pattern 6: Dead Letter Pattern

Handle permanently failed messages gracefully.

```javascript
// Main queue with DLX
await channel.assertQueue('orders', {
  durable: true,
  arguments: { 'x-dead-letter-exchange': 'orders.dlx' }
});

// Dead letter queue
await channel.assertExchange('orders.dlx', 'fanout', { durable: true });
await channel.assertQueue('orders.dlq', { durable: true });
await channel.bindQueue('orders.dlq', 'orders.dlx', '');

// DLQ monitor
channel.consume('orders.dlq', (msg) => {
  const data = JSON.parse(msg.content.toString());
  const death = msg.properties.headers['x-death'][0];
  
  alertOps({ data, reason: death.reason, count: death.count });
  saveToIncidentDatabase(data, death);
  channel.ack(msg);
});
```

---

## 25. Advanced RabbitMQ Concepts

### 🔴 Quorum Queues
Already covered in §17. Key points: use `x-queue-type: quorum` for all production queues. They use Raft consensus, survive minority node failures, and have built-in delivery limits.

### 🔴 Flow Control & Back Pressure

When producers publish faster than consumers process, RabbitMQ may run out of memory. Flow control slows down producers automatically.

```
Normal:         Producer ──▶ Exchange ──▶ Queue ──▶ Consumer
                (fast)                             (normal)

Under pressure: Producer ──X── [BLOCKED] ──▶ Queue ──▶ Consumer
                (throttled by RabbitMQ)

Connection receives "blocked" notification:
connection.on('blocked', (reason) => {
  // Stop publishing! RabbitMQ is under pressure
  this.shouldPublish = false;
});

connection.on('unblocked', () => {
  this.shouldPublish = true;
});
```

**Memory alarm threshold (default: 40% of RAM):**
```bash
# rabbitmq.conf
vm_memory_high_watermark.relative = 0.4
disk_free_limit.relative = 1.0   # 1x total disk space
```

### 🔴 Lazy Queues

Normal queues keep messages in RAM (fast). Lazy queues write messages to disk immediately (slower but handles massive backlogs).

```javascript
await channel.assertQueue('huge-backlog-queue', {
  durable: true,
  arguments: {
    'x-queue-mode': 'lazy',  // Write to disk immediately
  }
});

// Or set via policy (recommended):
// rabbitmqctl set_policy lazy "^lazy\." '{"queue-mode":"lazy"}' --apply-to queues
```

**When to use lazy queues:** When you expect large backlogs (millions of messages), consumer is consistently slower than producer.

### 🔴 Priority Queues

```javascript
await channel.assertQueue('priority-tasks', {
  durable: true,
  arguments: {
    'x-max-priority': 10,  // Priority range: 0 (lowest) to 10 (highest)
  }
});

// Publish with priority
channel.sendToQueue('priority-tasks', Buffer.from(data), {
  priority: 8,  // High priority — processed before priority 5, 3, etc.
});
```

**Important:** Priority queues add CPU overhead. Use only when you genuinely need it.

### 🔴 Single Active Consumer (SAC)

Guarantees only ONE consumer is active at a time (even if multiple are connected). Maintains ordering.

```javascript
await channel.assertQueue('ordered-processing', {
  durable: true,
  arguments: {
    'x-single-active-consumer': true,
    // If the active consumer fails, the next one automatically becomes active
  }
});
```

### 🔴 Alternate Exchange

Catches messages that don't match any binding (normally silently dropped).

```javascript
// Main exchange with alternate
await channel.assertExchange('main', 'direct', {
  durable: true,
  arguments: { 'alternate-exchange': 'unrouted' }
});

// Catch-all exchange
await channel.assertExchange('unrouted', 'fanout', { durable: true });
await channel.assertQueue('unrouted-messages', { durable: true });
await channel.bindQueue('unrouted-messages', 'unrouted', '');
// All unrouted messages land in 'unrouted-messages' for inspection
```

### 🟡 Consumer Timeout

If a consumer holds an unacknowledged message for too long, RabbitMQ forcibly closes the channel.

```bash
# rabbitmq.conf
consumer_timeout = 1800000  # 30 minutes (default)
```

```javascript
// If your processing takes > 30 min, heartbeat the message:
// Approach: break long jobs into smaller chunks, or use heartbeat pattern
```

### 🔴 Heartbeats

TCP connections can silently die (firewall idle timeout, network issue). Heartbeats detect dead connections.

```javascript
const connection = await amqp.connect({
  hostname: 'localhost',
  heartbeat: 60,  // Send heartbeat every 60 seconds
  // If no heartbeat received in 2x this, connection is declared dead
});
```

**Recommended:** Always set heartbeat to 60 seconds in production.

### 🔴 RabbitMQ Streams

A newer feature (RabbitMQ 3.9+) that provides Kafka-like functionality within RabbitMQ:
- Persistent, append-only log of messages
- Consumer can read from any offset (replay!)
- Much higher throughput than queues

```bash
rabbitmq-plugins enable rabbitmq_stream
# Use rabbitmq-stream-js client instead of amqplib
```

Use streams when: You need replay within RabbitMQ without migrating to Kafka.

### 🟡 Message Properties Reference

```javascript
channel.sendToQueue(queue, buffer, {
  contentType: 'application/json',    // MIME type
  contentEncoding: 'utf-8',          // Encoding
  headers: {                          // Custom headers (any key-value)
    'x-retry-count': 0,
    'source-service': 'order-service',
  },
  deliveryMode: 2,                    // 1 = non-persistent, 2 = persistent
  priority: 5,                        // 0-9, for priority queues
  correlationId: 'abc-123',          // Link response to request (RPC)
  replyTo: 'reply-queue-name',       // Where to send replies (RPC)
  expiration: '60000',               // Message TTL in ms (as string!)
  messageId: 'unique-message-id',    // Unique ID for deduplication
  timestamp: Math.floor(Date.now() / 1000), // Unix timestamp
  type: 'order.created',             // Message type descriptor
  userId: 'publisher-user',          // User who published
  appId: 'order-service',            // Application that published
});
```

---

## 26. Production Best Practices

### Infrastructure

```
✅ Always use Quorum Queues for important data (x-queue-type: quorum)
✅ Set up a 3-node cluster minimum for HA
✅ Use a load balancer in front of RabbitMQ nodes
✅ Configure memory and disk alarms appropriately
✅ Set heartbeat: 60 for all connections
✅ Enable Prometheus metrics plugin
✅ Set up alerts on queue depth, unacked count, memory, disk
```

### Naming Conventions

```
Exchanges:  <service>.<type>          → orders.events, payments.commands
Queues:     <service>.<action>        → payment.process, email.send
Routing:    <domain>.<entity>.<verb>  → order.payment.created
DLQ:        <queue-name>.dlq          → payment.process.dlq
Retry:      <queue-name>.retry        → payment.process.retry
```

### Connection & Channel Best Practices

```javascript
// ✅ One connection per process
const connection = await amqp.connect(url);

// ✅ Separate channels for publishing and consuming
const publishChannel = await connection.createConfirmChannel();
const consumeChannel = await connection.createChannel();

// ✅ Set prefetch on consume channel
await consumeChannel.prefetch(10);

// ✅ Handle connection/channel events
connection.on('error', handleConnectionError);
connection.on('close', handleConnectionClose);

// ❌ Don't create new connections for every publish
// ❌ Don't share channels between concurrent async operations
// ❌ Don't forget to set prefetch (defaults to unlimited)
```

### Message Design

```javascript
// ✅ Good message structure
const message = {
  // Identity
  messageId: uuid(),
  correlationId: requestId,  // Trace across services
  
  // Context
  eventType: 'order.created',
  version: '1.0',            // Schema version — allows evolution
  source: 'order-service',
  
  // Timing
  timestamp: new Date().toISOString(),
  
  // Payload
  data: {
    orderId: '123',
    // ... domain fields
  }
};

// ✅ Keep messages small (< 1MB ideally)
// ✅ For large payloads: store in S3/DB, publish reference
const message = {
  eventType: 'report.generated',
  reportId: '456',
  s3Key: 'reports/2024/report-456.pdf',  // Reference, not the file
};

// ✅ Always include messageId for deduplication
// ✅ Always use JSON with contentType: 'application/json'
// ✅ Version your message schemas
```

### Reliability Checklist

```
Publisher Side:
✅ Use createConfirmChannel() (publisher confirms)
✅ Handle confirm callbacks (resolve/reject promise)
✅ Set persistent: true for important messages
✅ Have a retry/fallback for publish failures
✅ Handle 'blocked' and 'unblocked' events

Consumer Side:
✅ Always use manual ack (noAck: false)
✅ Set appropriate prefetch count
✅ Handle null messages (consumer cancelled)
✅ Implement idempotent processing
✅ Distinguish transient vs permanent errors
✅ Configure DLX for failed messages

Queue Setup:
✅ durable: true for all production queues
✅ x-queue-type: quorum for critical queues
✅ x-dead-letter-exchange configured
✅ x-message-ttl to prevent stale messages
✅ x-max-length to prevent unbounded growth
```

### Graceful Shutdown

```javascript
async function gracefulShutdown() {
  console.log('Received shutdown signal');
  
  // 1. Stop accepting new messages
  await consumeChannel.cancel(consumerTag);  // Stop consuming
  
  // 2. Wait for in-flight messages to finish (with timeout)
  const shutdownTimeout = 30000; // 30s max
  await Promise.race([
    waitForInflightMessages(),
    new Promise(resolve => setTimeout(resolve, shutdownTimeout))
  ]);
  
  // 3. Close channels
  await consumeChannel.close();
  await publishChannel.close();
  
  // 4. Close connection
  await connection.close();
  
  console.log('Clean shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);
```

---

## 27. Common Mistakes

### Mistake 1: Not Setting Prefetch

```javascript
// ❌ Wrong — RabbitMQ dumps ALL messages into consumer buffer
channel.consume('tasks', handler);

// ✅ Correct — limit in-flight messages
await channel.prefetch(10);
channel.consume('tasks', handler);
```
**Problem caused:** Consumer receives 50,000 messages it can't process, appears to "hold" them, other consumers starve.

### Mistake 2: Using Auto-Ack for Critical Work

```javascript
// ❌ Wrong — if consumer crashes, message is lost
channel.consume('orders', processOrder, { noAck: true });

// ✅ Correct — ack only after successful processing
channel.consume('orders', async (msg) => {
  await processOrder(msg);
  channel.ack(msg);   // Only here, after success
}, { noAck: false });
```

### Mistake 3: Queue Name Collision with Different Options

```javascript
// Service A declares:
channel.assertQueue('tasks', { durable: true });

// Service B declares (different options):
channel.assertQueue('tasks', { durable: false });  // 💥 Error: PRECONDITION_FAILED

// ✅ Always agree on queue options across all services
// Or use assertQueue defensively — declare with the same options everywhere
```

### Mistake 4: Creating New Connections Per Request

```javascript
// ❌ Wrong — TCP connection creation is expensive
app.post('/orders', async (req, res) => {
  const connection = await amqp.connect(url);  // NEW CONNECTION EVERY REQUEST!
  const channel = await connection.createChannel();
  channel.sendToQueue('orders', Buffer.from(JSON.stringify(req.body)));
  await connection.close();
  res.sendStatus(200);
});

// ✅ Correct — reuse connection and channel
const { channel } = await setupRabbitMQ(); // Once at startup
app.post('/orders', async (req, res) => {
  channel.sendToQueue('orders', Buffer.from(JSON.stringify(req.body)));
  res.sendStatus(200);
});
```

### Mistake 5: Infinite Retry Loop

```javascript
// ❌ Wrong — immediate requeue on failure = infinite CPU spin
channel.consume('tasks', async (msg) => {
  try {
    await process(msg);
    channel.ack(msg);
  } catch (err) {
    channel.nack(msg, false, true);  // IMMEDIATELY requeued → infinite loop if always fails
  }
});

// ✅ Correct — use DLX + TTL based retry with counter
channel.consume('tasks', async (msg) => {
  try {
    await process(msg);
    channel.ack(msg);
  } catch (err) {
    const retryCount = (msg.properties.headers?.['x-retry-count'] || 0);
    if (retryCount < 3) {
      // nack to retry queue via DLX
      channel.nack(msg, false, false); // Goes to retry.queue via DLX
    } else {
      channel.nack(msg, false, false); // Goes to DLQ via DLX
    }
  }
});
```

### Mistake 6: Not Handling Channel Errors

```javascript
// ❌ Wrong — channel silently dies on error, consumer stops
const channel = await connection.createChannel();

// ✅ Correct — listen for channel errors
channel.on('error', (err) => {
  console.error('Channel error:', err);
  // Recreate the channel
  createChannel();
});

channel.on('close', () => {
  // Channel closed — reconnect if needed
});
```

### Mistake 7: Not Using Durable Queues + Persistent Messages Together

```javascript
// ❌ Partial durability — queue survives but messages don't
channel.assertQueue('orders', { durable: true });
channel.sendToQueue('orders', Buffer.from(data));  // No persistent: true!
// RabbitMQ restart → queue exists but messages are gone!

// ✅ Full durability — both required
channel.assertQueue('orders', { durable: true });
channel.sendToQueue('orders', Buffer.from(data), { persistent: true });
```

### Mistake 8: Publishing from Consumer Context

```javascript
// ❌ Risky — using consume channel to also publish
channel.consume('orders', async (msg) => {
  await process(msg);
  channel.publish('notifications', 'email', ...);  // Same channel!
  channel.ack(msg);
  // If publish fails and throws, ack is never called
});

// ✅ Correct — separate channels for consuming and publishing
const consumeChannel = await connection.createChannel();
const publishChannel = await connection.createConfirmChannel();

consumeChannel.consume('orders', async (msg) => {
  await process(msg);
  await publishChannel.publish('notifications', 'email', ...);  // Separate channel
  consumeChannel.ack(msg);
});
```

### Mistake 9: Ignoring Message Return

```javascript
// ❌ Wrong — if no queue matches, message silently disappears
channel.publish('my-exchange', 'no-match-key', Buffer.from(data));

// ✅ Correct — use mandatory: true and handle returns
channel.on('return', (msg) => {
  console.error('Message returned (no route):', msg.fields.routingKey);
  // Save to DB, alert, retry with correct routing key
});

channel.publish('my-exchange', 'key', Buffer.from(data), { mandatory: true });
```

---

## 28. Complete Practical Project

### E-Commerce Order Processing System

#### Project Structure

```
rabbitmq-ecommerce/
├── docker-compose.yml
├── package.json
├── shared/
│   ├── rabbitmq.js        ← Shared RabbitMQ connection manager
│   ├── infrastructure.js  ← Exchange/queue declarations
│   └── constants.js       ← Exchange/queue/routing key names
├── order-service/
│   ├── server.js          ← Express API
│   └── publisher.js       ← Publishes order events
├── payment-service/
│   └── worker.js          ← Processes payments
├── inventory-service/
│   └── worker.js          ← Updates inventory
├── notification-service/
│   └── worker.js          ← Sends emails/SMS
└── dlq-monitor/
    └── monitor.js         ← Watches dead letter queue
```

#### docker-compose.yml

```yaml
version: '3.8'
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports: ["5672:5672", "15672:15672"]
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: password
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 10

  order-service:
    build: ./order-service
    depends_on:
      rabbitmq:
        condition: service_healthy
    ports: ["3000:3000"]
    environment:
      RABBITMQ_URL: amqp://admin:password@rabbitmq:5672

  payment-service:
    build: ./payment-service
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_URL: amqp://admin:password@rabbitmq:5672
    deploy:
      replicas: 2  # 2 payment workers

  inventory-service:
    build: ./inventory-service
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_URL: amqp://admin:password@rabbitmq:5672

  notification-service:
    build: ./notification-service
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_URL: amqp://admin:password@rabbitmq:5672
    deploy:
      replicas: 3  # 3 notification workers

  dlq-monitor:
    build: ./dlq-monitor
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_URL: amqp://admin:password@rabbitmq:5672
```

#### shared/constants.js

```javascript
module.exports = {
  EXCHANGES: {
    ORDERS: 'ecommerce.orders',
    NOTIFICATIONS: 'ecommerce.notifications',
    DLX: 'ecommerce.dlx',
    RETRY: 'ecommerce.retry',
  },
  QUEUES: {
    PAYMENT: 'payment.process',
    INVENTORY: 'inventory.update',
    NOTIFICATION: 'notification.send',
    ORDER_DLQ: 'order.dlq',
    RETRY: 'order.retry',
  },
  ROUTING_KEYS: {
    ORDER_CREATED: 'order.created',
    ORDER_CANCELLED: 'order.cancelled',
    PAYMENT_SUCCESS: 'payment.success',
    PAYMENT_FAILED: 'payment.failed',
    INVENTORY_RESERVED: 'inventory.reserved',
    NOTIFY_EMAIL: 'notification.email',
    DEAD: 'dead',
  }
};
```

#### shared/rabbitmq.js

```javascript
const amqp = require('amqplib');
const { EXCHANGES, QUEUES, ROUTING_KEYS } = require('./constants');

let connection = null;
let publishChannel = null;

async function getConnection() {
  if (connection) return connection;
  
  let retries = 0;
  while (retries < 10) {
    try {
      connection = await amqp.connect(process.env.RABBITMQ_URL);
      connection.on('error', (err) => {
        console.error('Connection error:', err.message);
        connection = null;
      });
      connection.on('close', () => {
        console.log('Connection closed, will reconnect');
        connection = null;
      });
      console.log('Connected to RabbitMQ');
      return connection;
    } catch (err) {
      retries++;
      const delay = Math.min(1000 * retries, 10000);
      console.error(`Connect failed (attempt ${retries}), retrying in ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw new Error('Could not connect to RabbitMQ after 10 attempts');
}

async function getPublishChannel() {
  if (publishChannel) return publishChannel;
  const conn = await getConnection();
  publishChannel = await conn.createConfirmChannel();
  return publishChannel;
}

async function setupInfrastructure() {
  const conn = await getConnection();
  const ch = await conn.createChannel();

  // Exchanges
  await ch.assertExchange(EXCHANGES.ORDERS, 'topic', { durable: true });
  await ch.assertExchange(EXCHANGES.NOTIFICATIONS, 'direct', { durable: true });
  await ch.assertExchange(EXCHANGES.DLX, 'direct', { durable: true });
  await ch.assertExchange(EXCHANGES.RETRY, 'direct', { durable: true });

  // DLQ
  await ch.assertQueue(QUEUES.ORDER_DLQ, { durable: true });
  await ch.bindQueue(QUEUES.ORDER_DLQ, EXCHANGES.DLX, ROUTING_KEYS.DEAD);

  // Retry Queue (30s TTL → back to orders exchange)
  await ch.assertQueue(QUEUES.RETRY, {
    durable: true,
    arguments: {
      'x-message-ttl': 30000,
      'x-dead-letter-exchange': EXCHANGES.ORDERS,
    }
  });
  await ch.bindQueue(QUEUES.RETRY, EXCHANGES.RETRY, 'retry');

  // Payment Queue
  await ch.assertQueue(QUEUES.PAYMENT, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': EXCHANGES.RETRY,
      'x-dead-letter-routing-key': 'retry',
    }
  });
  await ch.bindQueue(QUEUES.PAYMENT, EXCHANGES.ORDERS, ROUTING_KEYS.ORDER_CREATED);

  // Inventory Queue
  await ch.assertQueue(QUEUES.INVENTORY, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': EXCHANGES.DLX,
      'x-dead-letter-routing-key': ROUTING_KEYS.DEAD,
    }
  });
  await ch.bindQueue(QUEUES.INVENTORY, EXCHANGES.ORDERS, ROUTING_KEYS.ORDER_CREATED);
  await ch.bindQueue(QUEUES.INVENTORY, EXCHANGES.ORDERS, ROUTING_KEYS.ORDER_CANCELLED);

  // Notification Queue
  await ch.assertQueue(QUEUES.NOTIFICATION, { durable: true });
  await ch.bindQueue(QUEUES.NOTIFICATION, EXCHANGES.ORDERS, 'payment.*');
  await ch.bindQueue(QUEUES.NOTIFICATION, EXCHANGES.ORDERS, 'order.created');

  await ch.close();
  console.log('RabbitMQ infrastructure ready ✅');
}

module.exports = { getConnection, getPublishChannel, setupInfrastructure };
```

#### order-service/server.js

```javascript
const express = require('express');
const { v4: uuid } = require('uuid');
const { getPublishChannel, setupInfrastructure } = require('../shared/rabbitmq');
const { EXCHANGES, ROUTING_KEYS } = require('../shared/constants');

const app = express();
app.use(express.json());

app.post('/orders', async (req, res) => {
  const order = {
    orderId: uuid(),
    userId: req.body.userId,
    items: req.body.items,
    totalAmount: req.body.totalAmount,
    currency: 'USD',
    status: 'CREATED',
    createdAt: new Date().toISOString(),
  };

  try {
    const channel = await getPublishChannel();
    
    await new Promise((resolve, reject) => {
      channel.publish(
        EXCHANGES.ORDERS,
        ROUTING_KEYS.ORDER_CREATED,
        Buffer.from(JSON.stringify(order)),
        {
          persistent: true,
          messageId: order.orderId,
          contentType: 'application/json',
          headers: { 'x-retry-count': 0 },
        },
        (err) => err ? reject(err) : resolve()
      );
    });

    console.log(`Order ${order.orderId} published`);
    res.status(201).json({ orderId: order.orderId, status: 'QUEUED' });
  } catch (err) {
    console.error('Failed to queue order:', err);
    res.status(500).json({ error: 'Failed to queue order' });
  }
});

async function start() {
  await setupInfrastructure();
  app.listen(3000, () => console.log('Order Service running on port 3000'));
}

start().catch(console.error);
```

#### payment-service/worker.js

```javascript
const { getConnection } = require('../shared/rabbitmq');
const { QUEUES, EXCHANGES, ROUTING_KEYS } = require('../shared/constants');

async function processPayment(order) {
  console.log(`Processing payment for order ${order.orderId}: $${order.totalAmount}`);
  
  // Simulate payment processing (60% success rate for demo)
  await new Promise(r => setTimeout(r, 500 + Math.random() * 1000));
  if (Math.random() < 0.4) throw new Error('Payment gateway timeout');
  
  return { paymentId: `pay_${Date.now()}`, status: 'success' };
}

async function start() {
  const connection = await getConnection();
  const consumeChannel = await connection.createChannel();
  const publishChannel = await connection.createConfirmChannel();
  
  await consumeChannel.prefetch(5);
  
  consumeChannel.consume(QUEUES.PAYMENT, async (msg) => {
    if (!msg) return;
    
    const order = JSON.parse(msg.content.toString());
    const retryCount = msg.properties.headers['x-retry-count'] || 0;
    
    console.log(`Payment worker received order ${order.orderId} (retry: ${retryCount})`);
    
    try {
      const result = await processPayment(order);
      
      // Publish success event
      await new Promise((resolve, reject) => {
        publishChannel.publish(
          EXCHANGES.ORDERS,
          ROUTING_KEYS.PAYMENT_SUCCESS,
          Buffer.from(JSON.stringify({ ...order, payment: result })),
          { persistent: true },
          (err) => err ? reject(err) : resolve()
        );
      });
      
      consumeChannel.ack(msg);
      console.log(`Payment SUCCESS for order ${order.orderId}`);
      
    } catch (err) {
      console.error(`Payment FAILED for order ${order.orderId}:`, err.message);
      
      if (retryCount < 3) {
        // Republish with incremented retry count to retry queue
        publishChannel.publish(
          EXCHANGES.ORDERS,
          ROUTING_KEYS.ORDER_CREATED,
          msg.content,
          {
            ...msg.properties,
            headers: {
              ...msg.properties.headers,
              'x-retry-count': retryCount + 1,
              'x-last-error': err.message,
            }
          }
        );
        consumeChannel.ack(msg); // Ack original, republished
      } else {
        // Max retries → publish failure event, nack to DLQ
        publishChannel.publish(
          EXCHANGES.ORDERS,
          ROUTING_KEYS.PAYMENT_FAILED,
          Buffer.from(JSON.stringify({ ...order, error: err.message })),
          { persistent: true }
        );
        consumeChannel.nack(msg, false, false);  // → DLQ via DLX
        console.error(`Order ${order.orderId} sent to DLQ after 3 retries`);
      }
    }
  });
  
  console.log('Payment worker started');
}

start().catch(console.error);
```

---

## 29. Debugging Guide

### Problem: Messages Not Being Consumed

```bash
# Check consumer count
rabbitmqctl list_queues name consumers messages_ready

# 0 consumers? → Start your consumer service
# > 0 consumers but not consuming? → Check prefetch and unacked count

rabbitmqctl list_queues name messages_ready messages_unacknowledged

# High unacked = consumers received messages but didn't ack
# → Check for processing errors / deadlocks / missing ack calls
```

### Problem: Messages Stuck in Queue

```bash
# Check if consumers are connected
rabbitmqctl list_consumers

# Check if consumer is active
rabbitmqctl list_queues name messages consumers active_consumers

# If active_consumers = 0 but consumers > 0 → prefetch limit hit
# Solution: increase prefetch OR add more consumers

# View messages in queue without consuming (for debugging)
# Via Management UI: Queues → Get Messages
```

### Problem: Messages Being Duplicated

```
Root causes:
1. Consumer processes message but connection dies before ack
   → RabbitMQ redelivers → processed twice

Solution: Implement idempotent consumers
- Use unique messageId + DB constraint
- Check-before-process pattern
```

```javascript
// Deduplication with Redis
const redis = require('redis');
const client = redis.createClient();

channel.consume('orders', async (msg) => {
  const messageId = msg.properties.messageId;
  
  // Check if already processed
  const alreadyProcessed = await client.setNX(`processed:${messageId}`, '1');
  if (!alreadyProcessed) {
    console.log(`Duplicate: ${messageId}, skipping`);
    channel.ack(msg);
    return;
  }
  
  // Set TTL so dedup cache doesn't grow forever
  await client.expire(`processed:${messageId}`, 86400); // 24h
  
  await processOrder(JSON.parse(msg.content.toString()));
  channel.ack(msg);
});
```

### Problem: RabbitMQ Memory Alarm

```bash
# Check memory
rabbitmq-diagnostics status | grep memory

# Check memory alarm
rabbitmq-diagnostics check_local_alarms

# Immediate fixes:
rabbitmqctl purge_queue queue-name    # Delete accumulated messages
rabbitmqctl set_vm_memory_high_watermark 0.7  # Increase threshold temporarily

# Long-term fixes:
# 1. Add more consumers to drain queues
# 2. Increase server RAM
# 3. Use lazy queues for queues with backlogs
# 4. Configure max-length on queues
```

### Problem: Disk Alarm

```bash
# Check disk
rabbitmq-diagnostics status | grep disk

# RabbitMQ blocks all publishers when disk is low!
# Fix:
rabbitmqctl set_disk_free_limit 1GB  # Temporarily lower threshold
# Long term: free disk space, add storage, use lazy queues
```

### Problem: Messages Continuously Retrying

```bash
# Check DLQ depth — if DLQ is ALSO growing, messages loop in retry
rabbitmqctl list_queues name messages | grep dlq

# Check retry count in message headers via Management UI:
# Queues → Get Messages → look at x-retry-count header

# If retry count keeps resetting → retry logic bug
# Check: are you properly incrementing x-retry-count?
```

### Problem: Consumer Too Slow

```bash
# Measure: publish rate vs consume rate in Management UI
# If publish_rate >> consume_rate:
# 1. Increase prefetch count
# 2. Add more consumer instances
# 3. Optimize processing code
# 4. Break up large tasks into smaller sub-tasks

# Add more consumers:
docker compose up --scale payment-worker=10
```

### Problem: Connection Closed / Channel Closed

```bash
# Check RabbitMQ logs
docker logs rabbitmq | grep -E "error|closed|refused"

# Common causes:
# 1. Wrong credentials → authentication_failure
# 2. Permission denied → ACCESS_REFUSED
# 3. Queue doesn't exist with those options → PRECONDITION_FAILED
# 4. Max channels per connection exceeded
# 5. Server shutdown / restart

# Fix PRECONDITION_FAILED (queue option mismatch):
rabbitmqctl delete_queue old-queue  # Delete old queue
# Then restart with correct options
```

### Debugging Checklist

```
[ ] Check queue consumer count: rabbitmqctl list_queues name consumers
[ ] Check unacked count: rabbitmqctl list_queues name messages_unacknowledged  
[ ] Check RabbitMQ logs: docker logs rabbitmq
[ ] Check memory alarms: rabbitmq-diagnostics check_local_alarms
[ ] Verify bindings: rabbitmqctl list_bindings
[ ] Test message flow: Publish test message via Management UI
[ ] Check connection count: rabbitmqctl list_connections
[ ] Verify consumer is acking: add debug log before channel.ack()
```

---

## 30. Interview Preparation

### 🟢 Beginner Questions (20)

**Q1: What is RabbitMQ?**
- **Short:** An open-source message broker that routes messages between services.
- **Detail:** RabbitMQ implements AMQP (Advanced Message Queuing Protocol). It decouples producers and consumers — producers publish messages to exchanges, which route them to queues, where consumers read them. Written in Erlang for high concurrency and reliability.

**Q2: What is a message broker?**
- **Short:** Middleware that routes messages between applications.
- **Detail:** A broker sits between producer and consumer services. Producers don't need to know who will process their messages, and consumers don't need to be running when messages are published.

**Q3: Why do we need message queues?**
- **Short:** Decoupling, buffering, and async processing.
- **Detail:** Without queues: Service A must call Service B synchronously; if B is down, A fails. With queues: A publishes a message and continues; B processes when ready. This enables resilience, scalability, and asynchronous workflows.

**Q4: What is a Producer?**
An application that creates and sends messages to an exchange. Example: an Order Service publishing "order.created" events.

**Q5: What is a Consumer?**
An application that reads messages from a queue and processes them. Example: an Email Service reading from an "email-send" queue.

**Q6: What is a Queue in RabbitMQ?**
A FIFO buffer that stores messages. Messages sit in the queue until a consumer reads and acknowledges them. Queues have names, and can be configured as durable, exclusive, or auto-delete.

**Q7: What is an Exchange?**
The routing component that receives messages from producers and distributes them to queues based on bindings and routing keys. Producers never publish directly to queues — they publish to exchanges.

**Q8: What are the types of exchanges?**
Four main types: **Direct** (exact routing key match), **Fanout** (broadcast to all bound queues), **Topic** (pattern matching with `*` and `#`), **Headers** (route based on message header attributes).

**Q9: What is a Routing Key?**
A string attached to a message that exchanges use to decide which queues to route to. Like the "address" on a letter. For direct exchanges, must exactly match the binding key. For topic exchanges, matched against patterns.

**Q10: What is a Binding?**
A link between an exchange and a queue, with an optional binding key. When a message arrives at an exchange, the exchange checks its bindings to decide which queues receive the message.

**Q11: What is a Virtual Host?**
An isolated logical namespace within RabbitMQ, like a separate RabbitMQ instance. Different vhosts have separate exchanges, queues, bindings, and permissions. Useful for multi-tenant systems or environment separation (dev/staging/prod on one broker).

**Q12: What is the difference between a Connection and a Channel?**
A **Connection** is a physical TCP connection to the RabbitMQ broker (expensive, one per process). A **Channel** is a lightweight virtual connection inside a TCP connection (cheap, one per thread/operation). Multiple channels share one TCP connection.

**Q13: What is message acknowledgement?**
Ack = Consumer telling RabbitMQ "I successfully processed this message, you can delete it." Until acked, RabbitMQ keeps the message in "unacknowledged" state and won't give it to another consumer.

**Q14: What happens if a consumer crashes without acking?**
RabbitMQ detects the connection closed. Messages held by that consumer move back to "ready" state and are delivered to the next available consumer. No message loss if consumer acknowledgement is configured.

**Q15: What is `noAck: true` (auto-ack)?**
Messages are automatically acknowledged the moment RabbitMQ delivers them to the consumer — without waiting for processing to complete. Faster but dangerous: if the consumer crashes during processing, the message is permanently lost.

**Q16: What is a Durable Queue?**
A queue that persists its definition to disk and survives a RabbitMQ server restart. Without durability, queues disappear when RabbitMQ restarts.

**Q17: What is a Persistent Message?**
A message written to disk (not just RAM) when enqueued. Persistent messages survive RabbitMQ restarts. Requires both a durable queue AND persistent message property.

**Q18: How does RabbitMQ differ from direct REST API calls?**
REST: synchronous, tight coupling, caller must wait. RabbitMQ: asynchronous, loose coupling, fire-and-forget. REST is better for queries (user waiting for response). RabbitMQ is better for commands/events (background work, notifications, workflows).

**Q19: What port does RabbitMQ use by default?**
5672 for AMQP (applications connect here). 15672 for the Management UI (browser). 5671 for AMQP over TLS.

**Q20: What is `amqplib` in Node.js?**
The most popular RabbitMQ client library for Node.js. Implements the AMQP 0-9-1 protocol. Use: `npm install amqplib`. Key functions: `amqp.connect()`, `connection.createChannel()`, `channel.assertQueue()`, `channel.publish()`, `channel.consume()`, `channel.ack()`.

---

### 🟡 Intermediate Questions (30)

**Q21: Explain the difference between Direct, Fanout, and Topic exchanges.**
- **Direct:** Routes messages to queues where binding key exactly equals routing key. Use for task-specific routing.
- **Fanout:** Ignores routing key, copies message to ALL bound queues. Use for broadcasting events.
- **Topic:** Pattern matching using `*` (one word) and `#` (zero or more words). Use for flexible microservice routing.

**Q22: When would you use a Fanout exchange vs Topic exchange?**
Fanout: When ALL subscribers need EVERY message (e.g., cache invalidation broadcast). Topic: When different subscribers want different subsets of messages (e.g., payment service only wants "payment.*" events). Topic is more flexible and preferred in microservices.

**Q23: What is Prefetch and why is it important?**
Prefetch (QoS) limits how many unacknowledged messages a consumer can hold at once. Without it, RabbitMQ sends all available messages to a consumer, even if it can't process them fast enough. This causes unfair distribution (fast consumers idle, slow ones overwhelmed) and risks OOM. Best practice: set `channel.prefetch(10)` for most workloads.

**Q24: Explain the difference between `ack`, `nack`, and `reject`.**
- `ack(msg)` — Successfully processed, delete from queue
- `nack(msg, allUpTo, requeue)` — Failed processing; can requeue or discard; can batch-nack
- `reject(msg, requeue)` — Same as nack but for one message only, no batch
- Key parameter: `requeue: true` puts message back, `requeue: false` discards (or sends to DLX)

**Q25: What is a Dead Letter Exchange (DLX)?**
An exchange that receives messages that "die" — rejected with requeue:false, expired by TTL, or dropped due to queue length limit. Configure on a queue with `x-dead-letter-exchange`. Enables safe handling of failed messages without losing them.

**Q26: What is a Dead Letter Queue (DLQ)?**
A regular queue bound to a DLX, used to capture dead letters. Dead messages accumulate here for inspection, alerting, and manual reprocessing. Every critical queue should have a DLQ setup.

**Q27: Why is Durable Queue ≠ Persistent Message?**
Durable queue means the queue DEFINITION survives restart (exchanges, bindings recreated). Persistent message means the message CONTENT is written to disk. For full durability: you need BOTH. A durable queue with non-persistent messages loses messages on restart — the queue exists but is empty.

**Q28: How do you implement retry logic in RabbitMQ?**
Best approach: TTL + DLX. Create a retry queue with `x-message-ttl` and `x-dead-letter-exchange` pointing back to the main exchange. Failed messages: nack → go to retry queue → wait TTL → automatically return to main queue. Track retry count in message headers. After max retries: send to DLQ.

**Q29: What are Publisher Confirms?**
A mechanism where the RabbitMQ broker confirms to the publisher that a message was received and persisted. Use `connection.createConfirmChannel()` and pass a callback to `publish()` or `sendToQueue()`. Without confirms: a crash between publish and broker receipt loses the message. Always use in production.

**Q30: What is the default exchange?**
An exchange with no name (""). Every queue is automatically bound to it using the queue's name as routing key. `channel.sendToQueue('my-queue', ...)` is equivalent to `channel.publish('', 'my-queue', ...)`.

**Q31: How do multiple consumers on the same queue work?**
RabbitMQ distributes messages in a round-robin fashion among consumers subscribed to the same queue. With prefetch(1): fair distribution (idle consumers get more). This is the "Competing Consumers" pattern — easy horizontal scaling by adding more consumer processes.

**Q32: What is Consumer Timeout?**
If a consumer holds an unacknowledged message for longer than the consumer_timeout setting (default: 30 minutes), RabbitMQ forces-closes the channel. Prevents hung consumers from blocking messages indefinitely.

**Q33: What is a Topic Exchange pattern? Explain `*` and `#`.**
- `*` matches exactly one word (one segment between dots)
- `#` matches zero or more words
- Example: `"order.#"` matches `order.created`, `order.item.added`, `order.item.removed`
- Example: `"*.failed"` matches `payment.failed`, `order.failed` but NOT `payment.card.failed`
- Example: `"#"` matches everything

**Q34: How would you handle a slow consumer?**
1. Increase consumer count (add more instances)
2. Increase prefetch count to process more concurrently
3. Optimize processing code
4. Use lazy queues to handle backlog
5. Set max-length on queues with drop-head or reject-publish overflow policy
6. Add monitoring alerts when queue depth exceeds threshold

**Q35: What happens when you `nack` with `requeue: true`?**
Message goes back to the HEAD of the queue (in front of other messages). If the consumer always fails, this creates an infinite hot loop — message bounces at CPU speed. Never use immediate requeue in catch blocks — use TTL-based retry via DLX instead.

**Q36: Explain idempotent consumers.**
A consumer is idempotent if processing the same message twice produces the same result as processing it once. Required because RabbitMQ can redeliver messages (at-least-once delivery). Implement via: unique constraint in DB on a business key, Redis NX check with message ID, or conditional updates (update only if status = 'pending').

**Q37: What is the Competing Consumers pattern?**
Multiple consumers subscribe to the same queue. RabbitMQ distributes messages among them. Enables easy horizontal scaling — just add more consumer instances. No code changes required. Use prefetch(1) for fair load distribution.

**Q38: What is a Quorum Queue?**
A queue type that uses Raft consensus for replication across cluster nodes. Messages confirmed only after majority of nodes acknowledge them. Safer than classic mirrored queues. Recommended for all production workloads. Configured with `x-queue-type: quorum`.

**Q39: What is message TTL?**
Time-To-Live: a message expires and becomes dead-lettered if it sits in a queue longer than the TTL. Set per-queue with `x-message-ttl` or per-message with `expiration` property. Useful for time-sensitive data (OTP codes, price quotes, session tokens).

**Q40: How do you handle connection failures in a Node.js consumer?**
Listen to `connection.on('error')` and `connection.on('close')`. On close, implement exponential backoff reconnection loop. Keep the consumer running (don't exit on connection error). Use a singleton connection manager class that all services share.

**Q41: What is `channel.prefetch()` global parameter?**
`channel.prefetch(count, false)` — limit applies PER consumer. `channel.prefetch(count, true)` — limit applies to all consumers on the channel combined. Usually use per-consumer (global: false).

**Q42: What is the difference between `assertQueue` and `checkQueue`?**
`assertQueue` — creates the queue if it doesn't exist, verifies options if it does. Safe to call repeatedly. `checkQueue` — throws an error if the queue doesn't exist. Use to verify a queue exists before publishing.

**Q43: What is a Temporary/Exclusive Queue?**
An exclusive queue is tied to the connection that created it — deleted when that connection closes. Auto-named (RabbitMQ generates unique name). Used for RPC reply queues, per-consumer subscriptions.

**Q44: How do you prevent message loss at both producer and consumer sides?**
- **Producer:** Publisher confirms (`createConfirmChannel`) + persistent messages
- **Consumer:** Manual ack + durable queues + idempotent processing
- **Infrastructure:** Quorum queues for replication

**Q45: What is Back Pressure in RabbitMQ?**
When queues grow too large and RabbitMQ runs low on memory/disk, it sends a "blocked" signal to connected producers, throttling them. Producers should listen to `connection.on('blocked')` and pause publishing. Prevents broker OOM crash.

**Q46: What is a Virtual Host?**
Logical isolation within one RabbitMQ broker. Different vhosts have separate exchanges, queues, bindings, users. Like separate databases in one DB server. Use for: multi-tenant isolation, environment separation (dev/prod), team isolation.

**Q47: What message properties are most important in production?**
- `persistent: true` — disk persistence
- `messageId` — deduplication
- `correlationId` — trace requests across services
- `contentType: 'application/json'` — type safety
- `timestamp` — message age tracking
- Headers: `x-retry-count`, `source-service`

**Q48: Explain the message lifecycle.**
Publish → Exchange → Binding match → Queue (status: ready) → Delivered to consumer (status: unacked) → Consumer acks → Deleted. If nacked with requeue: back to ready. If nacked without requeue: dead-lettered to DLX or discarded.

**Q49: What is `channel.cancel()`?**
Cancels a consumer subscription. The consumer stops receiving new messages. Useful for graceful shutdown: cancel consumer, wait for in-flight messages to finish processing, then close channel and connection.

**Q50: What is `autoDelete` queue property?**
Queue is automatically deleted when the last consumer unsubscribes. Useful for temporary queues that should clean themselves up. NOT the same as exclusive (exclusive is tied to connection, autoDelete is tied to last consumer).

---

### 🔴 Advanced Questions (30)

**Q51: How does RabbitMQ clustering work?**
Multiple RabbitMQ nodes share exchange/queue definitions, bindings, and user data (metadata). Queue contents are NOT replicated by default (use quorum queues for replication). Nodes communicate via Erlang distribution protocol. One node acts as leader per queue. If a node fails, classic queue messages on that node are lost (use quorum queues to avoid this).

**Q52: Explain Quorum Queues vs Classic Mirrored Queues.**
Classic mirrored queues (deprecated): Each message copied to all mirrors synchronously — slow, network intensive. Quorum queues (Raft-based): Messages replicated to majority (n/2+1) of nodes. Leader election automatic. Better failure tolerance, simpler reasoning. Quorum queues are the recommended replacement for all mirrored queues.

**Q53: What is the Raft consensus algorithm in RabbitMQ context?**
Quorum queues use Raft for leader election and log replication. A write (message publish) is committed only when a majority of replicas (quorum) confirm it. If the leader fails, remaining members elect a new leader. This guarantees no data loss as long as quorum is maintained.

**Q54: How would you design RabbitMQ for 10 million messages per day?**
- Use quorum queues on a 3+ node cluster
- Multiple consumers per queue (competing consumers)
- Set appropriate prefetch per consumer (10-50)
- Use lazy queues for queues that might have backlogs
- Separate exchanges per domain
- Pre-declare all queues at startup
- Use persistent messages + publisher confirms
- Monitor queue depth, add consumers automatically
- 10M/day = ~115/sec average — achievable with basic setup

**Q55: What is flow control and how does it work?**
When RabbitMQ approaches memory/disk limit, it sends AMQP `connection.blocked` frame to all producers, pausing them. Publishers must handle this by listening to `blocked`/`unblocked` events and stopping publication during block. This prevents broker crash but means producer must buffer messages elsewhere temporarily.

**Q56: What are Lazy Queues and when should you use them?**
Lazy queues store messages to disk immediately rather than in RAM. Trade-off: slower read/write but much lower memory consumption. Use when: processing is slower than publishing (backlog risk), queues contain millions of messages, you want consistent memory use. Configure with `x-queue-mode: lazy`.

**Q57: How do you handle Network Partitions in a RabbitMQ cluster?**
Network partition (split-brain): two cluster halves can't see each other but both think they're primary. RabbitMQ handles via partition handling modes:
- `ignore` — partition not handled (data loss risk)
- `autoheal` — minority side loses its state and resynchronizes (may lose recent messages)
- `pause-minority` — minority partition pauses until connectivity restored (safer, causes downtime)
- Quorum queues handle partitions naturally via Raft

**Q58: Explain publisher confirms vs transactions.**
Publisher confirms: async, broker ACKs each message individually, high performance. Transactions: synchronous, wraps multiple operations in a commit, very slow (10-100x slower). Transactions are the AMQP TX extension — almost never used in production. Publisher confirms are the right choice.

**Q59: What is a Single Active Consumer (SAC) and when do you need it?**
SAC ensures only one consumer is active on a queue at a time, even if multiple are connected. If active consumer fails, the next one automatically becomes active. Use for: ordered processing requirements, state-dependent consumers, cases where concurrent processing would cause conflicts.

**Q60: What is an Alternate Exchange?**
An exchange configured to receive messages that couldn't be routed to any queue (no matching bindings). Set with `alternate-exchange` argument. Without it, unroutable messages are silently dropped. Use alternate exchange to capture unrouted messages for debugging/alerting.

**Q61: How do you implement request/reply (RPC) with RabbitMQ?**
1. Client creates an exclusive reply queue
2. Client publishes request with `replyTo: replyQueueName` and `correlationId: uniqueId`
3. Server processes request, publishes response to `msg.properties.replyTo` with same `correlationId`
4. Client listens to reply queue, matches `correlationId`
5. Use timeout to avoid hanging indefinitely

**Q62: What is the delivery-limit on Quorum Queues?**
`x-delivery-limit` specifies maximum delivery attempts. After this many failed deliveries (nack), the message is dead-lettered automatically. Eliminates need for manual retry counting in header logic. Sets a safety ceiling on retry attempts.

**Q63: What is RabbitMQ Streams?**
A newer queue type (3.9+) providing Kafka-like persistent append-only log semantics within RabbitMQ. Features: multiple consumers reading independently from any offset (replay), high throughput, retention by size or time. Use when: you need replay, multiple independent consumer groups, high throughput streaming within the RabbitMQ ecosystem.

**Q64: How would you migrate from Classic to Quorum Queues in production?**
1. Create new quorum queue with different name
2. Update producers to publish to both old and new queue temporarily
3. Wait for old queue to drain
4. Switch consumers to new queue
5. Remove old queue
OR: use the `quorum_queue` feature flag + queue migration tooling in newer RabbitMQ versions.

**Q65: What is the difference between `mandatory` and `immediate` flags?**
`mandatory: true` — if the message cannot be routed to any queue, broker returns it to the publisher (triggers `channel.on('return')`). `immediate: true` (removed in RabbitMQ 3.x) — return message if no consumer is ready. In modern RabbitMQ, only `mandatory` is relevant.

**Q66: How does RabbitMQ handle memory pressure?**
1. `vm_memory_high_watermark` (default 40% of RAM) — when exceeded, RabbitMQ starts paging messages to disk
2. Producers receive `connection.blocked` signal
3. At absolute limit, RabbitMQ blocks all publishing

**Q67: What are the trade-offs of large message sizes?**
Large messages (>1MB): slower routing, higher memory use, network congestion, slower ack. Best practice: keep messages small (<100KB). For large data: store in S3/object storage, publish only the reference (URL/key) in the message.

**Q68: How do you do zero-downtime deployments with RabbitMQ consumers?**
1. Deploy new consumer version
2. Both old and new consumers run simultaneously (competing consumers)
3. New consumers process messages with new logic
4. Old consumers drain remaining in-flight messages
5. Stop old consumers
Key: make message format backwards-compatible, use schema versioning.

**Q69: What is the AMQP protocol and how does it work?**
Advanced Message Queuing Protocol — binary wire protocol for message brokers. AMQP 0-9-1 is what RabbitMQ implements. Defines: connections, channels, queues, exchanges, methods (publish, consume, ack), content frames. Amqplib translates your JS calls into AMQP frames over TCP.

**Q70: How would you debug a gradually growing queue?**
1. `rabbitmqctl list_queues name messages consumers` — check consumer count
2. Check processing rate vs publishing rate in Management UI
3. Check consumer error logs — are they failing silently?
4. `rabbitmqctl list_queues name messages_unacknowledged` — if growing, consumers receive but don't process
5. Check prefetch setting — if too low, consumers may be artificially limited
6. Profile consumer code for slow operations

**Q71: What causes PRECONDITION_FAILED error?**
Attempting to `assertQueue` with different properties than an existing queue. Example: queue exists as `durable: false`, you try to assert it as `durable: true`. Fix: delete old queue and recreate, or ensure all services agree on queue properties.

**Q72: How do you scale RabbitMQ write throughput?**
1. Use publish batching (multiple messages before waiting for confirms)
2. Use multiple channels and multiple publishers
3. Tune `vm_memory_high_watermark` appropriately
4. Use quorum queues (slower than classic but safer — accept the trade-off)
5. Run multiple RabbitMQ nodes in cluster
6. Use lazy queues to reduce memory pressure

**Q73: What is the Shovel and Federation plugin?**
**Shovel:** Moves messages from one queue/exchange to another, even across brokers or networks. Used for data migration, bridging clusters. **Federation:** Links exchanges or queues across different brokers that don't share a cluster. Used for multi-datacenter setups.

**Q74: How would you handle a DLQ that's growing too fast?**
Investigate: `rabbitmqctl list_queues name messages | grep dlq`
1. Get a few messages from DLQ: Management UI → Get Messages
2. Find root cause: look at `x-death` header for reason/time
3. Fix the consumer bug
4. Replay DLQ: consume DLQ and republish to original queue (after fix)
5. Set alert threshold on DLQ depth

**Q75: What is Heartbeat and why is it needed?**
AMQP heartbeat frames sent periodically to detect dead connections. Without heartbeats: a silently dropped TCP connection (firewall timeout, network failure) won't be detected — publisher/consumer thinks it's connected but messages are lost. Set `heartbeat: 60` in connection options. If no heartbeat received in 2×heartbeat seconds, connection declared dead and automatic reconnection begins.

**Q76: Explain Priority Queues and their caveats.**
Priority queues process higher-priority messages first. Configure with `x-max-priority: 10`. Caveats: adds CPU overhead (must scan all priorities, not just FIFO), small priority advantage (with many consumers it's probabilistic not guaranteed), not supported by Quorum Queues. Use only when you genuinely need prioritization.

**Q77: What is Consumer Cancellation Notification?**
When a queue is deleted, or a consumer's queue is unexpectedly removed, RabbitMQ sends a consumer cancellation notification — the consume callback is called with `null`. Always check: `if (msg === null) return;` in your consumer.

**Q78: How do you prevent split-brain in RabbitMQ cluster?**
Use `cluster_partition_handling = pause-minority` in `rabbitmq.conf`. Minority partition pauses all operations until connectivity is restored. Combined with quorum queues (which require majority for writes), this provides strong split-brain protection. Odd number of nodes (3, 5, 7) ensures clear majority.

**Q79: What is `x-expires` vs `x-message-ttl`?**
`x-expires`: Queue itself is deleted after N milliseconds of inactivity (no consumers, no activity). `x-message-ttl`: Individual messages in the queue expire after N milliseconds. Both are in milliseconds. Queue expiry is useful for temporary/reply queues that should auto-clean.

**Q80: What metrics would you alert on in production?**
- `queue_depth > 10000` — not enough consumers
- `unacked_messages growing` — consumer processing stuck
- `consumer_count = 0 for any critical queue` — consumer died, PagerDuty
- `memory > 80%` — approaching alarm threshold
- `disk_free < 5GB` — approaching disk alarm
- `connection_count suddenly drops` — mass disconnect event
- `DLQ depth > 0` — messages are failing

---

### 📋 Scenario-Based Questions (20)

**S1: Your payment queue has 50,000 messages backed up. What do you do?**
1. Check consumer count — if 0, restart consumers immediately
2. If consumers are running, check if they're stuck (high unacked count)
3. Add more consumer instances (scale out)
4. Increase prefetch count if consumers are healthy but slow
5. Check consumer logs for errors — failing silently?
6. Set up alert for queue depth > threshold to catch this earlier

**S2: A consumer is processing payment.success events and sometimes charging customers twice. What's wrong?**
The consumer is not idempotent. RabbitMQ delivers at-least-once — network blip before ack causes redelivery. Fix: before processing, check if payment ID already exists in DB (unique constraint). Use a DB transaction with unique paymentId to make the operation atomic and idempotent.

**S3: Messages are disappearing — published but never consumed. Debug.**
1. Check routing: does the routing key match any binding? `rabbitmqctl list_bindings`
2. Is the exchange correct? Check exchange type and routing key pattern
3. Is `mandatory: true` set? If not, unroutable messages are silently dropped
4. Is the queue durable? Did RabbitMQ restart and delete non-durable queue?
5. Publish a test message via Management UI, watch where it goes

**S4: Design RabbitMQ for a notification system that sends 10,000 emails per hour.**
10,000/hr = ~2.8/sec. Very manageable.
- Exchange: `notifications` (topic)
- Queue: `email.queue` (durable, `x-message-ttl: 3600000`, DLX configured)
- Binding: `notification.email`
- 3 email worker consumers (prefetch: 5 each = 15 concurrent)
- Rate limit: email provider limits to 100/min → max 100 concurrent across all workers
- DLQ: `email.dlq` for failed sends
- Retry: TTL 60s retry queue, max 3 attempts

**S5: How do you ensure no order events are lost if RabbitMQ goes down for 5 minutes?**
1. Messages already in queues: quorum queue + persistent messages → survive restart
2. Messages published during downtime: producers must handle connection failures. Options:
   - Buffer in application memory (risk: if app also restarts, lost)
   - Persist to PostgreSQL outbox table, replay after RabbitMQ comes back
   - Use publisher confirms + retry loop with local in-memory queue
   - Write to Redis stream as fallback, drain to RabbitMQ on reconnect

**S6: Your RabbitMQ node is at 85% memory. What are the immediate and long-term fixes?**
Immediate: purge non-critical queues, reduce prefetch (consumers hold fewer messages in memory), add more consumers to drain queues faster.
Long-term: increase RAM, switch to lazy queues for queues prone to backlogs, set `x-max-length` with overflow policy on queues, add cluster nodes, optimize consumer processing speed.

**S7: How would you migrate a high-traffic RabbitMQ system to quorum queues with zero downtime?**
1. Create new quorum queue (e.g., `orders.v2`)
2. Update producers: publish to BOTH `orders` (classic) and `orders.v2` (quorum)
3. Deploy new consumer version reading from `orders.v2`
4. Keep old consumers on `orders` until it drains
5. Once `orders` is empty and all consumers migrated, remove old queue
6. Stop dual-publishing — publish only to `orders.v2`
7. Rename or update references

**S8: A message is stuck in "Unacknowledged" state for hours. What happened?**
Consumer received it but never called `channel.ack()`. Possible causes: processing threw an exception but catch block doesn't ack/nack, infinite loop in processing, external API call hung without timeout, consumer process is alive but stuck. Fix: add explicit ack/nack in all code paths, add timeouts to external calls, implement `channel.on('error')` handler.

**S9: How do you implement event sourcing with RabbitMQ?**
RabbitMQ is not ideal for event sourcing (messages deleted after consumption, no replay). However, you can approximate it by: (1) persisting events to DB before publishing, (2) consuming and rebuilding state from DB for new services. For true event sourcing/replay, use Kafka or RabbitMQ Streams.

**S10: Design a multi-tenant SaaS system where each tenant's events are isolated.**
Option A: Separate vhost per tenant (strong isolation, more resources)
Option B: Per-tenant queues with naming convention (`tenant-{id}.orders`)
Option C: Single queues, filter by tenantId in consumer

Best approach for most cases: per-tenant queues with regex permissions (`^tenant-{tenantId}\..*`), one shared cluster, topic exchange with routing key `tenant.{id}.{event}`.

**S11: Your consumer processes messages in batches for efficiency. How do you implement it?**
```javascript
await channel.prefetch(50);  // Buffer 50 messages
let batch = [];
let batchTimer;

channel.consume(queue, (msg) => {
  batch.push(msg);
  clearTimeout(batchTimer);
  
  if (batch.length >= 50) {
    processBatch();
  } else {
    batchTimer = setTimeout(processBatch, 1000); // Max 1s wait
  }
});

async function processBatch() {
  const toProcess = [...batch];
  batch = [];
  await bulkInsert(toProcess.map(m => JSON.parse(m.content)));
  toProcess.forEach(msg => channel.ack(msg));
}
```

**S12: How do you implement a rate-limited consumer (max 10 API calls/second)?**
```javascript
const RateLimiter = require('limiter').RateLimiter;
const limiter = new RateLimiter({ tokensPerInterval: 10, interval: 'second' });

await channel.prefetch(1);  // Only take one at a time
channel.consume(queue, async (msg) => {
  await limiter.removeTokens(1);  // Wait for rate limit slot
  await callExternalAPI(JSON.parse(msg.content));
  channel.ack(msg);
});
```

**S13: An exchange was declared with wrong type. How do you fix it in production?**
You cannot change exchange type — must delete and recreate. Approach:
1. Create new exchange with correct type and different name
2. Update producers to publish to new exchange
3. Rebind queues to new exchange
4. Once no producers use old exchange, delete it
NEVER delete an exchange with active producers pointing to it.

**S14: How would you test RabbitMQ integrations?**
1. **Unit tests:** Mock the channel object, verify `publish()` called with correct args
2. **Integration tests:** Use Docker (testcontainers library) to spin up real RabbitMQ
3. **Contract tests:** Verify message format (JSON schema) matches what consumers expect
4. **Load tests:** Use k6 or custom script to publish N messages/second, verify consumer throughput

**S15: Your order events need to be consumed by a new analytics service without affecting existing consumers.**
Add new queue and bind to existing exchange:
```javascript
await channel.assertQueue('analytics.orders', { durable: true });
await channel.bindQueue('analytics.orders', 'orders', 'order.#');
```
Analytics service consumes from `analytics.orders`. The new binding is additive — no change to existing consumers. This is a major benefit of the exchange/queue separation model.

**S16: How do you implement saga pattern with RabbitMQ?**
Saga = distributed transaction via events.
```
Order Saga:
1. Order Service: ORDER_CREATED → publish → Payment Service
2. Payment Service: PAYMENT_SUCCESS → publish → Inventory Service
3. Inventory Service: INVENTORY_RESERVED → publish → Notification Service
4. Any step fails → compensating transaction published
   PAYMENT_FAILED → Inventory Service cancels reservation
                  → Order Service marks order failed
                  → Notification Service sends failure email
```
Use correlation ID to link all events in one saga.

**S17: What's your strategy for DLQ reprocessing after a bug fix?**
1. Identify root cause by examining DLQ messages (`x-death` headers)
2. Write fix and deploy
3. Test with a few DLQ messages manually via Management UI → "Move" to original queue
4. Write a DLQ replayer: consumer that reads from DLQ, validates/fixes message, republishes to original exchange
5. Monitor: ensure replayed messages process correctly
6. Consider rate limiting the replay to avoid flooding the system

**S18: How do you monitor that all critical queue consumers are alive?**
```javascript
// Health check endpoint that verifies consumer count
app.get('/health', async (req, res) => {
  const response = await fetch('http://admin:pass@localhost:15672/api/queues/%2F/orders');
  const queue = await response.json();
  
  if (queue.consumers === 0) {
    res.status(503).json({ status: 'unhealthy', reason: 'No consumers on orders queue' });
  } else {
    res.json({ status: 'healthy', consumers: queue.consumers });
  }
});
```
Add Prometheus alert: `rabbitmq_queue_consumers{queue="orders"} == 0` → PagerDuty.

**S19: How would you handle a message that's valid JSON but business-invalid (e.g., negative amount)?**
```javascript
channel.consume(queue, async (msg) => {
  const data = JSON.parse(msg.content.toString());
  
  // Business validation
  if (data.amount <= 0) {
    console.error('Invalid amount, rejecting permanently:', data);
    channel.reject(msg, false);  // requeue: false → DLQ (don't retry — it's always wrong)
    return;
  }
  
  await process(data);
  channel.ack(msg);
});
```
Business-invalid messages should be rejected without requeue — retrying won't fix them.

**S20: Describe a production incident with RabbitMQ and how you'd resolve it.**
**Scenario:** 3 AM alert — payment queue has 500,000 messages, 0 consumers.

**Response:**
1. Check: `rabbitmqctl list_queues name consumers messages`
2. Start payment worker instances immediately (reduce queue depth)
3. Check why workers went down: application crash? OOM? Kubernetes pod restart?
4. Examine logs: `kubectl logs payment-worker-xxx`
5. Fix root cause (e.g., worker OOM due to memory leak → fix + redeploy)
6. Monitor recovery: queue should drain over 30-60 min with workers running
7. Post-incident: add consumer_count==0 alert, add memory limit to worker pods, review auto-scaling policy

---

### 🏗️ Production/Architecture Questions (20)

**P1: What's your production RabbitMQ checklist before going live?**
See §26 Production Best Practices. Key items: quorum queues, 3-node cluster, durable queues, persistent messages, publisher confirms, manual acks, prefetch set, DLX/DLQ configured, TLS enabled, guest user deleted, monitoring/alerts set up, graceful shutdown implemented.

**P2: How do you size a RabbitMQ cluster?**
- **CPU:** 2-4 cores per node for moderate workloads
- **RAM:** 4-8GB per node minimum; rule of thumb: 2× expected peak queue memory
- **Disk:** 50GB+ per node; quorum queues need more disk
- **Nodes:** 3 minimum for HA; 5 for very high availability
- **Network:** Low latency between nodes critical for quorum

**P3: How do you document your RabbitMQ topology?**
Maintain an `exchanges.yml` or `topology.md` that lists every exchange, queue, binding, and routing key. Use infrastructure-as-code: declare topology from a shared config file that all services reference. Use naming conventions consistently. Document in ADRs (Architecture Decision Records).

**P4: When would you choose RabbitMQ over Amazon SQS?**
RabbitMQ: complex routing needed, on-premise requirement, open-source preference, request/reply (RPC) pattern, lower per-message cost at scale.
SQS: managed service preferred, simple FIFO or standard queue, deep AWS integration, no DevOps team for RabbitMQ, need SQS FIFO exact-once semantics.

**P5: How do you handle message schema evolution?**
1. Version your messages: `{ "version": "1.0", "data": {...} }`
2. Consumers check version and handle accordingly
3. Use Avro or Protobuf with a schema registry for strong typing
4. Backward compatible: add optional fields (never remove required fields)
5. If breaking change needed: new message type / new routing key

**P6: What's your strategy for zero-downtime schema changes?**
1. Deploy consumer that handles BOTH old and new schema
2. Deploy producer that publishes new schema
3. Gradually migrate — old messages drain
4. Remove old schema handling code in a later release

**P7: How would you implement a global rate limit across multiple consumer instances?**
Central rate limiter (Redis): each consumer acquires a token from Redis before processing. `SETNX + EXPIRE` pattern or Redis rate limiter library. Distributed rate limiting without a central bottleneck.

**P8: How do you secure RabbitMQ in a Kubernetes deployment?**
- Store credentials in Kubernetes Secrets
- Use cert-manager for TLS certificates
- Network policy to restrict RabbitMQ port access to only allowed pods
- Use RabbitMQ Kubernetes Operator for cluster management
- Separate service account per consumer/producer
- Rotate credentials regularly via external-secrets or Vault

**P9: What's the trade-off between more smaller queues vs fewer larger queues?**
More queues: better isolation, clearer semantics, independent scaling. More operational complexity.
Fewer queues: simpler topology. Risk: one service's backlog affects others sharing the queue.
Rule: one queue per consumer type/service. Never share a queue between unrelated consumers.

**P10: How do you plan capacity for a growing RabbitMQ system?**
1. Measure current: messages/second, avg message size, consumer throughput
2. Set up Prometheus metrics, Grafana dashboards
3. Trend queue depth over time
4. Load test: fire N×current load, see where bottlenecks appear
5. Add nodes when any node reaches 60% memory
6. Add consumers when queue depth consistently > 1000

---

## 31. Cheat Sheet

### Important Terminology

| Term | One-liner |
|------|-----------|
| **Broker** | RabbitMQ server |
| **Producer** | Publishes messages |
| **Consumer** | Reads messages |
| **Exchange** | Routes messages to queues |
| **Queue** | Stores messages |
| **Binding** | Rule: exchange → queue |
| **Routing Key** | Message address string |
| **Vhost** | Isolated namespace |
| **Connection** | TCP connection to broker |
| **Channel** | Virtual connection in TCP |
| **DLX** | Dead Letter Exchange |
| **DLQ** | Dead Letter Queue |
| **Prefetch** | Max unacked messages per consumer |
| **ACK** | "Message processed" signal |
| **NACK** | "Message failed" signal |

### Exchange Types

| Type | Route Logic | Use When |
|------|------------|---------|
| `direct` | Exact key match | Known destination |
| `fanout` | Broadcast all | Notify all services |
| `topic` | Pattern `*` `#` | Flexible routing |
| `headers` | Header attributes | Content-based routing |
| `` (default) | Queue name | Simple, no exchange |

### Queue Declaration Cheat Sheet

```javascript
await channel.assertQueue('name', {
  durable: true,            // Survive restart
  exclusive: false,         // One connection only
  autoDelete: false,        // Delete when last consumer leaves
  arguments: {
    'x-queue-type': 'quorum',          // Replicated (PRODUCTION MUST)
    'x-message-ttl': 60000,            // Message TTL (ms)
    'x-expires': 300000,               // Queue TTL (ms)
    'x-max-length': 10000,             // Max messages
    'x-dead-letter-exchange': 'dlx',   // DLX name
    'x-dead-letter-routing-key': 'dead', // DLX routing key
    'x-max-priority': 10,              // Enable priority (0-10)
    'x-single-active-consumer': true,  // One consumer at a time
  }
});
```

### Key Node.js APIs

```javascript
// Connection
const conn = await amqp.connect(url);
const conn = await amqp.connect({ hostname, port, username, password, vhost, heartbeat: 60 });

// Channels
const ch = await conn.createChannel();           // Regular channel
const ch = await conn.createConfirmChannel();    // With publisher confirms

// Exchange
await ch.assertExchange(name, type, { durable: true, autoDelete: false });
await ch.deleteExchange(name);

// Queue
const q = await ch.assertQueue(name, options);  // q.queue = queue name
await ch.checkQueue(name);   // Throws if doesn't exist
await ch.deleteQueue(name);
await ch.purgeQueue(name);   // Delete all messages

// Binding
await ch.bindQueue(queue, exchange, routingKey);
await ch.unbindQueue(queue, exchange, routingKey);

// Publish
ch.publish(exchange, routingKey, buffer, options);  // Returns bool
ch.sendToQueue(queue, buffer, options);             // Returns bool

// Consume
const { consumerTag } = await ch.consume(queue, handler, { noAck: false });
await ch.cancel(consumerTag);  // Stop consuming

// Acknowledgement
ch.ack(msg);                    // Success
ch.nack(msg, false, requeue);   // Fail (single)
ch.reject(msg, requeue);        // Fail (single, simpler)

// QoS
await ch.prefetch(count);       // Per-consumer limit
await ch.prefetch(count, true); // Per-channel limit

// Confirm channel
await ch.waitForConfirms(); // Wait for all pending confirms
```

### Acknowledgement Methods

| Method | Parameters | Use |
|--------|------------|-----|
| `ack(msg)` | msg | Processed successfully |
| `nack(msg, allUpTo, requeue)` | msg, bool, bool | Failed, can batch |
| `reject(msg, requeue)` | msg, bool | Failed, single message |

### Routing Patterns

| Pattern | Matches | Doesn't Match |
|---------|---------|--------------|
| `order.created` | `order.created` | `order.updated` |
| `order.*` | `order.created`, `order.failed` | `order.item.added` |
| `order.#` | `order.created`, `order.item.added` | `payment.created` |
| `#.failed` | `payment.failed`, `order.failed`, `a.b.failed` | `payment.success` |
| `#` | Everything | Nothing |

### CLI Quick Reference

```bash
# Status
rabbitmq-diagnostics status
rabbitmq-diagnostics check_running
rabbitmq-diagnostics check_local_alarms

# Queues
rabbitmqctl list_queues name messages consumers messages_ready messages_unacknowledged
rabbitmqctl purge_queue <queue>
rabbitmqctl delete_queue <queue>

# Users & Permissions
rabbitmqctl add_user <user> <pass>
rabbitmqctl set_user_tags <user> administrator
rabbitmqctl set_permissions -p <vhost> <user> ".*" ".*" ".*"
rabbitmqctl list_users
rabbitmqctl delete_user <user>

# Vhosts
rabbitmqctl add_vhost <vhost>
rabbitmqctl list_vhosts

# Bindings
rabbitmqctl list_bindings

# Cluster
rabbitmqctl cluster_status
rabbitmqctl join_cluster rabbit@<node>

# Plugins
rabbitmq-plugins enable rabbitmq_management
rabbitmq-plugins enable rabbitmq_prometheus
```

### Production Settings (rabbitmq.conf)

```ini
# Memory threshold (default 40%)
vm_memory_high_watermark.relative = 0.4

# Disk space (default 50MB — too low!)
disk_free_limit.absolute = 5GB

# Heartbeat
heartbeat = 60

# Max connections
max_connections = 10000

# Consumer timeout (30 minutes)
consumer_timeout = 1800000

# Cluster partition handling
cluster_partition_handling = pause-minority

# Default user (CHANGE IN PRODUCTION)
default_user = admin
default_pass = your-strong-password
```

### Common Patterns Summary

| Pattern | Exchange Type | Key Config |
|---------|--------------|-----------|
| Work Queue | default | durable queue, prefetch(1), persistent msg |
| Pub/Sub | fanout | exclusive queues per consumer |
| Routing | direct | binding per destination |
| Topics | topic | `*` and `#` patterns |
| RPC | direct | correlationId + replyTo |
| Retry | direct + TTL | DLX + retry queue |
| DLQ | direct | x-dead-letter-exchange |

### Interview One-Liners

- **RabbitMQ vs Kafka:** RabbitMQ = smart broker (routing, complex delivery); Kafka = dumb broker/smart consumer (log, replay, streaming)
- **Exchange vs Queue:** Exchange routes messages; Queue stores them
- **Durable ≠ Persistent:** Durable = queue survives restart; Persistent = message survives restart. Need both.
- **Auto-ack is dangerous:** Message deleted when delivered, not when processed. Crash = data loss.
- **Prefetch is essential:** Without it, one slow consumer gets all messages.
- **DLX is your safety net:** Every critical queue needs a dead letter exchange.
- **Publisher confirms = at-least-once:** Use createConfirmChannel() in production.
- **Channels, not connections:** One TCP connection, many channels — one channel per concurrent operation.
- **Quorum queues:** Use them for all production data. Raft consensus, survives node failure.
- **Idempotent consumers:** Design for at-least-once delivery — same message processed twice must be safe.

---

## 32. Learning Roadmap

### Phase 1 — Beginner (Week 1-2)
**Topics:**
- What is RabbitMQ, what problem does it solve
- Installation with Docker
- Producers, consumers, exchanges, queues, routing keys
- First Node.js application (hello world)
- Management UI basics

**Exercises:**
- Set up RabbitMQ with Docker
- Build producer and consumer scripts
- Send JSON messages, parse and log them
- Open Management UI, observe messages flowing

**Mini Project:** Simple job queue — producer sends tasks (compute fibonacci), consumer processes and logs results.

**Can explain before moving on:**
- What happens if consumer is down when producer sends? (Message waits in queue)
- What is the default exchange?
- What does `assertQueue` do?

---

### Phase 2 — Core Concepts (Week 3-4)
**Topics:**
- All four exchange types with examples
- Topic exchange routing patterns
- Manual vs auto acknowledgement
- Durable queues + persistent messages
- Prefetch and fair dispatch

**Exercises:**
- Build a topic exchange fanout system (order events → multiple services)
- Compare behavior with and without durable/persistent settings (restart RabbitMQ)
- Show how prefetch changes work distribution

**Mini Project:** Notification router — one publisher sends events like `user.registered`, `order.placed`, `payment.failed`. Build 3 consumers subscribed to different topics.

**Can explain before moving on:**
- Why durable queue ≠ persistent message
- What happens if you forget to `ack`?
- How does prefetch prevent unfair dispatch?

---

### Phase 3 — Practical Development (Week 5-6)
**Topics:**
- Publisher confirms (createConfirmChannel)
- Connection management and reconnection
- Channel lifecycle and channel per operation
- Error handling patterns
- Message properties (headers, messageId, correlationId)

**Exercises:**
- Build a connection manager class with automatic reconnection
- Publish 1000 messages with publisher confirms, measure confirmation rate
- Implement structured error handling (ack/nack/reject based on error type)

**Mini Project:** Simple order system — Order Service (Express API), Email Service (consumer), with publisher confirms and manual acks.

---

### Phase 4 — Reliability (Week 7-8)
**Topics:**
- Dead Letter Exchange and Dead Letter Queue
- TTL-based retry architecture
- Idempotent consumers
- Duplicate message handling
- Consumer timeout

**Exercises:**
- Set up complete DLX/DLQ infrastructure
- Build retry queue with exponential backoff (10s, 30s, 60s)
- Implement idempotent consumer with DB unique constraint
- Simulate consumer crash and verify message is not lost

**Mini Project:** Reliable order processor — with retry on failure, DLQ monitoring, duplicate protection.

**Can explain before moving on:**
- Design a retry architecture from scratch on a whiteboard
- Why immediate nack+requeue is dangerous
- What x-death header contains

---

### Phase 5 — Scaling (Week 9-10)
**Topics:**
- Competing consumers pattern
- Horizontal scaling with Docker Compose
- Message ordering guarantees and when they break
- Multiple queues and exchanges design
- Load testing and benchmarking

**Exercises:**
- Run 5 consumer instances for one queue, observe load distribution
- Design exchange/queue topology for an e-commerce system (5+ services)
- Write a producer that publishes 10,000 messages, measure throughput

**Mini Project:** E-commerce event bus — 4 services (order, payment, inventory, notification) connected via topic exchange.

---

### Phase 6 — Production (Week 11-12)
**Topics:**
- Security (users, vhosts, TLS, permissions)
- Monitoring with Management UI and Prometheus/Grafana
- Production configuration (memory limits, disk, heartbeats)
- Graceful shutdown
- Operational runbooks

**Exercises:**
- Set up user with minimal permissions (restrict to own queues only)
- Connect Prometheus to RabbitMQ, create Grafana dashboard
- Simulate memory pressure, observe flow control behavior
- Write graceful shutdown for consumer (stop consuming, drain in-flight)

**Mini Project:** Production-ready order system from Phase 4-5 with: TLS, dedicated users, Prometheus metrics, graceful shutdown, proper error handling.

---

### Phase 7 — Advanced (Week 13-14)
**Topics:**
- RabbitMQ clustering (3-node Docker setup)
- Quorum queues (configuration, behavior, trade-offs)
- Lazy queues and memory management
- Priority queues
- Single Active Consumer
- RabbitMQ Streams (intro)
- Shovel plugin
- Request/Reply (RPC) pattern

**Exercises:**
- Set up 3-node cluster, kill one node, verify quorum queue continues
- Compare throughput: classic vs quorum queues (benchmark)
- Implement RPC pattern with timeout
- Observe lazy queue memory behavior with 100,000 messages

---

### Phase 8 — Interview Preparation (Week 15-16)
**Topics:**
- Review all 30 interview questions (this guide)
- System design exercises (design X with RabbitMQ)
- Compare RabbitMQ with alternatives (Kafka, SQS, Redis Streams)
- Common mistakes and how to avoid them
- Production incident scenarios

**Exercises:**
- Answer every question in §30 out loud without notes
- System design: design WhatsApp-style notification system with RabbitMQ
- Write a troubleshooting runbook for the 5 most common RabbitMQ problems
- Do a mock interview with a colleague or record yourself

**Can explain before interviews:**
- Full message lifecycle: publish → route → queue → consume → ack
- Design a retry+DLQ architecture on a whiteboard
- Choose between RabbitMQ and Kafka for a given scenario
- Explain quorum queues and why they matter
- Debug 5 common production problems

---

*This guide was designed to take you from zero RabbitMQ knowledge to production-ready, interview-confident mastery. The best way to learn is to BUILD — every concept here should be implemented in code, not just read.*

*Happy messaging! 🐰*