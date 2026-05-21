# Simple Kafka

A lightweight Kafka-inspired distributed messaging system built in Java.

This project implements core concepts behind Apache Kafka including:

* Brokers
* Producers
* Consumers
* Partitions
* Topic management
* Basic replication logic
* ZooKeeper-based broker coordination
* Controller election
* Persistent message storage

The goal of the project is educational: understanding how distributed event streaming systems work internally.

---

# Tech Stack

* Java 11
* Maven
* Apache ZooKeeper
* Java NIO
* Concurrent collections & multithreading

---

# Project Structure

```text
src/main/java/com/simplekafka
│
├── broker
│   ├── BrokerInfo.java
│   ├── Partition.java
│   ├── Protocol.java
│   ├── SimpleKafkaBroker.java
│   └── ZookeeperClient.java
│
└── client
    ├── SimpleKafkaClient.java
    ├── SimpleKafkaConsumer.java
    └── SimpleKafkaProducer.java
```

---

# Features

## Broker

* Non-blocking socket server using Java NIO
* Broker registration in ZooKeeper
* Controller election
* Topic creation
* Partition assignment
* Persistent message storage
* Cluster metadata synchronization
* Multi-threaded request handling

## Producer

* Sends messages to brokers
* Random partition selection
* Topic metadata refresh
* UTF-8 message serialization

## Consumer

* Polls messages from partitions
* Reads persisted messages
* Offset-based consumption

## ZooKeeper Integration

* Broker discovery
* Ephemeral node registration
* Cluster coordination
* Controller failover handling

---

# Architecture

```text
                +-------------------+
                |    ZooKeeper      |
                | Cluster Metadata  |
                +---------+---------+
                          |
        ----------------------------------------
        |                  |                   |
+-------+------+  +--------+------+  +---------+------+
|   Broker 1   |  |   Broker 2    |  |   Broker 3     |
| Partitions   |  | Partitions    |  | Partitions     |
+-------+------+  +--------+------+  +---------+------+
        ^                  ^                   ^
        |                  |                   |
   +----+----+       +-----+-----+       +-----+-----+
   | Producer |       | Consumers |       | Consumers |
   +-----------+      +-----------+       +-----------+
```

---

# Prerequisites

Install:

* Java 11+
* Maven
* Apache ZooKeeper

Verify:

```bash
java -version
mvn -version
```

---

# Setup

## 1. Clone the Repository

```bash
git clone <your-repository-url>
cd my-own-kafka-main
```

## 2. Start ZooKeeper

Example:

```bash
zookeeper-server-start.sh zk.cfg
```

Or using Docker:

```bash
docker run -d --name zookeeper -p 2181:2181 zookeeper
```

---

# Build

```bash
mvn clean package
```

This generates a shaded JAR with dependencies.

---

# Running the Broker

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar \
com.simplekafka.broker.SimpleKafkaBroker
```

Example startup parameters may include:

```text
<broker-id> <host> <port> <zookeeper-port>
```

Example:

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar \
com.simplekafka.broker.SimpleKafkaBroker \
1 localhost 9092 2181
```

Run multiple brokers on different ports to simulate a cluster.

---

# Running the Producer

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar \
com.simplekafka.client.SimpleKafkaProducer \
localhost 9092 test-topic
```

The producer sends test messages automatically.

---

# Running the Consumer

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar \
com.simplekafka.client.SimpleKafkaConsumer \
localhost 9092 test-topic
```

---

# Data Storage

Messages are stored locally under:

```text
data/
```

Partition logs are persisted per broker and topic.

---

# Example Workflow

1. Start ZooKeeper
2. Start Broker 1
3. Start Broker 2
4. Start Broker 3
5. Start Producer
6. Start Consumer
7. Observe partition assignment and message flow

---

# Concepts Implemented

This project demonstrates:

* Distributed systems fundamentals
* Leader election
* Partitioning
* Replication concepts
* Network communication
* Fault tolerance basics
* Event streaming architecture
* Concurrent programming

---

# Current Limitations

This is an educational implementation and not production-ready.

Limitations include:

* Simplified replication
* No authentication or authorization
* Limited failure recovery
* Basic metadata handling
* No consumer groups
* No compression
* No retention policies
* No exactly-once guarantees
* Minimal protocol implementation

---

# Future Improvements

Potential enhancements:

* Consumer groups
* Replication synchronization
* Leader failover
* Message batching
* Compression
* Retention policies
* Offset management
* Monitoring dashboard
* REST API
* Docker Compose cluster setup
* Raft-based metadata management

---

# Learning Outcomes

Building this project helps understand:

* How Kafka brokers coordinate
* How partitions work
* How producers and consumers communicate
* Distributed coordination using ZooKeeper
* Persistent log-based messaging systems
* Scalable event-driven architecture

---
