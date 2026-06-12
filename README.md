# Simple Kafka — Distributed Messaging System

A lightweight Kafka-inspired distributed messaging system built in Java, implementing core event streaming concepts including broker coordination, partition management, producer-consumer patterns, and ZooKeeper-based cluster discovery.

Live Demo: N/A (Backend system — see Setup below)

---

Tech Stack

Language: Java 11
Build Tool: Maven
Coordination: Apache ZooKeeper
Networking: Java NIO
Concurrency: Multithreading, Concurrent Collections

---

Key Features

- Broker Architecture — Non-blocking socket server using Java NIO with multi-threaded request handling and persistent message storage.

- ZooKeeper Coordination — Broker discovery, ephemeral node registration, controller election, and cluster metadata synchronization.

- Producer-Consumer Pattern — Random partition selection for producers and offset-based message consumption for consumers.

- Partition Management — Topic creation, partition assignment, and per-broker log persistence for scalable message distribution.

- Fault Tolerance Basics — Controller failover handling and simplified replication concepts for distributed resilience.

---

Architecture Overview

ZooKeeper (Cluster Metadata)
  -> Broker 1 (Partitions + Storage)
  -> Broker 2 (Partitions + Storage)
  -> Broker 3 (Partitions + Storage)
  -> Producers send messages
  -> Consumers poll messages

---

What I Learned

Building this system deepened my understanding of distributed event streaming architecture and the complexity of coordinating multiple brokers through ZooKeeper. I gained practical experience with Java NIO for non-blocking I/O, leader election patterns, and the trade-offs between consistency and availability in partition-based messaging systems.

---

Project Structure

src/main/java/com/simplekafka
  broker/
    BrokerInfo.java
    Partition.java
    Protocol.java
    SimpleKafkaBroker.java
    ZookeeperClient.java
  client/
    SimpleKafkaClient.java
    SimpleKafkaConsumer.java
    SimpleKafkaProducer.java

---

Getting Started

Prerequisites:
- Java 11+
- Maven
- Apache ZooKeeper

Setup:

1. Clone the repository
git clone https://github.com/DIEGOHILLS/REPLACE-WITH-ACTUAL-REPO-NAME.git
cd my-own-kafka-main

2. Start ZooKeeper
docker run -d --name zookeeper -p 2181:2181 zookeeper

3. Build the project
mvn clean package

4. Start a broker
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker 1 localhost 9092 2181

5. Start the producer
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaProducer localhost 9092 test-topic

6. Start the consumer
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaConsumer localhost 9092 test-topic

---

Concepts Demonstrated

- Distributed systems fundamentals
- Leader election and controller failover
- Partitioning and replication concepts
- Network communication with Java NIO
- Event-driven architecture
- Concurrent programming and fault tolerance

---

---
