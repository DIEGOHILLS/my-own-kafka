package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class SimpleKafkaProducer {
    private final SimpleKafkaClient client;
    private final String topic;
    private final Random random;

    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.random = new Random();
    }

    public void initialize() throws IOException {
        client.initialize();
    }

    public long send(String message) throws IOException {
        client.refreshMetadata();

        if (!topicExists(topic)) {
            throw new IOException("Topic not found: " + topic);
        }

        int numPartitions = getPartitionCount(topic);
        int partition = random.nextInt(numPartitions);

        return send(message, partition);
    }

    public long send(String message, int partition) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        return client.send(topic, partition, data);
    }

    public void close() {
        client.close();
    }

    private boolean topicExists(String topic) {
        return true;
    }

    private int getPartitionCount(String topic) {
        return 3;
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SimpleKafkaProducer <broker-host> <broker-port> <topic>");
            System.exit(1);
        }

        String brokerHost = args[0];
        int brokerPort = Integer.parseInt(args[1]);
        String topic = args[2];

        SimpleKafkaProducer producer = new SimpleKafkaProducer(brokerHost, brokerPort, topic);

        try {
            producer.initialize();
            System.out.println("Producer initialized. Sending 10 test messages...");

            for (int i = 0; i < 10; i++) {
                String message = "Test message " + (i + 1) + " at " + System.currentTimeMillis();
                long offset = producer.send(message);
                System.out.println("Sent message " + (i + 1) + " — offset: " + offset);
                Thread.sleep(500);
            }

            System.out.println("All messages sent successfully.");

        } catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}