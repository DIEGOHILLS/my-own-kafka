package com.simplekafka.client;

import com.simplekafka.broker.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
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
        
        // Auto-create topic if it doesn't exist
        if (!topicExists(topic)) {
            createTopic();
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
        // Check cached metadata
        for (Protocol.TopicMetadata tm : client.topicMetadata.values()) {
            if (tm.name.equals(topic)) {
                return true;
            }
        }
        return false;
    }
    
    private int getPartitionCount(String topic) {
        Protocol.TopicMetadata tm = client.topicMetadata.get(topic);
        if (tm != null) {
            return tm.partitions.size();
        }
        return 3; // Default
    }
    
    private void createTopic() throws IOException {
        ByteBuffer request = Protocol.encodeCreateTopicRequest(topic, 3, (short) 2);
        
        SocketChannel brokerChannel = null;
        try {
            brokerChannel = SocketChannel.open(new InetSocketAddress(client.bootstrapBroker, client.bootstrapPort));
            brokerChannel.write(request);
            
            ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
            brokerChannel.read(responseBuffer);
            responseBuffer.flip();
            
            byte type = responseBuffer.get();
            short errorCode = responseBuffer.getShort();
            
            if (errorCode != 0 && type != Protocol.CREATE_TOPIC_RESPONSE) {
                throw new IOException("Failed to create topic, error code: " + errorCode);
            }
            
            // Refresh metadata after creation
            client.refreshMetadata();
            
        } catch (IOException e) {
            throw new IOException("Failed to create topic: " + e.getMessage(), e);
        } finally {
            if (brokerChannel != null) {
                try { brokerChannel.close(); } catch (IOException ignored) {}
            }
        }
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
