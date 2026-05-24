package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.ArrayList;

public class Protocol {

    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = (byte) 0xFF;

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = topic.getBytes();
        int size = 1 + 2 + topicBytes.length + 4 + 4 + message.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(PRODUCE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = topic.getBytes();
        int size = 1 + 2 + topicBytes.length + 4 + 8 + 4;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = topic.getBytes();
        int size = 1 + 2 + topicBytes.length + 4 + 2;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        byte[] topicBytes = topic.getBytes();
        int size = 1 + 2 + topicBytes.length + 4 + 8 + 4 + message.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put((byte) 0x21);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = topic.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length);

        buffer.put((byte) 0x22);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);

        buffer.flip();
        return buffer;
    }

    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte type = buffer.get();
        if (type != PRODUCE_RESPONSE) {
            return new ProduceResult(-1, "Invalid response type");
        }

        long offset = buffer.getLong();
        short errorCode = buffer.getShort();

        if (errorCode != 0) {
            int msgLen = buffer.getInt();
            byte[] msgBytes = new byte[msgLen];
            buffer.get(msgBytes);
            return new ProduceResult(offset, new String(msgBytes));
        }

        return new ProduceResult(offset, null);
    }

    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte type = buffer.get();
        if (type != FETCH_RESPONSE) {
            return new FetchResult(new byte[0][], "Invalid response type");
        }

        int messageCount = buffer.getInt();
        List<byte[]> messages = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            long offset = buffer.getLong();
            int msgLen = buffer.getInt();
            byte[] msgBytes = new byte[msgLen];
            buffer.get(msgBytes);
            messages.add(msgBytes);
        }

        short errorCode = buffer.getShort();
        String error = (errorCode != 0) ? "Error code: " + errorCode : null;

        return new FetchResult(messages.toArray(new byte[0][]), error);
    }

    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte type = buffer.get();
        if (type != METADATA_RESPONSE) {
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Invalid response type");
        }

        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();
        for (int i = 0; i < brokerCount; i++) {
            int id = buffer.getInt();
            int hostLen = buffer.getInt();
            byte[] hostBytes = new byte[hostLen];
            buffer.get(hostBytes);
            int port = buffer.getInt();
            brokers.add(new BrokerInfo(id, new String(hostBytes), port));
        }

        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            int nameLen = buffer.getInt();
            byte[] nameBytes = new byte[nameLen];
            buffer.get(nameBytes);
            String topicName = new String(nameBytes);

            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();
            for (int j = 0; j < partitionCount; j++) {
                int partitionId = buffer.getInt();
                int leader = buffer.getInt();
                int replicaCount = buffer.getInt();
                int[] replicas = new int[replicaCount];
                for (int k = 0; k < replicaCount; k++) {
                    replicas[k] = buffer.getInt();
                }
                partitions.add(new PartitionMetadata(partitionId, leader, replicas));
            }
            topics.add(new TopicMetadata(topicName, partitions));
        }

        return new MetadataResult(brokers, topics, null);
    }

    public static void sendErrorResponse(SocketChannel channel, String errorMessage) {
        try {
            byte[] msgBytes = errorMessage.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + msgBytes.length);

            buffer.put(ERROR_RESPONSE);
            buffer.putInt(msgBytes.length);
            buffer.put(msgBytes);

            buffer.flip();
            channel.write(buffer);
        } catch (Exception e) {

        }
    }

    public static class ProduceResult {
        public final long offset;
        public final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }
    }

    public static class FetchResult {
        public final byte[][] messages;
        public final String error;

        public FetchResult(byte[][] messages, String error) {
            this.messages = messages;
            this.error = error;
        }
    }

    public static class MetadataResult {
        public final List<BrokerInfo> brokers;
        public final List<TopicMetadata> topics;
        public final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }
    }

    public static class TopicMetadata {
        public final String name;
        public final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }
    }

    public static class PartitionMetadata {
        public final int id;
        public final int leader;
        public final int[] replicas;

        public PartitionMetadata(int id, int leader, int[] replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = replicas;
        }
    }
}