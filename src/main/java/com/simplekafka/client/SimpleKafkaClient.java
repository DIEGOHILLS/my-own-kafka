package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleKafkaClient {
    private final String bootstrapBroker;
    private final int bootstrapPort;
    private final Map<String, Protocol.TopicMetadata> topicMetadata;
    private SocketChannel bootstrapChannel;

    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.topicMetadata = new ConcurrentHashMap<>();
    }

    public void initialize() throws IOException {
        refreshMetadata();
    }

    public void refreshMetadata() throws IOException {
        if (bootstrapChannel == null || !bootstrapChannel.isOpen()) {
            bootstrapChannel = SocketChannel.open(new InetSocketAddress(bootstrapBroker, bootstrapPort));
        }

        ByteBuffer request = Protocol.encodeMetadataRequest();
        bootstrapChannel.write(request);

        ByteBuffer responseBuffer = ByteBuffer.allocate(65536);
        int bytesRead = bootstrapChannel.read(responseBuffer);
        if (bytesRead > 0) {
            responseBuffer.flip();
            Protocol.MetadataResult result = Protocol.decodeMetadataResponse(responseBuffer);

            if (result.error == null) {
                for (BrokerInfo broker : result.brokers) {
                    // Store broker info for routing
                }
                for (Protocol.TopicMetadata tm : result.topics) {
                    topicMetadata.put(tm.name, tm);
                }
            }
        }
    }

    public long send(String topic, int partition, byte[] message) throws IOException {
        Protocol.TopicMetadata tm = topicMetadata.get(topic);
        if (tm == null) {
            refreshMetadata();
            tm = topicMetadata.get(topic);
            if (tm == null) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        Protocol.PartitionMetadata pm = null;
        for (Protocol.PartitionMetadata p : tm.partitions) {
            if (p.id == partition) {
                pm = p;
                break;
            }
        }

        if (pm == null) {
            throw new IOException("Partition not found: " + partition);
        }

        SocketChannel brokerChannel = null;
        try {
            brokerChannel = SocketChannel.open(new InetSocketAddress(bootstrapBroker, bootstrapPort));

            ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, message);
            brokerChannel.write(request);

            ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
            brokerChannel.read(responseBuffer);
            responseBuffer.flip();

            Protocol.ProduceResult result = Protocol.decodeProduceResponse(responseBuffer);
            if (result.error != null) {
                throw new IOException("Produce failed: " + result.error);
            }

            return result.offset;

        } finally {
            if (brokerChannel != null) {
                brokerChannel.close();
            }
        }
    }

    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        Protocol.TopicMetadata tm = topicMetadata.get(topic);
        if (tm == null) {
            refreshMetadata();
            tm = topicMetadata.get(topic);
            if (tm == null) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        Protocol.PartitionMetadata pm = null;
        for (Protocol.PartitionMetadata p : tm.partitions) {
            if (p.id == partition) {
                pm = p;
                break;
            }
        }

        if (pm == null) {
            throw new IOException("Partition not found: " + partition);
        }

        SocketChannel brokerChannel = null;
        try {
            brokerChannel = SocketChannel.open(new InetSocketAddress(bootstrapBroker, bootstrapPort));

            ByteBuffer request = Protocol.encodeFetchRequest(topic, partition, offset, maxBytes);
            brokerChannel.write(request);

            ByteBuffer responseBuffer = ByteBuffer.allocate(maxBytes + 1024);
            brokerChannel.read(responseBuffer);
            responseBuffer.flip();

            Protocol.FetchResult result = Protocol.decodeFetchResponse(responseBuffer);
            if (result.error != null) {
                throw new IOException("Fetch failed: " + result.error);
            }

            List<byte[]> messages = new ArrayList<>();
            for (byte[] msg : result.messages) {
                messages.add(msg);
            }
            return messages;

        } finally {
            if (brokerChannel != null) {
                brokerChannel.close();
            }
        }
    }

    public void close() {
        try {
            if (bootstrapChannel != null && bootstrapChannel.isOpen()) {
                bootstrapChannel.close();
            }
        } catch (IOException e) {
            // Ignore close errors
        }
    }
}