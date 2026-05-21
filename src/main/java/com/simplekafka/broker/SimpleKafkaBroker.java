package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleKafkaBroker {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = "data";
    private static final int MAX_BYTES = 1024 * 1024;

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final Map<String, Map<Integer, Partition>> topics;
    private final ExecutorService executor;
    private final ServerSocketChannel serverChannel;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isController;
    private final Map<Integer, BrokerInfo> clusterMetadata;
    private final ZookeeperClient zkClient;

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(10);
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();

        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        this.zkClient = new ZookeeperClient("localhost", zkPort);
    }

    public void start() throws IOException {
        isRunning.set(true);

        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(brokerHost, brokerPort));

        registerWithZookeeper();
        electController();
        loadExistingTopics();

        executor.submit(this::acceptConnections);

        LOGGER.info("Broker " + brokerId + " started on " + brokerHost + ":" + brokerPort);
    }

    public void stop() {
        isRunning.set(false);
        try {
            serverChannel.close();

            for (Map<Integer, Partition> partitions : topics.values()) {
                for (Partition partition : partitions.values()) {
                    partition.close();
                }
            }

            executor.shutdown();
            zkClient.close();

            LOGGER.info("Broker " + brokerId + " stopped");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error stopping broker", e);
        }
    }

    private void registerWithZookeeper() {
        try {
            zkClient.connect();

            String brokerData = "{\"host\":\"" + brokerHost + "\",\"port\":" + brokerPort + ",\"endpoints\":[]}";
            zkClient.createEphemeralNode("/brokers/ids/" + brokerId, brokerData);

            clusterMetadata.put(brokerId, new BrokerInfo(brokerId, brokerHost, brokerPort));

            zkClient.watchChildren("/brokers/ids", this::onBrokersChanged);

            LOGGER.info("Registered broker " + brokerId + " with ZooKeeper");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
        }
    }

    private void onBrokersChanged(List<String> brokerIds) {
        try {
            for (String idStr : brokerIds) {
                int id = Integer.parseInt(idStr);
                if (!clusterMetadata.containsKey(id)) {
                    String data = zkClient.getData("/brokers/ids/" + idStr);
                    if (data != null) {
                        String host = extractJsonValue(data, "host");
                        int port = Integer.parseInt(extractJsonValue(data, "port"));
                        clusterMetadata.put(id, new BrokerInfo(id, host, port));
                    }
                }
            }

            clusterMetadata.keySet().removeIf(id -> !brokerIds.contains(String.valueOf(id)));

            if (isController.get()) {
                rebalancePartitions();
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error handling broker changes", e);
        }
    }

    private String extractJsonValue(String json, String key) {
        String searchKey = "\"" + key + "\":\"";
        int start = json.indexOf(searchKey);
        if (start == -1) {
            searchKey = "\"" + key + "\":";
            start = json.indexOf(searchKey);
            if (start == -1) return "";
            start += searchKey.length();
            int end = json.indexOf(",", start);
            if (end == -1) end = json.indexOf("}", start);
            return json.substring(start, end).replace("\"", "").trim();
        }
        start += searchKey.length();
        int end = json.indexOf("\"", start);
        return json.substring(start, end);
    }

    private void electController() {
        try {
            String controllerPath = "/controller";

            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) {
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                }
            }

            boolean becameController = false;
            if (!nodeExists) {
                becameController = zkClient.createEphemeralNode(controllerPath, String.valueOf(brokerId));
            }

            if (becameController) {
                isController.set(true);
                LOGGER.info("Broker " + brokerId + " became controller");
                rebalancePartitions();
            } else {
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed", e);
            executor.submit(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    private void onControllerChange() {
        LOGGER.info("Controller changed, re-evaluating");
        if (!isController.get()) {
            electController();
        }
    }

    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        if (!isController.get()) {
            LOGGER.warning("Only controller can create topics");
            return;
        }

        try {
            String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
            new File(topicDir).mkdirs();

            String topicConfig = "{\"partitions\":" + numPartitions + ",\"replication_factor\":" + replicationFactor + "}";
            zkClient.createPersistentNode("/topics/" + topic, topicConfig);

            Map<Integer, Partition> partitionMap = new ConcurrentHashMap<>();
            List<Integer> brokerIds = new ArrayList<>(clusterMetadata.keySet());

            for (int i = 0; i < numPartitions; i++) {
                int leader = brokerIds.get(i % brokerIds.size());

                List<Integer> followers = new ArrayList<>();
                for (int j = 1; j < replicationFactor && j < brokerIds.size(); j++) {
                    followers.add(brokerIds.get((i + j) % brokerIds.size()));
                }

                String partitionDir = topicDir + File.separator + i;
                Partition partition = new Partition(i, partitionDir, leader, followers);
                partitionMap.put(i, partition);

                String partitionState = "{\"leader\":" + leader + ",\"isr\":" + brokerIds + ",\"replicas\":" + followers + "}";
                zkClient.createPersistentNode("/topics/" + topic + "/partitions/" + i + "/state", partitionState);
            }

            topics.put(topic, partitionMap);
            zkClient.createPersistentNode("/topics/" + topic + "/notification", "created");

            LOGGER.info("Created topic " + topic + " with " + numPartitions + " partitions");

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create topic", e);
        }
    }

    private void loadExistingTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");
            for (String topic : topicNames) {
                loadTopic(topic);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load existing topics", e);
        }
    }

    private void loadTopic(String topic) throws Exception {
        if (topics.containsKey(topic)) {
            return;
        }

        String topicData = zkClient.getData("/topics/" + topic);
        if (topicData == null) {
            return;
        }

        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
        new File(topicDir).mkdirs();

        List<String> partitionIds = zkClient.getChildren("/topics/" + topic + "/partitions");
        Map<Integer, Partition> partitionMap = new ConcurrentHashMap<>();

        for (String partitionIdStr : partitionIds) {
            int partitionId = Integer.parseInt(partitionIdStr);
            String stateData = zkClient.getData("/topics/" + topic + "/partitions/" + partitionId + "/state");

            int leader = brokerId;
            List<Integer> followers = new ArrayList<>();

            if (stateData != null) {
                String leaderStr = extractJsonValue(stateData, "leader");
                if (!leaderStr.isEmpty()) {
                    leader = Integer.parseInt(leaderStr);
                }
            }

            String partitionDir = topicDir + File.separator + partitionId;
            Partition partition = new Partition(partitionId, partitionDir, leader, followers);
            partitionMap.put(partitionId, partition);
        }

        topics.put(topic, partitionMap);
        LOGGER.info("Loaded topic " + topic);
    }

    private void rebalancePartitions() {
        if (!isController.get()) {
            return;
        }

        try {
            for (Map.Entry<String, Map<Integer, Partition>> topicEntry : topics.entrySet()) {
                String topic = topicEntry.getKey();

                for (Map.Entry<Integer, Partition> partitionEntry : topicEntry.getValue().entrySet()) {
                    int partitionId = partitionEntry.getKey();
                    Partition partition = partitionEntry.getValue();

                    int currentLeader = partition.getLeader();
                    if (!clusterMetadata.containsKey(currentLeader)) {
                        List<Integer> availableBrokers = new ArrayList<>(clusterMetadata.keySet());
                        if (availableBrokers.isEmpty()) continue;

                        int newLeader = availableBrokers.get(0);
                        partition.setLeader(newLeader);

                        List<Integer> newFollowers = new ArrayList<>();
                        for (int i = 1; i < availableBrokers.size(); i++) {
                            newFollowers.add(availableBrokers.get(i));
                        }
                        partition.setFollowers(newFollowers);

                        String partitionState = "{\"leader\":" + newLeader + ",\"isr\":" + availableBrokers + ",\"replicas\":" + newFollowers + "}";
                        zkClient.createPersistentNode("/topics/" + topic + "/partitions/" + partitionId + "/state", partitionState);
                    }
                }
            }

            LOGGER.info("Rebalanced partitions");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to rebalance partitions", e);
        }
    }

    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partition = buffer.getInt();
        int msgLen = buffer.getInt();
        byte[] message = new byte[msgLen];
        buffer.get(message);

        Map<Integer, Partition> partitions = topics.get(topic);
        if (partitions == null) {
            Protocol.sendErrorResponse(clientChannel, "Topic not found: " + topic);
            return;
        }

        Partition p = partitions.get(partition);
        if (p == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition not found: " + partition);
            return;
        }

        if (p.getLeader() != brokerId) {
            BrokerInfo leaderBroker = clusterMetadata.get(p.getLeader());
            if (leaderBroker != null) {
                forwardToLeader(leaderBroker, clientChannel, buffer);
                return;
            } else {
                Protocol.sendErrorResponse(clientChannel, "Leader not available");
                return;
            }
        }

        long offset = p.append(message);
        replicateToFollowers(topic, p, message, offset);

        ByteBuffer response = ByteBuffer.allocate(1 + 8 + 2);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.putShort((short) 0);
        response.flip();
        clientChannel.write(response);
    }

    private void forwardToLeader(BrokerInfo leader, SocketChannel clientChannel, ByteBuffer buffer) {
        try {
            SocketChannel leaderChannel = SocketChannel.open(new InetSocketAddress(leader.getHost(), leader.getPort()));
            buffer.flip();
            leaderChannel.write(buffer);

            ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
            leaderChannel.read(responseBuffer);
            responseBuffer.flip();
            clientChannel.write(responseBuffer);
            leaderChannel.close();

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward to leader", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to leader");
        }
    }

    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        List<Integer> followers = partition.getFollowers();

        for (int followerId : followers) {
            executor.submit(() -> {
                try {
                    BrokerInfo follower = clusterMetadata.get(followerId);
                    if (follower == null) return;

                    SocketChannel followerChannel = SocketChannel.open(
                            new InetSocketAddress(follower.getHost(), follower.getPort()));

                    ByteBuffer replicateReq = Protocol.encodeReplicateRequest(topic, partition.getId(), offset, message);
                    followerChannel.write(replicateReq);

                    ByteBuffer ackBuffer = ByteBuffer.allocate(1024);
                    followerChannel.read(ackBuffer);
                    followerChannel.close();

                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Replication to follower " + followerId + " failed", e);
                }
            });
        }
    }

    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int msgLen = buffer.getInt();
        byte[] message = new byte[msgLen];
        buffer.get(message);

        Map<Integer, Partition> partitions = topics.get(topic);
        if (partitions == null) {
            Protocol.sendErrorResponse(clientChannel, "Topic not found");
            return;
        }

        Partition p = partitions.get(partition);
        if (p == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition not found");
            return;
        }

        p.append(message);

        ByteBuffer response = ByteBuffer.allocate(1 + 2);
        response.put((byte) 0x21);
        response.putShort((short) 0);
        response.flip();
        clientChannel.write(response);
    }

    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();

        Map<Integer, Partition> partitions = topics.get(topic);
        if (partitions == null) {
            Protocol.sendErrorResponse(clientChannel, "Topic not found: " + topic);
            return;
        }

        Partition p = partitions.get(partition);
        if (p == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition not found: " + partition);
            return;
        }

        if (offset < 0 || offset >= p.getNextOffset()) {
            ByteBuffer response = ByteBuffer.allocate(1 + 4 + 2);
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0);
            response.putShort((short) 0);
            response.flip();
            clientChannel.write(response);
            return;
        }

        List<byte[]> messages = p.readMessages(offset, maxBytes);

        int responseSize = 1 + 4;
        for (byte[] msg : messages) {
            responseSize += 8 + 4 + msg.length;
        }
        responseSize += 2;

        ByteBuffer response = ByteBuffer.allocate(responseSize);
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());

        long currentOffset = offset;
        for (byte[] msg : messages) {
            response.putLong(currentOffset++);
            response.putInt(msg.length);
            response.put(msg);
        }
        response.putShort((short) 0);
        response.flip();
        clientChannel.write(response);
    }

    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        List<BrokerInfo> brokers = new ArrayList<>(clusterMetadata.values());

        List<Protocol.TopicMetadata> topicMetadataList = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Partition>> topicEntry : topics.entrySet()) {
            String topicName = topicEntry.getKey();
            List<Protocol.PartitionMetadata> partitionMetadataList = new ArrayList<>();

            for (Map.Entry<Integer, Partition> partitionEntry : topicEntry.getValue().entrySet()) {
                int partitionId = partitionEntry.getKey();
                Partition p = partitionEntry.getValue();
                int[] replicas = new int[p.getFollowers().size() + 1];
                replicas[0] = p.getLeader();
                for (int i = 0; i < p.getFollowers().size(); i++) {
                    replicas[i + 1] = p.getFollowers().get(i);
                }
                partitionMetadataList.add(new Protocol.PartitionMetadata(partitionId, p.getLeader(), replicas));
            }

            topicMetadataList.add(new Protocol.TopicMetadata(topicName, partitionMetadataList));
        }

        int responseSize = 1 + 4;
        for (BrokerInfo broker : brokers) {
            responseSize += 4 + 4 + broker.getHost().getBytes().length + 4;
        }
        responseSize += 4;
        for (Protocol.TopicMetadata tm : topicMetadataList) {
            responseSize += 4 + tm.name.getBytes().length + 4;
            for (Protocol.PartitionMetadata pm : tm.partitions) {
                responseSize += 4 + 4 + 4 + pm.replicas.length * 4;
            }
        }
        responseSize += 2;

        ByteBuffer response = ByteBuffer.allocate(responseSize);
        response.put(Protocol.METADATA_RESPONSE);
        response.putInt(brokers.size());
        for (BrokerInfo broker : brokers) {
            response.putInt(broker.getId());
            byte[] hostBytes = broker.getHost().getBytes();
            response.putInt(hostBytes.length);
            response.put(hostBytes);
            response.putInt(broker.getPort());
        }
        response.putInt(topicMetadataList.size());
        for (Protocol.TopicMetadata tm : topicMetadataList) {
            byte[] nameBytes = tm.name.getBytes();
            response.putInt(nameBytes.length);
            response.put(nameBytes);
            response.putInt(tm.partitions.size());
            for (Protocol.PartitionMetadata pm : tm.partitions) {
                response.putInt(pm.id);
                response.putInt(pm.leader);
                response.putInt(pm.replicas.length);
                for (int replica : pm.replicas) {
                    response.putInt(replica);
                }
            }
        }
        response.putShort((short) 0);
        response.flip();
        clientChannel.write(response);
    }

    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        if (!isController.get()) {
            try {
                String controllerData = zkClient.getData("/controller");
                int controllerId = Integer.parseInt(controllerData);
                BrokerInfo controller = clusterMetadata.get(controllerId);
                if (controller != null) {
                    forwardToLeader(controller, clientChannel, buffer);
                    return;
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to forward to controller", e);
            }
            Protocol.sendErrorResponse(clientChannel, "No controller available");
            return;
        }

        createTopic(topic, numPartitions, replicationFactor);

        ByteBuffer response = ByteBuffer.allocate(1 + 2);
        response.put(Protocol.CREATE_TOPIC_RESPONSE);
        response.putShort((short) 0);
        response.flip();
        clientChannel.write(response);
    }

    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        try {
            loadTopic(topic);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topic from notification", e);
        }
    }

    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    clientChannel.configureBlocking(false);
                    executor.submit(() -> handleClient(clientChannel));
                }
                Thread.sleep(100);
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(65536);
            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);
                if (bytesRead > 0) {
                    buffer.flip();
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    clientChannel.close();
                    break;
                }
                Thread.sleep(50);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Client handler error", e);
        } finally {
            try {
                if (clientChannel.isOpen()) {
                    clientChannel.close();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing client channel", e);
            }
        }
    }

    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();
        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case (byte) 0x21:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case (byte) 0x22:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.out.println("Usage: SimpleKafkaBroker <broker-id> <host> <port> <zk-port>");
            System.out.println("Example: SimpleKafkaBroker 1 localhost 9091 2181");
            System.exit(1);
        }

        int brokerId = Integer.parseInt(args[0]);
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        int zkPort = Integer.parseInt(args[3]);

        SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, host, port, zkPort);
        broker.start();

        Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));
    }
}