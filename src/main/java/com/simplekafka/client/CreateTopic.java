package com.simplekafka.client;

import com.simplekafka.broker.Protocol;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class CreateTopic {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: CreateTopic <broker-host> <broker-port> <topic>");
            System.exit(1);
        }
        
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        
        try {
            SocketChannel channel = SocketChannel.open(new InetSocketAddress(host, port));
            ByteBuffer request = Protocol.encodeCreateTopicRequest(topic, 3, (short) 2);
            channel.write(request);
            
            ByteBuffer response = ByteBuffer.allocate(1024);
            channel.read(response);
            response.flip();
            
            byte type = response.get();
            short errorCode = response.getShort();
            
            if (errorCode == 0) {
                System.out.println("Topic '" + topic + "' created successfully");
            } else {
                System.out.println("Failed to create topic, error code: " + errorCode);
            }
            
            channel.close();
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
