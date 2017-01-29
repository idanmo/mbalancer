package memcached_sdn.experiment.memcached;

import com.google.common.collect.Maps;
import net.spy.memcached.DefaultHashAlgorithm;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Random;

/**
 * Created by idanmo on 1/9/16.
 */
public class MemcachedUDPClient {

    private final Map<String, Integer> serverToIndex;
    private InetAddress[] servers;
    private int[] ports;
    private DatagramSocket[] sockets;
    private short nextRequestId = (short) new Random().nextInt(32767);
    private String bindAddress = null;
    private int threadId = -1;


    public MemcachedUDPClient(final String[] servers) {
        this.servers = new InetAddress[servers.length];
        this.ports = new int[servers.length];
        this.serverToIndex = Maps.newHashMap();
        for (int i = 0; i < servers.length; i++) {
            String[] values = servers[i].split(":");
            try {
                this.servers[i] = InetAddress.getByName(values[0]);
                serverToIndex.put(this.servers[i].getHostAddress(), i);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            if (values.length == 1) {
                this.ports[i] = 11211;
            } else {
                this.ports[i] = Integer.parseInt(values[1]);
            }
        }
        this.sockets = new DatagramSocket[servers.length];
    }

    public MemcachedUDPClient(String[] servers, String bindAddress, int threadId) {
        this(servers);
        this.bindAddress = bindAddress;
        this.threadId = threadId;
    }

    static void print(int threadId, String message) {
        Timestamp timestamp = new Timestamp(new java.util.Date().getTime());
        String thread = (threadId == -1)? "MAIN" : String.valueOf(threadId);
        System.out.println(String.format("[%s] [t:%s] %s", timestamp, thread, message));
    }

    private short getNextRequestId() {
        short requestId = this.nextRequestId;
        this.nextRequestId++;
        if (this.nextRequestId > 30000) {
            this.nextRequestId = 0;
        }
        return requestId;
    }

    protected int pickServer(String key) {
        final long hash = DefaultHashAlgorithm.NATIVE_HASH.hash(key);
        return (int) (hash % servers.length);
    }

    private void verifySocketCreated(int serverIndex) {
        if (this.sockets[serverIndex] == null) {
            try {
                DatagramSocket socket;
                if (this.bindAddress != null) {
                    //int port = UDP_SOURCE_PORT + this.threadId;
                    int port = 0;
                    print(this.threadId, String.format("memcached client bound to: %s:%d", this.bindAddress, port));
                    socket = new DatagramSocket(new InetSocketAddress(this.bindAddress, port));
                } else {
                    socket = new DatagramSocket();
                }
                socket.setSoTimeout(1000);
                socket.setReceiveBufferSize(5000000);
                this.sockets[serverIndex] = socket;

            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void set(String key, String value) {
        int serverIndex = this.pickServer(key);
        this.verifySocketCreated(serverIndex);
        short requestId = getNextRequestId();
        String data = String.format(
                "set %s 0 0 %d\r\n%s\r\n", key, value.length(), value);

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        buffer.putShort(requestId);
        buffer.putShort((short)0x0000);
        buffer.putShort((short)0x0001);
        buffer.putShort((short)0x0000);
        buffer.put(data.getBytes());

        InetAddress address = this.servers[serverIndex];
        int port = this.ports[serverIndex];
        DatagramPacket sendPacket = new DatagramPacket(buffer.array(), buffer.position(), address, port);
        DatagramSocket socket = this.sockets[serverIndex];
        try {
            socket.send(sendPacket);
            byte[] receiveData = new byte[4096];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            socket.receive(receivePacket);
            byte[] receivedData = receivePacket.getData();
            String response = new String(receivedData);
            short receivedRequestId = getRequestIdFromHeader(receivedData);
            if (requestId != receivedRequestId) {
                throw new RuntimeException(String.format("sentRequestId != receivedRequestId [%d != %d]", requestId, receivedRequestId));
            }
            if (!response.contains("STORED")) {
                throw new RuntimeException(String.format("Error storing key %s: %s", key, response));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private short getRequestIdFromHeader(byte[] header) {
        ByteBuffer buf = ByteBuffer.wrap(header);
        //buf.flip();
        return buf.getShort();
    }

    public MemcachedUDPResult get(String key) {
        int serverIndex = this.pickServer(key);
        this.verifySocketCreated(serverIndex);
        short requestId = getNextRequestId();
        String data = String.format(
                "get %s\r\n", key);

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        buffer.putShort(requestId);
        buffer.putShort((short)0x0000);
        buffer.putShort((short)0x0001);
        buffer.putShort((short)0x0000);
        buffer.put(data.getBytes());

        InetAddress address = this.servers[serverIndex];
        int port = this.ports[serverIndex];
        DatagramPacket sendPacket = new DatagramPacket(buffer.array(), buffer.position(), address, port);
        DatagramSocket socket = this.sockets[serverIndex];
        //TraceableLogger.append(">>> sending request -> key=" + key + ", request.id=" + requestId + ", destination=" + address.toString() + ":" + port);
        short totalNumberOfPackets = -1;
        short packetsCounter = 0;
        int responseServerIndex = -1;
        try {
            socket.send(sendPacket);
            byte[] receiveData = new byte[4096];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

            short sequenceNumber = -1;

            String[] responseArray = null;

            while (sequenceNumber + 1 != totalNumberOfPackets) {
                socket.receive(receivePacket);
                byte[] receivedData = receivePacket.getData();
                String response = new String(receivedData).substring(0, receivePacket.getLength());

                ByteBuffer responseBuffer = ByteBuffer.wrap(receivedData);
                short receivedRequestId = responseBuffer.getShort();
                if (requestId != receivedRequestId) {
                    throw new RuntimeException(String.format("sentRequestId != receivedRequestId [%d != %d]", requestId, receivedRequestId));
                }

                packetsCounter++;
                sequenceNumber = responseBuffer.getShort();
                totalNumberOfPackets = responseBuffer.getShort();

                if (responseServerIndex == -1) {
                    responseServerIndex = serverToIndex.get(receivePacket.getAddress().getHostAddress());
                }

//                TraceableLogger.append(">>> packet >>> start " + requestId);
//                TraceableLogger.append("packet.length=" + receivePacket.getLength());
//                TraceableLogger.append("request.id=" + requestId);
//                TraceableLogger.append("* request.id=" + receivedRequestId);
//                TraceableLogger.append("sequence.number=" + sequenceNumber);
//                TraceableLogger.append("total_packets=" + totalNumberOfPackets);
//                TraceableLogger.append("response.length=" + response.length());
//                TraceableLogger.append(">>> packet >>> end " + requestId);

                response = response.substring(8);

                if (responseArray == null) {
                    responseArray = new String[totalNumberOfPackets];
                }

                responseArray[sequenceNumber] = response;
            }

            return new MemcachedUDPResult(responseArray, packetsCounter, totalNumberOfPackets, serverIndex, responseServerIndex);

        } catch (IOException e) {
            if (e instanceof SocketTimeoutException) {
                throw new PacketLostException(
                        key,
                        requestId,
                        serverIndex,
                        responseServerIndex,
                        servers[serverIndex].toString(),
                        ports[serverIndex],
                        packetsCounter,
                        totalNumberOfPackets);
            } else {
                throw new RuntimeException(e);
            }
        }


    }

    public void close() {
        for (DatagramSocket ds : this.sockets) {
            if (ds != null) {
                ds.close();
            }
        }
    }

    public static class PacketLostException extends RuntimeException {
        public final String key;
        public final int requestId;
        public final int serverIndex;
        public final int responseServerIndex;
        public final String serverAddress;
        public final int port;
        public final short receivedPacketsCount;
        public final short totalNumberOfPackets;

        public PacketLostException(String key, int requestId, int serverIndex, int responseServerIndex, String serverAddress, int port, short receivedPacketsCount, short totalNumberOfPackets) {
            super(String.format("Error in get operation [key=%s, requestId=%d, serverIndex=%d, server=%s:%d, receivedPackets=%d, totalPackets=%d]",
                    key,
                    requestId,
                    serverIndex,
                    serverAddress,
                    port,
                    receivedPacketsCount,
                    totalNumberOfPackets));
            this.key = key;
            this.requestId = requestId;
            this.serverIndex = serverIndex;
            this.responseServerIndex = responseServerIndex;
            this.serverAddress = serverAddress;
            this.port = port;
            this.receivedPacketsCount = receivedPacketsCount;
            this.totalNumberOfPackets = totalNumberOfPackets;
        }
    }


}

