package memcached_sdn.experiment.memcached;

/**
 * Created by idanmo on 3/11/16.
 */
public class MemcachedUDPResult {
    private final String[] responseArray;
    private final short packetsCount;
    private final short totalNumberOfPackets;
    private final int serverIndex;
    private final int responseServerIndex;

    public MemcachedUDPResult(String[] responseArray, short packetsCount, short totalNumberOfPackets, int serverIndex, int responseServerIndex) {
        this.responseArray = responseArray;
        this.packetsCount = packetsCount;
        this.totalNumberOfPackets = totalNumberOfPackets;
        this.serverIndex = serverIndex;
        this.responseServerIndex = responseServerIndex;
    }

    public String getValue() {
        final StringBuffer concatenatedResult = new StringBuffer();
        for (String s : responseArray) {
            concatenatedResult.append(s);
        }
        String[] values = concatenatedResult.toString().split("\r\n");
        if (values.length > 0 && values[0].startsWith("VALUE")) {
            for (int i = 1; i < values.length; i++) {
                if (values[i].equals("END")) {
                    return values[i-1];
                }
            }
        }
        throw new IllegalStateException();
    }

    public int getServerIndex() {
        return serverIndex;
    }

    public int getResponseServerIndex() {
        return responseServerIndex;
    }

    public int getPacketsLost() {
        return totalNumberOfPackets - packetsCount;
    }

    public boolean hasPacketsLost() {
        return getPacketsLost() > 0;
    }

    public short getTotalNumberOfPackets() {
        return totalNumberOfPackets;
    }

    public short getPacketsCount() {
        return packetsCount;
    }
}
