package memcached_sdn.experiment.data;

/**
 * Created by idanmo on 2/27/16.
 */
public class ExperimentData {

    private final String time;
    private final String key;
    private final double latency;
    private final int serverIndex;
    private final int responseServerIndex;
    private final int receivedPacketsCount;
    private final int totalPacketsCount;

    public ExperimentData(String time, String key, double latency, int serverIndex, int responseServerIndex, int receivedPacketsCount, int totalPacketsCount) {
        this.time = time;
        this.key = key;
        this.latency = latency;
        this.serverIndex = serverIndex;
        this.responseServerIndex = responseServerIndex;
        this.receivedPacketsCount = receivedPacketsCount;
        this.totalPacketsCount = totalPacketsCount;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%d,%d,%d/%d", time, key, Double.toString(latency), serverIndex, responseServerIndex, receivedPacketsCount, totalPacketsCount);
    }

}
