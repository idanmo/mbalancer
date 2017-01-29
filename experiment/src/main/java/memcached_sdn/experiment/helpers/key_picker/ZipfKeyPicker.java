package memcached_sdn.experiment.helpers.key_picker;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import memcached_sdn.experiment.helpers.Helpers;
import memcached_sdn.experiment.helpers.KeyValuePair;
import net.spy.memcached.DefaultHashAlgorithm;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.util.MathArrays;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by idanmo on 3/6/16.
 */
public class ZipfKeyPicker extends KeyPicker {

    private final Random random;
    private final int pickServerFactor;
    private int[] intWeights;
    private double[] weights;

    public ZipfKeyPicker(List<KeyValuePair<String, String>> objectsList, String dataFile, long seed, int pickServerFactor) {
        super(objectsList);
        this.pickServerFactor = pickServerFactor;
        this.random = (seed != -1) ? Helpers.createRandom(seed) : Helpers.random;
        if (dataFile != null) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(dataFile));
                String weightsLine = reader.readLine();
                if (!weightsLine.startsWith(";")) {
                    throw new IllegalStateException("intWeights line should begin with a ;");
                }
                String[] strWeights = weightsLine.replace(";", "").split(",");
                intWeights = new int[strWeights.length];
                for (int i = 0; i < intWeights.length; i++) {
                    intWeights[i] = Integer.parseInt(strWeights[i]);
                }
                Map<String, KeyValuePair<String, String>> objectsMap = new HashMap<>();
                for (KeyValuePair<String, String> pair : objectsList) {
                    objectsMap.put(pair.getKey(), pair);
                }
                while (!objectsList.isEmpty()) {
                    objectsList.remove(0);
                }
                String keysLine = reader.readLine();
                if (!keysLine.startsWith(";")) {
                    throw new IllegalStateException("keys line should begin with a ;");
                }
                String[] keys = keysLine.replace(";", "").split(",");
                for (int i = 0; i < keys.length; i++) {
                    objectsList.add(objectsMap.get(keys[i]));
                }
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                    }
                }
                this.weights = null;
            }
        } else {
            generateWeights(objectsList);
        }
    }

    private void generateWeights(List<KeyValuePair<String, String>> objectsList) {
        this.weights = new double[objectsList.size()];
        double sum = 0;
        int s = 1;
        for (int i = 1; i <= objectsList.size(); i++) {
            sum += Math.pow(i, -s);
        }
        // Generate zipf distribution
        for (int i = 0; i < weights.length; i++) {
            weights[i] = Math.pow(i+1, -s) / sum;
        }
        // Normalize to natural numbers
        double factor = 1 / weights[weights.length-1];
        intWeights = new int[weights.length];
        for (int i = 0; i < weights.length; i++) {
            //intWeights[i] = (int) Math.round(weights[i] * factor * 1000) - 1000;
            intWeights[i] = (int)(weights[i] * 990000.0);
        }
        // Convert to ranges
        for (int i = intWeights.length - 1; i >= 0; i--) {
            if (i != intWeights.length - 1) {
                intWeights[i] += intWeights[i+1];
            }
        }
//        for (int i = 0; i < intWeights.length; i++) {
//            System.out.println(intWeights[i]);
//        }
        Collections.shuffle(objectsList, this.random);
    }

    @Override
    public KeyValuePair<String, String> pickKey() {
        if (this.pickServerFactor != -1) {
            int randomValue = this.random.nextInt(100);
            int serverIndex = (randomValue < this.pickServerFactor) ? 0 : 1;
            int pickedIndex;
            KeyValuePair<String, String> kv;
            do {
                kv = pickKeyImpl();
                final long hash = DefaultHashAlgorithm.NATIVE_HASH.hash(kv.getKey());
                pickedIndex = (int) (hash % 2);
            } while (pickedIndex != serverIndex);
            return kv;
        }
        return pickKeyImpl();
    }

    private KeyValuePair<String, String> pickKeyImpl() {
        final int maxWeight = intWeights[0];
        final int minWeight = intWeights[intWeights.length-1];
        int randomValue = this.random.nextInt(maxWeight - minWeight) + minWeight;
        for (int i = 1; i < intWeights.length; i++) {
            if (randomValue >= intWeights[i]) {
                return objectsList.get(i-1);
            }
        }
        StringBuilder sb = new StringBuilder(String.format("Random value not in range. randomValue = %d, min = %d, max = %d", randomValue, minWeight, maxWeight));
        sb.append(Joiner.on(",").join(Arrays.asList(intWeights)));
        throw new RuntimeException(sb.toString());
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(intWeights[0]);
        for (int i = 1; i < intWeights.length; i++) {
            sb.append(",");
            sb.append(intWeights[i]);
        }
        sb.append("\n");
        sb.append(objectsList.get(0).getKey());
        for (int i = 1; i < objectsList.size(); i++) {
            sb.append(",");
            sb.append(objectsList.get(i).getKey());
        }
        return sb.toString();
    }

    public void printWeightPerServer(List<KeyValuePair<String, String>> objectsList, int numberOfServers, long requestsCount) {

        double[] sumPerServer = new double[numberOfServers];

        int aaa = 1000000;

        double[] requestsPercentagePerServer = new double[numberOfServers];
        for (int i = 0; i < numberOfServers; i++) {
            requestsPercentagePerServer[i] = 0;
        }

        for (int j = 0; j < aaa; j++) {
            generateWeights(objectsList);
            double[] weightPerServer = new double[numberOfServers];
            double totalWeight = 0;
            for (int i = 0; i < objectsList.size(); i++) {
                final long hash = DefaultHashAlgorithm.NATIVE_HASH.hash(objectsList.get(i).getKey());
                final int index = (int) (hash % numberOfServers);
                totalWeight += this.weights[i];
                weightPerServer[index] += this.weights[i];
            }
            if (weightPerServer[0] < weightPerServer[1]) {
                double ttt = weightPerServer[1];
                weightPerServer[1] = weightPerServer[0];
                weightPerServer[0] = ttt;
            }
            for (int i = 0; i < weightPerServer.length; i++) {
                sumPerServer[i] += weightPerServer[i];
                //System.out.println(String.format("Server #%d: %g", i, weightPerServer[i]));
            }

            int[] requestsPerServer = new int[numberOfServers];
            for (int i = 0; i < numberOfServers; i++) {
                requestsPerServer[i] = 0;
            }
            for (long l = 0; l < requestsCount; l++) {
                final KeyValuePair<String, String> kv = pickKey();
                final long hash = DefaultHashAlgorithm.NATIVE_HASH.hash(kv.getKey());
                final int index = (int) (hash % numberOfServers);
                requestsPerServer[index] += 1;
            }

            if (requestsPerServer[0] > requestsPerServer[1]) {
                int ttt = requestsPerServer[1];
                requestsPerServer[1] = requestsPerServer[0];
                requestsPerServer[0] = ttt;
            }

            int totalRequests = 0;
            for (int i = 0; i < requestsPerServer.length; i++) {
                totalRequests += requestsPerServer[i];
            }

            for (int i = 0; i < requestsPerServer.length; i++) {
                requestsPercentagePerServer[i] += (requestsPerServer[i] / (double) totalRequests) * 100;
            }

        }

        System.out.println("Average:");
        for (int i = 0; i < sumPerServer.length; i++) {
            double avg = sumPerServer[i] / aaa;
            System.out.println(String.format("Server #%d: %g", i, avg));
        }

        for (int i = 0; i < requestsPercentagePerServer.length; i++) {
            System.out.println(String.format("%d: %g", i, requestsPercentagePerServer[i] / aaa));
        }

    }

    private void generateWeights2(List<KeyValuePair<String, String>> objectsList) {
        final ZipfDistribution z = new ZipfDistribution(1000, 1.0);

        this.intWeights = new int[objectsList.size()];
        for (int i = 1; i <= objectsList.size(); i++) {
            System.out.println(z.probability(i) * 1000000);
            intWeights[i-1] = (int) Math.round(z.probability(i) * 1000000);
        }

//        this.intWeights = new int[objectsList.size()];
//        this.intWeights = z.sample(objectsList.size());
//        Arrays.sort(intWeights);
//        final ArrayList<Integer> list = Lists.newArrayListWithCapacity(intWeights.length);
//        for (int i = 0; i < intWeights.length; i++) {
//            list.add(intWeights[i]);
//        }
//        Collections.reverse(list);
//        for (int i = 0; i < intWeights.length; i++) {
//            intWeights[i] = list.get(i);
//        }
        Collections.shuffle(objectsList, this.random);
    }

    public void printWeightPerServer2(List<KeyValuePair<String, String>> objectsList, int numberOfServers) {
        double[] sumPerServer = new double[numberOfServers];

        double aaa = 100;

        for (int j = 0; j < aaa; j++) {
            generateWeights(objectsList);
            double[] weightPerServer = new double[numberOfServers];
            double totalWeight = 0;
            for (int i = 0; i < objectsList.size(); i++) {
                final long hash = DefaultHashAlgorithm.NATIVE_HASH.hash(objectsList.get(i).getKey());
                final int index = (int) (hash % numberOfServers);
                totalWeight += this.intWeights[i];
                weightPerServer[index] += this.intWeights[i];
            }
            if (weightPerServer[0] < weightPerServer[1]) {
                double ttt = weightPerServer[1];
                weightPerServer[1] = weightPerServer[0];
                weightPerServer[0] = ttt;
            }
            for (int i = 0; i < weightPerServer.length; i++) {
                sumPerServer[i] += weightPerServer[i];
            }
        }

        System.out.println("Average:");
        for (int i = 0; i < sumPerServer.length; i++) {
            double avg = sumPerServer[i] / aaa;
            System.out.println(String.format("Server #%d: %g", i, avg));
        }


    }

}
