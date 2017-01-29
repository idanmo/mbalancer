package memcached_sdn.experiment;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import memcached_sdn.experiment.helpers.Helpers;
import memcached_sdn.experiment.helpers.KeyValuePair;
import memcached_sdn.experiment.helpers.key_picker.KeyPicker;
import memcached_sdn.experiment.helpers.key_picker.RandomKeyPicker;
import memcached_sdn.experiment.helpers.key_picker.ZipfKeyPicker;
import memcached_sdn.experiment.memcached.MemcachedUDPClient;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.SerializingTranscoder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOGGER = Logger.getLogger("experiment");

    private static final long DEFAULT_SLEEP = 1000;
    private static final int DEFAULT_OBJECTS_COUNT = 1000;
    private static final int DEFAULT_OBJECT_SIZE = 1000;

    private static OptionSet parseProgramArgument(String[] args) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts(
                "servers", "Comma separated servers list (10.0.0.5:11211,10.0.0.6:11211...")
                .withRequiredArg().required();
        parser.accepts(
                "sleep", "Time in milliseconds to wait after each request.")
                .withOptionalArg().ofType(Long.class).defaultsTo(DEFAULT_SLEEP);
        parser.accepts(
                "write-objects", "Determines whether to write objects to the Memcached servers.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(true);
        parser.accepts(
                "objects-count", "The number of objects to write to the Memcached servers.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(DEFAULT_OBJECTS_COUNT);
        parser.accepts(
                "object-size", "Benchmark objects size in bytes.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(DEFAULT_OBJECT_SIZE);
        parser.accepts(
                "threads", "The number of threads to use for the experiment.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(1);
        parser.accepts(
                "bind", "Interface address to bind sent packets to.")
                .withOptionalArg().ofType(String.class);
        parser.accepts(
                "duration", "The duration in seconds the experiment should end after.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(-1);
        parser.accepts(
                "max-requests", "The maximum number of requests to be performed in the experiment.")
                .withOptionalArg().ofType(Long.class).defaultsTo(-1L);
        parser.accepts(
                "latency-output-file", "The file the latency output will be written to.")
                .withOptionalArg().ofType(String.class).defaultsTo("latency.txt");
        parser.accepts(
                "keys-distribution-method", "Key distribution method to use (random/zipf[-<random-seed>])")
                .withOptionalArg().ofType(String.class).defaultsTo("random");
        parser.accepts(
                "keys-distribution-file", "A previous output file containing keys distribution data.")
                .withOptionalArg().ofType(String.class);
        parser.accepts(
                "exact-requests-count", "Check on every thread that requests count has not been exceeded (may have a performance implication).")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        parser.accepts(
                "skip-sanity", "Skip the sanity check.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        parser.accepts(
                "print-zipf-weights", "Print zipf distribution weights.")
                .withOptionalArg().ofType(Boolean.class).defaultsTo(false);
        parser.accepts(
                "zipf-picker-factor", "Percentage for picking a key between two servers.")
                .withOptionalArg().ofType(Integer.class).defaultsTo(-1);
        try {
            return parser.parse(args);
        } catch (OptionException e) {
            parser.printHelpOn(System.out);
            System.exit(1);
        }
        return null;
    }

    private static List<KeyValuePair<String, String>> createObjectsList(int objectsCount, int objectSize) {
        final List<KeyValuePair<String, String>> objects = new ArrayList<>(objectsCount);
        for (int i = 0; i < objectsCount; i++) {
            String key = String.format("mem-key-%d", i);
            String value = Helpers.generateString(key, objectSize);
            KeyValuePair pair = new KeyValuePair(key, value);
            objects.add(pair);
        }
        return objects;
    }

    private static void performSanityCheck(MemcachedUDPClient client) {
        LOGGER.info("Performing sanity check...");
        client.set("key", "value");
        String result = client.get("key").getValue();
        if (!"value".equals(result)) {
            throw new IllegalStateException("Sanity check failed.");
        }
    }

    private static void runExperiment(OptionSet options) throws InterruptedException {
        final String[] servers = options.valueOf("servers").toString().split(",");
        final boolean writeObjects = Boolean.parseBoolean(options.valueOf("write-objects").toString());
        final int objectsCount = Integer.parseInt(options.valueOf("objects-count").toString());
        final int objectSize = Integer.parseInt(options.valueOf("object-size").toString());
        final long sleepTime = Long.parseLong(options.valueOf("sleep").toString());
        final int numberOfThreads = Integer.parseInt(options.valueOf("threads").toString());
        final String bindAddress = options.valueOf("bind") != null ? options.valueOf("bind").toString() : null;
        final int experimentDuration = Integer.parseInt(options.valueOf("duration").toString());
        final long maximumRequestsCount = Long.parseLong(options.valueOf("max-requests").toString());
        final String latencyFileName = options.valueOf("latency-output-file").toString();
        final String keysDistributionMethod = options.valueOf("keys-distribution-method").toString();
        final String keysDistributionFile = options.hasArgument("keys-distribution-file") ? options.valueOf("keys-distribution-file").toString() : null;
        final boolean exactRequestsCount = Boolean.parseBoolean(options.valueOf("exact-requests-count").toString());
        final boolean skipSanity = Boolean.parseBoolean(options.valueOf("skip-sanity").toString());
        final boolean printZipfWeights = Boolean.parseBoolean(options.valueOf("print-zipf-weights").toString());
        final int zipfPickerFactor = Integer.parseInt(options.valueOf("zipf-picker-factor").toString());

        if (maximumRequestsCount != -1 && experimentDuration != -1) {
            System.out.println("Only one of duration/max-requests can be set.");
            System.exit(1);
        }

        if (keysDistributionFile != null && !new File(keysDistributionFile).exists()) {
            throw new RuntimeException("keys-distribution-file " + keysDistributionFile + " not found.");
        }

        MemcachedUDPClient client = new MemcachedUDPClient(servers, bindAddress, -1);
        if (!skipSanity) {
            performSanityCheck(client);
        }

        LOGGER.info("Creating objects list...");
        final List<KeyValuePair<String, String>> objectsList = createObjectsList(objectsCount, objectSize);

        if (writeObjects) {
            InetSocketAddress[] serverAddresses = new InetSocketAddress[servers.length];
            for (int i = 0; i < servers.length; i++) {
                String[] values = servers[i].split(":");
                serverAddresses[i] = new InetSocketAddress(values[0], Integer.parseInt(values[1]));
            }
            MemcachedClient memcachedClient = null;
            try {
                memcachedClient = new MemcachedClient(serverAddresses);
                SerializingTranscoder transcoder = new SerializingTranscoder();
                transcoder.setCompressionThreshold(Integer.MAX_VALUE);
                LOGGER.info(String.format("Writing %d objects to Memcached...", objectsCount));
                for (KeyValuePair<String, String> pair : objectsList) {
                    memcachedClient.set(pair.getKey(), 0, pair.getValue(), transcoder).get();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (memcachedClient != null) {
                    memcachedClient.shutdown();
                }
            }
        }

        LOGGER.info("Starting experiment main loop...");

        KeyPicker keyPicker;
        if (keysDistributionMethod.equalsIgnoreCase("random")) {
            LOGGER.info("Initializing random key picker");
            keyPicker = new RandomKeyPicker(objectsList);
        } else if (keysDistributionMethod.startsWith("zipf")) {
            LOGGER.info("Initializing zipf key picker [file=" + keysDistributionFile + "]");
            String[] values = keysDistributionMethod.split("-");
            long seed = -1;
            if (values.length > 1) {
                seed = Long.parseLong(values[1]);
            }
            keyPicker = new ZipfKeyPicker(objectsList, keysDistributionFile, seed, zipfPickerFactor);
            if (printZipfWeights) {
                ZipfKeyPicker zkp = (ZipfKeyPicker) keyPicker;
                zkp.printWeightPerServer(objectsList, servers.length, maximumRequestsCount);
                System.exit(1);
            }
        } else {
            throw new IllegalArgumentException("Unrecognized keys distribution method: " + keysDistributionMethod);
        }

        final MemcachedConcurrentClient mcc = new MemcachedConcurrentClient(
                servers,
                numberOfThreads,
                sleepTime,
                bindAddress,
                experimentDuration,
                maximumRequestsCount,
                exactRequestsCount,
                latencyFileName,
                keyPicker,
                LOGGER);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("[CTRL+C] Stopping experiment.");
                try {
                    mcc.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }));

        mcc.run();

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        OptionSet options = parseProgramArgument(args);
        LOGGER.info("Starting the experiment...");
        runExperiment(options);
    }



}
