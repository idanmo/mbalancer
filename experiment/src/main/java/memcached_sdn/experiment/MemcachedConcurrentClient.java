package memcached_sdn.experiment;

import com.google.common.math.LongMath;
import memcached_sdn.experiment.data.ExperimentData;
import memcached_sdn.experiment.helpers.FileWriterCallable;
import memcached_sdn.experiment.helpers.KeyValuePair;
import memcached_sdn.experiment.helpers.key_picker.KeyPicker;
import memcached_sdn.experiment.memcached.MemcachedUDPClient;
import memcached_sdn.experiment.memcached.MemcachedUDPResult;

import java.util.Calendar;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by idanmo on 1/12/16.
 */
public class MemcachedConcurrentClient {


    private final Thread[] threads;
    private final int duration;
    private final long minimumRequestsCount;
    private final boolean exactRequestsCount;
    private final String latencyFileName;
    private final KeyPicker keyPicker;
    private final Logger logger;
    private final Queue<ExperimentData> latenciesQueue = new ConcurrentLinkedQueue<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicInteger globalRequestsCount = new AtomicInteger(0);
    private final FileWriterCallable latenciesFileWriter;
    private final long[] requestsCountPerThread;
    private long startTime = -1;

    public MemcachedConcurrentClient(final String[] servers,
                                     int numberOfThreads,
                                     final long sleepTime,
                                     final String bindAddress,
                                     int duration,
                                     final long minimumRequestsCount,
                                     boolean exactRequestsCount,
                                     String latencyFileName,
                                     final KeyPicker keyPicker,
                                     Logger logger) {
        this.duration = duration;
        this.minimumRequestsCount = minimumRequestsCount;
        this.exactRequestsCount = exactRequestsCount;
        this.latencyFileName = latencyFileName;
        this.keyPicker = keyPicker;
        this.latenciesFileWriter= new FileWriterCallable(
                this.latenciesQueue, latencyFileName, this.stop, minimumRequestsCount, keyPicker.toString(), servers);
        this.logger = logger;
        this.requestsCountPerThread = new long[numberOfThreads];
        this.threads = new Thread[numberOfThreads];
        for (int i = 0; i < this.threads.length; i++) {
            final int threadId = i;
            final boolean validateRequestsCount = this.exactRequestsCount;
            this.threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    final MemcachedUDPClient client = new MemcachedUDPClient(servers, bindAddress, threadId);
                    while (!stop.get()) {
                        try {
                            if (validateRequestsCount && globalRequestsCount.get() >= minimumRequestsCount) {
                                break;
                            }
                            KeyValuePair<String, String> pair = keyPicker.pickKey();
                            long currentNanoTime = System.nanoTime();
                            Calendar cal = Calendar.getInstance();
                            try {
                                MemcachedUDPResult result = client.get(pair.getKey());
                                double elapsedTime = (System.nanoTime() - currentNanoTime) / (double) 1000000;
                                latenciesQueue.add(new ExperimentData(
                                        String.format("%d:%d:%d:%d", cal.get(Calendar.HOUR), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND)),
                                        pair.getKey(),
                                        elapsedTime,
                                        result.getServerIndex(),
                                        result.getResponseServerIndex(),
                                        result.getPacketsCount(),
                                        result.getTotalNumberOfPackets()));
                                if (result.hasPacketsLost()) {
                                    latenciesQueue.add(new ExperimentData(
                                            String.format("%d:%d:%d:%d", cal.get(Calendar.HOUR), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND)),
                                            pair.getKey(),
                                            -1.0,
                                            result.getServerIndex(),
                                            result.getResponseServerIndex(),
                                            result.getPacketsCount(),
                                            result.getTotalNumberOfPackets()));
                                }
                            } catch (MemcachedUDPClient.PacketLostException e) {
                                latenciesQueue.add(new ExperimentData(
                                        String.format("%d:%d:%d:%d", cal.get(Calendar.HOUR), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND)),
                                        pair.getKey(),
                                        -1.0,
                                        e.serverIndex,
                                        e.responseServerIndex,
                                        e.receivedPacketsCount,
                                        e.totalNumberOfPackets));
                                //TraceableLogger.dump();
                                e.printStackTrace();
                            } finally {
                                if (stop.get()) {
                                    break;
                                }
                                if (validateRequestsCount) {
                                    globalRequestsCount.incrementAndGet();
                                }
                                requestsCountPerThread[threadId] += 1;
                                Thread.sleep(sleepTime);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }


                }
            });
        }
    }

    public void run() throws InterruptedException {

        if (this.minimumRequestsCount == 0) {
            return;
        }

        this.startTime = System.currentTimeMillis();

        this.executorService.submit(latenciesFileWriter);
        for (Thread t : this.threads) {
            t.start();
        }

        long deadline = System.currentTimeMillis() + (this.duration * 1000);

        this.logger.info("Experiment started!");

        while (!stop.get()) {
            Thread.sleep(1000L);
            if (this.duration != -1 && System.currentTimeMillis() > deadline) {
                this.logger.info("Deadline.");
                stop();
            }

            if (minimumRequestsCount != -1 && latenciesFileWriter.getWritesCount() >= minimumRequestsCount) {
                logger.info("Minimum requests count exceeded. Ending experiment...");
                stop();
            }

        }

        if (this.exactRequestsCount) {
            logger.info(String.format("Actual requests count: %d", this.globalRequestsCount.get()));
        }

        StringBuilder msg = new StringBuilder("Requests count per thread:\n");
        long totalRequests = 0;
        for (int i = 0; i < requestsCountPerThread.length; i++) {
            msg.append(String.format(" - %3d: %d\n", i, requestsCountPerThread[i]));
            totalRequests += requestsCountPerThread[i];
        }
        msg.append("Total requests: " + totalRequests);
        logger.info(msg.toString());
    }

    public synchronized void stop() throws InterruptedException {
        if (this.stop.get()) {
            return;
        }
        this.stop.set(true);
        System.out.println("Waiting for all threads to end...");
        for (Thread t : this.threads) {
//            System.out.println("Waiting for thread: " + t.getName());
            t.join();
//            System.out.println("Thread: " + t.getName() + " terminated.");
        }
        executorService.shutdown();
        System.out.println("Waiting for executor service to terminate...");
        executorService.awaitTermination(30, TimeUnit.SECONDS);
        System.out.println("Executor service terminated.");
        double time = (System.currentTimeMillis() - this.startTime) / (double) 1000;
        System.out.println("Done in " + time + " seconds.");

    }
}
