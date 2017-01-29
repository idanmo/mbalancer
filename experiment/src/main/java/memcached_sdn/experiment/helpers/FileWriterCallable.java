package memcached_sdn.experiment.helpers;

import com.google.common.base.Joiner;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by idanmo on 1/15/16.
 */
public class FileWriterCallable<T> implements Callable<Object> {

    private final Queue<T> data;
    private final String filename;
    private final AtomicBoolean stop;
    private final long maxEntriesToWrite;
    private final String fileHeader;
    private final String[] servers;
    private AtomicLong writesCounter = new AtomicLong(0);

    public FileWriterCallable(Queue<T> data, String filename, AtomicBoolean stop, long maxEntriesToWrite, String fileHeader, String[] servers) {
        this.data = data;
        this.filename = filename;
        this.stop = stop;
        this.maxEntriesToWrite = maxEntriesToWrite;
        this.fileHeader = fileHeader;
        this.servers = servers;
    }

    public long getWritesCount() {
        return this.writesCounter.get();
    }

    @Override
    public Object call() throws Exception {
        BufferedWriter bw = new BufferedWriter(new FileWriter(this.filename));
        try {
            for (String header : this.fileHeader.split("\n")) {
                bw.write(";");
                bw.write(header);
                bw.write("\n");
            }
            bw.write(";");
            bw.write(Joiner.on(",").join(this.servers));
            bw.write("\n");
            System.out.println("Starting asyc file writer...");
            while (!stop.get()) {
                if (!data.isEmpty()) {
                    T item = data.poll();
                    bw.write(String.format("%s\n", item.toString()));
                    writesCounter.incrementAndGet();
                    if (maxEntriesToWrite != -1 && writesCounter.get() == maxEntriesToWrite) {
                        break;
                    }
                } else {
                    Thread.sleep(20);
                }
            }
            System.out.println("Async file writer terminated.");
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (bw != null) {
                bw.flush();
                bw.close();
            }
        }
    }
}
