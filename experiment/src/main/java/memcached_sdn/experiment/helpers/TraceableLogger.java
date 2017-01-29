package memcached_sdn.experiment.helpers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by idanmo on 3/10/16.
 */
public class TraceableLogger {

    private static Map<Long, StringBuffer> buffers = new ConcurrentHashMap<>();

    public static void append(String text) {
        StringBuffer sb = buffers.get(Thread.currentThread().getId());
        if (sb == null) {
            sb = new StringBuffer();
            buffers.put(Thread.currentThread().getId(), sb);
        }
        sb.append(text);
        sb.append("\n");
    }

    public static void dump() {
        StringBuffer sb = buffers.get(Thread.currentThread().getId());
        if (sb != null) {
            System.out.println("======================");
            System.out.println(sb.toString());
            System.out.println("======================");
            buffers.remove(Thread.currentThread().getId());
        }

    }

}
