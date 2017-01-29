import memcached_sdn.experiment.helpers.Helpers;
import memcached_sdn.experiment.memcached.MemcachedUDPClient;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.SerializingTranscoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.assertEquals;

/**
 * Created by idanmo on 2/21/16.
 */
public class MemcachedClientTest {

    private MemcachedUDPClient client;

    @Before
    public void setUp() throws Exception {
        client = new MemcachedUDPClient(new String[]{"localhost:11211"});
    }

    @After
    public void tearDown() throws Exception {
        client.close();
    }

    @Test
    public void testReadWriteObject() {
        client.set("key", "value");
        assertEquals("value", client.get("key"));
    }

    @Test
    public void testReadWriteLargeObject() throws IOException, ExecutionException, InterruptedException {
        MemcachedClient memcachedClient = new MemcachedClient(new InetSocketAddress("localhost", 11211));
        String value = Helpers.generateString("test", 100000);
        SerializingTranscoder transcoder = new SerializingTranscoder();
        transcoder.setCompressionThreshold(Integer.MAX_VALUE);
        OperationFuture<Boolean> set = memcachedClient.set("key", 0, value, transcoder);
        set.get();
        System.out.println("value:");
        System.out.println(value);
        System.out.println("length=" + value.length());
        System.out.println();
        String result = client.get("key").getValue();
        assertEquals(value.length(), result.length());
        assertEquals(value, result);
    }

}
