package memcached_sdn.experiment.helpers;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 * Created by idanmo on 1/9/16.
 */
public class Helpers {

    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static Random random = new Random();

    public static String generateRandomString(int len){
        StringBuilder sb = new StringBuilder(len);
        for( int i = 0; i < len; i++ )
            sb.append(AB.charAt(random.nextInt(AB.length())));
        return sb.toString();
    }

    public static Random createRandom(long seed) {
        return new Random(seed);
    }

    public static String generateString(String key, int len) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(key.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            while (sb.length() < len) {
                StringBuffer buf = new StringBuffer();
                for (int j = 0; j < 32; j++) {
                    for (byte b : digest) {
                        buf.append(String.format("%02x", b & 0xff));
                    }
                }
                sb.append(buf.toString());
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

    }

}
