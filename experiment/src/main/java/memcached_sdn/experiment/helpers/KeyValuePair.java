package memcached_sdn.experiment.helpers;

/**
 * Created by idanmo on 1/23/16.
 */
public class KeyValuePair<TKey, TValue> {

    private final TKey key;
    private final TValue value;

    public KeyValuePair(TKey key, TValue value) {
        this.key = key;
        this.value = value;
    }

    public TKey getKey() {
        return key;
    }

    public TValue getValue() {
        return value;
    }
}
