package memcached_sdn.experiment.helpers.key_picker;

import memcached_sdn.experiment.helpers.KeyValuePair;

import java.util.List;

/**
 * Created by idanmo on 3/6/16.
 */
public abstract class KeyPicker {

    protected final List<KeyValuePair<String, String>> objectsList;

    public KeyPicker(List<KeyValuePair<String, String>> objectsList) {

        this.objectsList = objectsList;
    }

    public abstract KeyValuePair<String, String> pickKey();

}
