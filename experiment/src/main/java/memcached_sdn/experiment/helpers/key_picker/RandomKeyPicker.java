package memcached_sdn.experiment.helpers.key_picker;

import memcached_sdn.experiment.helpers.Helpers;
import memcached_sdn.experiment.helpers.KeyValuePair;

import java.util.List;

/**
 * Created by idanmo on 3/6/16.
 */
public class RandomKeyPicker extends KeyPicker {

    public RandomKeyPicker(List<KeyValuePair<String, String>> objectsList) {
        super(objectsList);
    }

    @Override
    public KeyValuePair<String, String> pickKey() {
        int randomObjectIndex = Helpers.random.nextInt(objectsList.size());
        return objectsList.get(randomObjectIndex);
    }

}
