package sad.project.broker.service.file;

import org.jetbrains.annotations.NotNull;
import sad.project.broker.service.cordinator.BrokerManager;


import java.util.Map;

public class Writer {
    public boolean write(Map.@NotNull Entry<String, byte[]> data) {
        String key = data.getKey();
        byte[] value = data.getValue();

        BrokerManager brokerManager = BrokerManager.getInstance();
        int brokerCount = brokerManager.getBrokerCount();
        String keyHash = HashManager.getInstance().getMessageHash(key);

        int id = brokerManager.getBrokerId();
        if (keyHash.hashCode() % brokerCount != id) {
            return false;
        }

        return SegmentHandler.getInstance().append(key, value);
    }
}
