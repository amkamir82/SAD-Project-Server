package sad.project.broker.service.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import sad.project.broker.service.cordinator.PrimaryBrokerManager;


import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Service
public class Writer {
    private final PrimaryBrokerManager brokerManager;
    private final HashManager hashManager;

    private HttpURLConnection replicaConnection;

    @Autowired
    public Writer(PrimaryBrokerManager brokerManager, HashManager hashManager) throws IOException {
        this.brokerManager = brokerManager;
        this.hashManager = hashManager;
//        this.replicaConnection = (HttpURLConnection) brokerManager.getReplica().openConnection();
//        replicaConnection.setRequestMethod("POST");
//        replicaConnection.setDoOutput(true);
    }

    @SneakyThrows
    public synchronized boolean write(Map.@NotNull Entry<String, byte[]> data) {
        String key = data.getKey();
        byte[] value = data.getValue();
        System.err.println(key);
        System.err.println(Arrays.toString(value));

        int brokerCount = brokerManager.getBrokerCount();
        String keyHash = hashManager.getMessageHash(key);
        System.err.println(keyHash.hashCode());


        if (keyHash.hashCode() % brokerCount != brokerManager.getId()) {
            return false;
        }
        System.err.println("here i am");
        boolean appended = brokerManager.getSegmentHandler().append(key, value);
        System.err.println("here i was");

        System.err.println(appended);
        if (!appended) {
            return false;
        }
//        if (!sendDataToReplica(key, value)) {
//            brokerManager.getSegmentHandler().removeElement(key);
//        }

        return true;
    }

//    @SneakyThrows
//    private boolean sendDataToReplica(String key, byte[] value) {
//        return true;
//        Map<String, byte[]> data = new HashMap<>();
//        data.put("key", key.getBytes(StandardCharsets.UTF_8));
//        data.put("value", value);
//        data.put("partition", String.valueOf(brokerManager.getId()).getBytes());
//
//        try {
//            ObjectMapper objectMapper = new ObjectMapper();
//            String jsonString = objectMapper.writeValueAsString(data);
//
//            try (OutputStream outputStream = replicaConnection.getOutputStream()) {
//                outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8));
//            }
//            int responseCode = replicaConnection.getResponseCode();
//            if (responseCode != 200) {
//                return false;
////            throw new Exception(""); // todo
//            }
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
}
