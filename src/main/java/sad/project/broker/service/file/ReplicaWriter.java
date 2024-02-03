//package sad.project.broker.service.file;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import lombok.SneakyThrows;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.stereotype.Service;
//import sad.project.broker.service.cordinator.BrokerManager;
//
//import java.io.OutputStream;
//import java.net.HttpURLConnection;
//import java.net.URL;
//import java.nio.charset.StandardCharsets;
//import java.util.HashMap;
//import java.util.Map;
//
//@Service
//public class ReplicaWriter {
//
//    private final URL address;
//    private final int partition;
//    public ReplicaWriter(URL address, int partition) {
//        this.address = address;
//        this.partition = partition;
//    }
//
//    @Autowired
//    public ReplicaWriter(@Qualifier("replicaBrokerManager") BrokerManager brokerManager, HashManager hashManager) {
//        this.brokerManager = brokerManager;
//        this.hashManager = hashManager;
//    }
//
//    @SneakyThrows
//    public void write(String key, byte[] value) {
//        Map<String, byte[]> data = new HashMap<>();
//        data.put("key", key.getBytes(StandardCharsets.UTF_8));
//        data.put("value", value);
//        data.put("partition", String.valueOf(partition).getBytes());
//
//
//        ObjectMapper objectMapper = new ObjectMapper();
//        String jsonString = objectMapper.writeValueAsString(data);
//
//        HttpURLConnection connection = (HttpURLConnection) address.openConnection();
//        connection.setRequestMethod("POST");
//        connection.setDoOutput(true);
//
//        connection.connect();
//        try (OutputStream outputStream = connection.getOutputStream()) {
//            outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8));
//        }
//        int responseCode = connection.getResponseCode();
//        if (responseCode != 200) {
//            return;
////            throw new Exception(""); // todo
//        }
//        connection.disconnect();
//    }
//}
