package sad.project.broker.service.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import sad.project.broker.service.cordinator.BrokerManager;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class SegmentHandler {

    private final Indexer indexer;
    private final BrokerManager brokerManager;
    private JSONObject lastSegment;

    private SegmentHandler(Indexer indexer, BrokerManager brokerManager) {
        this.indexer = indexer;
        this.brokerManager = brokerManager;
        lastSegment = new JSONObject(); // todo: maybe load from file
    }

    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public static SegmentHandler getInstance() {
        return new SegmentHandler(Indexer.getInstance(), BrokerManager.getInstance());
    }

    public synchronized boolean append(String key, byte[] value) {
        long writeIndex = indexer.getWriteIndex();
        int SEGMENT_SIZE = 10;
        int dataCounterInSegment = (int) (writeIndex % SEGMENT_SIZE);

        if (dataCounterInSegment == SEGMENT_SIZE - 1) {
            lastSegment = new JSONObject();
        }

        long segmentNumber = (long) Math.floor((double) writeIndex / SEGMENT_SIZE) + 1;
        String segmentPath = getSegmentPath(segmentNumber);
        try {
            lastSegment.put(key, value);
            try (FileWriter fileWriter = new FileWriter(segmentPath)) {
                fileWriter.write(lastSegment.toString());
            } catch (IOException e) {
                e.printStackTrace();
                throw e;
            }
            sendDataToReplica(key, value);
            return true;
        } catch (Exception e) {
            lastSegment.remove(key);
            // todo: it must be logged somewhere
            return false;
        }
    }

    @SneakyThrows
    private void sendDataToReplica(String key, byte[] value) {
        String segmentReplicaUrl = brokerManager.getReplicaAddress();
        Map<String, byte[]> data = new HashMap<>();
        data.put("key", key.getBytes(StandardCharsets.UTF_8));
        data.put("value", value);

        URL url = new URL(segmentReplicaUrl);

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonString = objectMapper.writeValueAsString(data);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        try (OutputStream outputStream = connection.getOutputStream()) {
            outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8));
        }
        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            throw new Exception(""); // todo
        }
        connection.disconnect();
    }


    private String getSegmentPath(long segmentNumber) {
        int brokerId = brokerManager.getBrokerId();
        return "partition_" + brokerId + "/segment_" + segmentNumber;
    }

}
