package sad.project.broker.service.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public abstract class SegmentHandler {
    protected Indexer indexer;
    protected JSONObject lastSegment;

    private final int SEGMENT_SIZE = 100;

    protected SegmentHandler(Indexer indexer) {
        this.indexer = indexer;
        this.lastSegment = loadLastSegment();
    }


    public synchronized boolean append(String key, byte[] value) {
        System.err.println("here");
        long writeIndex = indexer.getWriteIndex();
        int dataCounterInSegment = (int) (writeIndex % SEGMENT_SIZE);

        if (dataCounterInSegment == SEGMENT_SIZE - 1) {
            lastSegment = new JSONObject();
        }

        long segmentNumber = getCurrentSegmentNumber();
        String segmentPath = getSegmentPath(segmentNumber);
        try {
            lastSegment.put(key, value);
            try (FileWriter fileWriter = new FileWriter(segmentPath)) {
                fileWriter.write(lastSegment.toString());
            } catch (IOException e) {
                e.printStackTrace();
                throw e;
            }
            indexer.increaseWriteIndex();
            return true;
        } catch (Exception e) {
            lastSegment.remove(key);
            e.printStackTrace();
            // todo: it must be logged somewhere
        }
        return false;
    }

    public void removeElement(String key) throws IOException {
        lastSegment.remove(key);
        indexer.decreaseWriteIndex();
        try (FileWriter fileWriter = new FileWriter(getSegmentPath(getCurrentSegmentNumber()))) {
            fileWriter.write(lastSegment.toString());
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }


    private String getSegmentPath(long segmentNumber) {
        return "partition_" + indexer.getPartition() + "/segment_" + segmentNumber;
    }

    @SneakyThrows
    private JSONObject loadLastSegment() {
        String filePath = getSegmentPath(getCurrentSegmentNumber());

        File file = new File(filePath);
        if (!file.exists()) {
            return new JSONObject();
        }

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(file);
        return new JSONObject(jsonNode.toString());
    }

    private long getCurrentSegmentNumber() {
        long writeIndex = indexer.getWriteIndex();
        return (long) Math.floor((double) writeIndex / SEGMENT_SIZE) + 1;
    }

    ;
}
