package sad.project.broker.service.file;

import lombok.Getter;

@Getter
public class PrimaryIndexer {
    private long writeIndex;
    private long readIndex;
    private long syncIndex;

    private final int partition;

    public PrimaryIndexer(int partition) {
        this.partition = partition;
        writeIndex = 0;
        readIndex = 0;
        syncIndex = 0;
    }

    public void increaseSyncIndex() {
        this.syncIndex += 1;
    }

    public void increaseReadIndex() {
        this.readIndex += 1;
    }

    public void increaseWriteIndex() {
        this.writeIndex += 1;
    }

}
