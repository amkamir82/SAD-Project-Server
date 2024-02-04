package sad.project.broker.service.file;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Getter
public class Indexer {
    @Getter
    private long writeIndex;
    private long readIndex;
    private long syncIndex;

    private final int partition;

    public Indexer(int partition) {
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

    public void decreaseWriteIndex() {
        this.writeIndex -= 1;
    }

}
