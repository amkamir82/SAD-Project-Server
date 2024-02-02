package sad.project.broker.service.file;

import lombok.Getter;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

@Getter
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class Indexer {
    private long writeIndex;
    private long readIndex;
    private long syncIndex;

    private Indexer() {
        writeIndex = 0; // todo: it should be saved somewhere
        readIndex = 0;
        syncIndex = 0;
    }

    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public static Indexer getInstance() {
        return new Indexer();
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
