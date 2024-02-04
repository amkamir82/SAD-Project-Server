package sad.project.broker.service.cordinator;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import sad.project.broker.service.file.PrimarySegmentHandler;

import java.net.URL;


@Getter
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
@Component
public class PrimaryBrokerManager {

    private final PrimarySegmentHandler segmentHandler;

    @Setter
    private int brokerCount;

    @Setter
    private URL replica;

    private final int id;

    @Autowired
    public PrimaryBrokerManager(PrimarySegmentHandler segmentHandler) {
        this.segmentHandler = segmentHandler;
        this.id = Integer.parseInt(System.getenv("MainPartition"));
        this.brokerCount = Integer.parseInt(System.getenv("MainPartition")) + 2;
    }
}
