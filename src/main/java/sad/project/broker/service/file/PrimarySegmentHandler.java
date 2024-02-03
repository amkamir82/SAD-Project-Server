package sad.project.broker.service.file;

import lombok.Getter;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;

@Getter
@Scope("singleton")
@Component
public class PrimarySegmentHandler extends SegmentHandler {
    public PrimarySegmentHandler() {
        super(new Indexer(Integer.parseInt(System.getenv("MainPartition"))));
    }
}


