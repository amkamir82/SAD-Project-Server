package sad.project.broker.service.file;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class AppConfig {

//
//    @Bean("mainPartition")
//    public int mainPartition() {
//        return Integer.parseInt(System.getenv("MainPartition"));
//    }
//
//    @Bean("replicaPartition")
//    public int replicaPartition() {
//        return Integer.parseInt(System.getenv("ReplicaPartition"));
//    }
//
//    @Bean("mainIndexer")
//    public Indexer mainIndexer(@Qualifier("mainPartition") int partition) {
//        return new Indexer(partition);
//    }
//
//    @Bean("replicaIndexer")
//    public Indexer replicaIndexer(@Qualifier("replicaPartition") int partition) {
//        return new Indexer(partition);
//    }


    @Bean("primarySegmentHandler")
    public PrimarySegmentHandler mainSegmentHandler() {
        return new PrimarySegmentHandler();
    }

//    @Bean("replicaSegmentHandler")
//    public SegmentHandler replicaSegmentHandler(@Qualifier("replicaIndexer") Indexer indexer) {
//        return new SegmentHandler(indexer);
//    }
}