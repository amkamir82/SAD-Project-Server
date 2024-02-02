package sad.project.broker.service.cordinator;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;


@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class BrokerManager {
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public static BrokerManager getInstance() {
        return new BrokerManager();
    }

    public int getBrokerCount() {
        return 3;
    }

    public int getBrokerId() {
        return 1;
    }

    public String getReplicaAddress() {
        return null;
    }
}
