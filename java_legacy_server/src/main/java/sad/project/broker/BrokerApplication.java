package sad.project.broker;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import sad.project.broker.service.file.Writer;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class BrokerApplication {

    public static void main(String[] args) {
        SpringApplication.run(BrokerApplication.class, args);
    }

    @Bean
    public CommandLineRunner demo(Writer writer) {
        return (args) -> {
            // Example data
            String key = "exampleKey";
            byte[] value = "exampleValue".getBytes();
            System.err.println(key);

            // Create a map entry with the example data
            Map.Entry<String, byte[]> dataEntry = Map.entry(key, value);

            // Use the Writer service to write the data
            boolean result = writer.write(dataEntry);

            // Check the result
            if (result) {
                System.out.println("Data written successfully.");
            } else {
                System.out.println("Failed to write data.");
            }
        };
    }
}
