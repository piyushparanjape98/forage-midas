package com.jpmc.midascore;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

/**
 * Programmatic Embedded Kafka configuration for tests.
 * This allows us to control the temp directory used by Kafka.
 */
@TestConfiguration
public class EmbeddedKafkaConfig {
    
    @Bean
    public EmbeddedKafkaBroker embeddedKafka() {
        // Create broker with ephemeral port and custom properties
        EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(
            1,  // numPartitions
            true,  // controlledShutdown
            1   // numBrokers
        );
        
        // Set log directories to avoid system temp
        broker.brokerProperty("log.dirs", "C:/temp/midas-kafka-embedded");
        broker.brokerProperty("log.dir", "C:/temp/midas-kafka-embedded");
        broker.brokerProperty("listeners", "PLAINTEXT://127.0.0.1:0");
        broker.brokerProperty("port", "0");
        
        // Pre-create the directory to avoid permission issues
        try {
            new java.io.File("C:/temp/midas-kafka-embedded").mkdirs();
        } catch (Exception e) {
            // best effort
        }
        
        return broker;
    }
}
