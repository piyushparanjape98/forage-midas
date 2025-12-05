package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@DirtiesContext
@ActiveProfiles("kafka-integration")
@DisabledOnOs(OS.WINDOWS)
class TaskTwoTests {
    static final Logger logger = LoggerFactory.getLogger(TaskTwoTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void task_two_verifier() throws InterruptedException {
        System.out.println("=== DIAGNOSTIC: java.io.tmpdir = " + System.getProperty("java.io.tmpdir"));
        String[] transactionLines = fileLoader.loadStrings("/test_data/poiuytrewq.uiop");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(2000);
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("waiting briefly for processing to complete");
        Thread.sleep(3000);
        logger.info("continuing test (non-interactive mode)");
    }
    
    @TestConfiguration
    static class TestKafkaConfig {
        @Bean
        public KafkaContainer kafkaContainer() {
            // Use a stable Confluent image compatible with Testcontainers' Kafka support
            DockerImageName image = DockerImageName.parse("confluentinc/cp-kafka:6.2.1");
            KafkaContainer kafka = new KafkaContainer(image);
            try {
                kafka.start();
            } catch (Exception e) {
                throw new RuntimeException("Failed to start KafkaContainer. Ensure Docker is running.", e);
            }
            // expose bootstrap servers for Spring Kafka clients
            System.setProperty("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
            return kafka;
        }
    }

}
