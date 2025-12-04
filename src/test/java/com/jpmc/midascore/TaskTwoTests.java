package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@DirtiesContext
@ActiveProfiles("kafka-integration")
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://127.0.0.1:0", "port=0", "log.dirs=C:/temp/midas-kafka-two", "log.dir=C:/temp/midas-kafka-two"})
class TaskTwoTests {
    static final Logger logger = LoggerFactory.getLogger(TaskTwoTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void task_two_verifier() throws InterruptedException {
        System.out.println("=== DIAGNOSTIC: java.io.tmpdir = " + System.getProperty("java.io.tmpdir"));
        System.out.println("=== DIAGNOSTIC: log.dirs configured = C:/temp/midas-kafka-two");
        System.out.println("=== DIAGNOSTIC: midas-kafka-two exists? " + new java.io.File("C:/temp/midas-kafka-two").exists());
        System.out.println("=== DIAGNOSTIC: midas-kafka-two canWrite? " + new java.io.File("C:/temp/midas-kafka-two").canWrite());
        System.out.println("=== DIAGNOSTIC: midas-kafka-two isDirectory? " + new java.io.File("C:/temp/midas-kafka-two").isDirectory());
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
    static {
        try {
            new java.io.File("C:/temp/midas-kafka-two").mkdirs();
        } catch (Exception e) {
            // ignore - best effort
        }
    }

}
