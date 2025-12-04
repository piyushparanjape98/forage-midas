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
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://127.0.0.1:0", "port=0", "log.dirs=C:/temp/midas-kafka-three", "log.dir=C:/temp/midas-kafka-three"})
public class TaskThreeTests {
    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    static {
        try {
            new java.io.File("C:/temp/midas-kafka-three").mkdirs();
        } catch (Exception e) {
            // ignore - best effort
        }
    }

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void task_three_verifier() throws InterruptedException {
        System.out.println("=== DIAGNOSTIC: java.io.tmpdir = " + System.getProperty("java.io.tmpdir"));
        System.out.println("=== DIAGNOSTIC: log.dirs configured = C:/temp/midas-kafka-three");
        System.out.println("=== DIAGNOSTIC: midas-kafka-three exists? " + new java.io.File("C:/temp/midas-kafka-three").exists());
        System.out.println("=== DIAGNOSTIC: midas-kafka-three canWrite? " + new java.io.File("C:/temp/midas-kafka-three").canWrite());
        System.out.println("=== DIAGNOSTIC: midas-kafka-three isDirectory? " + new java.io.File("C:/temp/midas-kafka-three").isDirectory());
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
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
}
