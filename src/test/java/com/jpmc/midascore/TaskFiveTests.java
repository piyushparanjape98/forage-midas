package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Balance;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@DirtiesContext
@ActiveProfiles("kafka-integration")
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://127.0.0.1:0", "port=0", "log.dirs=C:/temp/midas-kafka-five", "log.dir=C:/temp/midas-kafka-five"})
public class TaskFiveTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFiveTests.class);

    static {
        try {
            new java.io.File("C:/temp/midas-kafka-five").mkdirs();
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

    @Autowired
    private BalanceQuerier balanceQuerier;


    @Test
    void task_five_verifier() throws InterruptedException {
        System.out.println("=== DIAGNOSTIC: java.io.tmpdir = " + System.getProperty("java.io.tmpdir"));
        System.out.println("=== DIAGNOSTIC: log.dirs configured = C:/temp/midas-kafka-five");
        System.out.println("=== DIAGNOSTIC: midas-kafka-five exists? " + new java.io.File("C:/temp/midas-kafka-five").exists());
        System.out.println("=== DIAGNOSTIC: midas-kafka-five canWrite? " + new java.io.File("C:/temp/midas-kafka-five").canWrite());
        System.out.println("=== DIAGNOSTIC: midas-kafka-five isDirectory? " + new java.io.File("C:/temp/midas-kafka-five").isDirectory());
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/rueiwoqp.tyruei");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(2000);

        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("submit the following output to complete the task (include begin and end output denotations)");
        StringBuilder output = new StringBuilder("\n").append("---begin output ---").append("\n");
        for (int i = 0; i < 13; i++) {
            Balance balance = balanceQuerier.query((long) i);
            output.append(balance.toString()).append("\n");
        }
        output.append("---end output ---");
        logger.info(output.toString());
    }
}
