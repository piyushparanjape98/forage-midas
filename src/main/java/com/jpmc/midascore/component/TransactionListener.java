package com.jpmc.midascore.component;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.jpmc.midascore.foundation.Transaction;

@Component
public class TransactionListener {

    @KafkaListener(topics = "${general.kafka-topic}")
    public void onTransaction(Transaction transaction) {
        // For Task 2 we only need to receive the message.
        // Keep a breakpoint on this line when running TaskTwoTests.
        System.out.println("Received transaction: " + transaction);
    }
}
