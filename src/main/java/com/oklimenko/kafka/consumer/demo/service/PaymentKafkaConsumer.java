package com.oklimenko.kafka.consumer.demo.service;

import com.oklimenko.kafka.consumer.demo.dto.Payment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PaymentKafkaConsumer {

    @KafkaListener(topics = "test", groupId = "group_1")
    public void processPayment(Payment payment) {
        log.info("Payment processes: {}", payment);
    }
}
