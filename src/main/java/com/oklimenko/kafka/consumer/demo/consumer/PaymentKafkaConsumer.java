package com.oklimenko.kafka.consumer.demo.consumer;

import com.oklimenko.kafka.consumer.demo.dto.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentKafkaConsumer {

    @KafkaListener(topics = "#{'${kafka.payment.topics.input}'.split(',')}",
            containerFactory = "paymentKafkaListenerContainerFactory")
    public void processPayment(Payment payment,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String msgKey) {
        log.debug(">>> Payment processing started: {}", payment);
        if (BigDecimal.ZERO.compareTo(payment.getAmount()) > 0) {
            log.error("Amount can't be negative, found in Payment");
            throw new RuntimeException("Amount can't be negative, found in Payment=" + payment);
        }
        log.debug("<<< Payment processed: {}", payment);
    }
}
