package com.oklimenko.kafka.consumer.demo.consumer;

import com.oklimenko.kafka.consumer.demo.dto.Payment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PaymentKafkaConsumer {
    @KafkaListener(topics = "#{'${kafka.payment.topic}'.split(',')}",
            containerFactory = "paymentKafkaListenerContainerFactory"
    )
    public void processPayment(Payment payment) {
//        System.out.println();
        log.info("Payment processed: {}", payment );
//        System.out.println("Payment p rocessed: " + payment );
    }

}
