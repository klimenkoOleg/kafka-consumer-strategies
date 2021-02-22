package com.oklimenko.kafka.consumer.demo.consumer;

import com.oklimenko.kafka.consumer.demo.config.AppPropertiesConfig;
import com.oklimenko.kafka.consumer.demo.dto.Payment;
import com.oklimenko.kafka.consumer.demo.exception.AbsUnavailableException;
import com.oklimenko.kafka.consumer.demo.service.AbsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentKafkaConsumer {

    private final KafkaTemplate<String, Payment> kafkaTemplate;
    private final AbsService absService;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final ThreadPoolTaskScheduler taskScheduler;
    private final AppPropertiesConfig appPropertiesConfig;

    @KafkaListener(topics = "#{'${kafka.payment.topics.input}'.split(',')}",
            containerFactory = "paymentKafkaListenerContainerFactory",
            id = "mainListener")
    public void processPayment(@Payload Payment payment,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String msgKey,
                               @Header(KafkaHeaders.GROUP_ID) String groupId,
                               Acknowledgment ack) {
        log.debug(">>> Payment processed: {}", payment);
        try {
            absService.transferPayment(payment);
            ack.acknowledge();
        } catch (Exception e) {
            String destinationTopic;
            if (isFatal(e)) {
                destinationTopic = topic + "._DLQ";
            } else {
                destinationTopic = getRetryDestinationTopic(topic, groupId);
            }
            log.error("Sending due to exception to topic={}, messageKey={}", destinationTopic, msgKey);
            payment.setErrorMessage(e.getMessage());
            ProducerRecord<String, Payment> record = new ProducerRecord<>(destinationTopic, msgKey, payment);
            record.headers().add("retry", "1".getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record);
            ack.acknowledge();
        }
        log.debug("<<< Payment processed: {}", payment);
    }

    private String getRetryDestinationTopic(String topic, String groupId) {
        return "test-retry";
//        return topic + "-" + groupId + "_RETRY";
    }

    private boolean isFatal(Exception e) {
        return !(e instanceof AbsUnavailableException);
    }

    // sleep containerr for rN sec
    // get the n sec from header
    // increase header n sec
    // num tryes - send to header
    //
    @KafkaListener(topics = "test-retry",
            containerFactory = "paymentKafkaListenerContainerFactory", id = "retryListener")
    public void processPaymentRetry(@Payload Payment payment,
                                    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                    @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String msgKey,
                                    @Header(KafkaHeaders.GROUP_ID) String groupId,
                                    @Header("retry") byte[] retryBytes,
                                    Acknowledgment ack) {
        log.debug(">>> Payment processed: {}", payment);
        Integer retry = Integer.valueOf(new String(retryBytes, StandardCharsets.UTF_8));
        try {
            MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("retryListener");
            listenerContainer.stop();
            Long delayMillis = appPropertiesConfig.getRetries().get(retry);
            if (delayMillis == null) {
                delayMillis = 1000L;
            }
            Instant retryAt = Instant.now().plus(delayMillis, ChronoUnit.MILLIS);
            taskScheduler.schedule(listenerContainer::start, retryAt);

            absService.transferPayment(payment);
            ack.acknowledge();
        } catch (Exception e) {
            // todo: logic move to DLQ if appPropertiesConfig.getKafkaPaymentRetries() is the last one
            Integer nextRetry = retry + 1;
            Long delayMillis = appPropertiesConfig.getRetries().get(nextRetry);

            String destinationTopic;
            if (delayMillis == null || isFatal(e)) {
                destinationTopic = topic + "._DLQ";
            } else {
                destinationTopic = getRetryDestinationTopic(topic, groupId);
            }
            log.error("Sending due to exception to topic={}, messageKey={}", destinationTopic, msgKey);
//            payment.setErrorMessage(e.getMessage());
//            kafkaTemplate.send(destinationTopic, msgKey, payment);
            payment.setErrorMessage(e.getMessage());
            ProducerRecord<String, Payment> record = new ProducerRecord<>(destinationTopic, msgKey, payment);
            record.headers().add("retry", String.valueOf(nextRetry).getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record);
            ack.acknowledge();
        }
        log.debug("<<< Payment processed: {}", payment);
    }

    /*@KafkaListener(topics = "test-retry",
            containerFactory = "paymentKafkaListenerContainerFactory")
    public void processPaymentRetry15mins(Payment payment,
                                         @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                         @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String msgKey,
                                         @Header(KafkaHeaders.GROUP_ID) String groupId) {
        log.debug(">>> Payment processed: {}", payment);
        try {
            absService.transferPayment(payment);
        } catch (Exception e) {
            String destinationTopic = topic + "._DLQ";
            log.error("Sending due to exception to topic={}, messageKey={}", destinationTopic, msgKey);
            payment.setErrorMessage(e.getMessage());
            kafkaTemplate.send(destinationTopic, msgKey, payment);
        }
        log.debug("<<< Payment processed: {}", payment);
    }*/


}
