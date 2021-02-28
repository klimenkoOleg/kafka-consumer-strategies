package com.oklimenko.kafka.consumer.demo.consumer;

import com.oklimenko.kafka.consumer.demo.config.AppPropertiesConfig;
import com.oklimenko.kafka.consumer.demo.dto.Payment;
import com.oklimenko.kafka.consumer.demo.exception.AbsUnavailableException;
import com.oklimenko.kafka.consumer.demo.exception.ConsumerMisconfigurationException;
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
        log.debug(">>> Payment processing started: {}", payment);
        try {
            absService.transferPayment(payment);
        } catch (Exception e) {
            log.error("Exception for messageKey={}", msgKey);
            String destinationTopic = calcDestinationTopic(topic, groupId, e);
            sendToRetry(payment, msgKey, e, destinationTopic, "1");
        }
        ack.acknowledge();
        log.debug("<<< Payment processed: {}", payment);
    }

    @KafkaListener(topics = "test.retry",
            containerFactory = "paymentKafkaListenerContainerFactory", id = "retryListener")
    public void processPaymentRetry(@Payload Payment payment,
                                    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                    @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String msgKey,
                                    @Header(KafkaHeaders.GROUP_ID) String groupId,
                                    @Header("retry") byte[] retryBytes,
                                    Acknowledgment ack) {
        log.debug(">>> Retry Payment processed: {}", payment);
        Integer retry = Integer.valueOf(new String(retryBytes, StandardCharsets.UTF_8));
        Long delayMillis = appPropertiesConfig.getRetries().get(retry);
        log.debug(">>> Retry # {}, delayMillis={}", retry, delayMillis);

        if (delayMillis == null) {
            throw new ConsumerMisconfigurationException("delayMillis is empty for retry # = " + retry);
        }
        try {
            sleepConsumer(delayMillis);
            absService.transferPayment(payment);
        } catch (Exception e) {
            log.error("Exception for messageKey={}", msgKey);
            Integer nextRetry = retry + 1;
            Long nextDelayMillis = appPropertiesConfig.getRetries().get(nextRetry);

            String destinationTopic = calcDestinationTopic(topic, groupId, e, nextDelayMillis);
            sendToRetry(payment, msgKey, e, destinationTopic, String.valueOf(nextRetry));
        }
        ack.acknowledge();
        log.debug("<<< Retry # {}, idempotencyKey={}", retry, payment.getIdempotencyKey());
    }

    private void sendToRetry(@Payload Payment payment, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String msgKey, Exception e, String destinationTopic, String s) {
        payment.setErrorMessage(e.getMessage());
        ProducerRecord<String, Payment> record = new ProducerRecord<>(destinationTopic, msgKey, payment);
        record.headers().add("retry", s.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);
    }

    private void sleepConsumer(Long delayMillis) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("retryListener");
        listenerContainer.stop();
        Instant retryAt = Instant.now().plus(delayMillis, ChronoUnit.MILLIS);
        taskScheduler.schedule(listenerContainer::start, retryAt);
    }

    private String calcDestinationTopic(String topic, String groupId, Exception e) {
        return calcTopicInternal(topic, groupId, e, false);
    }

    private String calcDestinationTopic(String topic, String groupId, Exception e, Long nextDelayMillis) {
        return calcTopicInternal(topic, groupId, e, nextDelayMillis == null);
    }

    private String calcTopicInternal(String topic, String groupId, Exception e, boolean toDlq) {
        String destinationTopic;
        if (toDlq || isFatal(e)) {
            destinationTopic = appPropertiesConfig.getKafkaTopicAccessoryDlq();
            log.error("DLQ sending due to exception to topic={}", destinationTopic);
        } else {
            destinationTopic = appPropertiesConfig.getKafkaTopicAccessoryRetry();
            log.debug("RETRY sending due to exception to topic={}", destinationTopic);
        }
        return destinationTopic;
    }

    private boolean isFatal(Exception e) {
        return !(e instanceof AbsUnavailableException);
    }

}
