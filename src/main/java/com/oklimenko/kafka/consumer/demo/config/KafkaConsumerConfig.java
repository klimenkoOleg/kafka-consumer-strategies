package com.oklimenko.kafka.consumer.demo.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oklimenko.kafka.consumer.demo.config.kafkaerror.KafkaErrorHandler;
import com.oklimenko.kafka.consumer.demo.dto.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.retry.RetryContext;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.springframework.kafka.listener.adapter.RetryingMessageListenerAdapter.CONTEXT_RECORD;

@Slf4j
@EnableRetry //
@EnableKafka
@Configuration
@ConditionalOnProperty(name = "kafka.consumer.payment.enabled", matchIfMissing = true)
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    public static final String RETRY = "_test-consumer-group_RETRY";

    private final AppPropertiesConfig appPropertiesConfig;
    private final KafkaTemplate<String, Payment> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     *  Kafka consumer factory setup - standard factory.
     * @return JSON factory.
     */
    @Bean
    public ConsumerFactory<String, Payment> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appPropertiesConfig.getKafkaServer());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, appPropertiesConfig.getKafkaConsumerGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        // key.deserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // value.deserializer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // spring.deserializer.key.delegate.class
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, JsonDeserializer.class);

        ErrorHandlingDeserializer<Payment> errorHandlingDeserializer
                = new ErrorHandlingDeserializer<>(new JsonDeserializer<>(Payment.class));

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                errorHandlingDeserializer);
    }

    /**
     * Kafka consumer factory setup - wrapper for concurrency.
     * @return wrapped factory.
     */
    @Bean("paymentKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Payment>
    promoMaterialsKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, Payment> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
//        factory.setErrorHandler(new KafkaErrorHandler());
        factory.setRetryTemplate(kafkaRetry());
        factory.setRecoveryCallback(this::retryOption1); //
        // option 3, minuses:
        // 1) executes retry logic on the main topic
        // 2) you are not free to giving a name for error topic
        // 3) blocks the main consumer while its waiting for the retry -> slow down
        // Option 4
        // factory.setErrorHandler(new SeekToCurrentErrorHandler(
        // new DeadLetterPublishingRecoverer(kafkaTemplate), new FixedBackOff(100, 3);
        // Option 5
        // factory.setErrorHandler(new SeekToCurrentErrorHandler(
        // new DeadLetterPublishingRecoverer(kafkaTemplate),
        // new ExponentialBackOff(100, 1.3D)setRetryTemplate));
        return factory;
    }

    private Object retryOption1(RetryContext retryContext) {
        ConsumerRecord consumerRecord = (ConsumerRecord) retryContext.getAttribute("record");
        log.info("Recovery is called for message {} ", consumerRecord.value());
        return Optional.empty();
    }

    private Object retryOption2(RetryContext retryContext) throws JsonProcessingException {
        ConsumerRecord consumerRecord = (ConsumerRecord) retryContext.getAttribute(CONTEXT_RECORD);
        String errorTopic = consumerRecord.topic().replace("_RETRY", "_ERROR");

        log.info("Sending to the error topic");
        kafkaTemplate.send(errorTopic, consumerRecord.key().toString(),
                objectMapper.readValue(consumerRecord.value().toString(), Payment.class));
        return Optional.empty();
    }

    private RetryTemplate kafkaRetry() {
        RetryTemplate retryTemplate = new RetryTemplate();
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();  // other policies are not better
        fixedBackOffPolicy.setBackOffPeriod(3 * 1000L);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(); // other policies are not better
        retryPolicy.setMaxAttempts(3);
        retryTemplate.setRetryPolicy(retryPolicy);
        return retryTemplate;
    }
}
