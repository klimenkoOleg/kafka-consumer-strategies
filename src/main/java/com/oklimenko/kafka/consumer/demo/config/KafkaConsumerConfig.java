package com.oklimenko.kafka.consumer.demo.config;

import com.oklimenko.kafka.consumer.demo.config.kafkaerror.KafkaErrorHandler;
import com.oklimenko.kafka.consumer.demo.dto.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.util.Strings;
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

@Slf4j
@EnableRetry //
@EnableKafka
@Configuration
@ConditionalOnProperty(name = "kafka.consumer.payment.enabled", matchIfMissing = true)
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final AppPropertiesConfig appPropertiesConfig;
    private final KafkaTemplate<String, Payment> kafkaTemplate;

    /**
     *  Kafka consumer factory setup - standard factory.
     * @return JSON factory.
     */
    @Bean
    public ConsumerFactory<String, Payment> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appPropertiesConfig.getKafkaServer());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, appPropertiesConfig.getKafkaConsumerGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

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
        factory.setRetryTemplate(kafkaRetry());
        factory.setRecoveryCallback(this::retryOption1);
        factory.setErrorHandler(new KafkaErrorHandler());
        return factory;
    }

    private Object retryOption1(RetryContext retryContext) {
        ConsumerRecord<String, Payment> consumerRecord = (ConsumerRecord) retryContext.getAttribute("record");
        Payment value = consumerRecord.value();
        log.info("Recovery is called for message {} ", value);
        if (Boolean.TRUE.equals(retryContext.getAttribute(RetryContext.EXHAUSTED))) {
            log.info("MOVED TO ERROR DLQ");
            value.setErrorMessage(getThrowableSafely(retryContext));
            kafkaTemplate.send( appPropertiesConfig.getKafkaTopicAccessoryDlq(),
                    consumerRecord.key(),
                    consumerRecord.value() );
        }
        return Optional.empty();
    }

    private String getThrowableSafely(RetryContext retryContext) {
        Throwable lastThrowable = retryContext.getLastThrowable();
        if (lastThrowable == null) {
            return Strings.EMPTY;
        }
        return lastThrowable.getMessage();
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
