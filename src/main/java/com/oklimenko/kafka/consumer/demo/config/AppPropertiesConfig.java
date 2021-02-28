package com.oklimenko.kafka.consumer.demo.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Map;

@Getter
@Setter
@Component
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "kafka.payment")
public class AppPropertiesConfig {
    @Value("${kafka.payment.bootstrapAddress}")
    private String kafkaServer;

    @Value("${kafka.payment.groupId}")
    private String kafkaConsumerGroupId;

    @Value("${kafka.payment.topics.input}")
    private String kafkaTopicAccessory;

    @Value("${kafka.payment.topics.dlq}")
    private String kafkaTopicAccessoryDlq;

    @Value("${kafka.payment.topics.retry}")
    private String kafkaTopicAccessoryRetry;

    private Map<Integer, Long> retries;
}
