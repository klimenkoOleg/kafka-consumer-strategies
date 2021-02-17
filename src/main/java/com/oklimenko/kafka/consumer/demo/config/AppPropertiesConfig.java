package com.oklimenko.kafka.consumer.demo.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Getter
@Component
@Configuration
public class AppPropertiesConfig {
    @Value("${kafka.payment.bootstrapAddress}")
    private String kafkaServer;

    @Value("${kafka.payment.groupId}")
    private String kafkaConsumerGroupId;

    @Value("${kafka.payment.topic}")
    private String kafkaTopicAccessory;
}
