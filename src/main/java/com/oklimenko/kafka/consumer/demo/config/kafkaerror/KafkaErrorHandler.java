package com.oklimenko.kafka.consumer.demo.config.kafkaerror;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ContainerAwareErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaErrorHandler implements ContainerAwareErrorHandler {

    public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {
        log.error("Error processing STARTED");
        this.doSeeks(records, consumer);
        if (!records.isEmpty()) {
            ConsumerRecord<?, ?> record = records.get(0);
            String topic = record.topic();
            long offset = record.offset();
            int partition = record.partition();
            Optional<DeserializationException> deserializationException = isDeserializationException(thrownException);

            if (deserializationException.isPresent()) {
                DeserializationException exception = deserializationException.get();
                String malformedMessage = new String(exception.getData());
                log.error("Kafka error. Skipping message with topic {} and offset {} - malformed message: {} , exception: {}",
                        topic, offset, malformedMessage, exception.getLocalizedMessage());

            } else {
                log.error("Kafka error. Skipping message with topic {} - offset {} - partition {} - exception {}",
                        topic, offset, partition, thrownException);
            }
        } else {
            log.error("Consumer exception - cause: {}", thrownException.getMessage());
        }

    }

    private Optional<DeserializationException> isDeserializationException(Exception thrownException) {
        if (thrownException.getClass().equals(DeserializationException.class)) {
            return Optional.of((DeserializationException)thrownException);
        }
        if (thrownException.getCause()!=null &&
                        DeserializationException.class.equals(thrownException.getCause().getClass())) {
            return Optional.of((DeserializationException)thrownException.getCause());
        }
        return Optional.empty();
    }

    private void doSeeks(List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer) {
        Map<TopicPartition, Long> partitions = new LinkedHashMap<>();
        AtomicBoolean first = new AtomicBoolean(true);
        records.forEach((record) -> {
            if (first.get()) {
                partitions.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1L);
            } else {
                partitions.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), (offset) -> {
                    return record.offset();
                });
            }

            first.set(false);
        });
        partitions.forEach(consumer::seek);
    }
}
