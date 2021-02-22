package com.oklimenko.kafka.consumer.demo.exception;

public class ConsumerMisconfigurationException extends RuntimeException {
    public ConsumerMisconfigurationException(String msg) {
        super(msg);
    }
}
