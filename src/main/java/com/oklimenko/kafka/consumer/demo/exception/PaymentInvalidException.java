package com.oklimenko.kafka.consumer.demo.exception;

public class PaymentInvalidException extends RuntimeException {
    public PaymentInvalidException(String msg) {
        super(msg);
    }
}
