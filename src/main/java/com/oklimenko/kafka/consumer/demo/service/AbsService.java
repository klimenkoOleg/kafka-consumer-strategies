package com.oklimenko.kafka.consumer.demo.service;

import com.oklimenko.kafka.consumer.demo.dto.Payment;

public interface AbsService {
    void transferPayment(Payment payment);
}
