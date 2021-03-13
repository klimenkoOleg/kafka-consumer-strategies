package com.oklimenko.kafka.consumer.demo.service.impl;

import com.oklimenko.kafka.consumer.demo.dto.Payment;
import com.oklimenko.kafka.consumer.demo.exception.AbsUnavailableException;
import com.oklimenko.kafka.consumer.demo.exception.PaymentInvalidException;
import com.oklimenko.kafka.consumer.demo.service.AbsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Slf4j
@Service
public class AbsServiceImpl implements AbsService {

    @Override
    public void transferPayment(Payment payment) {
        if (BigDecimal.ZERO.compareTo(payment.getAmount()) > 0) {
            log.error("Amount can't be negative, found in Payment");
            throw new PaymentInvalidException("Amount can't be negative, found in Payment=" + payment);
        }
        if (payment.getAmount().compareTo(new BigDecimal("1000000")) > 0) {
            log.error("ABS unavailable right now, sending to retry queue, Payment={}", payment);
            throw new AbsUnavailableException(payment.getIdempotencyKey());
        }
        // invocation of ABS goes here
        log.debug("ABS processed payment successfully");
    }
}
