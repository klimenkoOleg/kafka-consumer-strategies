package com.oklimenko.kafka.consumer.demo.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Payment DTO.
 *
 * @author oklimenko@gmail.com
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Payment {

    private UUID idempotencyKey;
    private BigDecimal amount;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime initiatedOn;

}
