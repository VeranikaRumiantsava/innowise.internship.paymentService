package org.innowise.internship.payment_service.dto.kafka;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Setter
@Getter
public class PaymentDTO {
    private Long orderId;
    private String status;
    private BigDecimal amount;
}
