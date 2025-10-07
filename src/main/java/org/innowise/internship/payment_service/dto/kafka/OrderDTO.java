package org.innowise.internship.payment_service.dto.kafka;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Setter
@Getter
public class OrderDTO {
    private Long orderId;
    private Long userId;
    private BigDecimal amount;
}
