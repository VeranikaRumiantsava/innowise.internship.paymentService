package org.innowise.internship.payment_service.dto;

import lombok.Getter;
import lombok.Setter;
import org.innowise.internship.payment_service.entities.PaymentStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Setter
@Getter
public class ResponsePaymentDTO {
    private String id;
    private Long orderId;
    private Long userId;
    private PaymentStatus status;
    private LocalDateTime timestamp;
    private BigDecimal paymentAmount;
}
