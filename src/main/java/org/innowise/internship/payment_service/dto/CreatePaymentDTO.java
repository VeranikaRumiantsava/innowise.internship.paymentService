package org.innowise.internship.payment_service.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.innowise.internship.payment_service.entities.PaymentStatus;

import java.math.BigDecimal;

@Getter
@Setter
public class CreatePaymentDTO {
    @NotNull(message = "orderId is required")
    private Long orderId;

    @NotNull(message = "userId is required")
    private Long userId;

    @NotNull(message = "status is required")
    private PaymentStatus status;

    @NotNull(message = "paymentAmount is required")
    @Positive(message = "paymentAmount must be positive")
    private BigDecimal paymentAmount;

}
