package org.innowise.internship.payment_service.dto;

import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.innowise.internship.payment_service.entities.PaymentStatus;

import java.math.BigDecimal;

@Setter
@Getter
public class UpdatePaymentDTO {
    private PaymentStatus status;

    @Positive(message = "paymentAmount must be positive")
    private BigDecimal paymentAmount;
}
