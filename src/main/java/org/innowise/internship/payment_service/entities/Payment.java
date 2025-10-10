package org.innowise.internship.payment_service.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Document(collection = "payments")
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Payment {
    @Id
    private String id;

    @Indexed
    private Long orderId;

    @Indexed
    private Long userId;

    @Indexed
    private PaymentStatus status;

    @Indexed
    private LocalDateTime timestamp;

    private BigDecimal paymentAmount;
}

