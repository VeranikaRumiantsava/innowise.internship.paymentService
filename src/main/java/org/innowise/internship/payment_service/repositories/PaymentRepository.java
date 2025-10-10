package org.innowise.internship.payment_service.repositories;

import org.innowise.internship.payment_service.entities.Payment;
import org.innowise.internship.payment_service.entities.PaymentStatus;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface PaymentRepository extends MongoRepository<Payment, String> {

    List<Payment> findByOrderId(Long orderId);

    List<Payment> findByUserId(Long userId);

    List<Payment> findByStatusIn(List<PaymentStatus> statuses);

    List<Payment> findByTimestampBetween(LocalDateTime start, LocalDateTime end);
}
