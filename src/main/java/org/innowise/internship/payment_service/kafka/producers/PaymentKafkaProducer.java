package org.innowise.internship.payment_service.kafka.producers;

import lombok.RequiredArgsConstructor;
import org.innowise.internship.payment_service.dto.kafka.PaymentDTO;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PaymentKafkaProducer {

    private final KafkaTemplate<String, PaymentDTO> kafkaTemplate;

    public void sendCreatePayment(PaymentDTO paymentDTO) {
        kafkaTemplate.send("CREATE_PAYMENT_TOPIC", paymentDTO);
    }
}
