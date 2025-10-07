package org.innowise.internship.payment_service.kafka.consumers;

import lombok.RequiredArgsConstructor;
import org.innowise.internship.payment_service.dto.CreatePaymentDTO;
import org.innowise.internship.payment_service.dto.kafka.OrderDTO;
import org.innowise.internship.payment_service.dto.kafka.PaymentDTO;
import org.innowise.internship.payment_service.services.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PaymentKafkaConsumer {

    private final PaymentService paymentService;

    @KafkaListener(topics = "CREATE_ORDER_TOPIC", groupId = "payment-group")
    public void handleCreateOrder(OrderDTO orderDTO) {

        CreatePaymentDTO createPaymentDTO = new CreatePaymentDTO();
        createPaymentDTO.setOrderId(orderDTO.getOrderId());
        createPaymentDTO.setUserId(orderDTO.getUserId());
        createPaymentDTO.setPaymentAmount(orderDTO.getAmount());

        paymentService.createPayment(createPaymentDTO);
    }
}