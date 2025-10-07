package org.innowise.internship.payment_service.services;

import lombok.RequiredArgsConstructor;
import org.innowise.internship.payment_service.clients.RandomNumberClient;
import org.innowise.internship.payment_service.dto.CreatePaymentDTO;
import org.innowise.internship.payment_service.dto.ResponsePaymentDTO;
import org.innowise.internship.payment_service.dto.UpdatePaymentDTO;
import org.innowise.internship.payment_service.dto.kafka.PaymentDTO;
import org.innowise.internship.payment_service.entities.Payment;
import org.innowise.internship.payment_service.entities.PaymentStatus;
import org.innowise.internship.payment_service.kafka.producers.PaymentKafkaProducer;
import org.innowise.internship.payment_service.mappers.PaymentMapper;
import org.innowise.internship.payment_service.repositories.PaymentRepository;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentMapper paymentMapper;
    private final RandomNumberClient randomNumberClient;
    private final PaymentKafkaProducer paymentKafkaProducer;


    public ResponsePaymentDTO createPayment(CreatePaymentDTO createPaymentDTO) {
        Payment payment = paymentMapper.createPaymentDTOTOPayment(createPaymentDTO);

        int randomNumber = getRandomNumberFromExternalAPI();
        payment.setStatus((randomNumber % 2 == 0) ? PaymentStatus.SUCCESS : PaymentStatus.FAILED);

        payment.setTimestamp(LocalDateTime.now());
        Payment savedPayment = paymentRepository.save(payment);

        PaymentDTO paymentDTO = new PaymentDTO();
        paymentDTO.setOrderId(savedPayment.getOrderId());
        paymentDTO.setAmount(savedPayment.getPaymentAmount());
        paymentDTO.setStatus(savedPayment.getStatus().name()); // либо как строку
        paymentKafkaProducer.sendCreatePayment(paymentDTO);

        return paymentMapper.PaymentToResponseDTO(savedPayment);
    }

    private int getRandomNumberFromExternalAPI() {
        String response = randomNumberClient.getRandomNumber();
        try {
            return Integer.parseInt(response.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public ResponsePaymentDTO updatePayment(String id, UpdatePaymentDTO updatePaymentDTO) {
        Payment existingPayment = paymentRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Payment not found"));
        paymentMapper.updatePaymentFromPaymentUpdateDTO(updatePaymentDTO, existingPayment);
        Payment updatedPayment = paymentRepository.save(existingPayment);
        return paymentMapper.PaymentToResponseDTO(updatedPayment);
    }

    public ResponsePaymentDTO getPaymentById(String id) {
        Payment payment = paymentRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Payment not found"));
        return paymentMapper.PaymentToResponseDTO(payment);
    }

    public List<ResponsePaymentDTO> getPaymentsByOrderId(Long orderId) {
        return paymentRepository.findByOrderId(orderId).stream()
                .map(paymentMapper::PaymentToResponseDTO)
                .collect(Collectors.toList());
    }

    public List<ResponsePaymentDTO> getPaymentsByUserId(Long userId) {
        return paymentRepository.findByUserId(userId).stream()
                .map(paymentMapper::PaymentToResponseDTO)
                .collect(Collectors.toList());
    }

    public List<ResponsePaymentDTO> getPaymentsByStatuses(List<PaymentStatus> statuses) {
        return paymentRepository.findByStatusIn(statuses).stream()
                .map(paymentMapper::PaymentToResponseDTO)
                .collect(Collectors.toList());
    }

    public BigDecimal getTotalPaymentsSum(LocalDateTime start, LocalDateTime end) {
        return paymentRepository.findByTimestampBetween(start, end).stream()
                .map(Payment::getPaymentAmount)
                .filter(Objects::nonNull)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
