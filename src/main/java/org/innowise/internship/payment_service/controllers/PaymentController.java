package org.innowise.internship.payment_service.controllers;

import lombok.RequiredArgsConstructor;
import org.innowise.internship.payment_service.dto.CreatePaymentDTO;
import org.innowise.internship.payment_service.dto.ResponsePaymentDTO;
import org.innowise.internship.payment_service.dto.UpdatePaymentDTO;
import org.innowise.internship.payment_service.entities.PaymentStatus;
import org.innowise.internship.payment_service.services.PaymentService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping
    public ResponseEntity<ResponsePaymentDTO> createPayment(@Valid @RequestBody CreatePaymentDTO createPaymentDTO) {
        ResponsePaymentDTO response = paymentService.createPayment(createPaymentDTO);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PutMapping("/{id}")
    public ResponseEntity<ResponsePaymentDTO> updatePayment(@PathVariable String id,
                                                            @Valid @RequestBody UpdatePaymentDTO updatePaymentDTO) {
        ResponsePaymentDTO response = paymentService.updatePayment(id, updatePaymentDTO);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/{id}")
    public ResponseEntity<ResponsePaymentDTO> getPaymentById(@PathVariable String id) {
        ResponsePaymentDTO response = paymentService.getPaymentById(id);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<List<ResponsePaymentDTO>> getPaymentsByOrderId(@PathVariable Long orderId) {
        List<ResponsePaymentDTO> responses = paymentService.getPaymentsByOrderId(orderId);
        return ResponseEntity.ok(responses);
    }

    @GetMapping("/user/{userId}")
    public ResponseEntity<List<ResponsePaymentDTO>> getPaymentsByUserId(@PathVariable Long userId) {
        List<ResponsePaymentDTO> responses = paymentService.getPaymentsByUserId(userId);
        return ResponseEntity.ok(responses);
    }

    @GetMapping("/statuses")
    public ResponseEntity<List<ResponsePaymentDTO>> getPaymentsByStatuses(@RequestParam List<PaymentStatus> statuses) {
        List<ResponsePaymentDTO> responses = paymentService.getPaymentsByStatuses(statuses);
        return ResponseEntity.ok(responses);
    }

    @GetMapping("/sum")
    public ResponseEntity<BigDecimal> getTotalPaymentsSum(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime end) {
        BigDecimal totalSum = paymentService.getTotalPaymentsSum(start, end);
        return ResponseEntity.ok(totalSum);
    }
}
