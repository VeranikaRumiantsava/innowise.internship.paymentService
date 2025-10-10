package org.innowise.internship.payment_service.services;

import org.innowise.internship.payment_service.clients.RandomNumberClient;
import org.innowise.internship.payment_service.dto.CreatePaymentDTO;
import org.innowise.internship.payment_service.dto.ResponsePaymentDTO;
import org.innowise.internship.payment_service.dto.UpdatePaymentDTO;
import org.innowise.internship.payment_service.entities.Payment;
import org.innowise.internship.payment_service.entities.PaymentStatus;
import org.innowise.internship.payment_service.exceptions.PaymentDoesNotExistsException;
import org.innowise.internship.payment_service.kafka.producers.PaymentKafkaProducer;
import org.innowise.internship.payment_service.mappers.PaymentMapper;
import org.innowise.internship.payment_service.repositories.PaymentRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private PaymentMapper paymentMapper;

    @Mock
    private RandomNumberClient randomNumberClient;

    @Mock
    private PaymentKafkaProducer paymentKafkaProducer;

    @InjectMocks
    private PaymentService paymentService;

    @Test
    void createPaymentShouldReturnResponsePaymentDTO_whenRandomEven() {
        CreatePaymentDTO createDTO = new CreatePaymentDTO();
        createDTO.setOrderId(1L);
        createDTO.setUserId(1L);
        createDTO.setPaymentAmount(new BigDecimal("100"));

        Payment payment = new Payment();
        Payment savedPayment = new Payment();
        savedPayment.setId("1");
        savedPayment.setOrderId(createDTO.getOrderId());
        savedPayment.setPaymentAmount(createDTO.getPaymentAmount());
        savedPayment.setStatus(PaymentStatus.SUCCESS);

        ResponsePaymentDTO responseDTO = new ResponsePaymentDTO();
        responseDTO.setId("1");

        Mockito.when(paymentMapper.createPaymentDTOTOPayment(createDTO)).thenReturn(payment);
        Mockito.when(randomNumberClient.getRandomNumber()).thenReturn("2");
        Mockito.when(paymentRepository.save(payment)).thenReturn(savedPayment);
        Mockito.when(paymentMapper.PaymentToResponseDTO(savedPayment)).thenReturn(responseDTO);

        ResponsePaymentDTO result = paymentService.createPayment(createDTO);

        Assertions.assertEquals(responseDTO, result);
        Mockito.verify(paymentKafkaProducer).sendCreatePayment(Mockito.any());
    }

    @Test
    void createPaymentShouldSetFailedWhenRandomIsOdd() {
        CreatePaymentDTO createDTO = new CreatePaymentDTO();
        createDTO.setOrderId(1L);
        createDTO.setUserId(1L);
        createDTO.setPaymentAmount(new BigDecimal("100"));

        Payment payment = new Payment();
        ResponsePaymentDTO responseDTO = new ResponsePaymentDTO();
        responseDTO.setId("1");

        Mockito.when(paymentMapper.createPaymentDTOTOPayment(createDTO)).thenReturn(payment);
        Mockito.when(randomNumberClient.getRandomNumber()).thenReturn("3");
        Mockito.when(paymentRepository.save(payment)).then(invocation -> {
            Assertions.assertEquals(PaymentStatus.FAILED, payment.getStatus());
            return payment;
        });
        Mockito.when(paymentMapper.PaymentToResponseDTO(payment)).thenReturn(responseDTO);

        ResponsePaymentDTO result = paymentService.createPayment(createDTO);

        Assertions.assertEquals(responseDTO, result);
        Mockito.verify(paymentKafkaProducer).sendCreatePayment(Mockito.any());
    }

    @Test
    void createPaymentShouldHandleNonNumericRandomAndSetFailed() {
        CreatePaymentDTO createDTO = new CreatePaymentDTO();
        createDTO.setOrderId(1L);
        createDTO.setUserId(1L);
        createDTO.setPaymentAmount(new BigDecimal("100"));

        Payment payment = new Payment();
        ResponsePaymentDTO responseDTO = new ResponsePaymentDTO();
        responseDTO.setId("1");

        Mockito.when(paymentMapper.createPaymentDTOTOPayment(createDTO)).thenReturn(payment);
        Mockito.when(randomNumberClient.getRandomNumber()).thenReturn("not-a-number");
        Mockito.when(paymentRepository.save(payment)).then(invocation -> {
            Assertions.assertEquals(PaymentStatus.FAILED, payment.getStatus());
            return payment;
        });
        Mockito.when(paymentMapper.PaymentToResponseDTO(payment)).thenReturn(responseDTO);

        ResponsePaymentDTO result = paymentService.createPayment(createDTO);

        Assertions.assertEquals(responseDTO, result);
        Mockito.verify(paymentKafkaProducer).sendCreatePayment(Mockito.any());
    }

    @Test
    void updatePaymentShouldReturnResponsePaymentDTO() {
        String id = "1";
        UpdatePaymentDTO updateDTO = new UpdatePaymentDTO();
        updateDTO.setPaymentAmount(new BigDecimal("200"));

        Payment existingPayment = new Payment();
        existingPayment.setId(id);
        existingPayment.setPaymentAmount(new BigDecimal("100"));

        Payment updatedPayment = new Payment();
        updatedPayment.setId(id);
        updatedPayment.setPaymentAmount(new BigDecimal("200"));

        ResponsePaymentDTO responseDTO = new ResponsePaymentDTO();
        responseDTO.setId(id);

        Mockito.when(paymentRepository.findById(id)).thenReturn(Optional.of(existingPayment));
        Mockito.doNothing().when(paymentMapper).updatePaymentFromPaymentUpdateDTO(updateDTO, existingPayment);
        Mockito.when(paymentRepository.save(existingPayment)).thenReturn(updatedPayment);
        Mockito.when(paymentMapper.PaymentToResponseDTO(updatedPayment)).thenReturn(responseDTO);

        ResponsePaymentDTO result = paymentService.updatePayment(id, updateDTO);

        Assertions.assertEquals(responseDTO, result);
        Assertions.assertEquals(new BigDecimal("200"), updatedPayment.getPaymentAmount());
    }

    @Test
    void updatePaymentShouldThrowWhenPaymentNotFound() {
        String id = "nonexistent";
        Mockito.when(paymentRepository.findById(id)).thenReturn(Optional.empty());

        Assertions.assertThrows(PaymentDoesNotExistsException.class,
                () -> paymentService.updatePayment(id, new UpdatePaymentDTO()));
    }

    @Test
    void getPaymentByIdShouldReturnResponsePaymentDTO() {
        String id = "1";
        Payment payment = new Payment();
        payment.setId(id);
        ResponsePaymentDTO dto = new ResponsePaymentDTO();
        dto.setId(id);

        Mockito.when(paymentRepository.findById(id)).thenReturn(Optional.of(payment));
        Mockito.when(paymentMapper.PaymentToResponseDTO(payment)).thenReturn(dto);

        ResponsePaymentDTO result = paymentService.getPaymentById(id);
        Assertions.assertEquals(dto, result);
    }

    @Test
    void getPaymentByIdShouldThrowWhenPaymentNotFound() {
        Mockito.when(paymentRepository.findById("1")).thenReturn(Optional.empty());

        Assertions.assertThrows(PaymentDoesNotExistsException.class,
                () -> paymentService.getPaymentById("1"));
    }

    @Test
    void getPaymentsByOrderIdShouldReturnList() {
        Long orderId = 1L;
        Payment payment = new Payment();
        payment.setOrderId(orderId);
        ResponsePaymentDTO dto = new ResponsePaymentDTO();
        dto.setId("1");

        Mockito.when(paymentRepository.findByOrderId(orderId)).thenReturn(List.of(payment));
        Mockito.when(paymentMapper.PaymentToResponseDTO(payment)).thenReturn(dto);

        List<ResponsePaymentDTO> result = paymentService.getPaymentsByOrderId(orderId);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(dto, result.get(0));
    }

    @Test
    void getPaymentsByOrderIdShouldReturnEmptyListWhenNoPayments() {
        Long orderId = 1L;
        Mockito.when(paymentRepository.findByOrderId(orderId)).thenReturn(List.of());

        List<ResponsePaymentDTO> result = paymentService.getPaymentsByOrderId(orderId);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void getPaymentsByUserIdShouldReturnList() {
        Long userId = 42L;
        Payment payment = new Payment();
        payment.setUserId(userId);
        ResponsePaymentDTO dto = new ResponsePaymentDTO();
        dto.setId("u-1");

        Mockito.when(paymentRepository.findByUserId(userId)).thenReturn(List.of(payment));
        Mockito.when(paymentMapper.PaymentToResponseDTO(payment)).thenReturn(dto);

        List<ResponsePaymentDTO> result = paymentService.getPaymentsByUserId(userId);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(dto, result.get(0));
    }

    @Test
    void getPaymentsByUserIdShouldReturnEmptyListWhenNoPayments() {
        Long userId = 42L;
        Mockito.when(paymentRepository.findByUserId(userId)).thenReturn(List.of());

        List<ResponsePaymentDTO> result = paymentService.getPaymentsByUserId(userId);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void getPaymentsByStatusesShouldReturnList() {
        Payment payment = new Payment();
        payment.setStatus(PaymentStatus.SUCCESS);
        ResponsePaymentDTO dto = new ResponsePaymentDTO();
        dto.setId("1");

        Mockito.when(paymentRepository.findByStatusIn(List.of(PaymentStatus.SUCCESS))).thenReturn(List.of(payment));
        Mockito.when(paymentMapper.PaymentToResponseDTO(payment)).thenReturn(dto);

        List<ResponsePaymentDTO> result = paymentService.getPaymentsByStatuses(List.of(PaymentStatus.SUCCESS));
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(dto, result.get(0));
    }

    @Test
    void getPaymentsByStatusesShouldReturnEmptyListWhenNoPayments() {
        List<PaymentStatus> statuses = List.of(PaymentStatus.SUCCESS);
        Mockito.when(paymentRepository.findByStatusIn(statuses)).thenReturn(List.of());

        List<ResponsePaymentDTO> result = paymentService.getPaymentsByStatuses(statuses);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void getTotalPaymentsSumShouldReturnSum() {
        LocalDateTime start = LocalDateTime.now().minusDays(1);
        LocalDateTime end = LocalDateTime.now();

        Payment p1 = new Payment();
        p1.setPaymentAmount(new BigDecimal("10"));
        Payment p2 = new Payment();
        p2.setPaymentAmount(new BigDecimal("20"));

        Mockito.when(paymentRepository.findByTimestampBetween(start, end)).thenReturn(List.of(p1, p2));

        BigDecimal total = paymentService.getTotalPaymentsSum(start, end);
        Assertions.assertEquals(new BigDecimal("30"), total);
    }

    @Test
    void getTotalPaymentsSumShouldReturnZeroWhenNoPayments() {
        LocalDateTime start = LocalDateTime.now().minusDays(1);
        LocalDateTime end = LocalDateTime.now();

        Mockito.when(paymentRepository.findByTimestampBetween(start, end)).thenReturn(List.of());

        BigDecimal total = paymentService.getTotalPaymentsSum(start, end);
        Assertions.assertEquals(BigDecimal.ZERO, total);
    }

    @Test
    void getTotalPaymentsSumShouldThrowWhenEndBeforeStart() {
        LocalDateTime start = LocalDateTime.now();
        LocalDateTime end = LocalDateTime.now().minusDays(1);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> paymentService.getTotalPaymentsSum(start, end));
    }
}
