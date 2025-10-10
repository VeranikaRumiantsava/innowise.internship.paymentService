package org.innowise.internship.payment_service.controllers;

import com.github.tomakehurst.wiremock.client.WireMock;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.innowise.internship.payment_service.clients.RandomNumberClient;
import org.innowise.internship.payment_service.dto.CreatePaymentDTO;
import org.innowise.internship.payment_service.dto.UpdatePaymentDTO;
import org.innowise.internship.payment_service.entities.Payment;
import org.innowise.internship.payment_service.entities.PaymentStatus;
import org.innowise.internship.payment_service.repositories.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@Testcontainers
@AutoConfigureMockMvc
public class PaymentControllerIT extends BaseIT {

    @MockBean
    private RandomNumberClient randomNumberClient;

    @Autowired
    private PaymentRepository paymentRepository;

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAll();
        BaseIT.WIREMOCK.resetAll();

        Mockito.when(randomNumberClient.getRandomNumber())
                .thenAnswer(invocation -> {
                    String url = BaseIT.WIREMOCK.baseUrl() + "/integers/?num=1&min=1&max=100&col=1&base=10&format=plain";
                    return new org.springframework.web.client.RestTemplate().getForObject(url, String.class);
                });
    }

    private CreatePaymentDTO createPaymentRequest(Long orderId, Long userId, BigDecimal amount) {
        CreatePaymentDTO createPaymentDTO = new CreatePaymentDTO();
        createPaymentDTO.setOrderId(orderId);
        createPaymentDTO.setUserId(userId);
        createPaymentDTO.setPaymentAmount(amount);
        createPaymentDTO.setStatus(PaymentStatus.PENDING);
        return createPaymentDTO;
    }

    private UpdatePaymentDTO createUpdateRequest(PaymentStatus status, BigDecimal amount) {
        UpdatePaymentDTO updatePaymentDTO = new UpdatePaymentDTO();
        updatePaymentDTO.setStatus(status);
        updatePaymentDTO.setPaymentAmount(amount);
        return updatePaymentDTO;
    }

    private RequestPostProcessor withUserId(Long userId) {
        return request -> {
            request.addHeader("X-User-Id", String.valueOf(userId));
            return request;
        };
    }

    private Consumer<String, String> createStringConsumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseIT.kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    private void clearKafkaTopic(String topic) {
        var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BaseIT.kafkaContainer.getBootstrapServers());

        try (var admin = AdminClient.create(adminProps)) {
            var partitions = admin.describeTopics(Collections.singletonList(topic))
                    .all()
                    .get()
                    .get(topic)
                    .partitions();

            var endOffsets = admin.listOffsets(
                    partitions.stream()
                            .map(p -> new TopicPartition(topic, p.partition()))
                            .collect(Collectors.toMap(tp -> tp, tp -> org.apache.kafka.clients.admin.OffsetSpec.latest()))
            ).all().get();

            var recordsToDelete = new HashMap<TopicPartition, RecordsToDelete>();
            for (var partitionInfo : partitions) {
                var tp = new TopicPartition(topic, partitionInfo.partition());
                long offset = endOffsets.get(tp).offset();
                recordsToDelete.put(tp, RecordsToDelete.beforeOffset(offset));
            }

            admin.deleteRecords(recordsToDelete).all().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to clear Kafka topic: " + topic, e);
        }
    }

    @Nested
    class CreatePaymentTests {

        private final String topic = "CREATE_PAYMENT_TOPIC";

        private Consumer<String, String> createTestConsumer() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseIT.kafkaContainer.getBootstrapServers());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            Consumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));
            return consumer;
        }

        @Test
        void createPaymentShouldReturn201AndPersistAndSendKafka_whenExternalReturnsEven() throws Exception {
            clearKafkaTopic(topic);

            BaseIT.WIREMOCK.stubFor(
                    WireMock.get(WireMock.urlPathEqualTo("/integers/"))
                            .withQueryParam("num", WireMock.equalTo("1"))
                            .withQueryParam("min", WireMock.equalTo("1"))
                            .withQueryParam("max", WireMock.equalTo("100"))
                            .withQueryParam("col", WireMock.equalTo("1"))
                            .withQueryParam("base", WireMock.equalTo("10"))
                            .withQueryParam("format", WireMock.equalTo("plain"))
                            .willReturn(WireMock.aResponse().withStatus(200).withBody("24"))
            );

            CreatePaymentDTO createPaymentDTO = createPaymentRequest(100L, 200L, new BigDecimal("123.45"));

            mockMvc.perform(post("/api/payments")
                            .with(withUserId(createPaymentDTO.getUserId()))
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(createPaymentDTO)))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.orderId").value(100))
                    .andExpect(jsonPath("$.userId").value(200))
                    .andExpect(jsonPath("$.status").value("SUCCESS"));

            var list = paymentRepository.findByOrderId(100L);
            assertThat(list).hasSize(1);
            Payment saved = list.get(0);
            assertThat(saved.getPaymentAmount()).isEqualByComparingTo(new BigDecimal("123.45"));
            assertThat(saved.getStatus()).isEqualTo(PaymentStatus.SUCCESS);

            Consumer<String, String> consumer = createTestConsumer();
            boolean messageReceived = false;

            long timeout = System.currentTimeMillis() + 5000;

            while (!messageReceived && System.currentTimeMillis() < timeout) {
                var recs = consumer.poll(java.time.Duration.ofMillis(200));
                for (var record : recs) {
                    String msg = record.value();
                    if (msg != null) {
                        assertThat(msg).contains("\"orderId\":100");
                        assertThat(msg).contains("\"status\":\"SUCCESS\"");
                        messageReceived = true;
                        break;
                    }
                }
            }

            assertThat(messageReceived).isTrue();
        }

        @Test
        void createPaymentShouldMarkFailedWhenExternalReturnsNonNumeric() throws Exception {
            clearKafkaTopic(topic);

            BaseIT.WIREMOCK.stubFor(
                    WireMock.get(WireMock.urlPathEqualTo("/integers/"))
                            .willReturn(WireMock.aResponse().withStatus(200).withBody("31"))
            );

            CreatePaymentDTO createPaymentDTO = createPaymentRequest(101L, 201L, new BigDecimal("10"));

            mockMvc.perform(post("/api/payments")
                            .with(withUserId(createPaymentDTO.getUserId()))
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(createPaymentDTO)))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.status").value("FAILED"));

            Payment saved = paymentRepository.findByOrderId(101L).get(0);
            assertThat(saved.getStatus()).isEqualTo(PaymentStatus.FAILED);

            Consumer<String, String> consumer = createTestConsumer();
            boolean messageReceived = false;

            long timeout = System.currentTimeMillis() + 5000;
            while (!messageReceived && System.currentTimeMillis() < timeout) {
                var recs = consumer.poll(java.time.Duration.ofMillis(200));
                for (var record : recs) {
                    String msg = record.value();
                    if (msg != null) {
                        assertThat(msg).contains("\"orderId\":101");
                        assertThat(msg).contains("\"status\":\"FAILED\"");
                        messageReceived = true;
                    }
                }
            }
            consumer.close();

            assertThat(messageReceived).isTrue();
        }
    }

    @Nested
    class GetAndQueryTests {

        @Test
        void getPaymentByIdShouldReturn200AndPayment() throws Exception {
            CreatePaymentDTO createPaymentDTO = createPaymentRequest(200L, 300L, new BigDecimal("1"));
            Payment payment = paymentMapper.createPaymentDTOTOPayment(createPaymentDTO);
            payment.setStatus(PaymentStatus.SUCCESS);
            payment.setTimestamp(LocalDateTime.now());
            Payment saved = paymentRepository.save(payment);

            mockMvc.perform(get("/api/payments/{id}", saved.getId())
                            .with(withUserId(createPaymentDTO.getUserId())))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.orderId").value(200))
                    .andExpect(jsonPath("$.status").value("SUCCESS"));
        }

        @Test
        void getPaymentsByOrderIdShouldReturnList() throws Exception {
            CreatePaymentDTO firstPaymentDTO = createPaymentRequest(300L, 400L, new BigDecimal("5"));
            CreatePaymentDTO secondPaymentDTO = createPaymentRequest(300L, 401L, new BigDecimal("7"));
            paymentRepository.save(paymentMapper.createPaymentDTOTOPayment(firstPaymentDTO));
            paymentRepository.save(paymentMapper.createPaymentDTOTOPayment(secondPaymentDTO));

            mockMvc.perform(get("/api/payments/order/{orderId}", 300L)
                            .with(withUserId(secondPaymentDTO.getUserId())))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.length()").value(2));
        }

        @Test
        void getTotalPaymentsSumShouldReturnBadRequestWhenEndBeforeStart() throws Exception {
            String start = "2025-01-02T00:00:00";
            String end = "2025-01-01T00:00:00";

            mockMvc.perform(get("/api/payments/sum")
                            .with(withUserId(1L))
                            .param("start", start)
                            .param("end", end))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message[0]").value(containsString("End date must be after start date")));
        }
    }

    @Nested
    class UpdateTests {

        @Test
        void updatePaymentShouldReturn200AndUpdated() throws Exception {
            CreatePaymentDTO createPaymentDTO = createPaymentRequest(400L, 500L, new BigDecimal("15"));
            Payment payment = paymentMapper.createPaymentDTOTOPayment(createPaymentDTO);
            payment.setStatus(PaymentStatus.PENDING);
            payment.setTimestamp(LocalDateTime.now());
            Payment saved = paymentRepository.save(payment);

            UpdatePaymentDTO updatePaymentDTO = createUpdateRequest(PaymentStatus.CANCELLED, new BigDecimal("20"));

            mockMvc.perform(put("/api/payments/{id}", saved.getId())
                            .with(withUserId(createPaymentDTO.getUserId()))
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(updatePaymentDTO)))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("CANCELLED"));

            Payment fromDb = paymentRepository.findById(saved.getId()).orElseThrow();
            assertThat(fromDb.getPaymentAmount()).isEqualByComparingTo(new BigDecimal("20"));
        }
    }
}
