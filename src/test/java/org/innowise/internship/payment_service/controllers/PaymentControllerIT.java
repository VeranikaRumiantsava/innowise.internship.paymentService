package org.innowise.internship.payment_service.controllers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import com.github.tomakehurst.wiremock.client.WireMock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@SpringBootTest
@Testcontainers
@AutoConfigureMockMvc
public class PaymentControllerIT extends BaseIT {

    @Autowired
    private PaymentRepository repo;

    private CreatePaymentDTO createDto(Long orderId, Long userId, BigDecimal amount) {
        CreatePaymentDTO dto = new CreatePaymentDTO();
        dto.setOrderId(orderId);
        dto.setUserId(userId);
        dto.setPaymentAmount(amount);
        dto.setStatus(PaymentStatus.PENDING);
        return dto;
    }

    private UpdatePaymentDTO updateDto(PaymentStatus status, BigDecimal amount) {
        UpdatePaymentDTO dto = new UpdatePaymentDTO();
        dto.setStatus(status);
        dto.setPaymentAmount(amount);
        return dto;
    }

    // Позволяет легко добавлять заголовок X-User-Id (если понадобится в будущем)
    private RequestPostProcessor withUserId(Long userId) {
        return request -> {
            request.addHeader("X-User-Id", String.valueOf(userId));
            return request;
        };
    }

    @BeforeEach
    void setUp() {
        repo.deleteAll();
        // reset WireMock mappings before each test
        BaseIT.WIREMOCK.resetAll();
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        @Primary
        public RandomNumberClient randomNumberClient() {
            return () -> {
                // call WireMock directly so Feign is not required in test context
                String url = BaseIT.WIREMOCK.baseUrl() + "/integers/?num=1&min=1&max=100&col=1&base=10&format=plain";
                return new org.springframework.web.client.RestTemplate().getForObject(url, String.class);
            };
        }
    }

    @Nested
    class CreatePaymentTests {

        @Test
        void createPaymentShouldReturn201AndPersistAndSendKafka_whenExternalReturnsEven() throws Exception {
            // stub WireMock -> even number
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

            CreatePaymentDTO dto = createDto(100L, 200L, new BigDecimal("123.45"));

            // perform request
            mockMvc.perform(post("/api/payments")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(dto)))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.orderId").value(100))
                    .andExpect(jsonPath("$.userId").value(200))
                    .andExpect(jsonPath("$.status").value("SUCCESS"));

            // assert DB
            var list = repo.findByOrderId(100L);
            assertThat(list).hasSize(1);
            Payment saved = list.get(0);
            assertThat(saved.getPaymentAmount()).isEqualByComparingTo(new BigDecimal("123.45"));
            assertThat(saved.getStatus()).isEqualTo(PaymentStatus.SUCCESS);

            // assert kafka message produced (poll testcontainer kafka)
            Consumer<String, String> consumer = createStringConsumer();
            consumer.subscribe(Collections.singletonList("CREATE_PAYMENT_TOPIC"));
            ConsumerRecords<String, String> recs = consumer.poll(java.time.Duration.ofSeconds(10));
            consumer.close();
            assertThat(recs.isEmpty()).isFalse();
            String msg = recs.iterator().next().value();
            assertThat(msg).contains("\"orderId\":100");
            assertThat(msg).contains("\"status\":\"SUCCESS\"");
        }

        @Test
        void createPaymentShouldMarkFailed_whenExternalReturnsNonNumeric() throws Exception {
            BaseIT.WIREMOCK.stubFor(
                    WireMock.get(WireMock.urlPathEqualTo("/integers/"))
                            .willReturn(WireMock.aResponse().withStatus(200).withBody("not-a-number"))
            );

            CreatePaymentDTO dto = createDto(101L, 201L, new BigDecimal("10"));

            mockMvc.perform(post("/api/payments")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(dto)))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.status").value("FAILED"));

            Payment saved = repo.findByOrderId(101L).get(0);
            assertThat(saved.getStatus()).isEqualTo(PaymentStatus.FAILED);
        }
    }

    @Nested
    class GetAndQueryTests {

        @Test
        void getPaymentByIdShouldReturn200AndPayment() throws Exception {
            CreatePaymentDTO dto = createDto(200L, 300L, new BigDecimal("1"));
            Payment p = paymentMapper.createPaymentDTOTOPayment(dto);
            p.setStatus(PaymentStatus.SUCCESS);
            p.setTimestamp(LocalDateTime.now());
            Payment saved = repo.save(p);

            mockMvc.perform(get("/api/payments/{id}", saved.getId()))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.orderId").value(200))
                    .andExpect(jsonPath("$.status").value("SUCCESS"));
        }

        @Test
        void getPaymentsByOrderIdShouldReturnList() throws Exception {
            CreatePaymentDTO dto1 = createDto(300L, 400L, new BigDecimal("5"));
            CreatePaymentDTO dto2 = createDto(300L, 401L, new BigDecimal("7"));
            repo.save(paymentMapper.createPaymentDTOTOPayment(dto1));
            repo.save(paymentMapper.createPaymentDTOTOPayment(dto2));

            mockMvc.perform(get("/api/payments/order/{orderId}", 300L))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.length()").value(2));
        }

        @Test
        void getTotalPaymentsSumShouldReturnBadRequestWhenEndBeforeStart() throws Exception {
            String start = "2025-01-02T00:00:00";
            String end = "2025-01-01T00:00:00";

            mockMvc.perform(get("/api/payments/sum")
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
            CreatePaymentDTO dto = createDto(400L, 500L, new BigDecimal("15"));
            Payment p = paymentMapper.createPaymentDTOTOPayment(dto);
            p.setStatus(PaymentStatus.PENDING);
            p.setTimestamp(LocalDateTime.now());
            Payment saved = repo.save(p);

            UpdatePaymentDTO upd = updateDto(PaymentStatus.CANCELLED, new BigDecimal("20"));

            mockMvc.perform(put("/api/payments/{id}", saved.getId())
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(upd)))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("CANCELLED"));

            Payment fromDb = repo.findById(saved.getId()).orElseThrow();
            assertThat(fromDb.getPaymentAmount()).isEqualByComparingTo(new BigDecimal("20"));
        }
    }

    // ----------------- helpers -----------------
    private Consumer<String, String> createStringConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseIT.KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(props);
    }
}