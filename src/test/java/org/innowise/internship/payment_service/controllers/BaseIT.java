package org.innowise.internship.payment_service.controllers;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.innowise.internship.payment_service.kafka.producers.PaymentKafkaProducer;
import org.innowise.internship.payment_service.mappers.PaymentMapper;
import org.innowise.internship.payment_service.repositories.PaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;


@SpringBootTest
@Testcontainers
@AutoConfigureMockMvc
public abstract class BaseIT {


    @Autowired
    protected MockMvc mockMvc;


    @Autowired
    protected ObjectMapper objectMapper;


    @Autowired
    protected PaymentRepository paymentRepository;

    @Autowired
    protected PaymentMapper paymentMapper;

    @Autowired
    protected PaymentKafkaProducer paymentKafkaProducer;

    @Container
    @ServiceConnection(name = "mongo")
    public static final MongoDBContainer mongoContainer = new MongoDBContainer("mongo:6.0.8");

    @Container
    @ServiceConnection(name = "kafka")
    public static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.5"));

    public static final WireMockServer  WIREMOCK = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    static {
        WIREMOCK.start();
    }


    @DynamicPropertySource
    static void registerDynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("MONGO_URI", mongoContainer::getReplicaSetUrl);
        registry.add("KAFKA_BOOTSTRAP_SERVERS", kafkaContainer::getBootstrapServers);
        registry.add("test.wiremock.base-url", WIREMOCK::baseUrl);
    }
}