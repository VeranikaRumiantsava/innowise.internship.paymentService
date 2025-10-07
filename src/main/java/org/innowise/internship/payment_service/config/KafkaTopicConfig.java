package org.innowise.internship.payment_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic createOrderTopic() {
        return TopicBuilder.name("CREATE_ORDER_TOPIC")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic createPaymentTopic() {
        return TopicBuilder.name("CREATE_PAYMENT_TOPIC")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
