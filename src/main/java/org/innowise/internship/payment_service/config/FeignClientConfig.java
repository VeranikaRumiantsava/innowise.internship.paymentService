package org.innowise.internship.payment_service.config;

import feign.RequestInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeignClientConfig {

    @Bean
    public RequestInterceptor requestInterceptor() {
        return requestTemplate ->
                requestTemplate.header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
    }
}
