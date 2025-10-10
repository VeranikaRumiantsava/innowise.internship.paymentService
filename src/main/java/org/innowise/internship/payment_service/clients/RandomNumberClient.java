package org.innowise.internship.payment_service.clients;

import org.innowise.internship.payment_service.config.FeignClientConfig;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

@FeignClient(
        name = "random-number-client",
        url = "https://www.random.org",
        fallback = RandomNumberFallback.class,
        configuration = FeignClientConfig.class
)
public interface RandomNumberClient {
    @GetMapping("/integers/?num=1&min=1&max=100&col=1&base=10&format=plain")
    String getRandomNumber();
}
