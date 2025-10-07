package org.innowise.internship.payment_service.clients;

public class RandomNumberFallback implements RandomNumberClient {

    @Override
    public String getRandomNumber() {
        return "42";
    }
}
