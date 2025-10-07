package org.innowise.internship.payment_service.exceptions;

public class PaymentDoesNotExistsException extends RuntimeException {
    public PaymentDoesNotExistsException(String massage) {
        super(massage);
    }
}
