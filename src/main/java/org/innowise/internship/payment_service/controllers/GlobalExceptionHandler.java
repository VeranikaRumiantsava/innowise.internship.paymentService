package org.innowise.internship.payment_service.controllers;

import java.util.List;

import jakarta.validation.ConstraintViolationException;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.innowise.internship.payment_service.dto.errors.ErrorResponse;
import org.innowise.internship.payment_service.exceptions.PaymentDoesNotExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(PaymentDoesNotExistsException.class)
    public ResponseEntity<ErrorResponse> handlePaymentNotFound(PaymentDoesNotExistsException ex) {
        log.warn("Payment not found: {}", ex.getMessage(), ex);
        return buildErrorResponse(
                List.of(ex.getMessage()),
                HttpStatus.NOT_FOUND,
                "Not found"
        );
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex) {
        log.info("Illegal argument: {}", ex.getMessage());
        return buildErrorResponse(
                List.of(ex.getMessage()),
                HttpStatus.BAD_REQUEST,
                "Bad request"
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(fieldError -> fieldError.getField() + ": " + fieldError.getDefaultMessage())
                .toList();

        log.info("Validation failed: {} errors", errors.size());
        errors.forEach(error -> log.debug("Validation error: {}", error));

        return buildErrorResponse(
                errors,
                HttpStatus.BAD_REQUEST,
                "Bad request"
        );
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolationException(ConstraintViolationException ex) {
        List<String> errors = ex.getConstraintViolations().stream()
                .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                .toList();

        log.info("Constraint violations: {} errors", errors.size());
        errors.forEach(error -> log.debug("Constraint violation: {}", error));

        return buildErrorResponse(
                errors,
                HttpStatus.BAD_REQUEST,
                "Bad request"
        );
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleHttpMessageNotReadable(HttpMessageNotReadableException ex) {
        Throwable rootCause = ex.getCause();
        String errorMessage = "Bad request";

        if (rootCause instanceof InvalidFormatException) {
            errorMessage = "Incorrect format in request body (invalid value type).";
            log.info("Invalid format exception: {}", rootCause.getMessage());
        } else if (ex.getMessage() != null && ex.getMessage().contains("Required request body is missing")) {
            errorMessage = "Required request body is missing";
            log.info("Missing request body");
        } else {
            log.warn("HttpMessageNotReadableException: {}", ex.getMessage(), ex);
        }

        return buildErrorResponse(
                List.of(errorMessage),
                HttpStatus.BAD_REQUEST,
                "Bad request"
        );
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatchException(MethodArgumentTypeMismatchException ex) {
        log.info("Method argument type mismatch: parameter '{}', value '{}'", ex.getName(), ex.getValue());

        String message = "Invalid path or query parameter: " + ex.getName();

        return buildErrorResponse(
                List.of(message),
                HttpStatus.BAD_REQUEST,
                "Bad request"
        );
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpectedExceptions(Exception ex) {
        log.error("Unexpected exception occurred: {}", ex.getMessage(), ex);

        return buildErrorResponse(
                List.of("Unexpected error occurred"),
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Internal server error"
        );
    }

    private ResponseEntity<ErrorResponse> buildErrorResponse(List<String> messages, HttpStatus status, String error) {
        ErrorResponse errorResponse = new ErrorResponse(messages, status.value(), error);
        return new ResponseEntity<>(errorResponse, status);
    }
}
