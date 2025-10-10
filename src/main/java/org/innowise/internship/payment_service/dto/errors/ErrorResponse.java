package org.innowise.internship.payment_service.dto.errors;

import lombok.Getter;
import lombok.Setter;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Setter
@Getter
public class ErrorResponse {
    final private List<String> message;
    final private String timestamp;
    final private int status;
    final private String error;

    public ErrorResponse(List<String> message, int status, String error) {
        this.message = message;
        this.timestamp = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        this.status = status;
        this.error = error;
    }
}
