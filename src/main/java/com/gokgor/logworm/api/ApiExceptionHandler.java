package com.gokgor.logworm.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.gokgor.logworm.kafka.KafkaRequestException;
import com.gokgor.logworm.message.InvalidQueryException;
import com.gokgor.logworm.topic.TopicNotFoundException;

import lombok.extern.slf4j.Slf4j;

/** Maps domain exceptions to RFC 9457 problem responses. */
@RestControllerAdvice
@Slf4j
public class ApiExceptionHandler {

    @ExceptionHandler(TopicNotFoundException.class)
    public ProblemDetail topicNotFound(TopicNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(InvalidQueryException.class)
    public ProblemDetail invalidQuery(InvalidQueryException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(KafkaRequestException.class)
    public ProblemDetail kafkaUnavailable(KafkaRequestException e) {
        log.warn("Kafka request failed", e);
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }
}
