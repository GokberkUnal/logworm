package com.gokgor.logworm.consumergroup;

public class ConsumerGroupNotFoundException extends RuntimeException {

    public ConsumerGroupNotFoundException(String groupId) {
        super("Consumer group not found: " + groupId);
    }
}
