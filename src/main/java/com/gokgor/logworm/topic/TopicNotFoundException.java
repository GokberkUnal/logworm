package com.gokgor.logworm.topic;

public class TopicNotFoundException extends RuntimeException {

    public TopicNotFoundException(String topic) {
        super("Topic not found: " + topic);
    }
}
