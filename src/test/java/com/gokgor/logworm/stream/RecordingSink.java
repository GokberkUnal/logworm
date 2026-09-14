package com.gokgor.logworm.stream;

import java.util.ArrayList;
import java.util.List;

/** Test sink that records every event and can be closed by the test to end the loop. */
final class RecordingSink implements StreamSink {

    record Event(String name, Object data) {
    }

    final List<Event> events = new ArrayList<>();
    final List<String> comments = new ArrayList<>();
    boolean open = true;
    boolean completed;
    Throwable failure;

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void send(String event, Object data) {
        events.add(new Event(event, data));
    }

    @Override
    public void comment(String text) {
        comments.add(text);
    }

    @Override
    public void complete() {
        completed = true;
        open = false;
    }

    @Override
    public void fail(Throwable error) {
        failure = error;
        open = false;
    }

    List<Object> data(String name) {
        return events.stream().filter(e -> e.name().equals(name)).map(Event::data).toList();
    }
}
