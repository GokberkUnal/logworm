package com.gokgor.logworm.stream;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** StreamSink backed by a Spring MVC SseEmitter. */
final class SseStreamSink implements StreamSink {

    private final SseEmitter emitter;
    private final AtomicBoolean open = new AtomicBoolean(true);

    SseStreamSink(SseEmitter emitter) {
        this.emitter = emitter;
        emitter.onCompletion(() -> open.set(false));
        emitter.onTimeout(() -> open.set(false));
        emitter.onError(e -> open.set(false));
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public void send(String event, Object data) throws IOException {
        try {
            emitter.send(SseEmitter.event().name(event).data(data, MediaType.APPLICATION_JSON));
        } catch (IllegalStateException e) {
            // emitter already completed (client disconnected between checks)
            open.set(false);
            throw new IOException("stream closed", e);
        } catch (IOException e) {
            open.set(false);
            throw e;
        }
    }

    @Override
    public void comment(String text) throws IOException {
        try {
            emitter.send(SseEmitter.event().comment(text));
        } catch (IllegalStateException e) {
            open.set(false);
            throw new IOException("stream closed", e);
        } catch (IOException e) {
            open.set(false);
            throw e;
        }
    }

    @Override
    public void complete() {
        if (open.compareAndSet(true, false)) {
            emitter.complete();
        }
    }

    @Override
    public void fail(Throwable error) {
        if (open.compareAndSet(true, false)) {
            emitter.completeWithError(error);
        }
    }
}
