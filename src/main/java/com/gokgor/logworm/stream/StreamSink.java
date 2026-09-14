package com.gokgor.logworm.stream;

import java.io.IOException;

/** Where a live tail writes its events; abstracts SseEmitter so the loop can be unit-tested. */
public interface StreamSink {

    /** False once the client went away or the stream was completed. */
    boolean isOpen();

    void send(String event, Object data) throws IOException;

    /** A comment line, used as keep-alive; ignored by EventSource clients. */
    void comment(String text) throws IOException;

    void complete();

    void fail(Throwable error);
}
