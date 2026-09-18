package com.gokgor.logworm.shell;

import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.gokgor.logworm.message.KafkaMessage;
import com.gokgor.logworm.stream.StreamEvents;
import com.gokgor.logworm.stream.StreamSink;

/** StreamSink that prints tail events to the console; {@link #close()} ends the tail loop. */
final class TerminalSink implements StreamSink {

    private final PrintWriter out;
    private final Function<KafkaMessage, String> line;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private Throwable failure;

    TerminalSink(PrintWriter out, Function<KafkaMessage, String> line) {
        this.out = out;
        this.line = line;
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public void send(String event, Object data) {
        switch (event) {
            case "message" -> out.println(line.apply((KafkaMessage) data));
            case "stats" -> {
                var s = (StreamEvents.Stats) data;
                out.println("[stats] emitted=" + s.emitted() + " dropped=" + s.dropped() + " skipped=" + s.skipped());
            }
            case "connected" -> {
                var c = (StreamEvents.Connected) data;
                out.println("Tailing " + c.topic() + " from offsets " + c.startOffsets() + " (max " + c.rate() + " msg/s). Press any key to stop.");
            }
            default -> { }
        }
        out.flush();
    }

    @Override
    public void comment(String text) {
        // keep-alive: nothing to show on a terminal
    }

    @Override
    public void complete() {
        open.set(false);
    }

    @Override
    public void fail(Throwable error) {
        failure = error;
        open.set(false);
    }

    void close() {
        open.set(false);
    }

    Throwable failure() {
        return failure;
    }
}
