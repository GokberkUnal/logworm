package com.gokgor.logworm.shell;

import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import com.gokgor.logworm.message.KafkaMessage;
import com.gokgor.logworm.stream.StreamEvents;
import com.gokgor.logworm.stream.StreamSink;

/** StreamSink that prints tail events to the console; {@link #close()} ends the tail loop. */
final class TerminalSink implements StreamSink {

    private final PrintWriter out;
    private final Predicate<KafkaMessage> filter;
    private final Function<KafkaMessage, String> line;
    private final Predicate<KafkaMessage> isAlert;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private Throwable failure;
    private long shown;
    private long hidden;
    private long alerts;

    TerminalSink(PrintWriter out, Predicate<KafkaMessage> filter, Function<KafkaMessage, String> line,
            Predicate<KafkaMessage> isAlert) {
        this.out = out;
        this.filter = filter;
        this.line = line;
        this.isAlert = isAlert;
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public void send(String event, Object data) {
        switch (event) {
            case "message" -> {
                var m = (KafkaMessage) data;
                if (!filter.test(m)) {
                    hidden++;
                    return;
                }
                shown++;
                if (isAlert.test(m)) {
                    alerts++;
                }
                out.println(line.apply(m));
            }
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

    long shown() {
        return shown;
    }

    long hidden() {
        return hidden;
    }

    long alerts() {
        return alerts;
    }
}
