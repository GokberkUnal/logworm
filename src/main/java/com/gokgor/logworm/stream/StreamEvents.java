package com.gokgor.logworm.stream;

import java.util.Map;

/** Payloads of the non-message SSE events. */
public final class StreamEvents {

    private StreamEvents() {
    }

    /** Sent once when the tail is positioned; offsets are where streaming starts (log end at connect time). */
    public record Connected(String topic, Map<Integer, Long> startOffsets, int rate) {
    }

    /**
     * Sent at most once per second, only when something was lost.
     *
     * @param emitted records delivered in the last window
     * @param dropped records that matched but exceeded the rate limit
     * @param skipped records jumped over because the consumer fell too far behind
     */
    public record Stats(long emitted, long dropped, long skipped) {
    }
}
