package com.gokgor.logworm.shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Remembers the last seen field values per record key so the pretty format can print
 * {@code previous --> current} when a field changed since the previous message with the same key.
 * Not thread-safe; one instance per {@code show} or {@code tail} run.
 */
final class ChangeTracker {

    /** One field of a message: {@code previous} is null when unknown or unchanged. */
    record Change(String field, String previous, String current) {
        boolean changed() {
            return previous != null;
        }
    }

    private final Map<String, Map<String, String>> lastByKey = new HashMap<>();

    /** Records the message and returns its fields with the changes against the previous one of the same key. */
    List<Change> track(String key, Map<String, String> fields) {
        Map<String, String> previous = lastByKey.put(key == null ? "" : key, new LinkedHashMap<>(fields));
        List<Change> out = new ArrayList<>(fields.size());
        for (var e : fields.entrySet()) {
            String before = previous == null ? null : previous.get(e.getKey());
            boolean changed = previous != null && previous.containsKey(e.getKey()) && !before.equals(e.getValue());
            out.add(new Change(e.getKey(), changed ? before : null, e.getValue()));
        }
        return out;
    }
}
