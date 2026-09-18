package com.gokgor.logworm.shell;

import java.util.List;

/**
 * The chosen view: which messages and which fields of them.
 *
 * @param fields JSON field paths to show (dotted, e.g. {@code payload.level}); empty = whole value
 */
public record ViewSettings(ViewMode mode, List<String> fields) {

    static final String SESSION_KEY = "view";

    public static ViewSettings defaults() {
        return new ViewSettings(ViewMode.ALL, List.of());
    }

    public ViewSettings withFields(List<String> fields) {
        return new ViewSettings(mode, List.copyOf(fields));
    }

    public ViewSettings withMode(ViewMode mode) {
        return new ViewSettings(mode, fields);
    }

    public boolean hasFields() {
        return !fields.isEmpty();
    }

    @Override
    public String toString() {
        return (mode == ViewMode.ALL ? "all messages" : "new messages only")
                + (hasFields() ? ", fields: " + String.join(", ", fields) : ", whole value");
    }
}
