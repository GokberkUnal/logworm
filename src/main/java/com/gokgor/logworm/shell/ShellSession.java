package com.gokgor.logworm.shell;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;


/**
 * Answers collected by the startup wizard and later choices made in the shell,
 * so commands can default to what the user already picked (e.g. a current topic).
 */
@InteractiveShellComponent
public class ShellSession {

    private final Map<String, Object> values = new LinkedHashMap<>();

    public void put(String key, Object value) {
        values.put(key, value);
    }

    public <T> Optional<T> get(String key, Class<T> type) {
        return Optional.ofNullable(values.get(key)).map(type::cast);
    }

    public Map<String, Object> all() {
        return Map.copyOf(values);
    }
}
