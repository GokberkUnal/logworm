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

    public ViewSettings view() {
        return get(ViewSettings.SESSION_KEY, ViewSettings.class).orElse(ViewSettings.defaults());
    }

    public void setView(ViewSettings view) {
        put(ViewSettings.SESSION_KEY, view);
    }

    public Optional<String> currentTopic() {
        return get(TopicSelectionStep.SESSION_KEY, String.class);
    }

    public void setCurrentTopic(String topic) {
        put(TopicSelectionStep.SESSION_KEY, topic);
    }

    /**
     * The topic a command should act on: the explicit option if given, otherwise the current one.
     *
     * @throws IllegalArgumentException when neither is available
     */
    public String resolveTopic(String explicit) {
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        return currentTopic().orElseThrow(() ->
                new IllegalArgumentException("No topic given and none selected; pass --name or run 'use --topic <name>'"));
    }
}
