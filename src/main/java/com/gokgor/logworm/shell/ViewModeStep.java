package com.gokgor.logworm.shell;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


import lombok.RequiredArgsConstructor;

/**
 * Wizard step 3: what to look at. All messages, only new ones, or specific fields of them
 * (fields are discovered from a sample of recent messages, then narrowed to all / new).
 */
@InteractiveShellComponent
@RequiredArgsConstructor
public class ViewModeStep {

    static final String ALL = "All messages  (newest first, whole value)";
    static final String NEW = "Only new messages  (live tail from now)";
    static final String FIELDS = "Specific fields  (pick JSON fields to show)";
    static final String OF_ALL = "All messages";
    static final String OF_NEW = "Only new messages";
    static final int SAMPLE = FieldSampler.SAMPLE;

    private final Choices choices;
    private final ShellSession session;
    private final FieldSampler sampler;

    public void ask() {
        ask(System.out);
    }

    void ask(PrintStream out) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(ALL, ALL);
        options.put(NEW, NEW);
        options.put(FIELDS, FIELDS);
        String picked = choices.select("What do you want to see?", options);

        ViewSettings view = switch (picked) {
            case NEW -> new ViewSettings(ViewMode.NEW, List.of());
            case FIELDS -> pickFields(out);
            default -> new ViewSettings(ViewMode.ALL, List.of());
        };
        session.setView(view);
        out.println("View: " + view);
    }

    private ViewSettings pickFields(PrintStream out) {
        List<String> fields = List.of();
        String topic = session.currentTopic().orElse(null);
        if (topic == null) {
            out.println("No topic selected, so fields cannot be sampled; set them later with 'fields --set a,b'.");
        } else {
            List<String> discovered;
            try {
                discovered = sampler.sample(topic);
            } catch (RuntimeException e) {
                out.println("Could not sample messages (" + e.getMessage() + ").");
                discovered = List.of();
            }
            if (discovered.isEmpty()) {
                out.println("No JSON fields found in the last " + SAMPLE + " messages of " + topic + "; showing whole values.");
            } else {
                fields = choices.selectMany("Which fields? (space to toggle, enter to confirm)", discovered);
                if (fields.isEmpty()) {
                    out.println("Nothing picked; showing whole values.");
                }
            }
        }
        Map<String, String> which = new LinkedHashMap<>();
        which.put(OF_ALL, OF_ALL);
        which.put(OF_NEW, OF_NEW);
        ViewMode mode = OF_NEW.equals(choices.select("Of which messages?", which)) ? ViewMode.NEW : ViewMode.ALL;
        return new ViewSettings(mode, fields);
    }
}
