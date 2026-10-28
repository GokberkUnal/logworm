package com.gokgor.logworm.shell;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.RequiredArgsConstructor;

/**
 * Wizard step 6: how messages are printed — pretty blocks, table, JSON lines, compact text lines
 * or CDC events. When the current topic carries CDC events, the CDC format is offered first.
 */
@InteractiveShellComponent
@RequiredArgsConstructor
public class OutputFormatStep {

    static final String PRETTY = "Pretty  (one block per message, changes shown as previous --> current)";
    static final String TABLE = "Table  (columns, one row per message)";
    static final String JSON = "JSON  (one JSON object per line, jq-friendly)";
    static final String LINE = "Line  (one compact text line per message)";
    static final String CDC = "CDC  (Debezium before/after: operation, table, changed columns)";
    static final String CDC_DETECTED = "CDC  (detected: this topic carries change events)";

    private final Choices choices;
    private final ShellSession session;
    private final FieldSampler sampler;

    public void ask() {
        ask(System.out);
    }

    void ask(PrintStream out) {
        boolean cdc = session.currentTopic().map(this::looksLikeCdc).orElse(false);
        Map<String, String> options = new LinkedHashMap<>();
        if (cdc) {
            options.put(CDC_DETECTED, OutputFormat.CDC.name());
        }
        options.put(PRETTY, OutputFormat.PRETTY.name());
        options.put(TABLE, OutputFormat.TABLE.name());
        options.put(JSON, OutputFormat.JSON.name());
        options.put(LINE, OutputFormat.LINE.name());
        if (!cdc) {
            options.put(CDC, OutputFormat.CDC.name());
        }
        OutputFormat format = OutputFormat.valueOf(choices.select("How should messages be printed?", options));
        session.setOutputFormat(format);
        out.println("Output format: " + format);
    }

    private boolean looksLikeCdc(String topic) {
        try {
            return sampler.looksLikeCdc(topic);
        } catch (RuntimeException e) {
            return false; // detection is only a hint
        }
    }
}
