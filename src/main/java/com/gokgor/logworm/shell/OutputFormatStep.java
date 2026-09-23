package com.gokgor.logworm.shell;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.RequiredArgsConstructor;

/** Wizard step 6: how messages are printed — pretty blocks, table, JSON lines or compact text lines. */
@InteractiveShellComponent
@RequiredArgsConstructor
public class OutputFormatStep {

    static final String PRETTY = "Pretty  (one block per message, changes shown as previous --> current)";
    static final String TABLE = "Table  (columns, one row per message)";
    static final String JSON = "JSON  (one JSON object per line, jq-friendly)";
    static final String LINE = "Line  (one compact text line per message)";

    private final Choices choices;
    private final ShellSession session;

    public void ask() {
        ask(System.out);
    }

    void ask(PrintStream out) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(PRETTY, OutputFormat.PRETTY.name());
        options.put(TABLE, OutputFormat.TABLE.name());
        options.put(JSON, OutputFormat.JSON.name());
        options.put(LINE, OutputFormat.LINE.name());
        OutputFormat format = OutputFormat.valueOf(choices.select("How should messages be printed?", options));
        session.setOutputFormat(format);
        out.println("Output format: " + format);
    }
}
