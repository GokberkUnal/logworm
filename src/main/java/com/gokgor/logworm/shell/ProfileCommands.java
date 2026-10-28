package com.gokgor.logworm.shell;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.springframework.shell.core.command.annotation.Command;
import org.springframework.shell.core.command.annotation.Option;

import lombok.RequiredArgsConstructor;

@InteractiveShellComponent
@RequiredArgsConstructor
public class ProfileCommands {

    private final ProfileStore store;
    private final ProfileStep step;

    @Command(name = "profiles", group = "Profiles", description = "List saved profiles (* = used last)")
    public String profiles() {
        List<String> names = store.names();
        if (names.isEmpty()) {
            return "No saved profiles; save the current settings with 'profile --save <name>'.";
        }
        String last = store.last().orElse(null);
        var sb = new StringBuilder();
        for (String n : names) {
            sb.append(n.equals(last) ? "* " : "  ").append(n).append("  ")
                    .append(store.load(n).map(Profile::summary).orElse("(unreadable)")).append('\n');
        }
        return sb.toString().stripTrailing();
    }

    @Command(name = "profile", group = "Profiles", description = "Save, load or delete a profile")
    public String profile(
            @Option(longName = "save", description = "Save the current settings under this name (overwrites)") String save,
            @Option(longName = "load", description = "Connect and apply a saved profile") String load,
            @Option(longName = "delete", description = "Delete a saved profile") String delete) {
        int given = (save != null ? 1 : 0) + (load != null ? 1 : 0) + (delete != null ? 1 : 0);
        if (given != 1) {
            throw new IllegalArgumentException("Pass exactly one of --save, --load or --delete");
        }
        if (save != null) {
            Profile p = step.save(save.trim());
            return "Saved profile " + p.name() + ": " + p.summary();
        }
        if (delete != null) {
            return store.delete(delete.trim()) ? "Deleted profile " + delete.trim() : "No profile named " + delete.trim();
        }
        Profile p = store.load(load.trim())
                .orElseThrow(() -> new IllegalArgumentException("No readable profile named " + load.trim()));
        var buffer = new ByteArrayOutputStream();
        boolean ok = step.apply(p, new PrintStream(buffer, true, StandardCharsets.UTF_8));
        String report = buffer.toString(StandardCharsets.UTF_8).stripTrailing();
        return ok ? report : report + "\nProfile not applied; the previous connection and settings are kept.";
    }
}
