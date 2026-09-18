package com.gokgor.logworm.shell;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

/** Remembers bootstrap addresses that connected successfully, newest first, in ~/.logworm/connections. */
@InteractiveShellComponent
@Slf4j
public class RecentConnections {

    static final int MAX = 10;

    private final Path file;

    public RecentConnections() {
        this(Path.of(System.getProperty("user.home"), ".logworm", "connections"));
    }

    RecentConnections(Path file) {
        this.file = file;
    }

    public List<String> list() {
        try {
            if (!Files.exists(file)) {
                return List.of();
            }
            return Files.readAllLines(file).stream().map(String::trim).filter(s -> !s.isEmpty()).toList();
        } catch (IOException e) {
            log.debug("Could not read {}: {}", file, e.getMessage());
            return List.of();
        }
    }

    public void remember(String bootstrapServers) {
        List<String> entries = new ArrayList<>(list());
        entries.remove(bootstrapServers);
        entries.addFirst(bootstrapServers);
        if (entries.size() > MAX) {
            entries = entries.subList(0, MAX);
        }
        try {
            Files.createDirectories(file.getParent());
            Files.write(file, entries);
        } catch (IOException e) {
            log.debug("Could not write {}: {}", file, e.getMessage());
        }
    }
}
