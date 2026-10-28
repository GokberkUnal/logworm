package com.gokgor.logworm.shell;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Saved profiles as {@code ~/.logworm/profiles/<name>.json}; the name of the one used last is
 * kept in {@code ~/.logworm/profiles/last}.
 */
@InteractiveShellComponent
@Slf4j
public class ProfileStore {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,39}");
    private static final String SUFFIX = ".json";
    private static final String LAST = "last";

    private final Path dir;
    private final JsonMapper json = JsonMapper.builder()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    public ProfileStore() {
        this(Path.of(System.getProperty("user.home"), ".logworm", "profiles"));
    }

    ProfileStore(Path dir) {
        this.dir = dir;
    }

    public static boolean isValidName(String name) {
        return name != null && NAME.matcher(name).matches();
    }

    public Path fileOf(String name) {
        return dir.resolve(name + SUFFIX);
    }

    /** Profile names, alphabetically. */
    public List<String> names() {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(f -> f.endsWith(SUFFIX))
                    .map(f -> f.substring(0, f.length() - SUFFIX.length()))
                    .filter(ProfileStore::isValidName)
                    .sorted()
                    .toList();
        } catch (IOException e) {
            log.debug("Could not list {}: {}", dir, e.getMessage());
            return List.of();
        }
    }

    public boolean exists(String name) {
        return isValidName(name) && Files.exists(fileOf(name));
    }

    /** Empty when the profile does not exist or cannot be read (logged). */
    public Optional<Profile> load(String name) {
        if (!exists(name)) {
            return Optional.empty();
        }
        try {
            Profile p = json.readValue(Files.readString(fileOf(name)), Profile.class);
            return Optional.of(new Profile(name, p.bootstrapServers(), p.topic(), p.view(), p.format(), p.rules(), p.lagWatch()));
        } catch (IOException | JacksonException | IllegalArgumentException e) {
            log.warn("Could not read profile {}: {}", fileOf(name), e.getMessage());
            return Optional.empty();
        }
    }

    /** Writes the profile and marks it as the last one used. */
    public void save(Profile profile) {
        if (!isValidName(profile.name())) {
            throw new IllegalArgumentException("Invalid profile name '" + profile.name()
                    + "'; use letters, digits, '.', '_' or '-' (max 40)");
        }
        try {
            Files.createDirectories(dir);
            Files.writeString(fileOf(profile.name()), json.writeValueAsString(profile));
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write " + fileOf(profile.name()), e);
        }
        markLast(profile.name());
    }

    public boolean delete(String name) {
        if (!exists(name)) {
            return false;
        }
        try {
            Files.delete(fileOf(name));
            if (last().filter(name::equals).isPresent()) {
                Files.deleteIfExists(dir.resolve(LAST));
            }
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException("Could not delete " + fileOf(name), e);
        }
    }

    /** The profile used or saved most recently, if it still exists. */
    public Optional<String> last() {
        try {
            Path file = dir.resolve(LAST);
            if (!Files.exists(file)) {
                return Optional.empty();
            }
            return Optional.of(Files.readString(file).trim()).filter(this::exists);
        } catch (IOException e) {
            log.debug("Could not read last profile: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public void markLast(String name) {
        try {
            Files.createDirectories(dir);
            Files.writeString(dir.resolve(LAST), name);
        } catch (IOException e) {
            log.debug("Could not remember last profile: {}", e.getMessage());
        }
    }
}
