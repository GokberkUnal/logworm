package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.gokgor.logworm.shell.Rule.Color;
import com.gokgor.logworm.shell.Rule.Condition;

class ProfileStoreTest {

    @TempDir
    Path dir;

    static Profile sample(String name) {
        RuleSet rules = RuleSet.empty()
                .plusFilter(Rule.filter("op", Condition.IN, "c,u"))
                .plusAlert(Rule.parse("price", "null", Color.RED, "no price"));
        return new Profile(name, "localhost:9092", "demo-logs",
                new ViewSettings(ViewMode.NEW, List.of("level", "payload.msg")),
                OutputFormat.JSON, rules, new LagWatch(List.of("billing"), 10, 20));
    }

    @Test
    void roundTripsEverySettingAndMarksLast() {
        var store = new ProfileStore(dir.resolve("profiles"));
        assertThat(store.names()).isEmpty();
        assertThat(store.last()).isEmpty();

        store.save(sample("prod"));
        store.save(sample("dev"));

        assertThat(store.names()).containsExactly("dev", "prod");
        assertThat(store.last()).contains("dev");
        assertThat(store.load("prod")).contains(sample("prod"));
    }

    @Test
    void missingPartsFallBackToDefaultsAndUnknownFieldsAreIgnored() throws Exception {
        var store = new ProfileStore(dir);
        Files.writeString(dir.resolve("old.json"), """
                {"bootstrapServers": "b:9092", "somethingNew": 1}
                """);

        Profile p = store.load("old").orElseThrow();

        assertThat(p.name()).isEqualTo("old");
        assertThat(p.topic()).isNull();
        assertThat(p.view()).isEqualTo(ViewSettings.defaults());
        assertThat(p.format()).isEqualTo(OutputFormat.PRETTY);
        assertThat(p.rules()).isEqualTo(RuleSet.empty());
        assertThat(p.lagWatch()).isEqualTo(LagWatch.defaults());
    }

    @Test
    void unreadableFileIsEmptyNotAnError() throws Exception {
        var store = new ProfileStore(dir);
        Files.writeString(dir.resolve("broken.json"), "{not json");

        assertThat(store.names()).containsExactly("broken");
        assertThat(store.load("broken")).isEmpty();
    }

    @Test
    void deleteForgetsLastAndRejectsBadNames() {
        var store = new ProfileStore(dir);
        store.save(sample("prod"));

        assertThat(store.delete("prod")).isTrue();
        assertThat(store.delete("prod")).isFalse();
        assertThat(store.last()).isEmpty();

        assertThat(ProfileStore.isValidName("../etc")).isFalse();
        assertThat(ProfileStore.isValidName(".hidden")).isFalse();
        assertThat(ProfileStore.isValidName("prod-eu_1.2")).isTrue();
        assertThatThrownBy(() -> store.save(sample("a/b"))).isInstanceOf(IllegalArgumentException.class);
    }
}
