package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecentConnectionsTest {

    @TempDir
    Path dir;

    @Test
    void remembersNewestFirstWithoutDuplicatesAndCapped() {
        var recent = new RecentConnections(dir.resolve("sub").resolve("connections"));
        assertThat(recent.list()).isEmpty();

        recent.remember("a:9092");
        recent.remember("b:9092");
        recent.remember("a:9092");
        assertThat(recent.list()).containsExactly("a:9092", "b:9092");

        for (int i = 0; i < RecentConnections.MAX + 5; i++) {
            recent.remember("h" + i + ":9092");
        }
        assertThat(recent.list()).hasSize(RecentConnections.MAX);
        assertThat(recent.list().getFirst()).isEqualTo("h" + (RecentConnections.MAX + 4) + ":9092");
    }
}
