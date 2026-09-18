package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.gokgor.logworm.shell.Rule.Color;

class LagWatchTest {

    @Test
    void colorsByThresholds() {
        var w = new LagWatch(List.of(), 100, 1000);

        assertThat(w.colorFor(null)).isEqualTo(Color.NONE);
        assertThat(w.colorFor(0L)).isEqualTo(Color.GREEN);
        assertThat(w.colorFor(99L)).isEqualTo(Color.GREEN);
        assertThat(w.colorFor(100L)).isEqualTo(Color.YELLOW);
        assertThat(w.colorFor(999L)).isEqualTo(Color.YELLOW);
        assertThat(w.colorFor(1000L)).isEqualTo(Color.RED);
    }

    @Test
    void groupSelectionAndDescription() {
        assertThat(LagWatch.defaults().includes("anything")).isTrue();
        var w = new LagWatch(List.of("a", "b"), 10, 20);
        assertThat(w.includes("a")).isTrue();
        assertThat(w.includes("c")).isFalse();
        assertThat(w.describe()).isEqualTo("a, b  warn ≥ 10  critical ≥ 20");
        assertThat(LagWatch.defaults().describe()).isEqualTo("all groups  warn ≥ 100  critical ≥ 1000");
    }

    @Test
    void rejectsInvalidThresholds() {
        assertThatThrownBy(() -> new LagWatch(List.of(), 50, 10)).hasMessageContaining("critical");
        assertThatThrownBy(() -> new LagWatch(List.of(), -1, 10)).hasMessageContaining(">= 0");
    }

    @Test
    void parsesGroupOption() {
        assertThat(GroupCommands.parseGroups("all")).isEmpty();
        assertThat(GroupCommands.parseGroups(" a , b,,c ")).containsExactly("a", "b", "c");
        assertThat(GroupCommands.statusOf(Color.RED)).isEqualTo("CRITICAL");
        assertThat(GroupCommands.statusOf(Color.NONE)).isEmpty();
    }
}
