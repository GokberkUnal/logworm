package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ShellSessionTest {

    private final ShellSession session = new ShellSession();

    @Test
    void resolveTopicPrefersExplicitThenCurrentThenFails() {
        assertThatThrownBy(() -> session.resolveTopic(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("use --topic");

        session.setCurrentTopic("demo-logs");
        assertThat(session.resolveTopic(null)).isEqualTo("demo-logs");
        assertThat(session.resolveTopic("  ")).isEqualTo("demo-logs");
        assertThat(session.resolveTopic("orders")).isEqualTo("orders");
        assertThat(session.currentTopic()).contains("demo-logs");
    }
}
