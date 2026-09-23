package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class MessageCommandsParseTest {

    @Test
    void parsesOutputFormats() {
        assertThat(OutputFormat.parse(" Json ")).isEqualTo(OutputFormat.JSON);
        assertThat(OutputFormat.parse("line")).isEqualTo(OutputFormat.LINE);
        assertThatThrownBy(() -> OutputFormat.parse("xml")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pretty, table, json or line");
        assertThat(OutputFormat.TABLE.toString()).isEqualTo("table");
    }

    @Test
    void parsesFieldLists() {
        assertThat(MessageCommands.parseFields("level, ctx.user ,,msg")).containsExactly("level", "ctx.user", "msg");
        assertThat(MessageCommands.parseFields("none")).isEmpty();
        assertThat(MessageCommands.parseFields("  ")).isEmpty();
        assertThat(MessageCommands.parseFields(null)).isEmpty();
    }
}
