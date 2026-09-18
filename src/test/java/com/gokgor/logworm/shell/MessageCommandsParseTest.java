package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MessageCommandsParseTest {

    @Test
    void parsesFieldLists() {
        assertThat(MessageCommands.parseFields("level, ctx.user ,,msg")).containsExactly("level", "ctx.user", "msg");
        assertThat(MessageCommands.parseFields("none")).isEmpty();
        assertThat(MessageCommands.parseFields("  ")).isEmpty();
        assertThat(MessageCommands.parseFields(null)).isEmpty();
    }
}
