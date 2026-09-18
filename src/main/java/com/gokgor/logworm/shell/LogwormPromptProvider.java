package com.gokgor.logworm.shell;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.springframework.shell.jline.PromptProvider;

import lombok.RequiredArgsConstructor;

/** {@code logworm> } or, once a topic is selected, {@code logworm demo-logs> }. */
@InteractiveShellComponent
@RequiredArgsConstructor
public class LogwormPromptProvider implements PromptProvider {

    private final ShellSession session;

    @Override
    public AttributedString getPrompt() {
        var b = new AttributedStringBuilder()
                .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN).bold())
                .append("logworm");
        session.currentTopic().ifPresent(t -> b
                .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN))
                .append(" ").append(t));
        return b.style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN).bold())
                .append("> ")
                .toAttributedString();
    }
}
