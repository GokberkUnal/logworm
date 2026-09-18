package com.gokgor.logworm.shell;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.springframework.shell.jline.PromptProvider;

@InteractiveShellComponent
public class LogwormPromptProvider implements PromptProvider {

    @Override
    public AttributedString getPrompt() {
        return new AttributedString("logworm> ", AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN).bold());
    }
}
