package com.gokgor.logworm.shell;


import lombok.RequiredArgsConstructor;

/**
 * Asked once, right after the banner and before the prompt appears.
 * Questions go in {@link #ask()} in the order they should be asked; answers land in {@link ShellSession}.
 */
@InteractiveShellComponent
@RequiredArgsConstructor
public class StartupWizard {

    private final Choices choices;
    private final ShellSession session;

    public void ask() {
        // Steps are added here one by one, e.g.:
        // session.put("topic", choices.select("Which topic?", topicNames));
    }
}
