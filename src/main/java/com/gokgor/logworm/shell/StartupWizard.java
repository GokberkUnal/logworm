package com.gokgor.logworm.shell;

import lombok.RequiredArgsConstructor;

/**
 * Asked once, right after the banner and before the prompt appears.
 * Steps run in the order listed in {@link #ask()}; each one stores its answer in {@link ShellSession}.
 */
@InteractiveShellComponent
@RequiredArgsConstructor
public class StartupWizard {

    private final KafkaConnectionStep kafkaConnection;

    public void ask() {
        kafkaConnection.ask();
        // next steps go here, in order
    }
}
