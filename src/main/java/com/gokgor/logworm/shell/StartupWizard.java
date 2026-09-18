package com.gokgor.logworm.shell;

import java.io.IOException;

import lombok.RequiredArgsConstructor;

/**
 * Asked once, right after the banner and before the prompt appears.
 * Steps run in the order listed in {@link #ask()}; each one stores its answer in {@link ShellSession}.
 * When a topic was picked, the chosen view is started right away.
 */
@InteractiveShellComponent
@RequiredArgsConstructor
public class StartupWizard {

    private final KafkaConnectionStep kafkaConnection;
    private final TopicSelectionStep topicSelection;
    private final ViewModeStep viewMode;
    private final MessageCommands messages;

    public void ask() {
        kafkaConnection.ask();
        topicSelection.ask();
        if (session().currentTopic().isEmpty()) {
            return; // nothing to look at yet; 'use --topic' and 'view' later
        }
        viewMode.ask();
        try {
            String out = session().view().mode() == ViewMode.NEW
                    ? messages.tail(null, null, null, null, null)
                    : messages.show(null, null, null, null, null);
            System.out.println(out);
        } catch (IOException | RuntimeException e) {
            System.out.println("Could not start the view: " + e.getMessage());
        }
    }

    private ShellSession session() {
        return topicSelection.session();
    }
}
