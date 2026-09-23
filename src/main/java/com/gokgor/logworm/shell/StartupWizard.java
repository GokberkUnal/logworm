package com.gokgor.logworm.shell;

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
    private final OutputFormatStep outputFormat;
    private final FilterAlertStep filterAlert;
    private final ConsumerGroupStep consumerGroups;
    private final MessageCommands messages;

    public void ask() {
        kafkaConnection.ask();
        topicSelection.ask();
        if (session().currentTopic().isEmpty()) {
            consumerGroups.ask();
            return; // nothing to look at yet; 'use --topic' and 'view' later
        }
        viewMode.ask();
        outputFormat.ask();
        filterAlert.ask();
        consumerGroups.ask();
        // A one-shot table is safe to print now; a live tail is not started here because the
        // interactive shell does not own the terminal yet — the user runs 'tail' at the prompt.
        if (session().view().mode() == ViewMode.NEW) {
            System.out.println("Live view ready. Type 'tail' to start it (Enter stops it).");
        } else {
            try {
                System.out.println(messages.show(null, null, null, null, null, null));
            } catch (RuntimeException e) {
                System.out.println("Could not show messages: " + e.getMessage());
            }
        }
    }

    private ShellSession session() {
        return topicSelection.session();
    }
}
