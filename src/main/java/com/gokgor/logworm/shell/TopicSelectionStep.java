package com.gokgor.logworm.shell;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.gokgor.logworm.topic.TopicService;
import com.gokgor.logworm.topic.TopicSummary;

import lombok.RequiredArgsConstructor;

/**
 * Wizard step 2: which topic to work on? The pick becomes the default for topic-scoped
 * commands ({@code topic}, {@code messages}, {@code tail} ...) until {@code use} changes it.
 */
@InteractiveShellComponent
@RequiredArgsConstructor
public class TopicSelectionStep {

    static final String SESSION_KEY = "topic";
    static final String SKIP = "Skip for now";
    static final String SHOW_INTERNAL = "Show internal topics...";

    private final Choices choices;
    private final ShellSession session;
    private final TopicService topicService;

    ShellSession session() {
        return session;
    }

    public void ask() {
        ask(System.out);
    }

    void ask(PrintStream out) {
        List<TopicSummary> topics;
        try {
            topics = topicService.listTopics();
        } catch (RuntimeException e) {
            out.println("Could not list topics (" + e.getMessage() + "); pick one later with 'use --topic <name>'.");
            return;
        }
        if (topics.isEmpty()) {
            out.println("No topics on this cluster yet; pick one later with 'use --topic <name>'.");
            return;
        }

        boolean includeInternal = false;
        while (true) {
            Map<String, String> options = new LinkedHashMap<>();
            for (TopicSummary t : topics) {
                if (includeInternal || !t.internal()) {
                    options.put(label(t), t.name());
                }
            }
            if (!includeInternal && topics.stream().anyMatch(TopicSummary::internal)) {
                options.put(SHOW_INTERNAL, SHOW_INTERNAL);
            }
            options.put(SKIP, SKIP);

            String picked = choices.select("Which topic?", options);
            if (SHOW_INTERNAL.equals(picked)) {
                includeInternal = true;
                continue;
            }
            if (SKIP.equals(picked)) {
                out.println("No topic selected; use 'use --topic <name>' when you need one.");
                return;
            }
            session.put(SESSION_KEY, picked);
            out.println("Using topic " + picked);
            return;
        }
    }

    static String label(TopicSummary t) {
        return t.name() + "  (" + t.partitionCount() + " partition" + (t.partitionCount() == 1 ? "" : "s")
                + (t.internal() ? ", internal" : "") + ")";
    }
}
