package com.gokgor.logworm.shell;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.gokgor.logworm.kafka.KafkaConnection;
import com.gokgor.logworm.topic.TopicService;
import com.gokgor.logworm.topic.TopicSummary;

import lombok.RequiredArgsConstructor;

/**
 * Wizard step 7: profiles. Before the first question it offers to start from a saved profile
 * (skipping the other steps); after the last one it offers to save the answers as a profile.
 */
@InteractiveShellComponent
@RequiredArgsConstructor
public class ProfileStep {

    static final String NEW = "New setup  (ask every question)";
    static final String OTHER = "Other profile...";
    static final String BACK = "← back";

    private final Choices choices;
    private final ShellSession session;
    private final ProfileStore store;
    private final KafkaConnectionStep kafkaConnection;
    private final KafkaConnection connection;
    private final TopicService topicService;

    public boolean offerLoad() {
        return offerLoad(System.out);
    }

    /** @return true when a profile was applied and the remaining wizard steps can be skipped */
    boolean offerLoad(PrintStream out) {
        List<String> names = store.names();
        if (names.isEmpty()) {
            return false;
        }
        Optional<String> last = store.last();
        List<String> others = names.stream().filter(n -> last.map(l -> !l.equals(n)).orElse(true)).toList();

        while (true) {
            Map<String, String> options = new LinkedHashMap<>();
            last.ifPresent(l -> options.put("Last profile: " + l + "  (" + summaryOf(l) + ")", l));
            if (!others.isEmpty()) {
                options.put(OTHER + "  (" + others.size() + ")", OTHER);
            }
            options.put(NEW, NEW);

            String picked = choices.select("Start from a saved profile?", options);
            if (NEW.equals(picked)) {
                return false;
            }
            if (OTHER.equals(picked)) {
                Map<String, String> profileOptions = new LinkedHashMap<>();
                others.forEach(n -> profileOptions.put(n + "  (" + summaryOf(n) + ")", n));
                profileOptions.put(BACK, BACK);
                picked = choices.select("Saved profiles", profileOptions);
                if (BACK.equals(picked)) {
                    continue;
                }
            }
            Optional<Profile> profile = store.load(picked);
            if (profile.isEmpty()) {
                out.println("Could not read profile " + picked + "; continuing with the full setup.");
                return false;
            }
            if (apply(profile.get(), out)) {
                return true;
            }
            out.println("Continuing with the full setup.");
            return false;
        }
    }

    /**
     * Connects to the profile's cluster and copies its settings into the session. A topic that no
     * longer exists is dropped with a note. Nothing changes when the connection fails.
     */
    boolean apply(Profile p, PrintStream out) {
        out.println("Loading profile " + p.name());
        if (!kafkaConnection.connectTo(p.bootstrapServers(), out)) {
            return false;
        }
        session.remove(TopicSelectionStep.SESSION_KEY);
        if (p.topic() != null) {
            if (topicExists(p.topic(), out)) {
                session.setCurrentTopic(p.topic());
                out.println("Using topic " + p.topic());
            } else {
                out.println("Topic " + p.topic() + " is not on this cluster; pick one with 'use --topic <name>'.");
            }
        }
        session.setView(p.view());
        session.setOutputFormat(p.format());
        session.setRules(p.rules());
        session.setLagWatch(p.lagWatch());
        out.println("View: " + p.view() + "  ·  format: " + p.format());
        out.println("Rules: " + p.rules().describe());
        out.println("Lag watch: " + p.lagWatch().describe());
        store.markLast(p.name());
        return true;
    }

    public void offerSave() {
        offerSave(System.out);
    }

    void offerSave(PrintStream out) {
        if (!choices.confirm("Save these settings as a profile?", true)) {
            return;
        }
        String suggested = session.currentTopic().filter(ProfileStore::isValidName).orElse("default");
        while (true) {
            String name = choices.text("Profile name", suggested).trim();
            if (!ProfileStore.isValidName(name)) {
                out.println("Use letters, digits, '.', '_' or '-' (max 40), starting with a letter or digit.");
                continue;
            }
            if (store.exists(name) && !choices.confirm("Profile " + name + " exists. Overwrite?", false)) {
                continue;
            }
            try {
                save(name);
                out.println("Saved profile " + name + " (" + store.fileOf(name) + "); the next start offers it first.");
            } catch (RuntimeException e) {
                out.println("Could not save profile: " + e.getMessage());
            }
            return;
        }
    }

    /** Saves the current session under {@code name}. */
    Profile save(String name) {
        String servers = session.get(KafkaConnectionStep.SESSION_KEY, String.class)
                .orElseGet(() -> connection.info().bootstrapServers());
        Profile p = Profile.of(name, servers, session);
        store.save(p);
        return p;
    }

    private String summaryOf(String name) {
        return store.load(name).map(Profile::summary).orElse("unreadable");
    }

    private boolean topicExists(String topic, PrintStream out) {
        try {
            return topicService.listTopics().stream().map(TopicSummary::name).anyMatch(topic::equals);
        } catch (RuntimeException e) {
            out.println("Could not check topics (" + e.getMessage() + ")");
            return true; // keep it; commands report a missing topic themselves
        }
    }
}
