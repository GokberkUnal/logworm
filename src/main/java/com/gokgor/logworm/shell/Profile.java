package com.gokgor.logworm.shell;

/**
 * Wizard answers saved under a name so the next start can skip the questions.
 * Missing parts (older or hand-edited files) fall back to the defaults.
 *
 * @param topic null when the profile was saved without a topic
 */
public record Profile(String name, String bootstrapServers, String topic, ViewSettings view,
                      OutputFormat format, RuleSet rules, LagWatch lagWatch) {

    public Profile {
        view = view != null ? view : ViewSettings.defaults();
        format = format != null ? format : OutputFormat.PRETTY;
        rules = rules != null ? rules : RuleSet.empty();
        lagWatch = lagWatch != null ? lagWatch : LagWatch.defaults();
    }

    /** Snapshot of what the session holds now; {@code servers} is the active connection. */
    public static Profile of(String name, String servers, ShellSession session) {
        return new Profile(name, servers, session.currentTopic().orElse(null), session.view(),
                session.outputFormat(), session.rules(), session.lagWatch());
    }

    /** One line for menus: {@code localhost:9092 · demo-logs · all messages, whole value · pretty · 2 rules}. */
    public String summary() {
        int ruleCount = rules.filters().size() + rules.alerts().size();
        return bootstrapServers + " · " + (topic != null ? topic : "no topic") + " · " + view + " · " + format
                + (ruleCount > 0 ? " · " + ruleCount + " rule" + (ruleCount == 1 ? "" : "s") : "");
    }
}
