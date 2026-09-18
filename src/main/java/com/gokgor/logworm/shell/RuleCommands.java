package com.gokgor.logworm.shell;

import org.springframework.shell.core.command.annotation.Command;
import org.springframework.shell.core.command.annotation.Option;

import com.gokgor.logworm.shell.Rule.Color;

import lombok.RequiredArgsConstructor;

@InteractiveShellComponent
@RequiredArgsConstructor
public class RuleCommands {

    private static final String WHEN_HELP = "null | notnull | eq:X | ne:X | contains:X | gt:N | lt:N";

    private final ShellSession session;

    @Command(name = "filter", group = "Rules", description = "Show only messages where a field matches")
    public String filter(
            @Option(longName = "field", required = true, description = "'key' or a dotted JSON path") String field,
            @Option(longName = "when", required = true, description = WHEN_HELP) String when) {
        Rule r = Rule.parse(field, when, Color.NONE, null);
        session.setRules(session.rules().plusFilter(r));
        return "Rules: " + session.rules().describe();
    }

    @Command(name = "alert", group = "Rules", description = "Color and label messages where a field matches")
    public String alert(
            @Option(longName = "field", required = true, description = "'key' or a dotted JSON path") String field,
            @Option(longName = "when", required = true, description = WHEN_HELP) String when,
            @Option(longName = "color", description = "red | yellow | green | cyan (default red)") String color,
            @Option(longName = "label", description = "Text shown on matches (default: the condition)") String label) {
        Color c = color == null ? Color.RED : Color.valueOf(color.trim().toUpperCase());
        Rule r = Rule.parse(field, when, c, label);
        session.setRules(session.rules().plusAlert(r));
        return "Rules: " + session.rules().describe();
    }

    @Command(name = "rules", group = "Rules", description = "List rules, or --clear to remove them all")
    public String rules(@Option(longName = "clear", description = "Remove all filters and alerts") boolean clear) {
        if (clear) {
            session.setRules(RuleSet.empty());
        }
        return "Rules: " + session.rules().describe();
    }
}
