package com.gokgor.logworm.shell;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.shell.jline.tui.component.flow.ComponentFlow;
import org.springframework.shell.jline.tui.component.flow.SelectItem;

/**
 * Thin wrapper over Spring Shell's ComponentFlow so a question is one line:
 * {@code choices.select("Pick a topic", topics)}.
 */
@InteractiveShellComponent
public class Choices {

    private static final String KEY = "answer";

    private final ComponentFlow.Builder flowBuilder;

    public Choices(ComponentFlow.Builder flowBuilder) {
        this.flowBuilder = flowBuilder;
    }

    /** Single choice from a list; returns the picked option. Arrow keys + Enter. */
    public String select(String question, List<String> options) {
        Map<String, String> items = new LinkedHashMap<>();
        options.forEach(o -> items.put(o, o));
        return select(question, items);
    }

    /** Single choice; map keys are shown, values are returned. */
    public String select(String question, Map<String, String> displayToValue) {
        var result = flowBuilder.clone().reset()
                .withSingleItemSelector(KEY)
                .name(question)
                .selectItems(displayToValue)
                .and().build().run();
        return result.getContext().get(KEY);
    }

    /** Multiple choice with space to toggle; returns the picked values. */
    public List<String> selectMany(String question, List<String> options) {
        var items = options.stream().map(o -> SelectItem.of(o, o)).toList();
        var result = flowBuilder.clone().reset()
                .withMultiItemSelector(KEY)
                .name(question)
                .selectItems(items)
                .and().build().run();
        List<String> picked = result.getContext().get(KEY);
        return picked != null ? picked : List.of();
    }

    public boolean confirm(String question, boolean defaultValue) {
        var result = flowBuilder.clone().reset()
                .withConfirmationInput(KEY)
                .name(question)
                .defaultValue(defaultValue)
                .and().build().run();
        Boolean answer = result.getContext().get(KEY);
        return answer != null ? answer : defaultValue;
    }

    public String text(String question, String defaultValue) {
        var result = flowBuilder.clone().reset()
                .withStringInput(KEY)
                .name(question)
                .defaultValue(defaultValue)
                .and().build().run();
        String answer = result.getContext().get(KEY);
        return answer != null && !answer.isBlank() ? answer : defaultValue;
    }
}
