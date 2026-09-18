package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.gokgor.logworm.shell.Rule.Color;
import com.gokgor.logworm.shell.Rule.Condition;

class FilterAlertStepTest {

    private final Choices choices = mock(Choices.class);
    private final FieldSampler sampler = mock(FieldSampler.class);
    private final ShellSession session = new ShellSession();
    private final ByteArrayOutputStream console = new ByteArrayOutputStream();
    private final FilterAlertStep step = new FilterAlertStep(choices, session, sampler);

    @BeforeEach
    void setUp() {
        session.setCurrentTopic("demo-logs");
        when(sampler.sampleWithKey("demo-logs")).thenReturn(List.of("key", "level", "amount"));
    }

    @Test
    void noFilterNoAlertsLeavesRulesEmpty() {
        when(choices.select(eq("Filter messages?"), anyMap())).thenReturn(FilterAlertStep.NO_FILTER);
        when(choices.select(eq("Alerts?"), anyMap())).thenReturn(FilterAlertStep.NO_ALERTS);

        step.ask(out());

        assertThat(session.rules().isEmpty()).isTrue();
        assertThat(console()).contains("Rules: no filters, no alerts");
    }

    @Test
    void filterThenAlertWithLabelAndColor() {
        when(choices.select(eq("Filter messages?"), anyMap())).thenReturn(FilterAlertStep.ADD_FILTER);
        when(choices.select(eq("Alerts?"), anyMap())).thenReturn(FilterAlertStep.ADD_ALERT);
        when(choices.select(eq("Which field?"), anyMap())).thenReturn("level", "amount");
        when(choices.select(startsWith("Condition for"), anyMap()))
                .thenReturn(Condition.EQUALS.name(), Condition.IS_NULL.name());
        when(choices.text(eq("Value"), anyString())).thenReturn("ERROR");
        when(choices.select(eq("Color for matches"), anyList())).thenReturn("RED");
        when(choices.text(eq("Label"), anyString())).thenReturn("missing amount");
        when(choices.confirm(anyString(), any(Boolean.class))).thenReturn(false);

        step.ask(out());

        RuleSet rules = session.rules();
        assertThat(rules.filters()).containsExactly(Rule.filter("level", Condition.EQUALS, "ERROR").withLabel("level == ERROR"));
        assertThat(rules.alerts()).containsExactly(new Rule("amount", Condition.IS_NULL, null, Color.RED, "missing amount"));
        assertThat(console()).contains("filters: level == ERROR; alerts: red when amount is null");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> fields = ArgumentCaptor.forClass(Map.class);
        verify(choices, org.mockito.Mockito.times(2)).select(eq("Which field?"), fields.capture());
        assertThat(fields.getValue().keySet()).containsExactly("key", "level", "amount", FilterAlertStep.OTHER_FIELD);
    }

    @Test
    void otherFieldIsTypedAndLoopsAddSeveralFilters() {
        when(choices.select(eq("Filter messages?"), anyMap())).thenReturn(FilterAlertStep.ADD_FILTER);
        when(choices.select(eq("Alerts?"), anyMap())).thenReturn(FilterAlertStep.NO_ALERTS);
        when(choices.select(eq("Which field?"), anyMap())).thenReturn(FilterAlertStep.OTHER_FIELD, "key");
        when(choices.text(startsWith("Field path"), anyString())).thenReturn(" payload.status ");
        when(choices.select(startsWith("Condition for"), anyMap()))
                .thenReturn(Condition.CONTAINS.name(), Condition.IS_NOT_NULL.name());
        when(choices.text(eq("Value"), anyString())).thenReturn("FAIL");
        when(choices.confirm(eq("Add another filter?"), any(Boolean.class))).thenReturn(true, false);

        step.ask(out());

        assertThat(session.rules().filters()).extracting(Rule::describe)
                .containsExactly("payload.status contains FAIL", "key is not null");
    }

    @Test
    void withoutTopicNothingIsAsked() {
        var empty = new ShellSession();
        new FilterAlertStep(choices, empty, sampler).ask(out());

        verify(choices, never()).select(anyString(), anyMap());
        assertThat(empty.rules().isEmpty()).isTrue();
    }

    private PrintStream out() {
        return new PrintStream(console, true, StandardCharsets.UTF_8);
    }

    private String console() {
        return console.toString(StandardCharsets.UTF_8);
    }
}
