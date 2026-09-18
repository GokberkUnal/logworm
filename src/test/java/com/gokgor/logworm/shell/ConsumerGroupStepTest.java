package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.gokgor.logworm.consumergroup.ConsumerGroupService;
import com.gokgor.logworm.consumergroup.ConsumerGroupSummary;
import com.gokgor.logworm.kafka.KafkaRequestException;

class ConsumerGroupStepTest {

    private static final List<ConsumerGroupSummary> GROUPS = List.of(
            new ConsumerGroupSummary("billing", "Stable", "Classic", false, 2, List.of("orders"), 12),
            new ConsumerGroupSummary("audit", "Empty", "Classic", false, 0, List.of("orders"), 5000));

    private final Choices choices = mock(Choices.class);
    private final ConsumerGroupService service = mock(ConsumerGroupService.class);
    private final ShellSession session = new ShellSession();
    private final ByteArrayOutputStream console = new ByteArrayOutputStream();
    private final ConsumerGroupStep step = new ConsumerGroupStep(choices, session, service);

    @Test
    void skipLeavesDefaults() {
        when(service.listGroups()).thenReturn(GROUPS);
        when(choices.select(anyString(), anyMap())).thenReturn(ConsumerGroupStep.SKIP);

        step.ask(out());

        assertThat(session.lagWatch()).isEqualTo(LagWatch.defaults());
        verify(choices, never()).text(anyString(), anyString());
    }

    @Test
    void allGroupsWithCustomThresholds() {
        when(service.listGroups()).thenReturn(GROUPS);
        when(choices.select(anyString(), anyMap())).thenReturn(ConsumerGroupStep.ALL);
        when(choices.text(eq("Warning lag (yellow from)"), anyString())).thenReturn("50");
        when(choices.text(eq("Critical lag (red from)"), anyString())).thenReturn("abc", "500");

        step.ask(out());

        assertThat(session.lagWatch()).isEqualTo(new LagWatch(List.of(), 50, 500));
        assertThat(console()).contains("Lag watch: all groups  warn ≥ 50  critical ≥ 500");
    }

    @Test
    void pickedGroupsAreParsedFromLabelsAndCriticalIsRaisedToWarning() {
        when(service.listGroups()).thenReturn(GROUPS);
        when(choices.select(anyString(), anyMap())).thenReturn(ConsumerGroupStep.PICK);
        when(choices.selectMany(anyString(), anyList())).thenReturn(List.of("audit  (Empty, 0 members, lag 5000)"));
        when(choices.text(eq("Warning lag (yellow from)"), anyString())).thenReturn("300");
        when(choices.text(eq("Critical lag (red from)"), anyString())).thenReturn("100");

        step.ask(out());

        verify(choices).selectMany(anyString(), eq(List.of(
                "billing  (Stable, 2 members, lag 12)", "audit  (Empty, 0 members, lag 5000)")));
        assertThat(session.lagWatch()).isEqualTo(new LagWatch(List.of("audit"), 300, 300));
        assertThat(console()).contains("Critical must be >= warning");
    }

    @Test
    void noGroupsOrErrorSkipsSilently() {
        when(service.listGroups()).thenReturn(List.of());
        step.ask(out());
        assertThat(console()).contains("No consumer groups on this cluster yet");

        when(service.listGroups()).thenThrow(new KafkaRequestException("Kafka did not respond within PT5S", null));
        step.ask(out());
        assertThat(console()).contains("Could not list consumer groups");

        verify(choices, never()).select(anyString(), anyMap());
        assertThat(session.lagWatch()).isEqualTo(LagWatch.defaults());
    }

    private PrintStream out() {
        return new PrintStream(console, true, StandardCharsets.UTF_8);
    }

    private String console() {
        return console.toString(StandardCharsets.UTF_8);
    }
}
