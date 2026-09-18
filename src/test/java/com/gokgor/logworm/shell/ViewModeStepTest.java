package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.gokgor.logworm.message.KafkaMessage;
import com.gokgor.logworm.message.MessagePage;
import com.gokgor.logworm.message.MessageService;

import tools.jackson.databind.json.JsonMapper;

class ViewModeStepTest {

    private final Choices choices = mock(Choices.class);
    private final MessageService messageService = mock(MessageService.class);
    private final ShellSession session = new ShellSession();
    private final ByteArrayOutputStream console = new ByteArrayOutputStream();
    private final ViewModeStep step = new ViewModeStep(choices, session, messageService);

    @Test
    void allAndNewSetTheModeWithoutSampling() {
        when(choices.select(anyString(), anyMap())).thenReturn(ViewModeStep.NEW);
        step.ask(out());
        assertThat(session.view()).isEqualTo(new ViewSettings(ViewMode.NEW, List.of()));

        when(choices.select(anyString(), anyMap())).thenReturn(ViewModeStep.ALL);
        step.ask(out());
        assertThat(session.view()).isEqualTo(new ViewSettings(ViewMode.ALL, List.of()));

        verify(messageService, never()).read(anyString(), any());
        assertThat(console()).contains("View: new messages only, whole value").contains("View: all messages, whole value");
    }

    @Test
    void specificFieldsSamplesTopicAndAsksWhichMessages() {
        session.setCurrentTopic("demo-logs");
        var json = JsonMapper.builder().build();
        var sample = List.of(
                new KafkaMessage(0, 1, Instant.EPOCH, "CREATE_TIME", "k", json.readTree("{\"level\":\"INFO\",\"service\":\"a\"}"), "json", List.of(), 0, 0),
                new KafkaMessage(0, 2, Instant.EPOCH, "CREATE_TIME", "k", json.readTree("{\"level\":\"WARN\",\"message\":\"x\"}"), "json", List.of(), 0, 0));
        when(messageService.read(eq("demo-logs"), any())).thenReturn(new MessagePage("demo-logs", 2, 2, sample));
        when(choices.select(eq("What do you want to see?"), anyMap())).thenReturn(ViewModeStep.FIELDS);
        when(choices.selectMany(anyString(), anyList())).thenReturn(List.of("level", "message"));
        when(choices.select(eq("Of which messages?"), anyMap())).thenReturn(ViewModeStep.OF_NEW);

        step.ask(out());

        verify(choices).selectMany(anyString(), eq(List.of("level", "service", "message")));
        assertThat(session.view()).isEqualTo(new ViewSettings(ViewMode.NEW, List.of("level", "message")));
        assertThat(console()).contains("View: new messages only, fields: level, message");
    }

    @Test
    void noJsonInSampleFallsBackToWholeValue() {
        session.setCurrentTopic("plain");
        when(messageService.read(eq("plain"), any())).thenReturn(new MessagePage("plain", 0, 0, List.of()));
        when(choices.select(eq("What do you want to see?"), anyMap())).thenReturn(ViewModeStep.FIELDS);
        when(choices.select(eq("Of which messages?"), anyMap())).thenReturn(ViewModeStep.OF_ALL);

        step.ask(out());

        verify(choices, never()).selectMany(anyString(), anyList());
        assertThat(session.view()).isEqualTo(new ViewSettings(ViewMode.ALL, List.of()));
        assertThat(console()).contains("No JSON fields found");
    }

    @Test
    void withoutTopicFieldsCannotBeSampled() {
        when(choices.select(eq("What do you want to see?"), anyMap())).thenReturn(ViewModeStep.FIELDS);
        when(choices.select(eq("Of which messages?"), anyMap())).thenReturn(ViewModeStep.OF_ALL);

        step.ask(out());

        verify(messageService, never()).read(anyString(), any());
        assertThat(console()).contains("No topic selected");
        assertThat(session.view().hasFields()).isFalse();
    }

    private PrintStream out() {
        return new PrintStream(console, true, StandardCharsets.UTF_8);
    }

    private String console() {
        return console.toString(StandardCharsets.UTF_8);
    }
}
