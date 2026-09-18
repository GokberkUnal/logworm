package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.gokgor.logworm.kafka.KafkaConnection;
import com.gokgor.logworm.kafka.KafkaRequestException;

class KafkaConnectionStepTest {

    private final Choices choices = mock(Choices.class);
    private final KafkaConnection connection = mock(KafkaConnection.class);
    private final RecentConnections recent = mock(RecentConnections.class);
    private final ShellSession session = new ShellSession();
    private final ByteArrayOutputStream console = new ByteArrayOutputStream();
    private KafkaConnectionStep step;

    @BeforeEach
    void setUp() {
        when(connection.configuredBootstrapServers()).thenReturn("localhost:9092");
        when(connection.info()).thenReturn(new KafkaConnection.Info("localhost:9092", null, 0));
        when(recent.list()).thenReturn(List.of("broker.old:9092", "localhost:9092"));
        step = new KafkaConnectionStep(choices, session, connection, recent);
    }

    @Test
    void pickingTheConfiguredAddressVerifiesInsteadOfReconnecting() {
        when(choices.select(anyString(), anyMap())).thenReturn("localhost:9092");
        when(connection.verify()).thenReturn(new KafkaConnection.Info("localhost:9092", "abc", 1));

        step.ask(out());

        verify(connection, never()).connect(anyString());
        verify(recent).remember("localhost:9092");
        assertThat(session.get(KafkaConnectionStep.SESSION_KEY, String.class)).contains("localhost:9092");
        assertThat(console()).contains("ok  (cluster abc, 1 broker)");
    }

    @Test
    void mainMenuHasThreeOptionsAndRecentOpensSubmenu() {
        when(choices.select(eq("Which Kafka cluster?"), anyMap()))
                .thenReturn(KafkaConnectionStep.RECENT, KafkaConnectionStep.RECENT);
        when(choices.select(eq("Recent connections"), anyMap()))
                .thenReturn(KafkaConnectionStep.BACK, "broker.old:9092");
        when(connection.connect("broker.old:9092")).thenReturn(new KafkaConnection.Info("broker.old:9092", "old", 3));

        step.ask(out());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        verify(choices, org.mockito.Mockito.times(2)).select(eq("Which Kafka cluster?"), captor.capture());
        assertThat(captor.getValue().keySet()).containsExactly(
                "localhost:9092  (application.yml)", "Recent connections...  (1)", "Other address...");
        verify(choices, org.mockito.Mockito.times(2)).select(eq("Recent connections"), captor.capture());
        assertThat(captor.getValue().keySet()).containsExactly("broker.old:9092", KafkaConnectionStep.BACK);
        verify(connection).connect("broker.old:9092");
    }

    @Test
    void recentOptionIsHiddenWhenNothingRecent() {
        when(recent.list()).thenReturn(List.of());
        when(choices.select(anyString(), anyMap())).thenReturn("localhost:9092");
        when(connection.verify()).thenReturn(new KafkaConnection.Info("localhost:9092", "abc", 1));

        step.ask(out());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        verify(choices).select(eq("Which Kafka cluster?"), captor.capture());
        assertThat(captor.getValue().keySet()).containsExactly("localhost:9092  (application.yml)", "Other address...");
    }

    @Test
    void otherAddressIsValidatedThenConnected() {
        when(choices.select(anyString(), anyMap())).thenReturn(KafkaConnectionStep.OTHER);
        when(choices.text(anyString(), anyString())).thenReturn("nonsense", "b1:9092,b2:9092");
        when(connection.connect("b1:9092,b2:9092")).thenReturn(new KafkaConnection.Info("b1:9092,b2:9092", "xyz", 2));

        step.ask(out());

        verify(connection).connect("b1:9092,b2:9092");
        verify(recent).remember("b1:9092,b2:9092");
        assertThat(console()).contains("Expected host:port").contains("2 brokers");
    }

    @Test
    void failureOffersRetryThenContinuesUnverified() {
        when(choices.select(anyString(), anyMap())).thenReturn("broker.old:9092");
        when(connection.connect("broker.old:9092")).thenThrow(new KafkaRequestException("Kafka did not respond within PT5S", null));
        when(choices.confirm(anyString(), any(Boolean.class))).thenReturn(true, false);

        step.ask(out());

        verify(connection, org.mockito.Mockito.times(2)).connect("broker.old:9092");
        verify(recent, never()).remember(anyString());
        assertThat(console()).contains("failed: Kafka did not respond").contains("Continuing without a verified connection");
        assertThat(session.get(KafkaConnectionStep.SESSION_KEY, String.class)).contains("broker.old:9092");
    }

    private PrintStream out() {
        return new PrintStream(console, true, StandardCharsets.UTF_8);
    }

    private String console() {
        return console.toString(StandardCharsets.UTF_8);
    }
}
