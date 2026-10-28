package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import com.gokgor.logworm.kafka.KafkaConnection;
import com.gokgor.logworm.topic.TopicService;
import com.gokgor.logworm.topic.TopicSummary;

class ProfileStepTest {

    @TempDir
    Path dir;

    private final Choices choices = mock(Choices.class);
    private final KafkaConnectionStep kafkaConnection = mock(KafkaConnectionStep.class);
    private final KafkaConnection connection = mock(KafkaConnection.class);
    private final TopicService topicService = mock(TopicService.class);
    private final ShellSession session = new ShellSession();
    private final ByteArrayOutputStream console = new ByteArrayOutputStream();
    private ProfileStore store;
    private ProfileStep step;

    @BeforeEach
    void setUp() {
        store = new ProfileStore(dir);
        step = new ProfileStep(choices, session, store, kafkaConnection, connection, topicService);
        when(topicService.listTopics()).thenReturn(List.of(new TopicSummary("demo-logs", 3, 1, false)));
    }

    @Test
    void noProfilesMeansNoQuestion() {
        assertThat(step.offerLoad(out())).isFalse();
        verify(choices, never()).select(anyString(), anyMap());
    }

    @SuppressWarnings("unchecked")
    @Test
    void lastProfileIsOfferedFirstAndAppliedOnPick() {
        store.save(ProfileStoreTest.sample("dev"));
        store.save(ProfileStoreTest.sample("prod"));
        when(choices.select(eq("Start from a saved profile?"), anyMap())).thenReturn("prod");
        when(kafkaConnection.connectTo(eq("localhost:9092"), any())).thenReturn(true);

        assertThat(step.offerLoad(out())).isTrue();

        ArgumentCaptor<Map<String, String>> options = ArgumentCaptor.forClass(Map.class);
        verify(choices).select(eq("Start from a saved profile?"), options.capture());
        assertThat(options.getValue().values()).containsExactly("prod", ProfileStep.OTHER, ProfileStep.NEW);
        Profile expected = ProfileStoreTest.sample("prod");
        assertThat(session.currentTopic()).contains("demo-logs");
        assertThat(session.view()).isEqualTo(expected.view());
        assertThat(session.outputFormat()).isEqualTo(OutputFormat.JSON);
        assertThat(session.rules()).isEqualTo(expected.rules());
        assertThat(session.lagWatch()).isEqualTo(expected.lagWatch());
    }

    @Test
    void otherProfileSubmenuAndBack() {
        store.save(ProfileStoreTest.sample("dev"));
        store.save(ProfileStoreTest.sample("prod"));
        when(choices.select(eq("Start from a saved profile?"), anyMap()))
                .thenReturn(ProfileStep.OTHER, ProfileStep.OTHER);
        when(choices.select(eq("Saved profiles"), anyMap())).thenReturn(ProfileStep.BACK, "dev");
        when(kafkaConnection.connectTo(anyString(), any())).thenReturn(true);

        assertThat(step.offerLoad(out())).isTrue();
        assertThat(store.last()).contains("dev");
    }

    @Test
    void failedConnectionFallsBackToFullSetupWithoutTouchingSession() {
        store.save(ProfileStoreTest.sample("prod"));
        when(choices.select(anyString(), anyMap())).thenReturn("prod");
        when(kafkaConnection.connectTo(anyString(), any())).thenReturn(false);

        assertThat(step.offerLoad(out())).isFalse();
        assertThat(session.all()).isEmpty();
        assertThat(console()).contains("Continuing with the full setup.");
    }

    @Test
    void missingTopicIsDroppedButOtherSettingsApply() {
        session.setCurrentTopic("old-topic");
        when(topicService.listTopics()).thenReturn(List.of());
        when(kafkaConnection.connectTo(anyString(), any())).thenReturn(true);

        assertThat(step.apply(ProfileStoreTest.sample("prod"), out())).isTrue();

        assertThat(session.currentTopic()).isEmpty();
        assertThat(session.outputFormat()).isEqualTo(OutputFormat.JSON);
        assertThat(console()).contains("Topic demo-logs is not on this cluster");
    }

    @Test
    void saveAsksNameRetriesInvalidAndRespectsOverwrite() {
        store.save(ProfileStoreTest.sample("demo-logs"));
        session.put(KafkaConnectionStep.SESSION_KEY, "other:9092");
        session.setCurrentTopic("demo-logs");
        when(choices.confirm(eq("Save these settings as a profile?"), anyBoolean())).thenReturn(true);
        when(choices.text(eq("Profile name"), eq("demo-logs"))).thenReturn("bad name", "demo-logs", "mine");
        when(choices.confirm(eq("Profile demo-logs exists. Overwrite?"), anyBoolean())).thenReturn(false);

        step.offerSave(out());

        assertThat(console()).contains("Use letters, digits");
        assertThat(store.load("demo-logs").orElseThrow().bootstrapServers()).isEqualTo("localhost:9092");
        Profile mine = store.load("mine").orElseThrow();
        assertThat(mine.bootstrapServers()).isEqualTo("other:9092");
        assertThat(mine.topic()).isEqualTo("demo-logs");
        assertThat(store.last()).contains("mine");
    }

    @Test
    void declineSaveWritesNothing() {
        when(choices.confirm(anyString(), anyBoolean())).thenReturn(false);
        step.offerSave(out());
        assertThat(store.names()).isEmpty();
        verify(choices, never()).text(anyString(), anyString());
    }

    private PrintStream out() {
        return new PrintStream(console, true, StandardCharsets.UTF_8);
    }

    private String console() {
        return console.toString(StandardCharsets.UTF_8);
    }
}
