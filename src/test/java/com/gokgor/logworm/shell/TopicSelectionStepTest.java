package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.gokgor.logworm.kafka.KafkaRequestException;
import com.gokgor.logworm.topic.TopicService;
import com.gokgor.logworm.topic.TopicSummary;

class TopicSelectionStepTest {

    private final Choices choices = mock(Choices.class);
    private final TopicService topicService = mock(TopicService.class);
    private final ShellSession session = new ShellSession();
    private final ByteArrayOutputStream console = new ByteArrayOutputStream();
    private final TopicSelectionStep step = new TopicSelectionStep(choices, session, topicService);

    private static final List<TopicSummary> TOPICS = List.of(
            new TopicSummary("__consumer_offsets", 50, 1, true),
            new TopicSummary("demo-logs", 3, 1, false),
            new TopicSummary("orders", 1, 1, false));

    @Test
    void picksATopicAndStoresItInSession() {
        when(topicService.listTopics()).thenReturn(TOPICS);
        when(choices.select(anyString(), anyMap())).thenReturn("demo-logs");

        step.ask(out());

        assertThat(session.currentTopic()).contains("demo-logs");
        assertThat(console()).contains("Using topic demo-logs");
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        verify(choices).select(eq("Which topic?"), captor.capture());
        assertThat(captor.getValue().keySet()).containsExactly(
                "demo-logs  (3 partitions)", "orders  (1 partition)", TopicSelectionStep.SHOW_INTERNAL, TopicSelectionStep.SKIP);
    }

    @Test
    void showInternalReopensTheListWithInternalTopics() {
        when(topicService.listTopics()).thenReturn(TOPICS);
        when(choices.select(anyString(), anyMap())).thenReturn(TopicSelectionStep.SHOW_INTERNAL, "__consumer_offsets");

        step.ask(out());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        verify(choices, times(2)).select(eq("Which topic?"), captor.capture());
        assertThat(captor.getValue().keySet()).containsExactly(
                "__consumer_offsets  (50 partitions, internal)", "demo-logs  (3 partitions)", "orders  (1 partition)", TopicSelectionStep.SKIP);
        assertThat(session.currentTopic()).contains("__consumer_offsets");
    }

    @Test
    void skipLeavesNoTopic() {
        when(topicService.listTopics()).thenReturn(TOPICS);
        when(choices.select(anyString(), anyMap())).thenReturn(TopicSelectionStep.SKIP);

        step.ask(out());

        assertThat(session.currentTopic()).isEmpty();
        assertThat(console()).contains("No topic selected");
    }

    @Test
    void emptyClusterOrKafkaErrorSkipsWithoutAsking() {
        when(topicService.listTopics()).thenReturn(List.of());
        step.ask(out());
        assertThat(console()).contains("No topics on this cluster");

        when(topicService.listTopics()).thenThrow(new KafkaRequestException("Kafka did not respond within PT5S", null));
        step.ask(out());
        assertThat(console()).contains("Could not list topics (Kafka did not respond");

        verify(choices, never()).select(anyString(), anyMap());
        assertThat(session.currentTopic()).isEmpty();
    }

    private PrintStream out() {
        return new PrintStream(console, true, StandardCharsets.UTF_8);
    }

    private String console() {
        return console.toString(StandardCharsets.UTF_8);
    }
}
