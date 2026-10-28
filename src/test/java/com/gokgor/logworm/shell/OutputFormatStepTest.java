package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class OutputFormatStepTest {

    private final Choices choices = mock(Choices.class);
    private final ShellSession session = new ShellSession();
    private final ByteArrayOutputStream console = new ByteArrayOutputStream();
    private final FieldSampler sampler = mock(FieldSampler.class);
    private final OutputFormatStep step = new OutputFormatStep(choices, session, sampler);

    @Test
    void defaultIsPrettyUntilAsked() {
        assertThat(session.outputFormat()).isEqualTo(OutputFormat.PRETTY);
    }

    @Test
    void pickStoresFormatInSession() {
        when(choices.select(anyString(), anyMap())).thenReturn("JSON");

        step.ask(new PrintStream(console, true, StandardCharsets.UTF_8));

        assertThat(session.outputFormat()).isEqualTo(OutputFormat.JSON);
        assertThat(console.toString(StandardCharsets.UTF_8)).contains("Output format: json");
    }

    @Test
    void offersPrettyTableJsonLineAndCdcInThatOrder() {
        when(choices.select(anyString(), anyMap())).thenReturn("LINE");
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> options = ArgumentCaptor.forClass(Map.class);

        step.ask(new PrintStream(console, true, StandardCharsets.UTF_8));

        verify(choices).select(eq("How should messages be printed?"), options.capture());
        assertThat(options.getValue().keySet()).containsExactly(
                OutputFormatStep.PRETTY, OutputFormatStep.TABLE, OutputFormatStep.JSON, OutputFormatStep.LINE, OutputFormatStep.CDC);
        assertThat(options.getValue().values()).containsExactly("PRETTY", "TABLE", "JSON", "LINE", "CDC");
        assertThat(session.outputFormat()).isEqualTo(OutputFormat.LINE);
    }

    @Test
    void detectedCdcTopicPutsCdcFirst() {
        session.setCurrentTopic("demo-cdc");
        when(sampler.looksLikeCdc("demo-cdc")).thenReturn(true);
        when(choices.select(anyString(), anyMap())).thenReturn("CDC");
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> options = ArgumentCaptor.forClass(Map.class);

        step.ask(new PrintStream(console, true, StandardCharsets.UTF_8));

        verify(choices).select(eq("How should messages be printed?"), options.capture());
        assertThat(options.getValue().keySet()).first().isEqualTo(OutputFormatStep.CDC_DETECTED);
        assertThat(options.getValue().values()).containsExactly("CDC", "PRETTY", "TABLE", "JSON", "LINE");
        assertThat(session.outputFormat()).isEqualTo(OutputFormat.CDC);
    }

    @Test
    void detectionFailureIsIgnored() {
        session.setCurrentTopic("t");
        when(sampler.looksLikeCdc("t")).thenThrow(new IllegalStateException("down"));
        when(choices.select(anyString(), anyMap())).thenReturn("PRETTY");

        step.ask(new PrintStream(console, true, StandardCharsets.UTF_8));

        assertThat(session.outputFormat()).isEqualTo(OutputFormat.PRETTY);
    }
}
