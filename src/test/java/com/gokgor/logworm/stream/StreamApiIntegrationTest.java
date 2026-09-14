package com.gokgor.logworm.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import com.gokgor.logworm.IntegrationTest;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@IntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StreamApiIntegrationTest {

    private static final String TOPIC = "it-stream";

    @LocalServerPort
    int port;

    @Autowired
    MockMvc mockMvc;

    @Autowired
    AdminClient adminClient;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    LiveTailService liveTailService;

    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();

    @BeforeAll
    void createTopic() throws Exception {
        adminClient.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        // a record produced BEFORE connecting must not be streamed (tail starts at the log end)
        kafkaTemplate.send(TOPIC, "old", "{\"level\":\"ERROR\",\"n\":-1}").get(10, TimeUnit.SECONDS);
    }

    @Test
    void streamsOnlyNewMatchingRecordsAsSse() throws Exception {
        var request = HttpRequest.newBuilder(URI.create(
                "http://localhost:" + port + "/api/topics/" + TOPIC + "/stream?value=ERROR&rate=50")).build();

        // Read the stream on a background thread; stop after the second "message" event.
        CompletableFuture<List<String>> lines = http.sendAsync(request, HttpResponse.BodyHandlers.ofLines())
                .thenApply(response -> {
                    assertThat(response.statusCode()).isEqualTo(200);
                    assertThat(response.headers().firstValue("Content-Type").orElse("")).startsWith("text/event-stream");
                    return collectUntilMessages(response.body(), 2);
                });

        awaitStreamOpen();
        for (int n = 0; n < 4; n++) {
            String level = n % 2 == 0 ? "ERROR" : "INFO";
            kafkaTemplate.send(TOPIC, "k" + n, "{\"level\":\"" + level + "\",\"n\":" + n + "}").get(10, TimeUnit.SECONDS);
        }

        List<String> received = lines.get(20, TimeUnit.SECONDS);

        List<String> eventNames = received.stream().filter(l -> l.startsWith("event:"))
                .map(l -> l.substring("event:".length()).trim()).toList();
        assertThat(eventNames).startsWith("connected");
        assertThat(eventNames.stream().filter("message"::equals).count()).isEqualTo(2);

        List<JsonNode> messages = dataAfter(received, "message");
        assertThat(messages).extracting(m -> m.get("key").asString()).containsExactly("k0", "k2");
        assertThat(messages).allSatisfy(m -> {
            assertThat(m.get("valueFormat").asString()).isEqualTo("json");
            assertThat(m.get("value").get("level").asString()).isEqualTo("ERROR");
        });

        JsonNode connected = dataAfter(received, "connected").getFirst();
        assertThat(connected.get("topic").asString()).isEqualTo(TOPIC);
        assertThat(connected.get("startOffsets").get("0").asLong()).isEqualTo(1); // "old" record sits at offset 0
        assertThat(connected.get("rate").asInt()).isEqualTo(50);
    }

    @Test
    void unknownTopicAndBadPartitionAreNormalErrors() throws Exception {
        mockMvc.perform(get("/api/topics/no-such-topic/stream"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Topic not found: no-such-topic"));

        mockMvc.perform(get("/api/topics/" + TOPIC + "/stream").param("partition", "7"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Topic " + TOPIC + " has no partition 7"));

        mockMvc.perform(get("/api/topics/" + TOPIC + "/stream").param("rate", "0"))
                .andExpect(status().isBadRequest());
    }

    // --- helpers -----------------------------------------------------------------------------

    private void awaitStreamOpen() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (liveTailService.openStreams() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        Thread.sleep(300); // let the tail seek to the end before producing
    }

    private static List<String> collectUntilMessages(Stream<String> body, int wanted) {
        List<String> lines = new ArrayList<>();
        int messages = 0;
        var it = body.iterator();
        while (it.hasNext()) {
            String line = it.next();
            lines.add(line);
            if (line.equals("event:message")) {
                messages++;
            }
            if (messages == wanted && line.isEmpty()) {
                break; // blank line terminates the SSE event
            }
        }
        body.close(); // closes the connection → server side sees the client leave
        return lines;
    }

    private List<JsonNode> dataAfter(List<String> lines, String event) {
        List<JsonNode> out = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).equals("event:" + event)) {
                for (int j = i + 1; j < lines.size(); j++) {
                    if (lines.get(j).startsWith("data:")) {
                        out.add(json.readTree(lines.get(j).substring("data:".length())));
                        break;
                    }
                }
            }
        }
        return out;
    }
}
