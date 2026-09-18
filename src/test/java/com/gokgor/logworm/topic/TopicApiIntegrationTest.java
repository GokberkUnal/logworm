package com.gokgor.logworm.topic;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import com.gokgor.logworm.IntegrationTest;

@IntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TopicApiIntegrationTest {

    private static final String TOPIC = "it-orders";

    @Autowired
    MockMvc mockMvc;

    @Autowired
    Admin adminClient;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @BeforeAll
    void createTopicAndProduce() throws Exception {
        adminClient.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1))).all().get(10, TimeUnit.SECONDS);

        // 3 messages to partition 0, 2 to partition 1
        for (int i = 0; i < 3; i++) {
            kafkaTemplate.send(TOPIC, 0, "k" + i, "v" + i).get(10, TimeUnit.SECONDS);
        }
        for (int i = 0; i < 2; i++) {
            kafkaTemplate.send(TOPIC, 1, "k" + i, "v" + i).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void listsCreatedTopic() throws Exception {
        mockMvc.perform(get("/api/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].name", hasItem(TOPIC)))
                .andExpect(jsonPath("$[?(@.name == '" + TOPIC + "')].partitionCount").value(2))
                .andExpect(jsonPath("$[?(@.name == '" + TOPIC + "')].replicationFactor").value(1))
                .andExpect(jsonPath("$[?(@.name == '" + TOPIC + "')].internal").value(false));
    }

    @Test
    void topicDetailReflectsProducedMessages() throws Exception {
        mockMvc.perform(get("/api/topics/" + TOPIC))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value(TOPIC))
                .andExpect(jsonPath("$.partitionCount").value(2))
                .andExpect(jsonPath("$.messageCount").value(5))
                .andExpect(jsonPath("$.partitions", hasSize(2)))
                .andExpect(jsonPath("$.partitions[0].id").value(0))
                .andExpect(jsonPath("$.partitions[0].earliestOffset").value(0))
                .andExpect(jsonPath("$.partitions[0].latestOffset").value(3))
                .andExpect(jsonPath("$.partitions[0].messageCount").value(3))
                .andExpect(jsonPath("$.partitions[0].leader").isNumber())
                .andExpect(jsonPath("$.partitions[0].replicas", hasSize(1)))
                .andExpect(jsonPath("$.partitions[0].inSyncReplicas", hasSize(1)))
                .andExpect(jsonPath("$.partitions[1].messageCount").value(2));
    }

    @Test
    void unknownTopicIsProblemDetail404() throws Exception {
        mockMvc.perform(get("/api/topics/does-not-exist"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.detail").value("Topic not found: does-not-exist"));
    }
}
