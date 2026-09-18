package com.gokgor.logworm.consumergroup;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import com.gokgor.logworm.IntegrationTest;

@IntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsumerGroupApiIntegrationTest {

    private static final String TOPIC = "it-cg-orders";
    private static final String GROUP = "it-cg-group";

    @Autowired
    MockMvc mockMvc;

    @Autowired
    Admin adminClient;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrapServers;

    private KafkaConsumer<String, String> consumer;

    /** 5 records in one partition; a real consumer joins the group, reads them all, commits offset 3 → lag 2. */
    @BeforeAll
    void produceAndCommit() throws Exception {
        adminClient.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        for (int i = 0; i < 5; i++) {
            kafkaTemplate.send(TOPIC, "k" + i, "v" + i).get(10, TimeUnit.SECONDS);
        }

        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, GROUP,
                ConsumerConfig.CLIENT_ID_CONFIG, "it-client",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(TOPIC));
        long deadline = System.currentTimeMillis() + 20_000;
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200));
        }
        consumer.commitSync(Map.of(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(3)));
    }

    @AfterAll
    void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void listsGroupWithLag() throws Exception {
        mockMvc.perform(get("/api/consumer-groups"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].groupId", hasItem(GROUP)))
                .andExpect(jsonPath("$[?(@.groupId == '" + GROUP + "')].state").value("Stable"))
                .andExpect(jsonPath("$[?(@.groupId == '" + GROUP + "')].memberCount").value(1))
                .andExpect(jsonPath("$[?(@.groupId == '" + GROUP + "')].topics[0]").value(TOPIC))
                .andExpect(jsonPath("$[?(@.groupId == '" + GROUP + "')].totalLag").value(2));
    }

    @Test
    void detailShowsMemberAssignmentAndPartitionLag() throws Exception {
        mockMvc.perform(get("/api/consumer-groups/" + GROUP))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.groupId").value(GROUP))
                .andExpect(jsonPath("$.state").value("Stable"))
                .andExpect(jsonPath("$.coordinator.id").isNumber())
                .andExpect(jsonPath("$.members", hasSize(1)))
                .andExpect(jsonPath("$.members[0].clientId").value("it-client"))
                .andExpect(jsonPath("$.members[0].assignments[0].topic").value(TOPIC))
                .andExpect(jsonPath("$.members[0].assignments[0].partition").value(0))
                .andExpect(jsonPath("$.partitions", hasSize(1)))
                .andExpect(jsonPath("$.partitions[0].topic").value(TOPIC))
                .andExpect(jsonPath("$.partitions[0].committedOffset").value(3))
                .andExpect(jsonPath("$.partitions[0].endOffset").value(5))
                .andExpect(jsonPath("$.partitions[0].lag").value(2))
                .andExpect(jsonPath("$.partitions[0].memberId").isString())
                .andExpect(jsonPath("$.totalLag").value(2));
    }

    @Test
    void unknownGroupIs404() throws Exception {
        mockMvc.perform(get("/api/consumer-groups/no-such-group"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Consumer group not found: no-such-group"));
    }
}
