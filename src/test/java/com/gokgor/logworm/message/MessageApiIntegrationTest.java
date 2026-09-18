package com.gokgor.logworm.message;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import com.gokgor.logworm.IntegrationTest;

@IntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MessageApiIntegrationTest {

    private static final String TOPIC = "it-messages";
    private static final String BASE = "/api/topics/" + TOPIC + "/messages";

    @Autowired
    MockMvc mockMvc;

    @Autowired
    Admin adminClient;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Partition 0: 10 JSON records, keys k0..k9, value {"i":n,"level":"ERROR"|"INFO"} (ERROR on even n),
     *              header "trace" = "t<n>", plus header "flag" on n=3.
     * Partition 1: 5 plain-text records "text-<n>" with key "p1-<n>".
     */
    @BeforeAll
    void produce() throws Exception {
        adminClient.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1))).all().get(10, TimeUnit.SECONDS);

        for (int n = 0; n < 10; n++) {
            String level = n % 2 == 0 ? "ERROR" : "INFO";
            var record = new ProducerRecord<>(TOPIC, 0, "k" + n,
                    "{\"i\":" + n + ",\"level\":\"" + level + "\"}");
            record.headers().add(new RecordHeader("trace", ("t" + n).getBytes(StandardCharsets.UTF_8)));
            if (n == 3) {
                record.headers().add(new RecordHeader("flag", "yes".getBytes(StandardCharsets.UTF_8)));
            }
            kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
        }
        for (int n = 0; n < 5; n++) {
            kafkaTemplate.send(TOPIC, 1, "p1-" + n, "text-" + n).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void tailReturnsNewestAcrossAllPartitions() throws Exception {
        mockMvc.perform(get(BASE))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.topic").value(TOPIC))
                .andExpect(jsonPath("$.count").value(15))
                .andExpect(jsonPath("$.scanned").value(15))
                .andExpect(jsonPath("$.messages", hasSize(15)));
    }

    @Test
    void tailWithLimitInOnePartitionIsNewestFirst() throws Exception {
        mockMvc.perform(get(BASE).param("partition", "0").param("limit", "3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(3))
                .andExpect(jsonPath("$.messages[*].offset", contains(9, 8, 7)))
                .andExpect(jsonPath("$.messages[0].key").value("k9"))
                .andExpect(jsonPath("$.messages[0].partition").value(0))
                .andExpect(jsonPath("$.messages[0].timestampType").value("CREATE_TIME"))
                .andExpect(jsonPath("$.messages[0].timestamp").isString());
    }

    @Test
    void rangeReadsForwardFromOffset() throws Exception {
        mockMvc.perform(get(BASE).param("partition", "0").param("offset", "4").param("limit", "3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messages[*].offset", contains(4, 5, 6)))
                .andExpect(jsonPath("$.messages[*].key", contains("k4", "k5", "k6")));
    }

    @Test
    void rangeBeyondEndIsEmpty() throws Exception {
        mockMvc.perform(get(BASE).param("partition", "0").param("offset", "999"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(0))
                .andExpect(jsonPath("$.messages", hasSize(0)));
    }

    @Test
    void jsonValuesAreParsedAndPlainTextKept() throws Exception {
        mockMvc.perform(get(BASE).param("partition", "0").param("offset", "2").param("limit", "1"))
                .andExpect(jsonPath("$.messages[0].valueFormat").value("json"))
                .andExpect(jsonPath("$.messages[0].value.i").value(2))
                .andExpect(jsonPath("$.messages[0].value.level").value("ERROR"))
                .andExpect(jsonPath("$.messages[0].headers[0].key").value("trace"))
                .andExpect(jsonPath("$.messages[0].headers[0].value").value("t2"))
                .andExpect(jsonPath("$.messages[0].keySize").value(2))
                .andExpect(jsonPath("$.messages[0].valueSize").value(23));

        mockMvc.perform(get(BASE).param("partition", "1").param("offset", "0").param("limit", "1"))
                .andExpect(jsonPath("$.messages[0].valueFormat").value("string"))
                .andExpect(jsonPath("$.messages[0].value").value("text-0"));

        mockMvc.perform(get(BASE).param("partition", "0").param("offset", "2").param("limit", "1").param("format", "STRING"))
                .andExpect(jsonPath("$.messages[0].valueFormat").value("string"))
                .andExpect(jsonPath("$.messages[0].value").value("{\"i\":2,\"level\":\"ERROR\"}"));
    }

    @Test
    void filtersByValueKeyAndHeader() throws Exception {
        mockMvc.perform(get(BASE).param("partition", "0").param("value", "ERROR"))
                .andExpect(jsonPath("$.count").value(5))
                .andExpect(jsonPath("$.scanned").value(10))
                .andExpect(jsonPath("$.messages[*].value.level", everyItem(is("ERROR"))));

        mockMvc.perform(get(BASE).param("key", "p1-"))
                .andExpect(jsonPath("$.count").value(5))
                .andExpect(jsonPath("$.messages[*].partition", everyItem(is(1))));

        mockMvc.perform(get(BASE).param("header", "flag"))
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.messages[0].key").value("k3"));

        mockMvc.perform(get(BASE).param("header", "trace=t7"))
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.messages[0].offset").value(7));

        mockMvc.perform(get(BASE).param("value", "ERROR").param("limit", "2"))
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.messages[*].offset", containsInAnyOrder(8, 6)));
    }

    @Test
    void validationAndNotFound() throws Exception {
        mockMvc.perform(get(BASE).param("offset", "3"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("offset requires a partition"));

        mockMvc.perform(get(BASE).param("partition", "9"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Topic " + TOPIC + " has no partition 9"));

        mockMvc.perform(get(BASE).param("limit", "0"))
                .andExpect(status().isBadRequest());

        mockMvc.perform(get("/api/topics/no-such-topic/messages"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Topic not found: no-such-topic"));
    }
}
