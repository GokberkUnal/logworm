package com.gokgor.logworm.topic;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.gokgor.logworm.api.ApiExceptionHandler;

@WebMvcTest(TopicController.class)
@Import(ApiExceptionHandler.class)
class TopicControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    TopicService topicService;

    @Test
    void listsTopics() throws Exception {
        when(topicService.listTopics()).thenReturn(List.of(
                new TopicSummary("demo-logs", 3, 1, false)));

        mockMvc.perform(get("/api/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("demo-logs"))
                .andExpect(jsonPath("$[0].partitionCount").value(3));
    }

    @Test
    void returnsTopicDetail() throws Exception {
        when(topicService.getTopic("demo-logs")).thenReturn(new TopicDetail(
                "demo-logs", false, 1, 1, 42,
                List.of(new PartitionInfo(0, 1, List.of(1), List.of(1), 0, 42, 42))));

        mockMvc.perform(get("/api/topics/demo-logs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messageCount").value(42))
                .andExpect(jsonPath("$.partitions[0].latestOffset").value(42));
    }

    @Test
    void returns404ForUnknownTopic() throws Exception {
        when(topicService.getTopic("nope")).thenThrow(new TopicNotFoundException("nope"));

        mockMvc.perform(get("/api/topics/nope"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Topic not found: nope"));
    }
}
