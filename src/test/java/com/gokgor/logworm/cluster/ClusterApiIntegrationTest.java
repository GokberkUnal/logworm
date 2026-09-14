package com.gokgor.logworm.cluster;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.emptyString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

import com.gokgor.logworm.IntegrationTest;

@IntegrationTest
class ClusterApiIntegrationTest {

    @Autowired
    MockMvc mockMvc;

    @Test
    void describesEmbeddedCluster() throws Exception {
        mockMvc.perform(get("/api/cluster"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterId").value(not(emptyString())))
                .andExpect(jsonPath("$.controllerId").isNumber())
                .andExpect(jsonPath("$.metadataVersion").isString())
                .andExpect(jsonPath("$.brokers", hasSize(1)))
                .andExpect(jsonPath("$.brokers[0].host").isString())
                .andExpect(jsonPath("$.brokers[0].port").isNumber());
    }
}
