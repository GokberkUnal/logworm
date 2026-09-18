package com.gokgor.logworm.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.gokgor.logworm.IntegrationTest;

@IntegrationTest
class KafkaConnectionIntegrationTest {

    @Autowired
    KafkaConnection connection;

    @Autowired
    Admin admin;

    @Test
    void verifyFillsInClusterInfo() {
        var info = connection.verify();

        assertThat(info.clusterId()).isNotBlank();
        assertThat(info.brokerCount()).isEqualTo(1);
        assertThat(connection.info()).isEqualTo(info);
    }

    @Test
    void failedConnectKeepsTheOldConnection() throws Exception {
        String before = connection.info().bootstrapServers();

        assertThatThrownBy(() -> connection.connect("localhost:1"))
                .isInstanceOf(KafkaRequestException.class);

        assertThat(connection.info().bootstrapServers()).isEqualTo(before);
        // the proxied Admin bean still works
        assertThat(admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS)).isNotBlank();
    }

    @Test
    void reconnectToSameClusterSwapsTheClient() throws Exception {
        String servers = connection.info().bootstrapServers();
        var before = connection.admin();

        var info = connection.connect(servers);

        assertThat(connection.admin()).isNotSameAs(before);
        assertThat(info.clusterId()).isNotBlank();
        assertThat(admin.listTopics().names().get(10, TimeUnit.SECONDS)).isNotNull();
    }
}
