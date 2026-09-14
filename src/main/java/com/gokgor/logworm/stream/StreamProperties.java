package com.gokgor.logworm.stream;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Live-tail settings (prefix "logworm.stream").
 *
 * @param maxConcurrent  open SSE streams allowed at once; more get HTTP 503
 * @param defaultRate    messages/second per stream when the client does not ask for a rate
 * @param maxRate        upper bound a client may request
 * @param heartbeat      idle time after which a keep-alive comment is sent
 * @param lagSkipFactor  when consumer lag exceeds {@code rate × lagSkipFactor}, jump to the end and report skipped records
 */
@ConfigurationProperties(prefix = "logworm.stream")
public record StreamProperties(
        Integer maxConcurrent,
        Integer defaultRate,
        Integer maxRate,
        Duration heartbeat,
        Integer lagSkipFactor) {

    public StreamProperties {
        maxConcurrent = maxConcurrent != null ? maxConcurrent : 20;
        defaultRate = defaultRate != null ? defaultRate : 100;
        maxRate = maxRate != null ? maxRate : 1000;
        heartbeat = heartbeat != null ? heartbeat : Duration.ofSeconds(15);
        lagSkipFactor = lagSkipFactor != null ? lagSkipFactor : 10;
    }
}
