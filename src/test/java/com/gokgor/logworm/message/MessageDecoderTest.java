package com.gokgor.logworm.message;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class MessageDecoderTest {

    private final MessageDecoder decoder = new MessageDecoder(JsonMapper.builder().build());

    @Test
    void decodesJsonValueWhenAuto() {
        var raw = decoder.toRaw(record("k1", "{\"level\":\"ERROR\",\"n\":3}", new RecordHeaders()));

        var msg = decoder.toMessage(raw, ValueFormat.AUTO);

        assertThat(msg.valueFormat()).isEqualTo("json");
        assertThat(msg.value()).isInstanceOf(JsonNode.class);
        assertThat(((JsonNode) msg.value()).get("level").asString()).isEqualTo("ERROR");
        assertThat(msg.key()).isEqualTo("k1");
        assertThat(msg.partition()).isEqualTo(2);
        assertThat(msg.offset()).isEqualTo(42);
        assertThat(msg.timestamp().toEpochMilli()).isEqualTo(1_700_000_000_000L);
        assertThat(msg.timestampType()).isEqualTo("CREATE_TIME");
        assertThat(msg.keySize()).isEqualTo(2);
        assertThat(msg.valueSize()).isEqualTo(23);
    }

    @Test
    void keepsStringWhenFormatIsString() {
        var raw = decoder.toRaw(record("k", "{\"a\":1}", new RecordHeaders()));

        var msg = decoder.toMessage(raw, ValueFormat.STRING);

        assertThat(msg.valueFormat()).isEqualTo("string");
        assertThat(msg.value()).isEqualTo("{\"a\":1}");
    }

    @Test
    void fallsBackToStringForInvalidJson() {
        var raw = decoder.toRaw(record("k", "{not json", new RecordHeaders()));

        var msg = decoder.toMessage(raw, ValueFormat.AUTO);

        assertThat(msg.valueFormat()).isEqualTo("string");
        assertThat(msg.value()).isEqualTo("{not json");
    }

    @Test
    void plainTextIsNotParsed() {
        var msg = decoder.toMessage(decoder.toRaw(record("k", "hello world", new RecordHeaders())), ValueFormat.AUTO);

        assertThat(msg.valueFormat()).isEqualTo("string");
        assertThat(msg.value()).isEqualTo("hello world");
    }

    @Test
    void tombstoneAndNullKey() {
        var rec = new ConsumerRecord<byte[], byte[]>("t", 0, 1, 0L, TimestampType.CREATE_TIME,
                -1, -1, null, null, new RecordHeaders(), Optional.empty());

        var msg = decoder.toMessage(decoder.toRaw(rec), ValueFormat.AUTO);

        assertThat(msg.key()).isNull();
        assertThat(msg.value()).isNull();
        assertThat(msg.valueFormat()).isNull();
        assertThat(msg.keySize()).isZero();
        assertThat(msg.valueSize()).isZero();
    }

    @Test
    void decodesHeadersPreservingDuplicates() {
        var headers = new RecordHeaders(List.of(
                new RecordHeader("trace-id", "abc".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("tag", "x".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("tag", "y".getBytes(StandardCharsets.UTF_8))));

        var raw = decoder.toRaw(record("k", "v", headers));

        assertThat(raw.headers()).containsExactly(
                new MessageHeader("trace-id", "abc"),
                new MessageHeader("tag", "x"),
                new MessageHeader("tag", "y"));
    }

    @Test
    void looksLikeJsonSkipsLeadingWhitespace() {
        assertThat(MessageDecoder.looksLikeJson("  \n {}".getBytes())).isTrue();
        assertThat(MessageDecoder.looksLikeJson("[1]".getBytes())).isTrue();
        assertThat(MessageDecoder.looksLikeJson("plain".getBytes())).isFalse();
        assertThat(MessageDecoder.looksLikeJson("   ".getBytes())).isFalse();
        assertThat(MessageDecoder.looksLikeJson(null)).isFalse();
    }

    private static ConsumerRecord<byte[], byte[]> record(String key, String value, RecordHeaders headers) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        byte[] v = value.getBytes(StandardCharsets.UTF_8);
        return new ConsumerRecord<>("t", 2, 42, 1_700_000_000_000L, TimestampType.CREATE_TIME,
                k.length, v.length, k, v, headers, Optional.empty());
    }
}
