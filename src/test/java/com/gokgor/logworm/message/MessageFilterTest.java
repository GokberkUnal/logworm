package com.gokgor.logworm.message;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

class MessageFilterTest {

    private static final RawMessage MSG = new RawMessage(null, "order-42",
            "{\"level\":\"ERROR\",\"msg\":\"boom\"}",
            List.of(new MessageHeader("trace-id", "abc"), new MessageHeader("tag", "x")));
    private static final RawMessage NULLS = new RawMessage(null, null, null, List.of());

    @Test
    void noFiltersMatchesEverything() {
        var p = MessageFilter.from(query(null, null, null));
        assertThat(p.test(MSG)).isTrue();
        assertThat(p.test(NULLS)).isTrue();
    }

    @Test
    void keyIsSubstringMatch() {
        assertThat(MessageFilter.from(query("order", null, null)).test(MSG)).isTrue();
        assertThat(MessageFilter.from(query("user", null, null)).test(MSG)).isFalse();
        assertThat(MessageFilter.from(query("order", null, null)).test(NULLS)).isFalse();
    }

    @Test
    void valueIsSubstringMatchOnRawText() {
        assertThat(MessageFilter.from(query(null, "ERROR", null)).test(MSG)).isTrue();
        assertThat(MessageFilter.from(query(null, "error", null)).test(MSG)).isFalse();
        assertThat(MessageFilter.from(query(null, "x", null)).test(NULLS)).isFalse();
    }

    @Test
    void headerByNameOrNameAndValue() {
        assertThat(MessageFilter.from(query(null, null, "trace-id")).test(MSG)).isTrue();
        assertThat(MessageFilter.from(query(null, null, "trace-id=abc")).test(MSG)).isTrue();
        assertThat(MessageFilter.from(query(null, null, "trace-id=zzz")).test(MSG)).isFalse();
        assertThat(MessageFilter.from(query(null, null, "missing")).test(MSG)).isFalse();
    }

    @Test
    void filtersCombineWithAnd() {
        assertThat(MessageFilter.from(query("order", "boom", "tag=x")).test(MSG)).isTrue();
        assertThat(MessageFilter.from(query("order", "boom", "tag=y")).test(MSG)).isFalse();
    }

    private static MessageQuery query(String key, String value, String header) {
        return new MessageQuery(null, null, 10, key, value, header, null);
    }
}
