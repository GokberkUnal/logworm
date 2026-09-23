package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.gokgor.logworm.message.KafkaMessage;

import tools.jackson.databind.json.JsonMapper;

class MessageFormatterTest {

    private final JsonMapper json = JsonMapper.builder().build();
    private final MessageFormatter formatter = new MessageFormatter(json);

    private KafkaMessage msg(String key, String jsonValue) {
        Object value = jsonValue == null ? null : json.readTree(jsonValue);
        return new KafkaMessage(1, 42, Instant.parse("2026-09-18T10:00:00Z"), "CREATE_TIME", key, value,
                value == null ? null : "json", List.of(), 0, 0);
    }

    @Test
    void discoversFieldsInFirstSeenOrderWithNestedPaths() {
        var fields = MessageFormatter.discoverFields(List.of(
                msg("k", "{\"level\":\"INFO\",\"ctx\":{\"user\":\"u1\",\"ip\":\"1.1.1.1\"}}"),
                msg("k", "{\"level\":\"WARN\",\"extra\":true}"),
                new KafkaMessage(0, 1, Instant.EPOCH, "CREATE_TIME", "k", "plain text", "string", List.of(), 0, 0)));

        assertThat(fields).containsExactly("level", "ctx.user", "ctx.ip", "extra");
    }

    @Test
    void fieldExtractsDottedPathsAndBlanksMissing() {
        var m = msg("k", "{\"level\":\"ERROR\",\"ctx\":{\"user\":\"u1\"},\"n\":3,\"arr\":[1,2]}");

        assertThat(MessageFormatter.field(m, "level")).isEqualTo("ERROR");
        assertThat(MessageFormatter.field(m, "ctx.user")).isEqualTo("u1");
        assertThat(MessageFormatter.field(m, "n")).isEqualTo("3");
        assertThat(MessageFormatter.field(m, "arr")).isEqualTo("[1,2]");
        assertThat(MessageFormatter.field(m, "nope")).isEmpty();
        assertThat(MessageFormatter.field(new KafkaMessage(0, 1, Instant.EPOCH, "CREATE_TIME", "k", "text", "string", List.of(), 0, 0), "level")).isEmpty();
    }

    @Test
    void rowAndLineFollowTheView() {
        var m = msg("order-1", "{\"level\":\"ERROR\",\"msg\":\"boom\"}");
        var whole = ViewSettings.defaults();
        var fields = whole.withFields(List.of("level", "msg"));

        assertThat(formatter.headers(whole)).containsExactly("part", "offset", "time", "key", "value");
        assertThat(formatter.headers(fields)).containsExactly("part", "offset", "time", "key", "level", "msg");

        Object[] row = formatter.row(m, fields, 80);
        assertThat(row[0]).isEqualTo(1);
        assertThat(row[1]).isEqualTo(42L);
        assertThat(row[3]).isEqualTo("order-1");
        assertThat(row[4]).isEqualTo("ERROR");
        assertThat(row[5]).isEqualTo("boom");

        assertThat(formatter.line(m, fields, 200)).isEqualTo(formatter.line(m, fields, 200).stripTrailing())
                .startsWith("p1@42 ").contains("order-1  level=ERROR  msg=boom");
        assertThat(formatter.line(m, whole, 200)).contains("{\"level\":\"ERROR\",\"msg\":\"boom\"}");
        assertThat(formatter.line(m, whole, 20)).hasSize(20).endsWith("…");
    }

    @Test
    void jsonLineCarriesMetaAndValueOrFields() {
        var m = msg("order-1", "{\"level\":\"ERROR\",\"ctx\":{\"user\":\"u1\"}}");
        var whole = ViewSettings.defaults();
        var fields = whole.withFields(List.of("level", "ctx.user", "nope"));

        assertThat(formatter.json(m, whole, null)).isEqualTo(
                "{\"partition\":1,\"offset\":42,\"timestamp\":\"2026-09-18T10:00:00Z\",\"key\":\"order-1\","
                + "\"value\":{\"level\":\"ERROR\",\"ctx\":{\"user\":\"u1\"}}}");
        assertThat(formatter.json(m, fields, null)).isEqualTo(
                "{\"partition\":1,\"offset\":42,\"timestamp\":\"2026-09-18T10:00:00Z\",\"key\":\"order-1\","
                + "\"fields\":{\"level\":\"ERROR\",\"ctx.user\":\"u1\",\"nope\":null}}");
        assertThat(formatter.json(msg("k", null), whole, null)).contains("\"value\":null");
        assertThat(formatter.json(new KafkaMessage(0, 1, Instant.EPOCH, "CREATE_TIME", "k", "plain", "string", List.of(), 0, 0), whole, null))
                .contains("\"value\":\"plain\"");
    }

    @Test
    void jsonLineIsColoredAndLabelledByAlert() {
        var m = msg("k", "{\"level\":\"ERROR\"}");
        var rules = RuleSet.empty().plusAlert(new Rule("level", Rule.Condition.EQUALS, "ERROR", Rule.Color.RED, "errors"));

        assertThat(formatter.json(m, ViewSettings.defaults(), rules))
                .startsWith(Rule.Color.RED.ansi + "{").endsWith("\"alert\":\"errors\"}" + Rule.Color.RESET);
    }

    @Test
    void prettyBlockShowsFieldsAndChangesSincePreviousMessageOfSameKey() {
        var tracker = new ChangeTracker();
        var first = formatter.pretty(msg("order-1", "{\"level\":\"INFO\",\"ctx\":{\"user\":\"u1\"},\"msg\":\"created\"}"),
                ViewSettings.defaults(), null, tracker);
        var second = formatter.pretty(msg("order-1", "{\"level\":\"ERROR\",\"ctx\":{\"user\":\"u1\"},\"msg\":\"boom\"}"),
                ViewSettings.defaults(), null, tracker);
        var otherKey = formatter.pretty(msg("order-2", "{\"level\":\"WARN\"}"), ViewSettings.defaults(), null, tracker);

        assertThat(first).isEqualTo("13:00:00.000  key=order-1  p1@42\n  level     INFO\n  ctx.user  u1\n  msg       created"
                .replace("13:00:00.000", java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
                        .withZone(java.time.ZoneId.systemDefault()).format(Instant.parse("2026-09-18T10:00:00Z"))));
        assertThat(second).contains("  level     INFO --> ERROR").contains("  ctx.user  u1\n").contains("  msg       created --> boom");
        assertThat(otherKey).contains("  level  WARN").doesNotContain("-->");
    }

    @Test
    void prettyBlockUsesSelectedFieldsAndAlertColor() {
        var view = ViewSettings.defaults().withFields(List.of("level", "nope"));
        var rules = RuleSet.empty().plusAlert(new Rule("level", Rule.Condition.EQUALS, "ERROR", Rule.Color.RED, "errors"));
        var block = formatter.pretty(msg("k", "{\"level\":\"ERROR\",\"msg\":\"x\"}"), view, rules, new ChangeTracker());

        assertThat(block).startsWith(Rule.Color.RED.ansi).endsWith(Rule.Color.RESET)
                .contains("key=k  p1@42  [errors]").contains("  level  ERROR\n  nope   -").doesNotContain("msg");
        assertThat(formatter.pretty(msg("k", null), ViewSettings.defaults(), null, new ChangeTracker())).contains("  value  <null>");
    }

    @Test
    void nullValueRendersAsPlaceholder() {
        var m = msg("k", null);
        assertThat(formatter.row(m, ViewSettings.defaults(), 80)[4]).isEqualTo("<null>");
        assertThat(formatter.row(m, ViewSettings.defaults().withFields(List.of("a")), 80)[4]).isEqualTo("");
    }
}
