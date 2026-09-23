package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.gokgor.logworm.message.KafkaMessage;
import com.gokgor.logworm.shell.Rule.Color;
import com.gokgor.logworm.shell.Rule.Condition;

import tools.jackson.databind.json.JsonMapper;

class RuleTest {

    private final JsonMapper json = JsonMapper.builder().build();

    private KafkaMessage msg(String key, String jsonValue) {
        Object value = jsonValue == null ? null : json.readTree(jsonValue);
        return new KafkaMessage(0, 1, Instant.EPOCH, "CREATE_TIME", key, value, value == null ? null : "json", List.of(), 0, 0);
    }

    private KafkaMessage text(String key, String value) {
        return new KafkaMessage(0, 1, Instant.EPOCH, "CREATE_TIME", key, value, "string", List.of(), 0, 0);
    }

    @Test
    void nullChecksCoverMissingJsonNullNonJsonAndKey() {
        var isNull = Rule.filter("price", Condition.IS_NULL, null);
        assertThat(isNull.matches(msg("k", "{\"price\":null}"))).isTrue();
        assertThat(isNull.matches(msg("k", "{\"other\":1}"))).isTrue();
        assertThat(isNull.matches(text("k", "plain"))).isTrue();
        assertThat(isNull.matches(msg("k", "{\"price\":0}"))).isFalse();

        assertThat(Rule.filter("price", Condition.IS_NOT_NULL, null).matches(msg("k", "{\"price\":0}"))).isTrue();
        assertThat(Rule.filter("key", Condition.IS_NULL, null).matches(msg(null, "{}"))).isTrue();
        assertThat(Rule.filter("key", Condition.IS_NULL, null).matches(msg("k", "{}"))).isFalse();
    }

    @Test
    void comparisonsWorkOnStringsNumbersAndNestedPaths() {
        var m = msg("order-42", "{\"level\":\"ERROR\",\"amount\":12.5,\"ctx\":{\"user\":\"u1\"}}");

        assertThat(Rule.filter("level", Condition.EQUALS, "ERROR").matches(m)).isTrue();
        assertThat(Rule.filter("level", Condition.NOT_EQUALS, "ERROR").matches(m)).isFalse();
        assertThat(Rule.filter("level", Condition.NOT_EQUALS, "INFO").matches(m)).isTrue();
        assertThat(Rule.filter("missing", Condition.NOT_EQUALS, "x").matches(m)).isTrue();
        assertThat(Rule.filter("key", Condition.CONTAINS, "order").matches(m)).isTrue();
        assertThat(Rule.filter("ctx.user", Condition.EQUALS, "u1").matches(m)).isTrue();
        assertThat(Rule.filter("amount", Condition.GREATER_THAN, "10").matches(m)).isTrue();
        assertThat(Rule.filter("amount", Condition.GREATER_THAN, "12.5").matches(m)).isFalse();
        assertThat(Rule.filter("amount", Condition.LESS_THAN, "100").matches(m)).isTrue();
        assertThat(Rule.filter("missing", Condition.GREATER_THAN, "1").matches(m)).isFalse();
        assertThat(Rule.filter("missing", Condition.LESS_THAN, "1").matches(m)).isFalse();
    }

    @Test
    void inMatchesAnyOfTheValues() {
        var update = msg("k", "{\"op\":\"u\"}");
        var delete = msg("k", "{\"op\":\"d\"}");
        var none = msg("k", "{}");
        Rule in = Rule.filter("op", Condition.IN, "c,u");
        Rule notIn = Rule.filter("op", Condition.NOT_IN, "c,u");

        assertThat(in.matches(update)).isTrue();
        assertThat(in.matches(delete)).isFalse();
        assertThat(in.matches(none)).isFalse();
        assertThat(notIn.matches(update)).isFalse();
        assertThat(notIn.matches(delete)).isTrue();
        assertThat(notIn.matches(none)).isTrue();
    }

    @Test
    void parsesWhenSyntaxAndDefaultsLabel() {
        Rule r = Rule.parse("price", "null", Color.RED, null);
        assertThat(r.condition()).isEqualTo(Condition.IS_NULL);
        assertThat(r.label()).isEqualTo("price is null");
        assertThat(r.color()).isEqualTo(Color.RED);

        Rule eq = Rule.parse("level", "eq:ERROR", null, "boom");
        assertThat(eq.condition()).isEqualTo(Condition.EQUALS);
        assertThat(eq.value()).isEqualTo("ERROR");
        assertThat(eq.label()).isEqualTo("boom");
        assertThat(eq.color()).isEqualTo(Color.NONE);

        assertThat(Rule.parse("k", "contains:a:b", null, null).value()).isEqualTo("a:b");
        Rule in = Rule.parse("op", "in: c , u,", null, null);
        assertThat(in.condition()).isEqualTo(Condition.IN);
        assertThat(in.values()).containsExactly("c", "u");
        assertThat(in.describe()).isEqualTo("op in (c, u)");
        assertThat(Rule.parse("op", "notin:d", null, null).describe()).isEqualTo("op not in (d)");
        assertThat(Rule.parse("n", "GT:5", null, null).describe()).isEqualTo("n > 5");

        assertThatThrownBy(() -> Rule.parse("x", "eq", null, null)).hasMessageContaining("needs a value");
        assertThatThrownBy(() -> Rule.parse("x", "between:1", null, null)).hasMessageContaining("Unknown condition");
        assertThatThrownBy(() -> Rule.parse(" ", "null", null, null)).hasMessageContaining("--field");
    }

    @Test
    void ruleSetFiltersWithAndAndPicksFirstAlert() {
        var rules = RuleSet.empty()
                .plusFilter(Rule.filter("level", Condition.EQUALS, "ERROR"))
                .plusFilter(Rule.filter("key", Condition.CONTAINS, "pay"))
                .plusAlert(Rule.parse("amount", "null", Color.RED, "no amount"))
                .plusAlert(Rule.parse("amount", "gt:100", Color.YELLOW, "big"));

        assertThat(rules.passes(msg("payment", "{\"level\":\"ERROR\"}"))).isTrue();
        assertThat(rules.passes(msg("payment", "{\"level\":\"INFO\"}"))).isFalse();
        assertThat(rules.passes(msg("auth", "{\"level\":\"ERROR\"}"))).isFalse();

        assertThat(rules.alertFor(msg("k", "{\"amount\":500}")).map(Rule::label)).contains("big");
        assertThat(rules.alertFor(msg("k", "{}")).map(Rule::label)).contains("no amount");
        assertThat(rules.alertFor(msg("k", "{\"amount\":5}"))).isEmpty();

        assertThat(rules.describe()).isEqualTo(
                "filters: level == ERROR AND key contains pay; alerts: red when amount is null, yellow when amount > 100");
        assertThat(RuleSet.empty().describe()).isEqualTo("no filters, no alerts");
    }
}
