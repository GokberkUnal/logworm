package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.gokgor.logworm.message.KafkaMessage;
import com.gokgor.logworm.shell.CdcEvent.ColumnChange;
import com.gokgor.logworm.shell.CdcEvent.Op;
import com.gokgor.logworm.shell.Rule.Color;

import tools.jackson.databind.json.JsonMapper;

class CdcFormatTest {

    private static final String SOURCE = "\"source\":{\"db\":\"demo\",\"schema\":\"shop\",\"table\":\"customers\"}";
    private static final String UPDATE = """
            {"before":{"id":1,"email":"a@old.io","status":"active","balance":10},
             "after":{"id":1,"email":"a@new.io","status":"active","balance":10},%s,"op":"u","ts_ms":1}""".formatted(SOURCE);

    private final JsonMapper json = JsonMapper.builder().build();
    private final MessageFormatter formatter = new MessageFormatter(json);

    private KafkaMessage msg(String value) {
        Object v = value == null ? null : json.readTree(value);
        return new KafkaMessage(0, 42, Instant.parse("2026-10-28T10:00:00Z"), "CREATE_TIME", "{\"id\":1}", v,
                v == null ? null : "json", List.of(), 0, 0);
    }

    private static String plain(String s) {
        return s.replaceAll("\u001B\\[[0-9;]*m", "");
    }

    @Test
    void parsesEnvelopeAndSchemaPayloadWrapper() {
        CdcEvent e = CdcEvent.parse(msg(UPDATE)).orElseThrow();
        assertThat(e.op()).isEqualTo(Op.UPDATE);
        assertThat(e.table()).isEqualTo("shop.customers");
        assertThat(e.changes()).containsExactly(new ColumnChange("email", "a@old.io", "a@new.io"));

        var wrapped = msg("{\"schema\":{\"type\":\"struct\"},\"payload\":{\"before\":null,\"after\":{\"id\":2},"
                + "\"source\":{\"db\":\"inv\",\"table\":\"orders\"},\"op\":\"c\"}}");
        CdcEvent c = CdcEvent.parse(wrapped).orElseThrow();
        assertThat(c.op()).isEqualTo(Op.INSERT);
        assertThat(c.table()).isEqualTo("inv.orders");
        assertThat(c.before()).isNull();

        assertThat(CdcEvent.parse(msg("{\"level\":\"INFO\",\"op\":\"x\"}"))).isEmpty();
        assertThat(CdcEvent.parse(msg("{\"op\":\"c\"}"))).isEmpty();
        assertThat(CdcEvent.parse(msg(null))).isEmpty();
    }

    @Test
    void changesIncludeAddedAndRemovedColumns() {
        var e = CdcEvent.parse(msg("{\"before\":{\"a\":1,\"gone\":\"x\"},\"after\":{\"a\":1,\"new\":null},\"op\":\"u\"}")).orElseThrow();
        assertThat(e.changes()).containsExactly(
                new ColumnChange("new", null, "null"),
                new ColumnChange("gone", "x", null));
    }

    @Test
    void updateShowsOnlyChangedColumnsWithColoredOperation() {
        String out = formatter.cdc(msg(UPDATE), ViewSettings.defaults(), RuleSet.empty(), new ChangeTracker());

        assertThat(out).contains(Color.YELLOW.ansi + "UPDATE" + Color.RESET);
        assertThat(plain(out)).isEqualTo("""
                %s  UPDATE  shop.customers  key={"id":1}  p0@42
                  email  a@old.io --> a@new.io
                  (3 unchanged)""".formatted(time()));
    }

    @Test
    void insertListsNewRowDeleteListsOldRow() {
        String insert = plain(formatter.cdc(msg("{\"before\":null,\"after\":{\"id\":1,\"name\":\"Ada\"}," + SOURCE + ",\"op\":\"c\"}"),
                ViewSettings.defaults(), null, new ChangeTracker()));
        assertThat(insert).endsWith("INSERT  shop.customers  key={\"id\":1}  p0@42\n  id    1\n  name  Ada");

        String delete = plain(formatter.cdc(msg("{\"before\":{\"id\":1},\"after\":null,\"op\":\"d\"}"),
                ViewSettings.defaults(), null, new ChangeTracker()));
        assertThat(delete).endsWith("DELETE  key={\"id\":1}  p0@42\n  id  1");

        String noImage = plain(formatter.cdc(msg("{\"before\":null,\"after\":{\"id\":1},\"op\":\"u\"}"),
                ViewSettings.defaults(), null, new ChangeTracker()));
        assertThat(noImage).endsWith("  id  1\n  (no before image; all columns shown)");
    }

    @Test
    void viewFieldsNarrowColumnsAndAlertColorsWholeBlock() {
        var view = ViewSettings.defaults().withFields(List.of("after.status", "op"));
        RuleSet rules = RuleSet.empty().plusAlert(Rule.parse("op", "eq:u", Color.RED, "changed"));

        String out = formatter.cdc(msg(UPDATE), view, rules, new ChangeTracker());

        assertThat(out).startsWith(Color.RED.ansi).endsWith(Color.RESET).doesNotContain(Color.YELLOW.ansi);
        assertThat(plain(out)).contains("UPDATE  shop.customers").contains("[changed]")
                .endsWith("(no column changed)");
        assertThat(MessageFormatter.cdcColumns(view)).containsExactly("status");
    }

    @Test
    void tombstoneAndNonCdcMessages() {
        assertThat(plain(formatter.cdc(msg(null), ViewSettings.defaults(), null, new ChangeTracker())))
                .isEqualTo(time() + "  TOMBSTONE  key={\"id\":1}  p0@42");

        var log = msg("{\"level\":\"INFO\"}");
        assertThat(formatter.cdc(log, ViewSettings.defaults(), null, new ChangeTracker()))
                .isEqualTo(formatter.pretty(log, ViewSettings.defaults(), null, new ChangeTracker()));
    }

    @Test
    void detectionNeedsAMajorityOfJsonMessages() {
        var cdc = msg(UPDATE);
        var log = msg("{\"level\":\"INFO\"}");
        assertThat(CdcEvent.looksLikeCdc(List.of(cdc, cdc, log, msg(null)))).isTrue();
        assertThat(CdcEvent.looksLikeCdc(List.of(cdc, log))).isFalse();
        assertThat(CdcEvent.looksLikeCdc(List.of())).isFalse();
    }

    private static String time() {
        return java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
                .withZone(java.time.ZoneId.systemDefault()).format(Instant.parse("2026-10-28T10:00:00Z"));
    }
}
