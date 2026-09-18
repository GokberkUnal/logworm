package com.gokgor.logworm.shell;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.springframework.shell.core.command.annotation.Command;
import org.springframework.shell.core.command.annotation.Option;

import com.gokgor.logworm.kafka.BrowserConsumerFactory;
import com.gokgor.logworm.kafka.LogwormKafkaProperties;
import com.gokgor.logworm.kafka.PartitionResolver;
import com.gokgor.logworm.message.KafkaMessage;
import com.gokgor.logworm.message.MessageDecoder;
import com.gokgor.logworm.message.MessageQuery;
import com.gokgor.logworm.message.MessageService;
import com.gokgor.logworm.message.ValueFormat;
import com.gokgor.logworm.stream.LiveTail;
import com.gokgor.logworm.stream.StreamProperties;
import com.gokgor.logworm.stream.StreamQuery;

import lombok.RequiredArgsConstructor;

@InteractiveShellComponent
@RequiredArgsConstructor
public class MessageCommands {

    private static final int DEFAULT_LIMIT = 50;

    private final MessageService messageService;
    private final BrowserConsumerFactory consumerFactory;
    private final MessageDecoder decoder;
    private final StreamProperties streamProperties;
    private final LogwormKafkaProperties kafkaProperties;
    private final ShellSession session;
    private final MessageFormatter formatter;
    private final Tables tables;
    private final Terminal terminal;

    @Command(name = "view", group = "Messages", description = "Show or change the view: --mode all|new, --fields a,b (or 'none')")
    public String view(
            @Option(longName = "mode", description = "all | new") String mode,
            @Option(longName = "fields", description = "Comma-separated JSON field paths, or 'none'") String fields) {
        ViewSettings v = session.view();
        if (mode != null) {
            v = v.withMode(ViewMode.valueOf(mode.trim().toUpperCase()));
        }
        if (fields != null) {
            v = v.withFields(parseFields(fields));
        }
        session.setView(v);
        return "View: " + v;
    }

    @Command(name = "show", group = "Messages", description = "Newest messages of the topic as a table (uses the selected fields)")
    public String show(
            @Option(longName = "name", description = "Topic (defaults to the selected one)") String name,
            @Option(longName = "limit", description = "How many (default 50)") Integer limit,
            @Option(longName = "partition", description = "Only this partition") Integer partition,
            @Option(longName = "key", description = "Key must contain") String key,
            @Option(longName = "value", description = "Value must contain") String value) {
        String topic = session.resolveTopic(name);
        ViewSettings view = session.view();
        RuleSet rules = session.rules();
        int wanted = limit != null ? limit : DEFAULT_LIMIT;
        // rule filters are applied here, after reading: read more so enough survive
        int toRead = rules.filters().isEmpty() ? wanted : Math.min(MessageQuery.MAX_LIMIT, wanted * 10);
        var page = messageService.read(topic, new MessageQuery(partition, null, toRead, key, value, null, ValueFormat.AUTO));
        List<KafkaMessage> matched = page.messages().stream().filter(rules::passes).limit(wanted).toList();
        if (matched.isEmpty()) {
            return "No messages in " + topic + (page.scanned() > 0 ? " matched the filters" : "");
        }
        List<KafkaMessage> oldestFirst = new ArrayList<>(matched);
        oldestFirst.sort((a, b) -> a.timestamp().equals(b.timestamp())
                ? Long.compare(a.offset(), b.offset()) : a.timestamp().compareTo(b.timestamp()));
        int valueWidth = Math.max(20, terminal.getWidth() - 50);
        boolean withAlert = !rules.alerts().isEmpty();
        String[] headers = formatter.headers(view, withAlert);
        long alerts = oldestFirst.stream().filter(m -> rules.alertFor(m).isPresent()).count();
        return topic + "  showing " + oldestFirst.size() + " of " + page.scanned() + " scanned  (" + view + ")"
                + (rules.isEmpty() ? "" : "  [" + rules.describe() + (withAlert ? "; " + alerts + " alert(s)" : "") + "]") + "\n"
                + tables.renderStyled(oldestFirst, headers,
                        m -> rules.alertFor(m).map(r -> r.color().ansi).orElse(null),
                        columns(headers.length, view, valueWidth, rules));
    }

    @Command(name = "tail", group = "Messages", description = "Live tail of the topic; any key stops it")
    public String tail(
            @Option(longName = "name", description = "Topic (defaults to the selected one)") String name,
            @Option(longName = "partition", description = "Only this partition") Integer partition,
            @Option(longName = "key", description = "Key must contain") String key,
            @Option(longName = "value", description = "Value must contain") String value,
            @Option(longName = "rate", description = "Max messages per second") Integer rate) throws IOException {
        String topic = session.resolveTopic(name);
        ViewSettings view = session.view();
        RuleSet rules = session.rules();
        int effectiveRate = Math.min(rate != null ? rate : streamProperties.defaultRate(), streamProperties.maxRate());
        var query = new StreamQuery(partition, key, value, null, ValueFormat.AUTO, effectiveRate);

        Consumer<byte[], byte[]> consumer = consumerFactory.create("logworm-console-tail");
        List<TopicPartition> partitions;
        try {
            partitions = PartitionResolver.resolve(consumer, topic, partition, kafkaProperties.requestTimeout());
        } catch (RuntimeException e) {
            consumer.close();
            throw e;
        }
        int width = terminal.getWidth() > 0 ? terminal.getWidth() : 160;
        var sink = new TerminalSink(terminal.writer(), rules::passes, m -> formatter.line(m, view, width, rules),
                m -> rules.alertFor(m).isPresent());
        var loop = new LiveTail(consumer, sink, topic, partitions, query, decoder,
                streamProperties.heartbeat(), streamProperties.lagSkipFactor(), System::nanoTime);
        Thread worker = Thread.ofVirtual().name("console-tail").start(loop);

        waitForAnyKey(sink);
        sink.close();
        try {
            worker.join(Duration.ofSeconds(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (sink.failure() != null) {
            return "Tail stopped with error: " + sink.failure().getMessage();
        }
        return "Tail stopped. " + sink.shown() + " shown" + (rules.alerts().isEmpty() ? "" : ", " + sink.alerts() + " alert(s)")
                + (rules.filters().isEmpty() ? "" : ", " + sink.hidden() + " hidden by filters") + ".";
    }

    /** Blocks until the user presses a key or the tail ends on its own. */
    private void waitForAnyKey(TerminalSink sink) throws IOException {
        Attributes saved = terminal.enterRawMode();
        try {
            while (sink.isOpen()) {
                int c = terminal.reader().read(200);
                if (c >= 0) {
                    return;
                }
            }
        } finally {
            terminal.setAttributes(saved);
        }
    }

    @SuppressWarnings("unchecked")
    private java.util.function.Function<KafkaMessage, Object>[] columns(int count, ViewSettings view, int valueWidth, RuleSet rules) {
        java.util.function.Function<KafkaMessage, Object>[] cols = new java.util.function.Function[count];
        for (int i = 0; i < count; i++) {
            final int idx = i;
            cols[i] = m -> formatter.row(m, view, valueWidth, rules)[idx];
        }
        return cols;
    }

    static List<String> parseFields(String spec) {
        if (spec == null || spec.isBlank() || spec.trim().equalsIgnoreCase("none")) {
            return List.of();
        }
        return Arrays.stream(spec.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }
}
