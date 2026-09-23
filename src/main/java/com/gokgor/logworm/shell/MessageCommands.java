package com.gokgor.logworm.shell;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
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

    @Command(name = "format", group = "Messages", description = "Show or change how messages are printed: --set pretty|table|json|line")
    public String format(@Option(longName = "set", description = "pretty | table | json | line") String set) {
        if (set != null) {
            session.setOutputFormat(OutputFormat.parse(set));
        }
        return "Output format: " + session.outputFormat();
    }

    @Command(name = "show", group = "Messages", description = "Newest messages of the topic (uses the selected fields and output format)")
    public String show(
            @Option(longName = "name", description = "Topic (defaults to the selected one)") String name,
            @Option(longName = "limit", description = "How many (default 50)") Integer limit,
            @Option(longName = "partition", description = "Only this partition") Integer partition,
            @Option(longName = "key", description = "Key must contain") String key,
            @Option(longName = "value", description = "Value must contain") String value,
            @Option(longName = "format", description = "pretty | table | json | line (defaults to the selected format)") String format) {
        String topic = session.resolveTopic(name);
        ViewSettings view = session.view();
        RuleSet rules = session.rules();
        OutputFormat out = resolveFormat(format);
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
        long alerts = oldestFirst.stream().filter(m -> rules.alertFor(m).isPresent()).count();
        String summary = topic + "  showing " + oldestFirst.size() + " of " + page.scanned() + " scanned  (" + view + ", " + out + ")"
                + (rules.isEmpty() ? "" : "  [" + rules.describe() + (withAlert ? "; " + alerts + " alert(s)" : "") + "]") + "\n";
        return summary + switch (out) {
            case PRETTY -> {
                var tracker = new ChangeTracker();
                yield oldestFirst.stream().map(m -> formatter.pretty(m, view, rules, tracker)).collect(Collectors.joining("\n\n"));
            }
            case JSON -> oldestFirst.stream().map(m -> formatter.json(m, view, rules)).collect(Collectors.joining("\n"));
            case LINE -> {
                int width = terminal.getWidth() > 0 ? terminal.getWidth() : 160;
                yield oldestFirst.stream().map(m -> formatter.line(m, view, width, rules)).collect(Collectors.joining("\n"));
            }
            case TABLE -> {
                String[] headers = formatter.headers(view, withAlert);
                yield tables.renderStyled(oldestFirst, headers,
                        m -> rules.alertFor(m).map(r -> r.color().ansi).orElse(null),
                        columns(headers.length, view, valueWidth, rules));
            }
        };
    }

    @Command(name = "tail", group = "Messages", description = "Live tail of the topic; Enter stops it")
    public String tail(
            @Option(longName = "name", description = "Topic (defaults to the selected one)") String name,
            @Option(longName = "partition", description = "Only this partition") Integer partition,
            @Option(longName = "key", description = "Key must contain") String key,
            @Option(longName = "value", description = "Value must contain") String value,
            @Option(longName = "rate", description = "Max messages per second") Integer rate,
            @Option(longName = "format", description = "pretty | json | line (table falls back to line)") String format) throws IOException {
        String topic = session.resolveTopic(name);
        ViewSettings view = session.view();
        RuleSet rules = session.rules();
        OutputFormat out = resolveFormat(format);
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
        var tracker = new ChangeTracker();
        Function<KafkaMessage, String> render = switch (out) {
            case PRETTY -> m -> formatter.pretty(m, view, rules, tracker) + "\n";
            case JSON -> m -> formatter.json(m, view, rules);
            default -> m -> formatter.line(m, view, width, rules);
        };
        var sink = new TerminalSink(terminal.writer(), rules::passes, render, m -> rules.alertFor(m).isPresent());
        var loop = new LiveTail(consumer, sink, topic, partitions, query, decoder,
                streamProperties.heartbeat(), streamProperties.lagSkipFactor(), System::nanoTime);
        Thread worker = Thread.ofVirtual().name("console-tail").start(loop);

        var stop = ConsoleInput.watchForKey(terminal);
        try {
            while (sink.isOpen() && !stop.stopped()) {
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            stop.cancel();
        }
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

    @SuppressWarnings("unchecked")
    private Function<KafkaMessage, Object>[] columns(int count, ViewSettings view, int valueWidth, RuleSet rules) {
        Function<KafkaMessage, Object>[] cols = new Function[count];
        for (int i = 0; i < count; i++) {
            final int idx = i;
            cols[i] = m -> formatter.row(m, view, valueWidth, rules)[idx];
        }
        return cols;
    }

    private OutputFormat resolveFormat(String explicit) {
        return explicit == null ? session.outputFormat() : OutputFormat.parse(explicit);
    }

    static List<String> parseFields(String spec) {
        if (spec == null || spec.isBlank() || spec.trim().equalsIgnoreCase("none")) {
            return List.of();
        }
        return Arrays.stream(spec.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }
}
