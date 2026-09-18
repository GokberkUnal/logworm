package com.gokgor.logworm.shell;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.gokgor.logworm.kafka.KafkaConnection;

import lombok.RequiredArgsConstructor;

/**
 * Wizard step 1: which Kafka cluster? Offers the configured address and recently used ones,
 * or lets the user type a new one, then verifies the connection before moving on.
 */
@InteractiveShellComponent
@RequiredArgsConstructor
public class KafkaConnectionStep {

    static final String SESSION_KEY = "bootstrapServers";
    /** Shown and returned as-is so the selector echoes something readable after the pick. */
    static final String OTHER = "Other address...";
    static final String RECENT = "Recent connections...";
    static final String BACK = "← back";

    private final Choices choices;
    private final ShellSession session;
    private final KafkaConnection connection;
    private final RecentConnections recent;

    public void ask() {
        ask(System.out);
    }

    void ask(PrintStream out) {
        while (true) {
            String servers = pickAddress(out);
            out.print("Connecting to " + servers + " ... ");
            out.flush();
            try {
                KafkaConnection.Info info = servers.equals(connection.info().bootstrapServers())
                        ? connection.verify()
                        : connection.connect(servers);
                out.println("ok  (cluster " + info.clusterId() + ", " + info.brokerCount() + " broker"
                        + (info.brokerCount() == 1 ? "" : "s") + ")");
                recent.remember(servers);
                session.put(SESSION_KEY, servers);
                return;
            } catch (RuntimeException e) {
                out.println("failed: " + rootMessage(e));
                if (!choices.confirm("Try another address?", true)) {
                    out.println("Continuing without a verified connection; use 'connect --servers <host:port>' later.");
                    session.put(SESSION_KEY, servers);
                    return;
                }
            }
        }
    }

    private String pickAddress(PrintStream out) {
        String configured = connection.configuredBootstrapServers();
        List<String> recentOthers = recent.list().stream().filter(r -> !r.equals(configured)).toList();

        while (true) {
            Map<String, String> options = new LinkedHashMap<>();
            options.put(configured + "  (application.yml)", configured);
            if (!recentOthers.isEmpty()) {
                options.put(RECENT + "  (" + recentOthers.size() + ")", RECENT);
            }
            options.put(OTHER, OTHER);

            String picked = choices.select("Which Kafka cluster?", options);
            if (RECENT.equals(picked)) {
                Map<String, String> recentOptions = new LinkedHashMap<>();
                recentOthers.forEach(r -> recentOptions.put(r, r));
                recentOptions.put(BACK, BACK);
                String r = choices.select("Recent connections", recentOptions);
                if (!BACK.equals(r)) {
                    return r;
                }
                continue;
            }
            if (!OTHER.equals(picked)) {
                return picked;
            }
            break;
        }
        while (true) {
            String typed = choices.text("Bootstrap servers (host:port[,host:port])", configured).trim();
            if (typed.matches("[^\\s,]+:\\d+(,[^\\s,]+:\\d+)*")) {
                return typed;
            }
            out.println("Expected host:port, e.g. broker1:9092,broker2:9092");
        }
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
    }
}
