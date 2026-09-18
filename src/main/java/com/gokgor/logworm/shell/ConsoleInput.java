package com.gokgor.logworm.shell;

import java.util.concurrent.atomic.AtomicBoolean;

import org.jline.terminal.Terminal;

/**
 * Waiting for the user to press Enter while a long-running command (tail, watch) prints.
 *
 * <p>The terminal is left in cooked mode (line-buffered): switching to raw mode via
 * {@code enterRawMode()} blocks on the JLine FFM terminal while its background reader is parked
 * on the same tty. So input arrives a line at a time, hence "press Enter".
 *
 * <p>The newline that submitted the command itself is still buffered when the command starts,
 * so the reader ignores one keystroke that arrives almost immediately (the leftover) and waits
 * for the next one, which is the user's actual stop.
 */
final class ConsoleInput {

    private static final long LEFTOVER_WINDOW_MILLIS = 150;

    private ConsoleInput() {
    }

    /** Starts a background thread that flips once the user presses Enter. */
    static StopSignal watchForKey(Terminal terminal) {
        AtomicBoolean stopped = new AtomicBoolean(false);
        Thread reader = Thread.ofVirtual().name("console-linewait").start(() -> {
            try {
                // Drain bytes buffered from the command's own submission (CR/LF); the first read
                // that actually has to wait is the user's real keystroke.
                while (true) {
                    long before = System.currentTimeMillis();
                    int c = terminal.reader().read();
                    long waited = System.currentTimeMillis() - before;
                    System.err.println("DEBUG read c=" + c + " waited=" + waited + "ms");
                    if (c < 0 || waited >= LEFTOVER_WINDOW_MILLIS) {
                        break;
                    }
                }
            } catch (Exception e) {
                // terminal closed / interrupted: treat as a stop
            } finally {
                stopped.set(true);
            }
        });
        return new StopSignal(stopped, reader);
    }

    record StopSignal(AtomicBoolean flag, Thread reader) {

        boolean stopped() {
            return flag.get();
        }

        /** Stop watching; interrupts the blocking read if it is still parked. */
        void cancel() {
            if (reader.isAlive()) {
                reader.interrupt();
            }
        }
    }
}
