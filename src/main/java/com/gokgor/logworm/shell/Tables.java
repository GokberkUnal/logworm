package com.gokgor.logworm.shell;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.jline.terminal.Terminal;

/**
 * Renders rows as a box-drawn table. Own implementation (instead of Spring Shell's TableBuilder)
 * so a whole row can be wrapped in ANSI color without breaking the column alignment.
 */
@InteractiveShellComponent
public class Tables {

    private static final int FALLBACK_WIDTH = 120;
    private static final Pattern ANSI = Pattern.compile("\\[[0-9;]*m");

    private final Terminal terminal;

    public Tables(Terminal terminal) {
        this.terminal = terminal;
    }

    @SafeVarargs
    public final <T> String render(List<T> rows, String[] headers, Function<T, Object>... columns) {
        return renderStyled(rows, headers, r -> null, columns);
    }

    /**
     * @param rowStyle ANSI prefix for a row (e.g. {@code Rule.Color.RED.ansi}) or null for plain
     */
    @SafeVarargs
    public final <T> String renderStyled(List<T> rows, String[] headers, Function<T, String> rowStyle, Function<T, Object>... columns) {
        List<String[]> cells = new ArrayList<>();
        List<String> styles = new ArrayList<>();
        for (T row : rows) {
            String[] line = new String[columns.length];
            for (int c = 0; c < columns.length; c++) {
                Object v = columns[c].apply(row);
                line[c] = v == null ? "" : String.valueOf(v);
            }
            cells.add(line);
            styles.add(rowStyle.apply(row));
        }
        return render(headers, cells, styles);
    }

    static String render(String[] headers, List<String[]> rows, List<String> styles) {
        int n = headers.length;
        int[] width = new int[n];
        for (int c = 0; c < n; c++) {
            width[c] = visibleLength(headers[c]);
        }
        for (String[] r : rows) {
            for (int c = 0; c < n; c++) {
                width[c] = Math.max(width[c], visibleLength(r[c]));
            }
        }
        var sb = new StringBuilder();
        sb.append(border('┌', '┬', '┐', width)).append('\n');
        sb.append(line(headers, width, null)).append('\n');
        sb.append(border('├', '┼', '┤', width)).append('\n');
        for (int i = 0; i < rows.size(); i++) {
            sb.append(line(rows.get(i), width, styles.get(i))).append('\n');
        }
        sb.append(border('└', '┴', '┘', width));
        return sb.toString();
    }

    private static String border(char left, char mid, char right, int[] width) {
        var sb = new StringBuilder().append(left);
        for (int c = 0; c < width.length; c++) {
            sb.append("─".repeat(width[c]));
            sb.append(c == width.length - 1 ? right : mid);
        }
        return sb.toString();
    }

    private static String line(String[] cells, int[] width, String style) {
        var sb = new StringBuilder();
        boolean styled = style != null && !style.isEmpty();
        if (styled) {
            sb.append(style);
        }
        sb.append('│');
        for (int c = 0; c < width.length; c++) {
            String cell = cells[c] == null ? "" : cells[c];
            sb.append(cell).append(" ".repeat(Math.max(0, width[c] - visibleLength(cell)))).append('│');
        }
        if (styled) {
            sb.append(Rule.Color.RESET);
        }
        return sb.toString();
    }

    static int visibleLength(String s) {
        return s == null ? 0 : ANSI.matcher(s).replaceAll("").length();
    }

    public int terminalWidth() {
        return terminal.getWidth() > 0 ? terminal.getWidth() : FALLBACK_WIDTH;
    }
}
