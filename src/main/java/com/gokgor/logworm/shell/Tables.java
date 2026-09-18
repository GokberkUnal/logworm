package com.gokgor.logworm.shell;

import java.util.List;
import java.util.function.Function;

import org.jline.terminal.Terminal;
import org.springframework.shell.jline.tui.table.ArrayTableModel;
import org.springframework.shell.jline.tui.table.BorderStyle;
import org.springframework.shell.jline.tui.table.TableBuilder;

/** Renders rows as a bordered table sized to the terminal. */
@InteractiveShellComponent
public class Tables {

    private static final int FALLBACK_WIDTH = 120;

    private final Terminal terminal;

    public Tables(Terminal terminal) {
        this.terminal = terminal;
    }

    @SafeVarargs
    public final <T> String render(List<T> rows, String[] headers, Function<T, Object>... columns) {
        Object[][] data = new Object[rows.size() + 1][headers.length];
        data[0] = headers;
        for (int r = 0; r < rows.size(); r++) {
            for (int c = 0; c < columns.length; c++) {
                Object v = columns[c].apply(rows.get(r));
                data[r + 1][c] = v == null ? "" : v;
            }
        }
        int width = terminal.getWidth() > 0 ? terminal.getWidth() : FALLBACK_WIDTH;
        return new TableBuilder(new ArrayTableModel(data))
                .addFullBorder(BorderStyle.fancy_light)
                .build()
                .render(width);
    }
}
