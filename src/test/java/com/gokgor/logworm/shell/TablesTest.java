package com.gokgor.logworm.shell;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.gokgor.logworm.shell.Rule.Color;

class TablesTest {

    @Test
    void alignsColumnsAndColorsWholeRows() {
        String table = Tables.render(
                new String[] {"id", "name"},
                List.of(new String[] {"1", "alpha"}, new String[] {"22", "b"}),
                Arrays.asList(null, Color.RED.ansi));

        String[] lines = table.split("\n");
        assertThat(lines).containsExactly(
                "┌──┬─────┐",
                "│id│name │",
                "├──┼─────┤",
                "│1 │alpha│",
                Color.RED.ansi + "│22│b    │" + Color.RESET,
                "└──┴─────┘");
        // every line has the same visible width, color codes excluded
        assertThat(Arrays.stream(lines).map(Tables::visibleLength).distinct()).hasSize(1);
    }

    @Test
    void emptyRowsStillRenderHeader() {
        String table = Tables.render(new String[] {"x"}, List.of(), List.of());
        assertThat(table).isEqualTo("┌─┐\n│x│\n├─┤\n└─┘");
    }
}
