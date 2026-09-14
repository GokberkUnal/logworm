package com.gokgor.logworm.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class MessageQueryTest {

    @Test
    void defaultsAndNormalisation() {
        var q = new MessageQuery(null, null, 100, " ", "", null, null);

        assertThat(q.isTail()).isTrue();
        assertThat(q.hasFilters()).isFalse();
        assertThat(q.key()).isNull();
        assertThat(q.value()).isNull();
        assertThat(q.format()).isEqualTo(ValueFormat.AUTO);
        assertThat(q.scanWindow()).isEqualTo(100);
    }

    @Test
    void filtersWidenTheScanWindowUpToTheCap() {
        assertThat(new MessageQuery(null, null, 100, "k", null, null, null).scanWindow()).isEqualTo(1000);
        assertThat(new MessageQuery(null, null, 1000, "k", null, null, null).scanWindow()).isEqualTo(MessageQuery.MAX_SCAN);
    }

    @Test
    void offsetRequiresPartition() {
        assertThatThrownBy(() -> new MessageQuery(null, 5L, 10, null, null, null, null))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("partition");
        assertThat(new MessageQuery(0, 5L, 10, null, null, null, null).isTail()).isFalse();
    }

    @Test
    void rejectsOutOfRangeValues() {
        assertThatThrownBy(() -> new MessageQuery(null, null, 0, null, null, null, null))
                .isInstanceOf(InvalidQueryException.class);
        assertThatThrownBy(() -> new MessageQuery(null, null, MessageQuery.MAX_LIMIT + 1, null, null, null, null))
                .isInstanceOf(InvalidQueryException.class);
        assertThatThrownBy(() -> new MessageQuery(0, -1L, 10, null, null, null, null))
                .isInstanceOf(InvalidQueryException.class);
        assertThatThrownBy(() -> new MessageQuery(-1, null, 10, null, null, null, null))
                .isInstanceOf(InvalidQueryException.class);
        assertThatThrownBy(() -> new MessageQuery(null, null, 10, null, null, "  ", null))
                .isInstanceOf(InvalidQueryException.class);
    }
}
