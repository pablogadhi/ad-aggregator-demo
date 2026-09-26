package dev.sdl.clickaggregator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class KeepClickTest {

    static final long NOW = 1_790_000_000_000L;
    static final long HOUR = 3_600_000L;

    KeepClick udf() {
        KeepClick f = new KeepClick();
        f.initForTest(() -> NOW);
        return f;
    }

    @Test
    void keepsFreshClicks() {
        KeepClick f = udf();
        assertThat(f.eval(1L, 2L, 0, Instant.ofEpochMilli(NOW - 5_000), HOUR)).isTrue();
        assertThat(f.eval(1L, 2L, 3, Instant.ofEpochMilli(NOW - HOUR), HOUR)).isTrue(); // boundary
        assertThat(f.eval(1L, 2L, 0, Instant.ofEpochMilli(NOW + 2_000), HOUR)).isTrue(); // clock skew
        assertThat(f.lateCount()).isZero();
    }

    @Test
    void dropsAndCountsLateClicks() {
        KeepClick f = udf();
        assertThat(f.eval(1L, 2L, 0, Instant.ofEpochMilli(NOW - HOUR - 1), HOUR)).isFalse();
        assertThat(f.eval(1L, 2L, 0, Instant.ofEpochMilli(NOW - 3 * HOUR), HOUR)).isFalse();
        assertThat(f.lateCount()).isEqualTo(2);
        assertThat(f.malformedCount()).isZero();
    }

    @Test
    void dropsAndCountsMalformedRecords() {
        KeepClick f = udf();
        Instant t = Instant.ofEpochMilli(NOW);
        assertThat(f.eval(null, 2L, 0, t, HOUR)).isFalse();
        assertThat(f.eval(1L, null, 0, t, HOUR)).isFalse();
        assertThat(f.eval(1L, 2L, null, t, HOUR)).isFalse();
        assertThat(f.eval(1L, 2L, 0, null, HOUR)).isFalse();
        assertThat(f.malformedCount()).isEqualTo(4);
    }

    @Test
    void isNotDeterministic() {
        assertThat(new KeepClick().isDeterministic()).isFalse();
    }
}
