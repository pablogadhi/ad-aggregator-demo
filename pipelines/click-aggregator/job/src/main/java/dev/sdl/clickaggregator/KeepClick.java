package dev.sdl.clickaggregator;

import java.time.Instant;
import java.util.function.LongSupplier;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code KEEP_CLICK(ad_id, advertiser_id, salt, clicked_at, cutoff_ms)}: the stage-1 filter.
 *
 * <ul>
 *   <li>malformed (a required field is NULL after lenient JSON parsing) -> dropped, counted in
 *       {@code clicksDroppedMalformed};
 *   <li>{@code clicked_at} older than {@code cutoff_ms} in processing time -> dropped, counted in
 *       {@code clicksDroppedLate}. Why: aggregation state expires after STATE_TTL (2 h); a click
 *       for a minute whose state has expired would restart that key's count from 1 and the upsert
 *       would overwrite the stored (larger) total. Dropping at 1 h keeps a safe margin.
 * </ul>
 *
 * <p>Both counters are operator metrics (Prometheus reporter on the TaskManagers), and drops are
 * logged at most every 10 s per subtask.
 */
public class KeepClick extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(KeepClick.class);
    private static final long LOG_EVERY_MS = 10_000;

    private transient Counter late;
    private transient Counter malformed;
    private transient LongSupplier clock;
    private transient long nextLogAt;

    @Override
    public void open(FunctionContext context) {
        late = context.getMetricGroup().counter("clicksDroppedLate");
        malformed = context.getMetricGroup().counter("clicksDroppedMalformed");
    }

    /** For tests: a fixed clock and plain counters instead of the metric group. */
    void initForTest(LongSupplier testClock) {
        clock = testClock;
        late = new SimpleCounter();
        malformed = new SimpleCounter();
    }

    long lateCount() {
        return late.getCount();
    }

    long malformedCount() {
        return malformed.getCount();
    }

    public boolean eval(
            Long adId,
            Long advertiserId,
            Integer salt,
            @DataTypeHint("TIMESTAMP_LTZ(3)") Instant clickedAt,
            long cutoffMs) {
        if (adId == null || advertiserId == null || salt == null || clickedAt == null) {
            malformed.inc();
            maybeLog("dropping malformed click record (missing ad_id/advertiser_id/salt/clicked_at)");
            return false;
        }
        long now = clock != null ? clock.getAsLong() : System.currentTimeMillis();
        long ageMs = now - clickedAt.toEpochMilli();
        if (ageMs > cutoffMs) {
            late.inc();
            maybeLog(
                    "dropping late click: ad_id="
                            + adId
                            + " clicked_at="
                            + clickedAt
                            + " age_ms="
                            + ageMs
                            + " (total late drops "
                            + late.getCount()
                            + ")");
            return false;
        }
        return true;
    }

    private void maybeLog(String message) {
        long now = System.currentTimeMillis();
        if (now >= nextLogAt) {
            nextLogAt = now + LOG_EVERY_MS;
            LOG.warn(message);
        }
    }

    /** Depends on the wall clock: must be evaluated per record, never constant-folded. */
    @Override
    public boolean isDeterministic() {
        return false;
    }
}
