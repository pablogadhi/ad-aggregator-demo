package dev.sdl.clickaggregator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class ClickAggregatorJobTest {

    static final Map<String, String> ENV =
            Map.of(
                    "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                    "ANALYTICS_DB_JDBC_URL", "jdbc:postgresql://analytics-db-rw:5432/app",
                    "ANALYTICS_DB_USER", "app",
                    "ANALYTICS_DB_PASSWORD", "it's-secret");

    static List<String> statements() throws Exception {
        return ClickAggregatorJob.statements(
                ClickAggregatorJob.render(
                        ClickAggregatorJob.loadSql(), ClickAggregatorJob.variables(ENV)));
    }

    static String find(List<String> statements, String prefix) {
        return statements.stream().filter(s -> s.startsWith(prefix)).findFirst().orElseThrow();
    }

    @Test
    void rendersEveryPlaceholderAndEscapesQuotes() throws Exception {
        List<String> stmts = statements();
        String all = String.join("\n", stmts);
        assertThat(all).doesNotContain("${").contains("'it''s-secret'").contains("3600000");
        assertThat(stmts).anyMatch(s -> s.startsWith("CREATE TABLE clicks"));
        assertThat(stmts).anyMatch(s -> s.startsWith("CREATE TABLE click_counts"));
        assertThat(stmts.get(stmts.size() - 1)).startsWith("INSERT INTO click_counts");
        assertThat(all).doesNotContain("--");
    }

    @Test
    void failsFastWithoutConnectionVariables() {
        assertThatThrownBy(() -> ClickAggregatorJob.variables(Map.of("KAFKA_BOOTSTRAP_SERVERS", "x")))
                .hasMessageContaining("ANALYTICS_DB_JDBC_URL");
    }

    /**
     * Plans the real job (Kafka source + JDBC sink, no connections are opened) and checks the
     * properties the design relies on.
     */
    @Test
    void planIsTwoStageLocalGlobalWithAnUpsertKeyMatchingThePrimaryKey() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        List<String> stmts = statements();
        String insert = stmts.remove(stmts.size() - 1);
        ClickAggregatorJob.run(tEnv, stmts);
        String plan = tEnv.explainSql(insert, ExplainDetail.CHANGELOG_MODE);
        System.out.println(plan);
        String optimized = plan.substring(plan.indexOf("== Optimized Physical Plan =="));
        optimized = optimized.substring(0, optimized.indexOf("== Optimized Execution Plan =="));

        assertThat(count(optimized, "LocalGroupAggregate(")).isEqualTo(2);
        assertThat(count(optimized, "GlobalGroupAggregate(")).isEqualTo(2);
        assertThat(optimized).contains("MiniBatchAssigner");
        // no SinkUpsertMaterializer, and the sink only receives inserts/update-afters (no
        // UPDATE_BEFORE, which the JDBC upsert writer would execute as a DELETE)
        assertThat(optimized).doesNotContain("upsertMaterialize=[true]");
        String sinkLine = optimized.lines().filter(l -> l.contains("Sink(")).findFirst().orElseThrow();
        String sinkInput =
                optimized.lines().dropWhile(l -> !l.contains("Sink(")).skip(1).findFirst().orElseThrow();
        assertThat(sinkLine).contains("table=[default_catalog.default_database.click_counts]");
        assertThat(sinkInput).contains("changelogMode=[I,UA,D]").doesNotContain("UB");
    }

    private static int count(String haystack, String needle) {
        int n = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) {
            n++;
        }
        return n;
    }

    /**
     * End to end on a local MiniCluster: the job's own SETs, UDF and query, with the Kafka source
     * swapped for a JSON file in the same format, and the INSERT's query collected as a changelog.
     */
    @Test
    @Timeout(180)
    void countsClicksPerAdAndMinuteAcrossSaltsAndDropsLateAndMalformed(@TempDir Path dir)
            throws Exception {
        Instant now = Instant.now();
        Instant minute = now.minus(5, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MINUTES);
        Instant next = minute.plus(1, ChronoUnit.MINUTES);
        List<String> lines = new ArrayList<>();
        // ad 1: 3 normal clicks in `minute`
        for (int i = 0; i < 3; i++) {
            lines.add(event(1, 10, 0, minute.plusSeconds(10 + i)));
        }
        // ad 2 (hot, salted): 5 clicks over 5 salts in `minute`, 2 clicks in `next`
        for (int salt = 0; salt < 5; salt++) {
            lines.add(event(2, 10, salt, minute.plusSeconds(30 + salt)));
        }
        lines.add(event(2, 10, 7, next.plusSeconds(1)));
        lines.add(event(2, 10, 8, next.plusMillis(59_999)));
        // ad 3: late (2 h old) -> dropped
        lines.add(event(3, 11, 0, now.minus(2, ChronoUnit.HOURS)));
        // malformed / incomplete -> dropped
        lines.add("{not json");
        lines.add("{\"click_id\":\"x\",\"ad_id\":4,\"advertiser_id\":12,\"user_id\":\"u\","
                + "\"clicked_at\":\"" + ts(minute) + "\",\"receiver\":\"r\"}"); // no salt
        Files.write(dir.resolve("clicks.json"), lines);

        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        List<String> stmts = new ArrayList<>();
        for (String s : statements()) {
            if (s.contains("execution.checkpointing.interval")) {
                continue; // bounded test input: no checkpoints needed to collect results
            }
            if (s.startsWith("CREATE TABLE clicks")) {
                s = s.substring(0, s.indexOf(") WITH (")) + ") WITH ("
                        + "'connector' = 'filesystem', 'path' = '" + dir.toUri() + "', "
                        + "'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601', "
                        + "'json.ignore-parse-errors' = 'true')";
            }
            stmts.add(s);
        }
        String insert = stmts.remove(stmts.size() - 1);
        // small local MiniCluster: few subtasks (network buffers), fail instead of restarting
        tEnv.getConfig().set("parallelism.default", "2");
        tEnv.getConfig().set("restart-strategy.type", "none");
        ClickAggregatorJob.run(tEnv, stmts);
        String query = insert.replaceFirst("^INSERT INTO click_counts\\s*", "");

        Map<String, Long> counts = new HashMap<>();
        Map<String, Long> advertiser = new HashMap<>();
        TableResult result = tEnv.executeSql(query);
        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                String key = row.getField(0) + "@" + row.getField(2);
                switch (row.getKind()) {
                    case INSERT, UPDATE_AFTER -> {
                        counts.put(key, (Long) row.getField(3));
                        advertiser.put(key, (Long) row.getField(1));
                        assertThat(row.getField(4)).isNotNull(); // updated_at
                    }
                    case UPDATE_BEFORE, DELETE -> counts.remove(key);
                }
            }
        }
        LocalDateTime m0 = LocalDateTime.ofInstant(minute, ZoneOffset.UTC); // UTC minute start
        LocalDateTime m1 = LocalDateTime.ofInstant(next, ZoneOffset.UTC);
        assertThat(counts).isEqualTo(Map.of("1@" + m0, 3L, "2@" + m0, 5L, "2@" + m1, 2L));
        assertThat(advertiser.get("2@" + m0)).isEqualTo(10L);
    }

    static String ts(Instant t) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                .withZone(ZoneOffset.UTC)
                .format(t);
    }

    static String event(long ad, long advertiser, int salt, Instant at) {
        return String.format(
                "{\"click_id\":\"%s\",\"ad_id\":%d,\"advertiser_id\":%d,\"salt\":%d,"
                        + "\"user_id\":\"u-%s\",\"clicked_at\":\"%s\",\"receiver\":\"pod@node\"}",
                java.util.UUID.randomUUID(), ad, advertiser, salt, at.toEpochMilli(), ts(at));
    }
}
