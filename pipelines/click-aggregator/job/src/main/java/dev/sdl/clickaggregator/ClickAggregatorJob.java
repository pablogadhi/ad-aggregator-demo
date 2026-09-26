package dev.sdl.clickaggregator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.util.TimeUtils;

/**
 * Runs {@code click-aggregator.sql} (the whole job is Flink SQL) in application mode.
 *
 * <p>The runner only (1) fills {@code ${VAR}} placeholders from the environment -- the connection
 * contracts {@code kafka-conn} / {@code analytics-db-conn} and the knobs in design/contracts/config.md
 * -- (2) registers the {@link KeepClick} UDF and (3) executes the statements in order.
 */
public final class ClickAggregatorJob {

    public static final String SQL_RESOURCE = "click-aggregator.sql";

    static final List<String> REQUIRED =
            List.of(
                    "KAFKA_BOOTSTRAP_SERVERS",
                    "ANALYTICS_DB_JDBC_URL",
                    "ANALYTICS_DB_USER",
                    "ANALYTICS_DB_PASSWORD");

    static final Map<String, String> DEFAULTS =
            Map.of(
                    "CLICKS_TOPIC", "clicks",
                    "CONSUMER_GROUP", "click-aggregator",
                    "CHECKPOINT_INTERVAL", "10s",
                    "STATE_TTL", "2h",
                    "LATE_CUTOFF", "1h");

    private static final Pattern VAR = Pattern.compile("\\$\\{([A-Z0-9_]+)}");
    private static final Pattern SET =
            Pattern.compile("(?is)^SET\\s+'([^']+)'\\s*=\\s*'([^']*)'$");

    private ClickAggregatorJob() {}

    public static void main(String[] args) throws Exception {
        List<String> statements = statements(render(loadSql(), variables(System.getenv())));
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        TableResult result = run(tEnv, statements);
        // In application mode the INSERT is submitted asynchronously; the cluster runs the job.
        result.getJobClient()
                .ifPresent(c -> System.out.println("submitted click-aggregator job " + c.getJobID()));
    }

    /** Environment + defaults + derived values; fails fast on a missing connection variable. */
    static Map<String, String> variables(Map<String, String> env) {
        Map<String, String> vars = new LinkedHashMap<>(DEFAULTS);
        env.forEach(
                (k, v) -> {
                    if (v != null && !v.isEmpty()) {
                        vars.put(k, v);
                    }
                });
        List<String> missing = new ArrayList<>();
        for (String name : REQUIRED) {
            if (!vars.containsKey(name)) {
                missing.add(name);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException("missing environment variables: " + missing);
        }
        vars.put(
                "LATE_CUTOFF_MS",
                Long.toString(TimeUtils.parseDuration(vars.get("LATE_CUTOFF")).toMillis()));
        return vars;
    }

    static String loadSql() throws IOException {
        try (InputStream in =
                ClickAggregatorJob.class.getClassLoader().getResourceAsStream(SQL_RESOURCE)) {
            if (in == null) {
                throw new IOException(SQL_RESOURCE + " not on the classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** Replaces ${VAR}; values land inside SQL string literals, so single quotes are doubled. */
    static String render(String sql, Map<String, String> vars) {
        Matcher m = VAR.matcher(stripComments(sql));
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String value = vars.get(m.group(1));
            if (value == null) {
                throw new IllegalStateException("no value for ${" + m.group(1) + "}");
            }
            m.appendReplacement(out, Matcher.quoteReplacement(value.replace("'", "''")));
        }
        m.appendTail(out);
        return out.toString();
    }

    static String stripComments(String sql) {
        StringBuilder out = new StringBuilder();
        for (String line : sql.split("\n", -1)) {
            if (!line.strip().startsWith("--")) {
                out.append(line).append('\n');
            }
        }
        return out.toString();
    }

    /** Splits on ';' at the end of a line (no statement in the file has one mid-line). */
    static List<String> statements(String sql) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (String line : sql.split("\n")) {
            String trimmed = line.stripTrailing();
            if (trimmed.endsWith(";")) {
                current.append(trimmed, 0, trimmed.length() - 1);
                String stmt = current.toString().strip();
                if (!stmt.isEmpty()) {
                    result.add(stmt);
                }
                current.setLength(0);
            } else {
                current.append(line).append('\n');
            }
        }
        if (!current.toString().isBlank()) {
            throw new IllegalArgumentException("unterminated statement: " + current);
        }
        return result;
    }

    /** Applies SETs to the table config, runs DDL, and returns the result of the last INSERT. */
    static TableResult run(TableEnvironment tEnv, List<String> statements) {
        tEnv.createTemporarySystemFunction("KEEP_CLICK", KeepClick.class);
        TableResult last = null;
        for (String stmt : statements) {
            Matcher set = SET.matcher(stmt);
            if (set.matches()) {
                tEnv.getConfig().set(set.group(1), set.group(2));
            } else {
                last = tEnv.executeSql(stmt);
            }
        }
        return last;
    }
}
