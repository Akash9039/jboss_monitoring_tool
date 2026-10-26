package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.config.JBossConfig;
import com.rh.monitor.config.ServerContext;
import com.rh.monitor.util.TimeUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import com.rh.monitor.service.HistoryTracker;
import com.rh.monitor.util.DomainHelper;
import org.jboss.logging.Logger;

import java.util.regex.*;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class TransactionResource {

    private static final Logger LOG = Logger.getLogger(TransactionResource.class);

    @Inject
    JBossManagementClient client;

    @Inject
    JBossConfig config;

    @Inject
    ServerContext serverContext;

    @Inject
    HistoryTracker historyTracker;

    @GET
    @Path("/transactions")
    public Map<String, Object> getTransactionStats() {
        LOG.info("Fetching transaction statistics");
        try {
            boolean isServerSelected = false;
            try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

            List<JsonNode> dataNodes = new ArrayList<>();
            List<String> serverNames = new ArrayList<>();

            if (!isServerSelected) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (!domainServers.isEmpty()) {
                    // One wildcard read returns every server's transactions subsystem.
                    Map<String, JsonNode> byServer = client.readWildcardAcrossServers(
                            List.of("subsystem", "transactions"), true, false);
                    for (Map.Entry<String, JsonNode> entry : byServer.entrySet()) {
                        dataNodes.add(entry.getValue().get("result"));
                        serverNames.add(entry.getKey());
                    }
                }
            }

            if (dataNodes.isEmpty()) {
                JsonNode result = client.readResource(List.of("subsystem", "transactions"));
                if (!"success".equals(result.path("outcome").asText())) {
                    return Map.of("error", result.path("failure-description").asText("Unknown error"));
                }
                dataNodes.add(result.get("result"));
            }

            int committed = 0, aborted = 0, timedOut = 0, inflight = 0, nested = 0;
            int heuristics = 0, appRollbacks = 0, resourceRollbacks = 0, systemRollbacks = 0;
            int defaultTimeout = 300;
            String nodeIdentifier = null;

            for (JsonNode data : dataNodes) {
                committed += si(data, "number-of-committed-transactions");
                aborted += si(data, "number-of-aborted-transactions");
                timedOut += si(data, "number-of-timed-out-transactions");
                inflight += si(data, "number-of-inflight-transactions");
                nested += si(data, "number-of-nested-transactions");
                heuristics += si(data, "number-of-heuristics");
                appRollbacks += si(data, "number-of-application-rollbacks");
                resourceRollbacks += si(data, "number-of-resource-rollbacks");
                systemRollbacks += si(data, "number-of-system-rollbacks");
                defaultTimeout = data.path("default-timeout").asInt(300);
                if (nodeIdentifier == null) nodeIdentifier = data.path("node-identifier").asText(null);
            }

            int total = committed + aborted;
            double abortRate = Math.round((double) aborted / Math.max(total, 1) * 10000) / 100.0;

            String health = "HEALTHY";
            if (abortRate > 20 || timedOut > 10) health = "CRITICAL";
            else if (abortRate > 10 || timedOut > 5) health = "WARNING";
            else if (abortRate > 5) health = "MODERATE";

            Map<String, Object> txn = new LinkedHashMap<>();
            txn.put("committed", committed);
            txn.put("aborted", aborted);
            txn.put("timed_out", timedOut);
            txn.put("inflight", inflight);
            txn.put("nested", nested);
            txn.put("heuristics", heuristics);
            txn.put("total", total);
            txn.put("abort_rate_percent", abortRate);
            txn.put("application_rollbacks", appRollbacks);
            txn.put("resource_rollbacks", resourceRollbacks);
            txn.put("system_rollbacks", systemRollbacks);
            txn.put("default_timeout", defaultTimeout);
            txn.put("enable_tsm_status", dataNodes.get(0).path("enable-tsm-status").asBoolean(false));
            txn.put("jts", dataNodes.get(0).path("jts").asBoolean(false));
            txn.put("node_identifier", nodeIdentifier);
            txn.put("health", health);
            if (!serverNames.isEmpty()) txn.put("servers", serverNames);

            txn.put("event_log_file", historyTracker.getTransactionEventLogPath().toString());
            return Map.of("status", "ok", "transactions", txn);
        } catch (Exception e) {
            return Map.of("error", "Failed to get transaction stats: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @GET
    @Path("/transaction-event-log")
    @Produces(MediaType.TEXT_PLAIN)
    public String getTransactionEventLog() {
        LOG.info("Fetching transaction event log");
        java.nio.file.Path logPath = historyTracker.getTransactionEventLogPath();
        if (!Files.exists(logPath)) return "No transaction events recorded yet. File: " + logPath;
        try {
            return Files.readString(logPath);
        } catch (IOException e) {
            return "Error reading transaction event log: " + e.getMessage();
        }
    }

    @GET
    @Path("/transaction-events")
    public Map<String, Object> getTransactionEvents() {
        LOG.info("Fetching transaction events data");
        return Map.of("status", "ok",
                "events", new ArrayList<>(HistoryTracker.getTxEventLog()),
                "event_log_file", historyTracker.getTransactionEventLogPath().toString());
    }

    @GET
    @Path("/transactions/history")
    public Map<String, Object> getTransactionHistory() {
        LOG.info("Fetching transaction history");
        String jbossHome = config != null ? config.getJbossHome() : System.getenv("JBOSS_HOME");
        boolean isServerSelected = false;
        try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

        List<Map<String, Object>> transactions;
        String logFile;

        if (!isServerSelected && client.isDomainMode()) {
            transactions = new ArrayList<>();
            List<DomainHelper.ServerLog> domainLogs = DomainHelper.findAllDomainLogs(config.getDomainHomeMap(), "transaction.log");
            List<String> logFiles = new ArrayList<>();
            for (DomainHelper.ServerLog sl : domainLogs) {
                List<Map<String, Object>> parsed = parseTxLogFromPath(sl.path);
                for (Map<String, Object> t : parsed) t.put("server_name", sl.serverName);
                transactions.addAll(parsed);
                logFiles.add(sl.path);
            }
            transactions.sort((a, b) -> {
                String aTime = (String) a.getOrDefault("begin_time", "");
                String bTime = (String) b.getOrDefault("begin_time", "");
                return bTime.compareTo(aTime);
            });
            logFile = String.join(", ", logFiles);
        } else {
            transactions = parseTxLog();
            logFile = findTxLog();
        }

        if (transactions.isEmpty()) {
            if (logFile == null) {
                return Map.of("status", "not_configured",
                        "message", "Transaction logging not configured. Enable com.arjuna TRACE logger in JBoss.",
                        "transactions", List.of());
            }
            return Map.of("status", "ok", "transactions", List.of(),
                    "message", "No transactions found in log.", "log_file", logFile);
        }

        correlateTxWithSpy(transactions);
        correlateTxWithAccessLog(transactions);

        int committed = 0, aborted = 0, inflightCount = 0;
        double totalDuration = 0;
        for (Map<String, Object> t : transactions) {
            String outcome = (String) t.getOrDefault("outcome", "");
            if ("COMMITTED".equals(outcome)) committed++;
            else if ("ABORTED".equals(outcome)) aborted++;
            else if ("IN_FLIGHT".equals(outcome)) inflightCount++;
            totalDuration += ((Number) t.getOrDefault("duration_ms", 0.0)).doubleValue();
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "ok");
        resp.put("transactions", transactions.size() > 500 ? transactions.subList(0, 500) : transactions);
        resp.put("total", transactions.size());
        resp.put("committed", committed);
        resp.put("aborted", aborted);
        resp.put("inflight", inflightCount);
        resp.put("avg_duration_ms", Math.round(totalDuration / Math.max(transactions.size(), 1) * 100) / 100.0);
        resp.put("log_file", logFile);
        return resp;
    }

    // --- Transaction log parsing ---

    private static final String TS_RE = "((?:\\d{4}-\\d{2}-\\d{2} )?\\d{2}:\\d{2}:\\d{2},\\d{3})";
    private static final Pattern BEGIN_RE = Pattern.compile(
            "^" + TS_RE + "\\s+\\w+\\s+\\[[\\w.]+\\]\\s+" +
                    "\\(([^)]+)\\)\\s+BasicAction::Begin\\(\\) for action-id\\s+(\\S+)");
    private static final Pattern END_RE = Pattern.compile(
            "^" + TS_RE + "\\s+\\w+\\s+\\[[\\w.]+\\]\\s+" +
                    "\\(([^)]+)\\)\\s+BasicAction::End\\(\\) for action-id\\s+(\\S+)");
    private static final Pattern RESULT_RE = Pattern.compile(
            "^" + TS_RE + "\\s+\\w+\\s+\\[[\\w.]+\\]\\s+" +
                    "\\(([^)]+)\\)\\s+BasicAction::End\\(\\) result for action-id \\(([^)]+)\\) is \\(([^)]+)\\)");
    private static final Pattern ABORT_RE = Pattern.compile(
            "^" + TS_RE + "\\s+\\w+\\s+\\[[\\w.]+\\]\\s+" +
                    "\\(([^)]+)\\)\\s+BasicAction::Abort\\(\\) for action-id\\s+(\\S+)");
    private static final Pattern STATUS_RE = Pattern.compile(
            "TransactionReaper::remove \\( BasicAction:\\s+(\\S+)\\s+status:\\s+(\\S+)");

    private List<Map<String, Object>> parseTxLog() {
        String logPath = findTxLog();
        if (logPath == null) return List.of();
        return parseTxLogFromPath(logPath);
    }

    private List<Map<String, Object>> parseTxLogFromPath(String logPath) {
        Map<String, Map<String, Object>> pending = new LinkedHashMap<>();
        List<Map<String, Object>> transactions = new ArrayList<>();
        int maxLines = 20000;

        try {
            List<String> lines = Files.readAllLines(Paths.get(logPath));
            int start = Math.max(0, lines.size() - maxLines);

            for (int i = start; i < lines.size(); i++) {
                String line = lines.get(i).trim();

                Matcher m = BEGIN_RE.matcher(line);
                if (m.find()) {
                    Map<String, Object> tx = new LinkedHashMap<>();
                    tx.put("tx_id", m.group(3));
                    tx.put("thread", m.group(2));
                    tx.put("begin_time", m.group(1));
                    tx.put("end_time", null);
                    tx.put("outcome", null);
                    tx.put("two_phase_result", null);
                    pending.put(m.group(3), tx);
                    continue;
                }

                m = END_RE.matcher(line);
                if (m.find()) {
                    Map<String, Object> tx = pending.get(m.group(3));
                    if (tx != null) tx.put("end_time", m.group(1));
                    continue;
                }

                m = RESULT_RE.matcher(line);
                if (m.find()) {
                    Map<String, Object> tx = pending.get(m.group(3));
                    if (tx != null) {
                        tx.put("two_phase_result", m.group(4));
                        if (tx.get("end_time") == null) tx.put("end_time", m.group(1));
                    }
                    continue;
                }

                m = ABORT_RE.matcher(line);
                if (m.find()) {
                    Map<String, Object> tx = pending.get(m.group(3));
                    if (tx != null) {
                        tx.put("end_time", m.group(1));
                        tx.put("outcome", "ABORTED");
                    }
                    continue;
                }

                m = STATUS_RE.matcher(line);
                if (m.find()) {
                    String txId = m.group(1);
                    String status = m.group(2);
                    Map<String, Object> tx = pending.remove(txId);
                    if (tx != null) {
                        tx.put("outcome", status.replace("ActionStatus.", ""));
                        String begin = (String) tx.get("begin_time");
                        String end = (String) tx.get("end_time");
                        tx.put("duration_ms", (begin != null && end != null) ? TimeUtils.spyTimeDiff(begin, end) : 0.0);
                        transactions.add(tx);
                    }
                }
            }
        } catch (Exception e) {
            // log parse error silently
        }

        String nowStr = TimeUtils.nowSpyFormat();
        for (Map<String, Object> tx : pending.values()) {
            if (tx.get("outcome") == null) tx.put("outcome", "IN_FLIGHT");
            String begin = (String) tx.get("begin_time");
            String end = tx.get("end_time") != null ? (String) tx.get("end_time") : nowStr;
            tx.put("duration_ms", begin != null ? TimeUtils.spyTimeDiff(begin, end) : 0.0);
            transactions.add(tx);
        }

        transactions.sort((a, b) -> {
            String aTime = (String) a.getOrDefault("begin_time", "");
            String bTime = (String) b.getOrDefault("begin_time", "");
            return bTime.compareTo(aTime);
        });

        return transactions;
    }

    // --- JDBC spy log correlation ---

    private static final Pattern SPY_RE = Pattern.compile(
            "^" + TS_RE + "\\s+\\w+\\s+\\[jboss\\.jdbc\\.spy\\]\\s+" +
                    "\\(([^)]+)\\)\\s+(java:jboss/datasources/\\S+)\\s+\\[(\\w+)\\]\\s+(.*)");

    private void correlateTxWithSpy(List<Map<String, Object>> transactions) {
        // Domain, all-servers view (no server selected): a transaction must correlate ONLY with
        // its own server's spy queries. Worker thread names ("default task-1") repeat across
        // servers, so a single merged index would mis-attribute — and findJdbcSpyLog() would only
        // read the first server's log, leaving every other server's transactions without SQL.
        if (isDomainAllServers()) {
            Map<String, Map<String, List<Map<String, Object>>>> byServer = new HashMap<>();
            for (DomainHelper.ServerLog sl : DomainHelper.findAllDomainLogs(config.getDomainHomeMap(), "jdbc-spy.log")) {
                byServer.put(sl.serverName, indexByThread(parseSpyQueries(sl.path)));
            }
            for (Map<String, Object> tx : transactions) {
                Map<String, List<Map<String, Object>>> tq = byServer.get((String) tx.get("server_name"));
                applySpyMatch(tx, tq != null ? tq : Map.of());
            }
            return;
        }

        String spyPath = findJdbcSpyLog();
        if (spyPath == null) return;

        List<Map<String, Object>> queries = parseSpyQueries(spyPath);
        if (queries.isEmpty()) return;

        Map<String, List<Map<String, Object>>> threadQueries = indexByThread(queries);
        for (Map<String, Object> tx : transactions) applySpyMatch(tx, threadQueries);
    }

    /** Match a transaction to its spy queries (same thread, executed within the tx's time span). */
    private void applySpyMatch(Map<String, Object> tx, Map<String, List<Map<String, Object>>> threadQueries) {
        String thread = (String) tx.get("thread");
        String begin = (String) tx.get("begin_time");
        String end = tx.get("end_time") != null ? (String) tx.get("end_time") : TimeUtils.nowSpyFormat();

        List<Map<String, Object>> matched = new ArrayList<>();
        for (Map<String, Object> q : threadQueries.getOrDefault(thread, List.of())) {
            String qTs = (String) q.getOrDefault("timestamp", "");
            if (!qTs.isEmpty() && qTs.compareTo(begin) >= 0 && qTs.compareTo(end) <= 0) {
                Map<String, Object> mq = new LinkedHashMap<>();
                mq.put("sql", q.get("sql"));
                mq.put("datasource", q.get("datasource"));
                mq.put("exec_time_ms", q.get("exec_time_ms"));
                mq.put("params", q.get("params"));
                mq.put("execute_type", q.get("execute_type"));
                matched.add(mq);
            }
        }
        tx.put("queries", matched);
        tx.put("query_count", matched.size());
    }

    /** True when running in domain mode with no specific server selected (aggregated view). */
    private boolean isDomainAllServers() {
        boolean hasServerCtx = false;
        try { hasServerCtx = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}
        return !hasServerCtx && client.isDomainMode();
    }

    /** Group log records (spy queries / access requests) by their worker thread name. */
    private Map<String, List<Map<String, Object>>> indexByThread(List<Map<String, Object>> items) {
        Map<String, List<Map<String, Object>>> idx = new HashMap<>();
        for (Map<String, Object> it : items) {
            String thread = (String) it.get("thread");
            if (thread != null) idx.computeIfAbsent(thread, k -> new ArrayList<>()).add(it);
        }
        return idx;
    }

    // --- Access log correlation (which HTTP endpoint drove each transaction) ---
    // Join key: worker thread name + time-window overlap. A JTA transaction runs inside the
    // HTTP request that opened it, on the same worker thread, so the access-log entry whose
    // [start, start+response_time] window best wraps the transaction is the owning endpoint.

    // Standalone: %h %t "%r" %s %D %b %I|%S   (last field = thread|session)
    private static final Pattern ACCESS_LOG_RE = Pattern.compile(
            "^(\\S+)\\s+(?:\\S+\\s+\\S+\\s+)?\\[([^\\]]+)\\]\\s+\"(\\S+)\\s+(\\S+)\\s+\\S+\"\\s+(\\d{3})\\s+(\\d+)(.*)$");
    private static final Pattern ACCESS_TAIL_RE = Pattern.compile("^\\s+(\\d+|-)\\s+(.+?)\\|(.*)$");
    // Labeled format (Undertow "...%b ... Thread: \"default task-1\" TimeTaken: 0.123 ..."):
    // the worker thread and the time may appear in any order and be followed by other labels,
    // so match each independently and stop the value at the next "Label:" token or end of line.
    private static final Pattern ACCESS_THREAD_LABEL_RE = Pattern.compile("Thread:\\s*\"?([^\"]+?)\"?(?=\\s+\\w+:|\\s*$)");
    private static final Pattern ACCESS_TIME_LABEL_RE = Pattern.compile("(?:TimeTaken|ResponseTime|Duration|Elapsed):\\s*\"?([0-9.]+)\"?");
    private static final DateTimeFormatter ACCESS_LOG_DATE =
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
    private static final DateTimeFormatter TX_TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
    private static final long TX_ACCESS_SLACK_MS = 2000; // clock skew / rounding tolerance
    private static final long MS_PER_DAY = 86_400_000L;
    // Time-of-day part of a transaction-log timestamp, with or without a leading date.
    private static final Pattern TX_TIME_RE = Pattern.compile("(\\d{2}):(\\d{2}):(\\d{2}),(\\d{3})");

    private void correlateTxWithAccessLog(List<Map<String, Object>> transactions) {
        // Domain, all-servers view: scope each transaction to its OWN server's access log
        // (thread names collide across servers; a merged index mis-attributes endpoints).
        if (isDomainAllServers()) {
            Map<String, Map<String, List<Map<String, Object>>>> byServer = new HashMap<>();
            for (String name : new String[]{"access.log", "access_log.log"}) {
                for (DomainHelper.ServerLog sl : DomainHelper.findAllDomainLogs(config.getDomainHomeMap(), name)) {
                    Map<String, List<Map<String, Object>>> idx =
                            byServer.computeIfAbsent(sl.serverName, k -> new HashMap<>());
                    for (Map<String, Object> r : parseAccessRequests(sl.path)) {
                        String thread = (String) r.get("thread");
                        if (thread != null) idx.computeIfAbsent(thread, x -> new ArrayList<>()).add(r);
                    }
                }
            }
            for (Map<String, Object> tx : transactions) {
                Map<String, List<Map<String, Object>>> idx = byServer.get((String) tx.get("server_name"));
                applyAccessMatch(tx, idx != null ? idx : Map.of());
            }
            return;
        }

        List<String> accessPaths = findAccessLogs();
        if (accessPaths.isEmpty()) return;

        List<Map<String, Object>> reqs = new ArrayList<>();
        for (String p : accessPaths) reqs.addAll(parseAccessRequests(p));
        if (reqs.isEmpty()) return;

        Map<String, List<Map<String, Object>>> threadReqs = indexByThread(reqs);
        for (Map<String, Object> tx : transactions) applyAccessMatch(tx, threadReqs);
    }

    /** Match a transaction to the HTTP request (endpoint) that drove it, on the same thread. */
    private void applyAccessMatch(Map<String, Object> tx, Map<String, List<Map<String, Object>>> threadReqs) {
        String thread = (String) tx.get("thread");
        String beginStr = (String) tx.get("begin_time");
        String endStr = tx.get("end_time") != null ? (String) tx.get("end_time") : null;

        // The arjuna transaction log's default pattern often has NO date (just "HH:mm:ss,SSS"),
        // while the access log always carries a full date. When the tx timestamp has no date we
        // can't build an absolute epoch, so compare both sides by time-of-day (ms since midnight).
        // Both logs use the same local wall clock, so time-of-day lines up. Only today's
        // access_log.log is read (rotated dated files are ignored), so cross-day collisions can't occur.
        boolean txHasDate = beginStr != null && beginStr.strip().matches("^\\d{4}-\\d{2}-\\d{2}.*");
        long begin = txHasDate ? wallTxMs(beginStr) : txMsOfDay(beginStr);
        if (thread == null || begin < 0) return;
        long end = endStr != null ? (txHasDate ? wallTxMs(endStr) : txMsOfDay(endStr)) : begin;
        if (end < 0) end = begin;
        if (end < begin) end = begin;

        Map<String, Object> best = null;
        long bestGap = Long.MAX_VALUE;
        for (Map<String, Object> r : threadReqs.getOrDefault(thread, List.of())) {
            // Undertow renders %t at request COMPLETION, so wall_ms is the end of the
            // request and its start is wall_ms - response_time.
            long completion = ((Number) r.getOrDefault("wall_ms", -1L)).longValue();
            if (completion < 0) continue;
            if (!txHasDate) completion = ((completion % MS_PER_DAY) + MS_PER_DAY) % MS_PER_DAY;
            long rt = ((Number) r.getOrDefault("response_time_ms", 0L)).longValue();
            long reqStart = completion - rt;
            long winStart = reqStart - TX_ACCESS_SLACK_MS;
            long winEnd = completion + TX_ACCESS_SLACK_MS;
            // Request window must overlap the transaction's [begin, end].
            if (winEnd < begin || winStart > end) continue;
            long gap = Math.abs(reqStart - begin); // request that most tightly wraps the tx begin
            if (gap < bestGap) {
                bestGap = gap;
                best = r;
            }
        }

        if (best != null) {
            String method = (String) best.getOrDefault("method", "");
            String uri = (String) best.getOrDefault("uri", "");
            tx.put("api_method", method);
            tx.put("api_uri", uri);
            tx.put("api_status", best.get("status"));
            tx.put("api_endpoint", (method + " " + uri).strip());
        }
    }

    /**
     * All existing access-log files (both common names: "access.log" and Undertow's default
     * "access_log.log"), across every candidate location. We read them ALL and merge, rather
     * than returning the first match: a server can have a stale "access.log" left over next to a
     * live "access_log.log" (or vice-versa), and picking the wrong one silently breaks endpoint
     * correlation. Stale entries are harmless — they just fall outside every transaction window.
     */
    private List<String> findAccessLogs() {
        List<String> out = new ArrayList<>();
        for (String name : new String[]{"access.log", "access_log.log"}) {
            for (String c : logCandidates(name)) {
                if (!out.contains(c) && Files.isRegularFile(Paths.get(c))) out.add(c);
            }
        }
        return out;
    }

    /** Minimal access-log parse: thread, wall-clock ms, method, uri, status, response time. */
    private List<Map<String, Object>> parseAccessRequests(String logPath) {
        List<Map<String, Object>> out = new ArrayList<>();
        int maxLines = 20000;
        try {
            List<String> all = Files.readAllLines(Paths.get(logPath));
            List<String> lines = all.subList(Math.max(0, all.size() - maxLines), all.size());
            for (String raw : lines) {
                Matcher m = ACCESS_LOG_RE.matcher(raw.strip());
                if (!m.matches()) continue;

                long wallMs = accessWallMs(m.group(2));
                if (wallMs < 0) continue;
                String method = m.group(3);
                String uri = m.group(4);
                int status = Integer.parseInt(m.group(5));
                long firstNum = Long.parseLong(m.group(6));
                String remainder = m.group(7);

                long responseTimeMs = 0;
                String thread = null;
                Matcher tail = remainder != null ? ACCESS_TAIL_RE.matcher(remainder) : null;
                if (tail != null && tail.matches()) {
                    responseTimeMs = firstNum;                 // standalone: %D before %b
                    thread = tail.group(2).strip();
                } else if (remainder != null) {
                    // Labeled format: firstNum was %b (bytes), the real time is the TimeTaken label.
                    Matcher tl = ACCESS_THREAD_LABEL_RE.matcher(remainder);
                    if (tl.find()) thread = tl.group(1).strip();
                    Matcher tm = ACCESS_TIME_LABEL_RE.matcher(remainder);
                    if (tm.find()) responseTimeMs = labelTimeToMs(tm.group(1));
                }
                if (thread == null || thread.isBlank()) continue;

                // Strip query string so URIs group cleanly (e.g. /api/files?x=1 -> /api/files).
                int q = uri.indexOf('?');
                if (q >= 0) uri = uri.substring(0, q);

                Map<String, Object> r = new LinkedHashMap<>();
                r.put("thread", thread);
                r.put("wall_ms", wallMs);
                r.put("method", method);
                r.put("uri", uri);
                r.put("status", status);
                r.put("response_time_ms", responseTimeMs);
                out.add(r);
            }
        } catch (Exception ignored) {}
        return out;
    }

    /**
     * A labeled TimeTaken/ResponseTime value to milliseconds. Undertow's %T writes seconds with
     * a decimal (e.g. "0.123", "120.032"); a bare integer is treated as milliseconds (%D).
     */
    private long labelTimeToMs(String raw) {
        try {
            if (raw.indexOf('.') >= 0) return Math.round(Double.parseDouble(raw) * 1000.0);
            return Long.parseLong(raw);
        } catch (Exception e) {
            return 0;
        }
    }

    /** Access-log timestamp -> local wall-clock epoch ms (offset dropped to match tx-log times). */
    private long accessWallMs(String tsRaw) {
        try {
            return ZonedDateTime.parse(tsRaw, ACCESS_LOG_DATE)
                    .toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception e) {
            return -1;
        }
    }

    /** Transaction-log timestamp ("yyyy-MM-dd HH:mm:ss,SSS") -> wall-clock epoch ms. */
    private long wallTxMs(String ts) {
        if (ts == null || ts.isBlank()) return -1;
        try {
            return LocalDateTime.parse(ts.strip(), TX_TS_FMT).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception e) {
            return -1;
        }
    }

    /** Milliseconds-since-midnight for a tx-log timestamp ("[yyyy-MM-dd ]HH:mm:ss,SSS"); -1 on failure. */
    private long txMsOfDay(String ts) {
        if (ts == null || ts.isBlank()) return -1;
        Matcher m = TX_TIME_RE.matcher(ts);
        if (!m.find()) return -1;
        return (Long.parseLong(m.group(1)) * 3600L
                + Long.parseLong(m.group(2)) * 60L
                + Long.parseLong(m.group(3))) * 1000L
                + Long.parseLong(m.group(4));
    }

    private List<Map<String, Object>> parseSpyQueries(String logPath) {
        List<Map<String, Object>> queries = new ArrayList<>();
        Map<String, Map<String, Object>> pending = new HashMap<>();

        try {
            List<String> lines = Files.readAllLines(Paths.get(logPath));
            int start = Math.max(0, lines.size() - 10000);

            for (int i = start; i < lines.size(); i++) {
                Matcher m = SPY_RE.matcher(lines.get(i).trim());
                if (!m.find()) continue;

                String timestamp = m.group(1);
                String thread = m.group(2);
                String jndi = m.group(3);
                String objType = m.group(4);
                String operation = m.group(5).trim();
                String dsName = jndi.replace("java:jboss/datasources/", "");
                String key = thread + "|" + dsName;

                if ("Connection".equals(objType) && operation.startsWith("prepareStatement(")) {
                    String sql = operation.substring("prepareStatement(".length());
                    if (sql.endsWith(")")) sql = sql.substring(0, sql.length() - 1);
                    // Strip trailing int params
                    String[] trailing = sql.split(",");
                    if (trailing.length >= 3) {
                        try {
                            Integer.parseInt(trailing[trailing.length - 1].trim());
                            Integer.parseInt(trailing[trailing.length - 2].trim());
                            sql = String.join(",", Arrays.copyOf(trailing, trailing.length - 2));
                        } catch (NumberFormatException ignored) {}
                    }
                    Map<String, Object> q = new LinkedHashMap<>();
                    q.put("sql", sql.trim());
                    q.put("datasource", dsName);
                    q.put("thread", thread);
                    q.put("prepare_time", timestamp);
                    q.put("params", new ArrayList<String>());
                    q.put("execute_time", null);
                    pending.put(key, q);
                } else if ("PreparedStatement".equals(objType) && pending.containsKey(key)) {
                    Map<String, Object> q = pending.get(key);
                    if ("close()".equals(operation) && q.get("execute_time") != null) {
                        pending.remove(key);
                        double execMs = TimeUtils.spyTimeDiff((String) q.get("execute_time"), timestamp);
                        @SuppressWarnings("unchecked")
                        List<String> params = (List<String>) q.get("params");
                        String paramsStr = String.join("; ", params.size() > 10 ? params.subList(0, 10) : params);
                        if (params.size() > 10) paramsStr += " ... (+" + (params.size() - 10) + " more)";

                        Map<String, Object> query = new LinkedHashMap<>();
                        query.put("sql", q.get("sql"));
                        query.put("datasource", q.get("datasource"));
                        query.put("thread", q.get("thread"));
                        query.put("timestamp", q.get("execute_time"));
                        query.put("exec_time_ms", execMs);
                        query.put("params", paramsStr);
                        query.put("execute_type", q.getOrDefault("execute_type", ""));
                        queries.add(query);
                    } else if (operation.startsWith("set") && operation.contains("(")) {
                        @SuppressWarnings("unchecked")
                        List<String> params = (List<String>) q.get("params");
                        params.add(operation);
                    } else if ("executeQuery()".equals(operation) || "executeUpdate()".equals(operation) || "execute()".equals(operation)) {
                        q.put("execute_time", timestamp);
                        q.put("execute_type", operation.replace("()", ""));
                    }
                }
            }
        } catch (Exception ignored) {}

        queries.sort((a, b) -> {
            String aTs = (String) a.getOrDefault("timestamp", "");
            String bTs = (String) b.getOrDefault("timestamp", "");
            return bTs.compareTo(aTs);
        });

        return queries;
    }

    // --- File locators ---

    private String findTxLog() {
        return findLog("transaction.log");
    }

    private String findJdbcSpyLog() {
        return findLog("jdbc-spy.log");
    }

    private String findLog(String filename) {
        for (String c : logCandidates(filename)) {
            if (Files.isRegularFile(Paths.get(c))) return c;
        }
        return null;
    }

    /** Ordered list of paths where {@code filename} might live (server-context/domain aware). */
    private List<String> logCandidates(String filename) {
        List<String> candidates = new ArrayList<>();
        boolean hasServerCtx = false;
        try { hasServerCtx = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

        boolean isDomain = client.isDomainMode();
        if (hasServerCtx && isDomain) {
            String hostHome = config.getHomeForHost(serverContext.getHost());
            candidates.add(hostHome + "/domain/servers/" + serverContext.getServer() + "/log/" + filename);
            for (String home : config.getAllDomainHomes()) {
                String path = home + "/domain/servers/" + serverContext.getServer() + "/log/" + filename;
                if (!candidates.contains(path)) candidates.add(path);
            }
        } else if (isDomain) {
            for (String home : config.getAllDomainHomes()) {
                if (DomainHelper.isDomainLayout(home)) {
                    java.io.File serversDir = new java.io.File(home, "domain/servers");
                    java.io.File[] dirs = serversDir.listFiles(java.io.File::isDirectory);
                    if (dirs != null) {
                        for (java.io.File dir : dirs) {
                            candidates.add(new java.io.File(dir, "log/" + filename).getPath());
                        }
                    }
                }
            }
        }
        String jbossHome = config != null ? config.getJbossHome() : System.getenv("JBOSS_HOME");
        if (!isDomain && jbossHome != null) {
            candidates.add(jbossHome + "/standalone/log/" + filename);
        }
        return candidates;
    }

    private int si(JsonNode parent, String field) {
        JsonNode n = parent.path(field);
        if (n.isMissingNode() || n.isNull()) return 0;
        return n.asInt(0);
    }
}
