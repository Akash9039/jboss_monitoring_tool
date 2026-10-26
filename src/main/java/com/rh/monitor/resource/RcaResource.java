package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rh.monitor.client.AnthropicClient;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jboss.logging.Logger;

import java.util.stream.Collectors;

import com.rh.monitor.util.TimeUtils;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class RcaResource {

    private static final Logger LOG = Logger.getLogger(RcaResource.class);

    @Inject AnthropicClient ai;
    private final ObjectMapper mapper = new ObjectMapper();

    // ── Thread dump patterns ────────────────────────────────────────────
    private static final Pattern THREAD_HEADER_RE = Pattern.compile("^\"(.+?)\"\\s+");
    private static final Pattern TID_RE = Pattern.compile("tid=(0x[\\da-fA-F]+)");
    // Anchors use ^\s* (not ^\s+): after a log prefix is stripped, a frame/state/lock line
    // may have lost its original leading indentation.
    private static final Pattern STATE_RE = Pattern.compile("^\\s*java\\.lang\\.Thread\\.State:\\s+(\\S+)");
    private static final Pattern FRAME_RE = Pattern.compile("^\\s*at\\s+(.+)");
    private static final Pattern LOCK_WAITING_RE = Pattern.compile(
            "^\\s*-\\s+(?:waiting on|waiting to lock|parking to wait for)\\s+<([^>]+)>\\s*\\(a\\s+([^)]+)\\)?");
    private static final Pattern LOCK_WAITING_SIMPLE_RE = Pattern.compile(
            "^\\s*-\\s+(?:waiting on|waiting to lock|parking to wait for)\\s+<([^>]+)>");
    private static final Pattern LOCK_OWNER_RE = Pattern.compile("owned by \"([^\"]+)\"");
    private static final Pattern LOCK_LOCKED_RE = Pattern.compile("^\\s*-\\s+locked\\s+<([^>]+)>");
    private static final Pattern DEADLOCK_RE = Pattern.compile("Found (\\d+) deadlock", Pattern.CASE_INSENSITIVE);
    // Marks the start of jstack's deadlock summary block, which RE-LISTS the deadlocked
    // threads' stacks (with their "- locked"/"- waiting to lock" lines) under "Name":
    // labels that don't match THREAD_HEADER_RE. Without this guard those re-listed lock
    // lines get misattributed to the last real thread, corrupting lock ownership.
    private static final Pattern DEADLOCK_SECTION_RE = Pattern.compile(
            "Java-level deadlock|Java stack information for the threads listed above",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern STATE_INLINE_RE = Pattern.compile(
            "\\b(runnable|sleeping|waiting\\s+on\\s+condition|in\\s+Object\\.wait|blocked\\s+on\\s+monitor\\s+entry|waiting\\s+for\\s+monitor\\s+entry)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern CPU_RE = Pattern.compile("cpu=(\\d+(?:\\.\\d+)?)ms");
    private static final Pattern ELAPSED_RE = Pattern.compile("elapsed=(\\d+(?:\\.\\d+)?)s");
    // Strips a leading server/console log prefix so dumps captured into server.log / console
    // (e.g. "18:08:23,123 INFO  [stdout] (thread) \"name\" #12 ...") parse like a plain jstack dump.
    // Matches: [yyyy-MM-dd] HH:mm:ss,SSS LEVEL [category] (logging-thread)  — category & thread optional.
    private static final Pattern LOG_LINE_PREFIX_RE = Pattern.compile(
            "^\\s*(?:\\d{4}-\\d{2}-\\d{2}[ T])?\\d{2}:\\d{2}:\\d{2}[,.]\\d{3}\\s+\\w+\\s+" +
                    "(?:\\[[^\\]]+\\]\\s+)?(?:\\([^)]*\\)\\s+)?");

    // ── Access log parsing (format-agnostic) ────────────────────────────
    // Access-log patterns vary widely (field order, extra columns, date format, response time
    // in ms vs seconds, combined referer/agent, thread/session). Rather than one rigid regex,
    // we anchor on the one thing nearly every access log shares: a quoted request line followed
    // by a 3-digit status code. Everything before that anchor is the prefix (host + timestamp),
    // everything after is the tail (bytes / response time / thread), classified by shape.
    //
    // ANCHOR: the FIRST quoted string immediately followed by a 3-digit status. Anchoring on the
    // "...\" <status>" pair avoids mistaking a quoted referer/user-agent for the request line.
    private static final Pattern ACCESS_REQ_STATUS_RE = Pattern.compile(
            "\"([^\"]*)\"\\s+(\\d{3})(?=\\s|$)");
    // Bracketed timestamp, e.g. [17/Aug/2026:10:00:00 +0000] — used by common/combined.
    private static final Pattern ACCESS_BRACKET_TIME_RE = Pattern.compile("\\[([^\\]]+)\\]");
    // ISO-ish timestamp anywhere in the prefix (fallback when no bracketed time is present).
    private static final Pattern ACCESS_ISO_TIME_RE = Pattern.compile(
            "(\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(?:[.,]\\d{1,9})?(?:\\s?[+-]\\d{2}:?\\d{2}|Z)?)");
    // A leading IPv4/IPv6/hostname token (host is the first field in almost every layout).
    private static final Pattern ACCESS_HOST_RE = Pattern.compile("^(\\S+)");
    // A response-time token carrying an explicit unit is unambiguous: 12ms, 0.5s, 1500us, 900µs, 30ns.
    private static final Pattern ACCESS_TIME_UNIT_RE = Pattern.compile(
            "^(\\d+(?:\\.\\d+)?)(ms|s|us|µs|ns)$", Pattern.CASE_INSENSITIVE);

    // Some custom formats emit labeled key:value fields (e.g. Thread: "default task-1",
    // TimeTaken: 0.023, SessionID: -). The value may be quoted or a bare token; a quoted
    // value can contain spaces (the worker thread name usually does), an unquoted one runs
    // up to the next "Key:" label or end of line. These are extracted BEFORE quote-stripping,
    // because the thread name would otherwise be discarded along with the referer/user-agent.
    private static final Pattern ACCESS_LABEL_THREAD_RE = Pattern.compile(
            "(?i)\\b(?:worker[-_ ]?)?thread(?:[-_ ]?name)?\\s*:\\s*(?:\"([^\"]*)\"|([^\",]+?))(?=\\s+[\\w-]+\\s*:|\\s*$)");
    private static final Pattern ACCESS_LABEL_TIME_RE = Pattern.compile(
            "(?i)\\b(?:time[-_ ]?taken|response[-_ ]?time|resp[-_ ]?time|duration|elapsed|latency)\\s*:\\s*(?:\"([^\"]*)\"|(\\S+))");
    private static final Pattern ACCESS_LABEL_SESSION_RE = Pattern.compile(
            "(?i)\\b(?:session[-_ ]?id|jsessionid|session)\\s*:\\s*(?:\"([^\"]*)\"|(\\S+))");
    // Removes ANY "Key: value" (quoted or bare) segment so labeled fields don't pollute the
    // positional integer/word classification of the tail.
    private static final Pattern ACCESS_LABEL_PAIR_RE = Pattern.compile(
            "\\b[\\w-]+\\s*:\\s*(?:\"[^\"]*\"|\\S+)");

    // Multiple date formats are tried in order; the first that parses wins.
    private static final DateTimeFormatter[] ACCESS_LOG_DATES = new DateTimeFormatter[] {
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH),   // Apache common
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH),     // common, no zone
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss.SSS Z", Locale.ENGLISH),
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,                                    // 2026-08-26T17:00:00+00:00
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,                                     // 2026-08-26T17:00:00
    };

    // ── Server log pattern ──────────────────────────────────────────────
    private static final Pattern SERVER_LOG_RE = Pattern.compile(
            "^((?:\\d{4}-\\d{2}-\\d{2} )?\\d{2}:\\d{2}:\\d{2},\\d{3})\\s+(\\w+)\\s+\\[([^\\]]+)\\]\\s+\\(([^)]+)\\)\\s+(.*)");
    private static final Pattern EXCEPTION_RE = Pattern.compile(
            "^(\\S+(?:Exception|Error|Throwable))(?::\\s*(.*))?$");

    // ── GC log patterns ─────────────────────────────────────────────────
    // JDK 11+ unified: [timestamp][uptime][level][gc] GC(N) Pause ... NNNms
    private static final Pattern GC_UNIFIED_RE = Pattern.compile(
            "\\[([^\\]]+)\\].*GC\\((\\d+)\\)\\s+(Pause\\s+\\S+(?:\\s+\\([^)]*\\))*).*?(\\d+(?:\\.\\d+)?)ms");
    // JDK 11+ unified with memory: 256M->128M(512M)
    private static final Pattern GC_MEM_RE = Pattern.compile("(\\d+)M->(\\d+)M\\((\\d+)M\\)");
    // JDK 8: timestamp: uptime: [GC (cause) ... , N.NNN secs]
    private static final Pattern GC_JDK8_RE = Pattern.compile(
            "^(\\S+):\\s+[\\d.]+:\\s+\\[((?:Full )?GC)\\s+\\(([^)]+)\\).*?,\\s+(\\d+\\.\\d+)\\s+secs\\]");

    // ── JDBC spy log pattern ───────────────────────────────────────────
    private static final Pattern RCA_SPY_RE = Pattern.compile(
            "^((?:\\d{4}-\\d{2}-\\d{2} )?\\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\w+\\s+\\[jboss\\.jdbc\\.spy\\]\\s+" +
                    "\\(([^)]+)\\)\\s+(java:jboss/datasources/\\S+)\\s+\\[(\\w+)\\]\\s+(.*)");

    // ── Arjuna transaction log patterns ─────────────────────────────────
    private static final String RCA_TS_RE = "((?:\\d{4}-\\d{2}-\\d{2} )?\\d{2}:\\d{2}:\\d{2},\\d{3})";
    private static final Pattern TX_BEGIN_RE = Pattern.compile(
            "^" + RCA_TS_RE + "\\s+\\w+\\s+\\[[\\w.]+\\]\\s+\\(([^)]+)\\)\\s+BasicAction::Begin\\(\\) for action-id\\s+(\\S+)");
    private static final Pattern TX_END_RE = Pattern.compile(
            "^" + RCA_TS_RE + "\\s+\\w+\\s+\\[[\\w.]+\\]\\s+\\(([^)]+)\\)\\s+BasicAction::End\\(\\) for action-id\\s+(\\S+)");
    private static final Pattern TX_RESULT_RE = Pattern.compile(
            "^" + RCA_TS_RE + "\\s+\\w+\\s+\\[[\\w.]+\\]\\s+\\(([^)]+)\\)\\s+BasicAction::End\\(\\) result for action-id \\(([^)]+)\\) is \\(([^)]+)\\)");
    private static final Pattern TX_ABORT_RE = Pattern.compile(
            "^" + RCA_TS_RE + "\\s+\\w+\\s+\\[[\\w.]+\\]\\s+\\(([^)]+)\\)\\s+BasicAction::Abort\\(\\) for action-id\\s+(\\S+)");
    private static final Pattern TX_STATUS_RE = Pattern.compile(
            "TransactionReaper::remove \\( BasicAction:\\s+(\\S+)\\s+status:\\s+(\\S+)");

    // ═══════════════════════════════════════════════════════════════════
    // Main endpoint
    // ═══════════════════════════════════════════════════════════════════

    @POST
    @Path("/rca/analyze")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> analyze(Map<String, Object> input) {
        LOG.info("RCA analysis requested");
        if (input == null) input = Map.of();
        String threadDumpText = (String) input.get("thread_dump");
        String serverLogText = (String) input.get("server_log");
        String accessLogText = (String) input.get("access_log");
        String gcLogText = (String) input.get("gc_log");
        String jdbcSpyText = (String) input.get("jdbc_spy_log");
        String txLogText = (String) input.get("tx_log");

        List<String> filesAnalyzed = new ArrayList<>();

        // A dump file may contain several snapshots (a jstack loop). Split them and
        // analyze one coherent snapshot (default: the latest) instead of merging all.
        List<Map<String, Object>> dumpSnapshots = splitThreadDumps(threadDumpText);
        int selectedDumpIndex = pickDumpIndex(input.get("dump_index"), dumpSnapshots.size());
        if (!dumpSnapshots.isEmpty()) {
            threadDumpText = (String) dumpSnapshots.get(selectedDumpIndex).get("text");
        }

        List<Map<String, Object>> threads = List.of();
        if (threadDumpText != null && !threadDumpText.isBlank()) {
            threads = parseThreadDump(threadDumpText);
            filesAnalyzed.add("thread_dump");
        }

        List<Map<String, Object>> requests = List.of();
        if (accessLogText != null && !accessLogText.isBlank()) {
            requests = parseAccessLog(accessLogText);
            filesAnalyzed.add("access_log");
        }

        List<Map<String, Object>> logEntries = List.of();
        if (serverLogText != null && !serverLogText.isBlank()) {
            logEntries = parseServerLog(serverLogText);
            filesAnalyzed.add("server_log");
        }

        List<Map<String, Object>> gcEvents = List.of();
        if (gcLogText != null && !gcLogText.isBlank()) {
            gcEvents = parseGcLog(gcLogText);
            filesAnalyzed.add("gc_log");
        }

        List<Map<String, Object>> spyQueries = List.of();
        if (jdbcSpyText != null && !jdbcSpyText.isBlank()) {
            spyQueries = parseJdbcSpyLog(jdbcSpyText);
            filesAnalyzed.add("jdbc_spy_log");
        }

        List<Map<String, Object>> txEntries = List.of();
        if (txLogText != null && !txLogText.isBlank()) {
            txEntries = parseTxLog(txLogText);
            filesAnalyzed.add("tx_log");
        }

        if (filesAnalyzed.isEmpty()) {
            return Map.of("status", "error",
                    "message", "No data provided. Upload at least one file.");
        }

        LOG.infof("RCA analyzing %d files: %s", filesAnalyzed.size(), filesAnalyzed);

        // ── Analyze each source ─────────────────────────────────────
        Map<String, Object> threadAnalysis = analyzeThreads(threads);
        List<Map<String, Object>> slowApis = analyzeSlowApis(requests);
        List<Map<String, Object>> errorCorrelation = correlateErrors(logEntries, requests, threads);
        Map<String, Object> gcAnalysis = analyzeGc(gcEvents);
        Map<String, Object> jdbcSpyAnalysis = analyzeJdbcSpy(spyQueries);
        Map<String, Object> txAnalysis = analyzeTx(txEntries, spyQueries);
        List<Map<String, Object>> timeline = buildTimeline(requests, logEntries, gcEvents);

        // ── Correlate across sources ────────────────────────────────
        enrichWithThreadData(slowApis, threads, requests, parseDumpTimeMs(threadDumpText));
        enrichWithGcImpact(slowApis, gcEvents, requests);
        enrichWithServerLogErrors(slowApis, logEntries, requests);

        // ── Generate findings ───────────────────────────────────────
        String threadDumpTime = parseThreadDumpTime(threadDumpText);
        // Provenance: make it explicit this analysis reflects the uploaded dump snapshot, not live threads.
        threadAnalysis.put("source", "uploaded_thread_dump");
        threadAnalysis.put("dump_provided", threadDumpText != null && !threadDumpText.isBlank());
        threadAnalysis.put("dump_time", threadDumpTime);
        List<Map<String, Object>> findings = generateFindings(
                threads, requests, logEntries, gcEvents,
                threadAnalysis, slowApis, gcAnalysis,
                jdbcSpyAnalysis, txAnalysis, threadDumpTime);

        // Cross-file correlation: stitch each slow/failed request across all logs by thread + time.
        Map<String, Map<String, Object>> threadStacks = buildThreadStacks(threads);
        findings.addAll(correlateRequestTraces(
                requests, spyQueries, txEntries, logEntries, gcEvents, threadStacks, threadDumpTime));

        findings.sort((a, b) -> {
            int sa = severityOrder((String) a.get("severity"));
            int sb = severityOrder((String) b.get("severity"));
            return sa - sb;
        });

        // Collapse findings that describe the same issue (same category + endpoint/title)
        // into a single entry with an occurrence count.
        findings = dedupeFindings(findings);

        // ── Summary ─────────────────────────────────────────────────
        long slowCount = requests.stream()
                .filter(r -> ((Number) r.getOrDefault("response_time_ms", 0)).longValue() > 5000)
                .count();
        long errorCount = requests.stream()
                .filter(r -> ((Number) r.getOrDefault("status", 200)).intValue() >= 400)
                .count();
        int blockedThreads = threads.isEmpty() ? 0 :
                (int) threads.stream().filter(t -> "BLOCKED".equals(t.get("thread_state"))).count();
        int deadlocked = 0;
        if (threadDumpText != null) {
            Matcher dlm = DEADLOCK_RE.matcher(threadDumpText);
            if (dlm.find()) deadlocked = Integer.parseInt(dlm.group(1));
        }
        long serverErrors = logEntries.stream()
                .filter(e -> "ERROR".equals(e.get("level")) || "FATAL".equals(e.get("level"))).count();
        long serverWarnings = logEntries.stream()
                .filter(e -> "WARN".equals(e.get("level"))).count();
        long longGcPauses = gcEvents.stream()
                .filter(g -> ((Number) g.getOrDefault("pause_ms", 0)).doubleValue() > 200).count();
        double totalGcPauseMs = gcEvents.stream()
                .mapToDouble(g -> ((Number) g.getOrDefault("pause_ms", 0)).doubleValue()).sum();

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("files_analyzed", filesAnalyzed);
        summary.put("total_requests", requests.size());
        summary.put("slow_requests", slowCount);
        // Whether the access log carried per-request response times (%D). Without it the Slow
        // APIs tab has no timing to rank, so the UI shows a hint pointing at the log pattern.
        boolean accessLogAnalyzed = filesAnalyzed.contains("access_log");
        boolean accessLogHasResponseTime = requests.stream()
                .anyMatch(r -> Boolean.TRUE.equals(r.get("has_response_time")));
        summary.put("access_log_analyzed", accessLogAnalyzed);
        summary.put("access_log_has_response_time", accessLogHasResponseTime);
        summary.put("error_requests", errorCount);
        summary.put("total_threads", threads.size());
        summary.put("blocked_threads", blockedThreads);
        summary.put("deadlocked_threads", deadlocked);
        summary.put("gc_pauses", gcEvents.size());
        summary.put("long_gc_pauses", longGcPauses);
        summary.put("total_gc_pause_ms", Math.round(totalGcPauseMs * 100.0) / 100.0);
        summary.put("server_errors", serverErrors);
        summary.put("server_warnings", serverWarnings);
        summary.put("total_spy_queries", spyQueries.size());
        summary.put("total_transactions", txEntries.size());
        summary.put("findings_count", findings.size());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("summary", summary);
        result.put("findings", findings);
        result.put("slow_apis", slowApis);
        result.put("thread_analysis", threadAnalysis);
        result.put("error_correlation", errorCorrelation);
        result.put("jdbc_spy_analysis", jdbcSpyAnalysis);
        result.put("tx_analysis", txAnalysis);
        result.put("gc_analysis", gcAnalysis);
        result.put("timeline", timeline.subList(0, Math.min(500, timeline.size())));

        result.put("thread_stacks", threadStacks);

        // Expose the snapshots found in the uploaded file so the UI can offer a picker.
        result.put("dumps", dumpMetadata(dumpSnapshots));
        result.put("selected_dump_index", selectedDumpIndex);

        return result;
    }

    // Splits a thread-dump file into individual snapshots (a jstack loop concatenates many).
    // Each entry: { text, time, thread_count }. Returns empty when no dump text was supplied.
    private List<Map<String, Object>> splitThreadDumps(String text) {
        List<Map<String, Object>> dumps = new ArrayList<>();
        if (text == null || text.isBlank()) return dumps;

        StringBuilder cur = null;
        String pendingTime = null;   // most recent timestamp seen before a dump boundary
        String curTime = null;
        for (String rawLine : text.split("\n", -1)) {
            String line = rawLine.endsWith("\r") ? rawLine.substring(0, rawLine.length() - 1) : rawLine;

            Matcher tm = DUMP_TIME_RE.matcher(line);
            if (tm.find()) pendingTime = tm.group(1) + " " + tm.group(2);

            // "Full thread dump ..." marks the start of a snapshot in jstack output.
            if (line.contains("Full thread dump")) {
                if (cur != null) dumps.add(makeDumpEntry(cur.toString(), curTime));
                cur = new StringBuilder();
                curTime = pendingTime;
                if (pendingTime != null) cur.append(pendingTime).append("\n"); // keep time parseable
            }
            if (cur != null) cur.append(line).append("\n");
        }
        if (cur != null) dumps.add(makeDumpEntry(cur.toString(), curTime));

        // No "Full thread dump" marker (some tools omit it) — treat the whole file as one dump.
        if (dumps.isEmpty()) dumps.add(makeDumpEntry(text, parseThreadDumpTime(text)));
        return dumps;
    }

    private Map<String, Object> makeDumpEntry(String text, String time) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("text", text);
        m.put("time", time);
        int count = 0;
        for (String line : text.split("\n")) {
            if (THREAD_HEADER_RE.matcher(LOG_LINE_PREFIX_RE.matcher(line).replaceFirst("")).find()) count++;
        }
        m.put("thread_count", count);
        return m;
    }

    // Clamps a requested dump index; defaults to the latest snapshot.
    private int pickDumpIndex(Object requested, int size) {
        if (size <= 0) return 0;
        int def = size - 1; // latest
        if (requested instanceof Number) {
            int i = ((Number) requested).intValue();
            if (i >= 0 && i < size) return i;
        }
        return def;
    }

    // Strips the bulky "text" from each snapshot, leaving only display metadata.
    private List<Map<String, Object>> dumpMetadata(List<Map<String, Object>> dumps) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (int i = 0; i < dumps.size(); i++) {
            Map<String, Object> d = dumps.get(i);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("index", i);
            m.put("time", d.get("time"));
            m.put("thread_count", d.get("thread_count"));
            out.add(m);
        }
        return out;
    }

    private Map<String, Map<String, Object>> buildThreadStacks(List<Map<String, Object>> threads) {
        if (threads.isEmpty()) return Map.of();
        Map<String, Map<String, Object>> stacks = new LinkedHashMap<>();
        for (Map<String, Object> t : threads) {
            String name = (String) t.get("thread_name");
            if (name == null) continue;
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("thread_name", name);
            entry.put("thread_state", t.getOrDefault("thread_state", "UNKNOWN"));
            entry.put("lock_name", t.get("lock_name"));
            entry.put("lock_owner", t.get("lock_owner"));
            entry.put("daemon", t.getOrDefault("daemon", false));
            @SuppressWarnings("unchecked")
            List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());
            entry.put("stack_trace", stack);
            entry.put("top_frame", stack.isEmpty() ? "" : stack.get(0));
            stacks.put(name, entry);
        }
        return stacks;
    }

    // ═══════════════════════════════════════════════════════════════════
    // Parsers
    // ═══════════════════════════════════════════════════════════════════

    private List<Map<String, Object>> parseThreadDump(String text) {
        List<Map<String, Object>> threads = new ArrayList<>();
        Map<String, Object> current = null;

        for (String rawLine : text.split("\n")) {
            // Drop trailing CR and strip any leading log/console prefix so dumps embedded
            // in server.log / console parse the same as a plain jstack dump.
            String line = rawLine.endsWith("\r") ? rawLine.substring(0, rawLine.length() - 1) : rawLine;
            line = LOG_LINE_PREFIX_RE.matcher(line).replaceFirst("");

            // Once jstack's deadlock summary begins, stop attributing lines to any thread:
            // it re-lists deadlocked threads' stacks under "Name": labels that aren't real
            // headers, so their lock lines would otherwise pollute the last-seen thread.
            if (DEADLOCK_SECTION_RE.matcher(line).find()) {
                if (current != null) { threads.add(current); current = null; }
                continue;
            }

            Matcher hm = THREAD_HEADER_RE.matcher(line);
            if (hm.find()) {
                if (current != null) threads.add(current);
                current = new LinkedHashMap<>();
                current.put("thread_name", hm.group(1));

                Matcher tidM = TID_RE.matcher(line);
                current.put("thread_id", tidM.find() ? tidM.group(1) : String.valueOf(threads.size()));

                String state = "UNKNOWN";
                Matcher stateM = STATE_INLINE_RE.matcher(line);
                if (stateM.find()) state = inlineStateToJava(stateM.group(1));
                current.put("thread_state", state);
                current.put("stack_trace", new ArrayList<String>());
                current.put("lock_name", null);
                current.put("lock_class", null);
                current.put("lock_owner", null);
                current.put("locked_monitors", new ArrayList<String>());
                current.put("daemon", line.contains(" daemon "));

                Matcher cpuM = CPU_RE.matcher(line);
                current.put("cpu_ms", cpuM.find() ? Double.parseDouble(cpuM.group(1)) : -1.0);
                Matcher elapsedM = ELAPSED_RE.matcher(line);
                current.put("elapsed_s", elapsedM.find() ? Double.parseDouble(elapsedM.group(1)) : -1.0);
                continue;
            }

            if (current == null) continue;

            Matcher sm = STATE_RE.matcher(line);
            if (sm.find()) {
                current.put("thread_state", sm.group(1));
                continue;
            }

            Matcher fm = FRAME_RE.matcher(line);
            if (fm.find()) {
                @SuppressWarnings("unchecked")
                List<String> stack = (List<String>) current.get("stack_trace");
                stack.add(fm.group(1).strip());
                continue;
            }

            Matcher lm = LOCK_WAITING_RE.matcher(line);
            if (lm.find()) {
                current.put("lock_name", lm.group(1));
                current.put("lock_class", lm.group(2));
                Matcher ownerM = LOCK_OWNER_RE.matcher(line);
                if (ownerM.find()) current.put("lock_owner", ownerM.group(1));
                continue;
            }
            Matcher lms = LOCK_WAITING_SIMPLE_RE.matcher(line);
            if (lms.find() && current.get("lock_name") == null) {
                current.put("lock_name", lms.group(1));
                Matcher ownerM = LOCK_OWNER_RE.matcher(line);
                if (ownerM.find()) current.put("lock_owner", ownerM.group(1));
                continue;
            }

            Matcher llm = LOCK_LOCKED_RE.matcher(line);
            if (llm.find()) {
                @SuppressWarnings("unchecked")
                List<String> monitors = (List<String>) current.get("locked_monitors");
                monitors.add(llm.group(1));
            }
        }
        if (current != null) threads.add(current);
        return threads;
    }

    // Format-agnostic access-log parser. Two passes:
    //   Pass 1 — per line, anchor on the quoted request + status, pull host/timestamp from the
    //            prefix and classify the trailing tokens (bytes / response time / thread). JSON
    //            access-log lines are parsed directly. Explicit response times (decimal seconds,
    //            or a unit like 12ms / 0.5s) are captured immediately since they're unambiguous.
    //   Pass 2 — for logs where response time and bytes are both bare integers, decide which
    //            column is which across the whole file (the bytes column is the one that ever
    //            shows '-', which %D/%T never do; otherwise default to the %D %b order), then
    //            build the final entries.
    private List<Map<String, Object>> parseAccessLog(String text) {
        List<Map<String, Object>> raw = new ArrayList<>();
        for (String line : text.split("\n")) {
            line = line.strip();
            if (line.isEmpty()) continue;

            // JSON access-log line (Undertow/EAP JSON formatter or similar).
            if (line.startsWith("{") && line.endsWith("}")) {
                Map<String, Object> j = parseJsonAccessLine(line);
                if (j != null) { j.put("_final", Boolean.TRUE); raw.add(j); }
                continue;
            }

            // Anchor: the first quoted string immediately followed by a 3-digit status code.
            Matcher anchor = ACCESS_REQ_STATUS_RE.matcher(line);
            if (!anchor.find()) continue;
            String requestLine = anchor.group(1).strip();
            int status = safeParseInt(anchor.group(2), 0);
            String prefix = line.substring(0, anchor.start()).strip();
            String tail = line.substring(anchor.end()).strip();

            // Request line = "METHOD URI PROTO" (may be "-" for a bad request).
            String method = "-";
            String uri = "/";
            if (!requestLine.isEmpty() && !"-".equals(requestLine)) {
                String[] rp = requestLine.split("\\s+");
                method = rp[0];
                if (rp.length > 1) uri = rp[1];
            }

            // Host = first token of the prefix; timestamp = bracketed, else an ISO time anywhere.
            String remote = "-";
            Matcher hm = ACCESS_HOST_RE.matcher(prefix);
            if (hm.find()) remote = hm.group(1);
            String tsRaw = null;
            Matcher bt = ACCESS_BRACKET_TIME_RE.matcher(prefix);
            if (bt.find()) tsRaw = bt.group(1).strip();
            if (tsRaw == null) {
                Matcher it = ACCESS_ISO_TIME_RE.matcher(prefix);
                if (it.find()) tsRaw = it.group(1).strip();
            }

            // First pull out any labeled key:value fields (Thread:/TimeTaken:/SessionID: ...)
            // while the original quotes are intact — a quoted thread name would otherwise be
            // stripped away with the referer/user-agent below.
            String labelThread = null, labelSession = null;
            Long labelMs = null;
            Matcher thm = ACCESS_LABEL_THREAD_RE.matcher(tail);
            if (thm.find()) labelThread = (thm.group(1) != null ? thm.group(1) : thm.group(2)).strip();
            Matcher tim = ACCESS_LABEL_TIME_RE.matcher(tail);
            if (tim.find()) labelMs = parseTimeValueToMs(tim.group(1) != null ? tim.group(1) : tim.group(2));
            Matcher ssm = ACCESS_LABEL_SESSION_RE.matcher(tail);
            if (ssm.find()) labelSession = (ssm.group(1) != null ? ssm.group(1) : ssm.group(2)).strip();

            // Classify the remaining tail: drop quoted referer/agent, then drop labeled pairs,
            // leaving positional integer/dash slots, an explicit response time (decimal seconds
            // or unit-suffixed), and leftover words (thread/session for unlabeled formats).
            String tailNoQuotes = tail.replaceAll("\"[^\"]*\"", " ");
            tailNoQuotes = ACCESS_LABEL_PAIR_RE.matcher(tailNoQuotes).replaceAll(" ").strip();
            List<String> intSlots = new ArrayList<>();
            Long explicitMs = null;
            List<String> words = new ArrayList<>();
            if (!tailNoQuotes.isEmpty()) {
                for (String tok : tailNoQuotes.split("\\s+")) {
                    if (tok.isEmpty()) continue;
                    Matcher um = ACCESS_TIME_UNIT_RE.matcher(tok);
                    if (um.matches()) {
                        explicitMs = toMillis(Double.parseDouble(um.group(1)), um.group(2).toLowerCase());
                    } else if (tok.matches("\\d+\\.\\d+")) {   // decimal, no unit → seconds (%T)
                        explicitMs = Math.round(Double.parseDouble(tok) * 1000.0);
                    } else if (tok.matches("\\d+") || "-".equals(tok)) {
                        intSlots.add(tok);
                    } else {
                        words.add(tok);
                    }
                }
            }

            Map<String, Object> r = new LinkedHashMap<>();
            r.put("remote", remote);
            r.put("ts_raw", tsRaw);
            r.put("method", method);
            r.put("uri", uri);
            r.put("status", status);
            r.put("int_slots", intSlots);
            r.put("explicit_ms", explicitMs);
            r.put("words", words);
            r.put("label_thread", labelThread);
            r.put("label_session", labelSession);
            r.put("label_ms", labelMs);
            raw.add(r);
        }

        // Decide, across the whole file, which bare-integer column is response time vs bytes.
        int[] cols = inferTimeBytesColumns(raw);
        int timeCol = cols[0], bytesCol = cols[1];

        List<Map<String, Object>> entries = new ArrayList<>();
        for (Map<String, Object> r : raw) {
            if (Boolean.TRUE.equals(r.get("_final"))) {   // JSON entry already complete
                r.remove("_final");
                entries.add(r);
                continue;
            }
            @SuppressWarnings("unchecked")
            List<String> intSlots = (List<String>) r.get("int_slots");
            Long explicitMs = (Long) r.get("explicit_ms");

            Long labelMs = (Long) r.get("label_ms");

            long responseTime = 0, bytes = 0;
            boolean hasResponseTime = false;
            if (labelMs != null) {                 // labeled TimeTaken:/ResponseTime: wins
                responseTime = labelMs;
                hasResponseTime = true;
                if (!intSlots.isEmpty()) bytes = slotVal(intSlots.get(0));
            } else if (explicitMs != null) {
                responseTime = explicitMs;
                hasResponseTime = true;
                if (!intSlots.isEmpty()) bytes = slotVal(intSlots.get(0));  // remaining int is bytes
            } else {
                if (timeCol >= 0 && timeCol < intSlots.size()) {
                    responseTime = slotVal(intSlots.get(timeCol));
                    hasResponseTime = true;
                }
                if (bytesCol >= 0 && bytesCol < intSlots.size()) {
                    bytes = slotVal(intSlots.get(bytesCol));
                } else if (bytesCol < 0 && !intSlots.isEmpty()) {
                    bytes = slotVal(intSlots.get(0));
                }
            }

            String threadName = (String) r.get("label_thread");
            String sessionId = (String) r.get("label_session");
            if (threadName == null) {   // no labeled thread → recover from leftover words
                @SuppressWarnings("unchecked")
                List<String> words = (List<String>) r.get("words");
                if (words != null && !words.isEmpty()) {
                    String joined = String.join(" ", words);
                    if (joined.contains("|")) {   // "thread|session" combined token
                        String[] parts = joined.split("\\|", -1);
                        threadName = parts[0].strip();
                        if (sessionId == null && parts.length > 1) sessionId = parts[1].strip();
                    } else {
                        threadName = joined.strip();
                    }
                }
            }
            if ("-".equals(sessionId)) sessionId = null;

            String tsRaw = (String) r.get("ts_raw");
            Object[] tsp = parseAccessTimestamp(tsRaw);
            String ts = tsp[0] != null ? (String) tsp[0] : tsRaw;
            long tsEpochMs = (Long) tsp[1];
            long tsLocalMs = (Long) tsp[2];

            String uri = (String) r.get("uri");
            String[] pathParts = uri.split("/");
            String deployment = pathParts.length > 1 ? pathParts[1] : "/";

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("remote", r.get("remote"));
            entry.put("timestamp", ts);
            entry.put("timestamp_ms", tsEpochMs);
            entry.put("timestamp_local_ms", tsLocalMs);
            entry.put("method", r.get("method"));
            entry.put("uri", uri);
            entry.put("deployment", deployment);
            entry.put("status", r.get("status"));
            entry.put("response_time_ms", responseTime);
            entry.put("has_response_time", hasResponseTime);
            entry.put("bytes", bytes);
            if (threadName != null && !threadName.isEmpty() && !"-".equals(threadName)) {
                entry.put("thread", threadName);
            }
            if (sessionId != null && !sessionId.isEmpty() && !"-".equals(sessionId)) {
                entry.put("session_id", sessionId);
            }
            entries.add(entry);
        }
        return entries;
    }

    private static long slotVal(String s) {
        return "-".equals(s) ? 0 : safeParseLong(s, 0);
    }

    // Parses a labeled response-time value (TimeTaken:/ResponseTime:/...). Accepts a unit
    // suffix (4200ms, 0.5s), a decimal with no unit (seconds, e.g. 0.023 → 23ms), or a bare
    // integer (milliseconds). Returns null for "-" / blank / unparseable.
    private static Long parseTimeValueToMs(String v) {
        if (v == null) return null;
        v = v.strip();
        if (v.isEmpty() || "-".equals(v)) return null;
        Matcher um = ACCESS_TIME_UNIT_RE.matcher(v);
        if (um.matches()) return toMillis(Double.parseDouble(um.group(1)), um.group(2).toLowerCase());
        try {
            if (v.indexOf('.') >= 0) return Math.round(Double.parseDouble(v) * 1000.0);  // seconds
            return Long.parseLong(v);                                                    // milliseconds
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static long toMillis(double v, String unit) {
        switch (unit) {
            case "s":            return Math.round(v * 1000.0);
            case "ms":           return Math.round(v);
            case "us": case "µs": return Math.round(v / 1000.0);
            case "ns":           return Math.round(v / 1_000_000.0);
            default:             return Math.round(v);
        }
    }

    // Returns {timeColumnIndex, bytesColumnIndex} into each record's bare-integer "int_slots".
    // timeColumnIndex == -1 means the log has no positional response time (e.g. common/combined).
    private int[] inferTimeBytesColumns(List<Map<String, Object>> raw) {
        int maxCols = 0;
        for (Map<String, Object> r : raw) {
            if (Boolean.TRUE.equals(r.get("_final")) || r.get("explicit_ms") != null) continue;
            @SuppressWarnings("unchecked")
            List<String> slots = (List<String>) r.get("int_slots");
            if (slots != null) maxCols = Math.max(maxCols, slots.size());
        }
        if (maxCols <= 1) return new int[]{ -1, 0 };  // single int column is bytes; no response time

        // Two+ integer columns: bytes is the one that ever shows '-' (%D/%T never emit '-').
        boolean col0Dash = false, col1Dash = false;
        for (Map<String, Object> r : raw) {
            if (Boolean.TRUE.equals(r.get("_final")) || r.get("explicit_ms") != null) continue;
            @SuppressWarnings("unchecked")
            List<String> slots = (List<String>) r.get("int_slots");
            if (slots == null) continue;
            if (slots.size() > 0 && "-".equals(slots.get(0))) col0Dash = true;
            if (slots.size() > 1 && "-".equals(slots.get(1))) col1Dash = true;
        }
        if (col0Dash && !col1Dash) return new int[]{ 1, 0 };  // col0 = bytes, col1 = response time
        if (col1Dash && !col0Dash) return new int[]{ 0, 1 };  // col0 = response time, col1 = bytes
        return new int[]{ 0, 1 };                             // default to the %D %b column order
    }

    // Parses an access-log timestamp against several known formats.
    // Returns {displayString|null, epochMs (Long), localMs (Long)}.
    private Object[] parseAccessTimestamp(String tsRaw) {
        if (tsRaw == null || tsRaw.isBlank()) return new Object[]{ null, 0L, 0L };
        for (DateTimeFormatter fmt : ACCESS_LOG_DATES) {
            try {
                ZonedDateTime zdt = ZonedDateTime.parse(tsRaw, fmt);
                return new Object[]{ zdt.toInstant().toString(),
                        zdt.toInstant().toEpochMilli(),
                        zdt.toLocalDateTime().toInstant(java.time.ZoneOffset.UTC).toEpochMilli() };
            } catch (Exception ignore) { /* try next */ }
            try {
                LocalDateTime ldt = LocalDateTime.parse(tsRaw, fmt);
                long ms = ldt.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
                return new Object[]{ tsRaw, ms, ms };  // no zone → wall-clock used for both
            } catch (Exception ignore) { /* try next */ }
        }
        return new Object[]{ tsRaw, 0L, 0L };
    }

    // Best-effort parse of a JSON access-log line. Field names are matched case-insensitively and
    // ignoring '_' / '-', so remoteHost, remote_host and REMOTEHOST all match.
    private Map<String, Object> parseJsonAccessLine(String line) {
        try {
            com.fasterxml.jackson.databind.JsonNode n = mapper.readTree(line);
            if (n == null || !n.isObject()) return null;

            String method = jsonFind(n, "requestmethod", "method", "verb");
            String uri = jsonFind(n, "requestpath", "requesturi", "path", "uri", "url");
            String reqLine = jsonFind(n, "requestline", "request");
            if ((method == null || uri == null) && reqLine != null) {
                String[] rp = reqLine.strip().split("\\s+");
                if (method == null) method = rp.length > 0 ? rp[0] : "-";
                if (uri == null) uri = rp.length > 1 ? rp[1] : "/";
            }
            if (uri == null) return null;   // not an access record
            if (method == null) method = "-";

            int status = safeParseInt(jsonFind(n, "responsecode", "statuscode", "status", "responsestatus"), 0);
            String tsRaw = jsonFind(n, "datetime", "timestamp", "date", "time", "@timestamp", "eventtime");
            String rtStr = jsonFind(n, "responsetime", "responsetimemillis", "duration", "elapsed",
                    "timetaken", "latency");
            String bytesStr = jsonFind(n, "bytessent", "bytes", "responsesize", "size", "contentlength");
            String remote = jsonFind(n, "remotehost", "remoteaddr", "remoteip", "clientip", "client", "host");
            String thread = jsonFind(n, "thread", "threadname", "workerthread");

            long responseTime = 0;
            boolean hasRt = false;
            if (rtStr != null && !rtStr.isBlank() && !"-".equals(rtStr)) {
                try { responseTime = Math.round(Double.parseDouble(rtStr)); hasRt = true; } catch (Exception ignore) {}
            }
            long bytes = 0;
            if (bytesStr != null && !bytesStr.isBlank() && !"-".equals(bytesStr)) {
                try { bytes = Math.round(Double.parseDouble(bytesStr)); } catch (Exception ignore) {}
            }

            Object[] tsp = parseAccessTimestamp(tsRaw);
            String ts = tsp[0] != null ? (String) tsp[0] : tsRaw;
            long epochMs = (Long) tsp[1], localMs = (Long) tsp[2];

            String[] pathParts = uri.split("/");
            String deployment = pathParts.length > 1 ? pathParts[1] : "/";

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("remote", remote != null ? remote : "-");
            entry.put("timestamp", ts);
            entry.put("timestamp_ms", epochMs);
            entry.put("timestamp_local_ms", localMs);
            entry.put("method", method);
            entry.put("uri", uri);
            entry.put("deployment", deployment);
            entry.put("status", status);
            entry.put("response_time_ms", responseTime);
            entry.put("has_response_time", hasRt);
            entry.put("bytes", bytes);
            if (thread != null && !thread.isBlank() && !"-".equals(thread)) entry.put("thread", thread.strip());
            return entry;
        } catch (Exception e) {
            return null;
        }
    }

    // Case/separator-insensitive lookup of the first matching field in a JSON object.
    private String jsonFind(com.fasterxml.jackson.databind.JsonNode node, String... keys) {
        Set<String> want = new HashSet<>(Arrays.asList(keys));
        Iterator<String> it = node.fieldNames();
        while (it.hasNext()) {
            String fn = it.next();
            String norm = fn.toLowerCase().replaceAll("[_\\-]", "");
            if (want.contains(norm)) {
                com.fasterxml.jackson.databind.JsonNode v = node.get(fn);
                if (v != null && !v.isNull()) {
                    String s = v.asText();
                    if (s != null && !s.isEmpty()) return s;
                }
            }
        }
        return null;
    }

    // Console-redirect categories carry the app's real severity inside the message text.
    private static final Pattern EMBEDDED_LEVEL_RE = Pattern.compile(
            "^(?:[\\w.]+:)?(WARN|WARNING|INFO|DEBUG|TRACE|ERROR|SEVERE|FATAL)\\b",
            Pattern.CASE_INSENSITIVE);

    // For stdout/stderr entries, honor an explicit level prefix in the message (e.g. "log4j:WARN ...")
    // instead of the ERROR level JBoss assigns to all captured console output.
    private String effectiveLevel(String jbossLevel, String logger, String message) {
        if (logger == null) return jbossLevel;
        String lc = logger.toLowerCase();
        if (!lc.contains("stderr") && !lc.contains("stdout")) return jbossLevel;
        Matcher m = EMBEDDED_LEVEL_RE.matcher(message.strip());
        if (!m.find()) return jbossLevel;
        String inner = m.group(1).toUpperCase();
        if ("WARNING".equals(inner)) inner = "WARN";
        if ("SEVERE".equals(inner)) inner = "ERROR";
        return inner;
    }

    private List<Map<String, Object>> parseServerLog(String text) {
        List<Map<String, Object>> entries = new ArrayList<>();
        Map<String, Object> current = null;

        for (String line : text.split("\n")) {
            Matcher m = SERVER_LOG_RE.matcher(line.stripTrailing());
            if (m.matches()) {
                if (current != null) entries.add(current);

                String timestamp = m.group(1);
                String level = m.group(2).toUpperCase();
                String logger = m.group(3);
                String thread = m.group(4);
                String message = m.group(5).strip();

                // JBoss captures the app's stdout/stderr at ERROR/INFO level, but the app's own
                // logging framework prints its real severity in the text (e.g. "log4j:WARN ...").
                // Trust that embedded level so benign console noise isn't counted as a server error.
                level = effectiveLevel(level, logger, message);

                long tsMs = 0;
                try {
                    LocalDateTime ldt = LocalDateTime.parse(timestamp,
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS"));
                    tsMs = ldt.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                } catch (Exception ignored) {}

                current = new LinkedHashMap<>();
                current.put("timestamp", timestamp);
                current.put("timestamp_ms", tsMs);
                current.put("level", level);
                current.put("logger", logger);
                current.put("thread", thread);
                current.put("message", message);
                current.put("stacktrace", new ArrayList<String>());
                current.put("exception_class", null);
            } else if (current != null) {
                String trimmed = line.stripTrailing();
                if (!trimmed.isEmpty()) {
                    @SuppressWarnings("unchecked")
                    List<String> st = (List<String>) current.get("stacktrace");
                    st.add(trimmed);
                    if (current.get("exception_class") == null) {
                        Matcher ex = EXCEPTION_RE.matcher(trimmed.strip());
                        if (ex.matches()) current.put("exception_class", ex.group(1));
                    }
                }
            }
        }
        if (current != null) entries.add(current);
        return entries;
    }

    private List<Map<String, Object>> parseGcLog(String text) {
        List<Map<String, Object>> events = new ArrayList<>();
        for (String line : text.split("\n")) {
            line = line.strip();
            if (line.isEmpty()) continue;

            // JDK 11+ unified format
            Matcher um = GC_UNIFIED_RE.matcher(line);
            if (um.find()) {
                Map<String, Object> event = new LinkedHashMap<>();
                String tsRaw = um.group(1);
                event.put("timestamp", tsRaw);
                long tsMs = 0;
                try {
                    tsMs = ZonedDateTime.parse(tsRaw, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                            .toInstant().toEpochMilli();
                } catch (Exception e) {
                    try {
                        tsMs = ZonedDateTime.parse(tsRaw, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
                                .toInstant().toEpochMilli();
                    } catch (Exception ignored) {}
                }
                event.put("timestamp_ms", tsMs);
                event.put("gc_id", safeParseInt(um.group(2), -1));
                event.put("type", um.group(3).strip());
                event.put("pause_ms", Double.parseDouble(um.group(4)));
                event.put("full_gc", line.toLowerCase().contains("pause full"));

                Matcher mem = GC_MEM_RE.matcher(line);
                if (mem.find()) {
                    event.put("heap_before_mb", safeParseInt(mem.group(1), 0));
                    event.put("heap_after_mb", safeParseInt(mem.group(2), 0));
                    event.put("heap_max_mb", safeParseInt(mem.group(3), 0));
                    event.put("heap_reclaimed_mb",
                            safeParseInt(mem.group(1), 0) - safeParseInt(mem.group(2), 0));
                }
                events.add(event);
                continue;
            }

            // JDK 8 format
            Matcher jm = GC_JDK8_RE.matcher(line);
            if (jm.find()) {
                Map<String, Object> event = new LinkedHashMap<>();
                event.put("timestamp", jm.group(1));
                long tsMs = 0;
                try {
                    tsMs = ZonedDateTime.parse(jm.group(1), DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                            .toInstant().toEpochMilli();
                } catch (Exception ignored) {}
                event.put("timestamp_ms", tsMs);
                event.put("gc_id", -1);
                String gcType = jm.group(2);
                event.put("type", gcType + " (" + jm.group(3) + ")");
                double secs = Double.parseDouble(jm.group(4));
                event.put("pause_ms", Math.round(secs * 1000.0 * 100.0) / 100.0);
                event.put("full_gc", gcType.contains("Full"));
                events.add(event);
            }
        }
        return events;
    }

    private List<Map<String, Object>> parseJdbcSpyLog(String text) {
        List<Map<String, Object>> entries = new ArrayList<>();
        for (String line : text.split("\n")) {
            Matcher m = RCA_SPY_RE.matcher(line.strip());
            if (!m.matches()) continue;
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("timestamp", m.group(1));
            entry.put("thread", m.group(2));
            String jndi = m.group(3);
            entry.put("datasource", jndi.replace("java:jboss/datasources/", ""));
            entry.put("jndi", jndi);
            entry.put("object_type", m.group(4));
            entry.put("operation", m.group(5).strip());
            entries.add(entry);
        }

        List<Map<String, Object>> queries = new ArrayList<>();
        Map<String, Map<String, Object>> pending = new LinkedHashMap<>();

        for (Map<String, Object> e : entries) {
            String op = (String) e.get("operation");
            String thread = (String) e.get("thread");
            String ds = (String) e.get("datasource");
            String key = thread + "|" + ds;

            if ("Connection".equals(e.get("object_type")) && op.startsWith("prepareStatement(")) {
                String sql = op.substring("prepareStatement(".length());
                if (sql.endsWith(")")) sql = sql.substring(0, sql.length() - 1);
                String[] trailing = sql.split(",");
                if (trailing.length >= 3) {
                    try {
                        Integer.parseInt(trailing[trailing.length - 1].strip());
                        Integer.parseInt(trailing[trailing.length - 2].strip());
                        sql = String.join(",", Arrays.copyOf(trailing, trailing.length - 2));
                    } catch (NumberFormatException ignored) {}
                }
                Map<String, Object> p = new LinkedHashMap<>();
                p.put("sql", sql.strip());
                p.put("datasource", ds);
                p.put("thread", thread);
                p.put("prepare_time", e.get("timestamp"));
                p.put("params", new ArrayList<String>());
                p.put("execute_time", null);
                p.put("execute_type", "");
                pending.put(key, p);
            } else if ("PreparedStatement".equals(e.get("object_type")) && pending.containsKey(key)) {
                Map<String, Object> p = pending.get(key);
                if ("close()".equals(op) && p.get("execute_time") != null) {
                    Map<String, Object> q = pending.remove(key);
                    double execMs = TimeUtils.spyTimeDiff((String) q.get("execute_time"), (String) e.get("timestamp"));
                    @SuppressWarnings("unchecked")
                    List<String> params = (List<String>) q.get("params");
                    String paramsStr = String.join("; ", params.subList(0, Math.min(10, params.size())));
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
                } else if (op.startsWith("set") && op.contains("(")) {
                    @SuppressWarnings("unchecked")
                    List<String> params = (List<String>) p.get("params");
                    params.add(op);
                } else if ("executeQuery()".equals(op) || "executeUpdate()".equals(op) || "execute()".equals(op)) {
                    p.put("execute_time", e.get("timestamp"));
                    p.put("execute_type", op.replace("()", ""));
                }
            }
        }

        // Mark remaining pending queries with execute_time as inflight
        for (Map<String, Object> q : pending.values()) {
            if (q.get("execute_time") != null) {
                @SuppressWarnings("unchecked")
                List<String> params = (List<String>) q.get("params");
                String paramsStr = String.join("; ", params.subList(0, Math.min(10, params.size())));
                if (params.size() > 10) paramsStr += " ... (+" + (params.size() - 10) + " more)";

                Map<String, Object> query = new LinkedHashMap<>();
                query.put("sql", q.get("sql"));
                query.put("datasource", q.get("datasource"));
                query.put("thread", q.get("thread"));
                query.put("timestamp", q.get("execute_time"));
                query.put("exec_time_ms", -1.0);
                query.put("params", paramsStr);
                query.put("execute_type", q.getOrDefault("execute_type", ""));
                query.put("inflight", true);
                queries.add(query);
            }
        }

        queries.sort((a, b) -> ((String) b.getOrDefault("timestamp", "")).compareTo(
                (String) a.getOrDefault("timestamp", "")));
        return queries;
    }

    private List<Map<String, Object>> parseTxLog(String text) {
        Map<String, Map<String, Object>> pending = new LinkedHashMap<>();
        List<Map<String, Object>> transactions = new ArrayList<>();

        for (String line : text.split("\n")) {
            line = line.strip();
            if (line.isEmpty()) continue;

            Matcher m = TX_BEGIN_RE.matcher(line);
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

            m = TX_END_RE.matcher(line);
            if (m.find()) {
                Map<String, Object> tx = pending.get(m.group(3));
                if (tx != null) tx.put("end_time", m.group(1));
                continue;
            }

            m = TX_RESULT_RE.matcher(line);
            if (m.find()) {
                Map<String, Object> tx = pending.get(m.group(3));
                if (tx != null) {
                    tx.put("two_phase_result", m.group(4));
                    if (tx.get("end_time") == null) tx.put("end_time", m.group(1));
                }
                continue;
            }

            m = TX_ABORT_RE.matcher(line);
            if (m.find()) {
                Map<String, Object> tx = pending.get(m.group(3));
                if (tx != null) {
                    tx.put("end_time", m.group(1));
                    tx.put("outcome", "ABORTED");
                }
                continue;
            }

            m = TX_STATUS_RE.matcher(line);
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

        // Finalize pending transactions
        for (Map<String, Object> tx : pending.values()) {
            String begin = (String) tx.get("begin_time");
            String end = (String) tx.get("end_time");
            if (tx.get("outcome") == null) {
                if (end != null) {
                    tx.put("outcome", "COMMITTED");
                } else {
                    tx.put("outcome", "IN_FLIGHT");
                }
            }
            tx.put("duration_ms", (begin != null && end != null) ? TimeUtils.spyTimeDiff(begin, end) : 0.0);
            transactions.add(tx);
        }

        transactions.sort((a, b) -> {
            String aTime = (String) a.getOrDefault("begin_time", "");
            String bTime = (String) b.getOrDefault("begin_time", "");
            return bTime.compareTo(aTime);
        });
        return transactions;
    }

    // ═══════════════════════════════════════════════════════════════════
    // Analysis methods
    // ═══════════════════════════════════════════════════════════════════

    private Map<String, Object> analyzeThreads(List<Map<String, Object>> threads) {
        if (threads.isEmpty()) {
            // Mutable: the caller adds provenance keys (source/dump_provided/dump_time) afterward.
            // A Map.of() here throws UnsupportedOperationException when no thread dump is uploaded,
            // which previously 500'd any analysis run without a thread dump (e.g. access log only).
            Map<String, Object> empty = new LinkedHashMap<>();
            empty.put("total", 0);
            empty.put("provided", false);
            return empty;
        }

        Map<String, Integer> stateSummary = new LinkedHashMap<>();
        List<Map<String, Object>> blockedThreads = new ArrayList<>();
        List<Map<String, Object>> waitingThreads = new ArrayList<>();
        Map<String, List<Map<String, Object>>> contentionGroups = new LinkedHashMap<>();
        int daemonCount = 0;

        Map<String, Integer> threadCategories = new LinkedHashMap<>();

        for (Map<String, Object> t : threads) {
            String state = (String) t.getOrDefault("thread_state", "UNKNOWN");
            stateSummary.merge(state, 1, Integer::sum);

            if (Boolean.TRUE.equals(t.get("daemon"))) daemonCount++;

            String name = (String) t.getOrDefault("thread_name", "");
            String category = classifyThread(name);
            threadCategories.merge(category, 1, Integer::sum);

            if ("BLOCKED".equals(state)) {
                blockedThreads.add(t);
                String lock = (String) t.get("lock_name");
                if (lock != null) {
                    contentionGroups.computeIfAbsent(lock, k -> new ArrayList<>()).add(t);
                }
            } else if ("WAITING".equals(state) || "TIMED_WAITING".equals(state)) {
                waitingThreads.add(t);
            }
        }

        // Lock ownership: which thread holds each monitor (from each thread's "- locked <...>"
        // frames). Built once and reused for contention-owner resolution and deadlock detection.
        Map<String, String> lockOwnership = new LinkedHashMap<>();
        Map<String, String> threadWaitsOn = new LinkedHashMap<>();
        for (Map<String, Object> t : threads) {
            String tName = (String) t.get("thread_name");
            String waitLock = (String) t.get("lock_name");
            if (waitLock != null) threadWaitsOn.put(tName, waitLock);
            @SuppressWarnings("unchecked")
            List<String> monitors = (List<String>) t.getOrDefault("locked_monitors", List.of());
            for (String mon : monitors) lockOwnership.put(mon, tName);
        }

        List<Map<String, Object>> contentionList = new ArrayList<>();
        for (var entry : contentionGroups.entrySet()) {
            List<Map<String, Object>> waiters = entry.getValue();
            if (waiters.isEmpty()) continue;
            String lockClass = (String) waiters.get(0).getOrDefault("lock_class", "unknown");
            // Prefer an explicit "owned by" (rare on "waiting to lock" lines); otherwise fall
            // back to the thread that reported holding this monitor via a "- locked" frame.
            String owner = (String) waiters.get(0).get("lock_owner");
            if (owner == null) owner = lockOwnership.get(entry.getKey());

            Map<String, Object> group = new LinkedHashMap<>();
            group.put("lock", entry.getKey());
            group.put("lock_class", lockClass);
            group.put("owner_thread", owner);
            group.put("waiter_count", waiters.size());
            group.put("waiters", waiters.stream()
                    .map(w -> (String) w.get("thread_name"))
                    .collect(Collectors.toList()));
            contentionList.add(group);
        }
        contentionList.sort((a, b) ->
                Integer.compare((int) b.get("waiter_count"), (int) a.get("waiter_count")));

        // Deadlock detection (reuses lockOwnership/threadWaitsOn built above).
        Set<String> deadlockThreads = new LinkedHashSet<>();
        for (var entry : threadWaitsOn.entrySet()) {
            String threadA = entry.getKey();
            String lockA = entry.getValue();
            String ownerA = lockOwnership.get(lockA);
            if (ownerA != null && !ownerA.equals(threadA)) {
                String lockB = threadWaitsOn.get(ownerA);
                if (lockB != null) {
                    String ownerB = lockOwnership.get(lockB);
                    if (threadA.equals(ownerB)) {
                        deadlockThreads.add(threadA);
                        deadlockThreads.add(ownerA);
                    }
                }
            }
        }

        Map<String, Object> analysis = new LinkedHashMap<>();
        analysis.put("provided", true);
        analysis.put("total", threads.size());
        analysis.put("daemon_count", daemonCount);
        analysis.put("non_daemon_count", threads.size() - daemonCount);
        analysis.put("state_summary", stateSummary);
        analysis.put("thread_categories", threadCategories);
        analysis.put("blocked_threads", blockedThreads.stream().map(t -> {
            Map<String, Object> bt = new LinkedHashMap<>();
            bt.put("thread_name", t.get("thread_name"));
            bt.put("lock_name", t.get("lock_name"));
            bt.put("lock_class", t.get("lock_class"));
            bt.put("lock_owner", t.get("lock_owner"));
            @SuppressWarnings("unchecked")
            List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());
            bt.put("top_frames", stack.subList(0, Math.min(5, stack.size())));
            return bt;
        }).collect(Collectors.toList()));
        analysis.put("waiting_threads", waitingThreads.stream().map(t -> {
            Map<String, Object> wt = new LinkedHashMap<>();
            wt.put("thread_name", t.get("thread_name"));
            wt.put("thread_state", t.get("thread_state"));
            wt.put("lock_name", t.get("lock_name"));
            wt.put("lock_class", t.get("lock_class"));
            @SuppressWarnings("unchecked")
            List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());
            wt.put("top_frames", stack.subList(0, Math.min(3, stack.size())));
            String waitReason = classifyWaitReason(stack);
            wt.put("wait_reason", waitReason);
            return wt;
        }).collect(Collectors.toList()));
        analysis.put("contention_groups", contentionList);
        analysis.put("deadlock_threads", new ArrayList<>(deadlockThreads));

        analysis.put("all_threads", threads.stream().map(t -> {
            Map<String, Object> at = new LinkedHashMap<>();
            at.put("thread_name", t.get("thread_name"));
            at.put("thread_state", t.getOrDefault("thread_state", "UNKNOWN"));
            at.put("daemon", Boolean.TRUE.equals(t.get("daemon")));
            String tName = (String) t.getOrDefault("thread_name", "");
            at.put("category", classifyThread(tName));
            @SuppressWarnings("unchecked")
            List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());
            at.put("top_frame", stack.isEmpty() ? "" : stack.get(0));
            at.put("stack_depth", stack.size());
            at.put("lock_name", t.get("lock_name"));
            at.put("lock_owner", t.get("lock_owner"));
            double cpuMs = ((Number) t.getOrDefault("cpu_ms", -1.0)).doubleValue();
            double elapsedS = ((Number) t.getOrDefault("elapsed_s", -1.0)).doubleValue();
            if (cpuMs >= 0) at.put("cpu_ms", Math.round(cpuMs * 100.0) / 100.0);
            if (elapsedS >= 0) at.put("elapsed_s", Math.round(elapsedS * 100.0) / 100.0);
            return at;
        }).collect(Collectors.toList()));

        // Long-running threads: high CPU or RUNNABLE with deep stacks
        List<Map<String, Object>> longRunning = threads.stream()
                .filter(t -> {
                    double cpuMs = ((Number) t.getOrDefault("cpu_ms", -1.0)).doubleValue();
                    double elapsedS = ((Number) t.getOrDefault("elapsed_s", -1.0)).doubleValue();
                    String state = (String) t.getOrDefault("thread_state", "");
                    @SuppressWarnings("unchecked")
                    List<String> stk = (List<String>) t.getOrDefault("stack_trace", List.of());
                    // High CPU (>1s), or RUNNABLE with deep stack and some elapsed time
                    return cpuMs > 1000
                            || (cpuMs > 100 && "RUNNABLE".equals(state) && stk.size() > 3)
                            || ("BLOCKED".equals(state) && elapsedS > 60);
                })
                .sorted((a, b) -> Double.compare(
                        ((Number) b.getOrDefault("cpu_ms", 0.0)).doubleValue(),
                        ((Number) a.getOrDefault("cpu_ms", 0.0)).doubleValue()))
                .limit(30)
                .map(t -> {
                    Map<String, Object> lr = new LinkedHashMap<>();
                    lr.put("thread_name", t.get("thread_name"));
                    lr.put("thread_state", t.getOrDefault("thread_state", "UNKNOWN"));
                    lr.put("daemon", Boolean.TRUE.equals(t.get("daemon")));
                    lr.put("category", classifyThread((String) t.getOrDefault("thread_name", "")));
                    double cpuMs = ((Number) t.getOrDefault("cpu_ms", -1.0)).doubleValue();
                    double elapsedS = ((Number) t.getOrDefault("elapsed_s", -1.0)).doubleValue();
                    if (cpuMs >= 0) lr.put("cpu_ms", Math.round(cpuMs * 100.0) / 100.0);
                    if (elapsedS >= 0) lr.put("elapsed_s", Math.round(elapsedS * 100.0) / 100.0);
                    @SuppressWarnings("unchecked")
                    List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());
                    lr.put("top_frames", stack.subList(0, Math.min(5, stack.size())));
                    lr.put("stack_depth", stack.size());
                    return lr;
                })
                .collect(Collectors.toList());
        analysis.put("long_running_threads", longRunning);

        return analysis;
    }

    private String classifyWaitReason(List<String> stack) {
        String stackStr = String.join(" ", stack).toLowerCase();
        if (stackStr.contains("object.wait")) return "Object.wait()";
        if (stackStr.contains("locksupport.park")) return "LockSupport.park()";
        if (stackStr.contains("unsafe.park")) return "Unsafe.park()";
        if (stackStr.contains("thread.sleep")) return "Thread.sleep()";
        if (stackStr.contains("selector.select")) return "Selector.select()";
        if (stackStr.contains("queue.take") || stackStr.contains("queue.poll")) return "Queue wait";
        if (stackStr.contains("socket") || stackStr.contains("socketread")) return "Socket I/O";
        if (stackStr.contains("accept")) return "Accept connection";
        if (stackStr.contains("getconnection") || stackStr.contains("connectionpool")) return "Connection pool";
        return "Unknown";
    }

    private List<Map<String, Object>> analyzeSlowApis(List<Map<String, Object>> requests) {
        if (requests.isEmpty()) return List.of();

        Map<String, Map<String, Object>> byEndpoint = new LinkedHashMap<>();
        for (Map<String, Object> r : requests) {
            String method = (String) r.getOrDefault("method", "?");
            String uri = normalizeUri((String) r.getOrDefault("uri", "/"));
            String key = method + " " + uri;

            byEndpoint.computeIfAbsent(key, k -> {
                Map<String, Object> s = new LinkedHashMap<>();
                s.put("method", method);
                s.put("uri", uri);
                s.put("deployment", r.get("deployment"));
                s.put("hit_count", 0);
                s.put("total_time_ms", 0L);
                s.put("min_time_ms", Long.MAX_VALUE);
                s.put("max_time_ms", 0L);
                s.put("error_count", 0);
                s.put("client_error_count", 0); // 4xx
                s.put("server_error_count", 0); // 5xx
                s.put("status_codes", new LinkedHashMap<String, Integer>());
                s.put("threads", new LinkedHashSet<String>());
                s.put("slow_request_count", 0);
                s.put("first_hit_ms", Long.MAX_VALUE);
                s.put("last_hit_ms", 0L);
                // thread name -> last time (epoch ms) that thread served this endpoint
                s.put("thread_last_hit", new LinkedHashMap<String, Long>());
                return s;
            });

            Map<String, Object> s = byEndpoint.get(key);
            s.put("hit_count", (int) s.get("hit_count") + 1);
            long rt = ((Number) r.getOrDefault("response_time_ms", 0)).longValue();
            s.put("total_time_ms", (long) s.get("total_time_ms") + rt);
            s.put("min_time_ms", Math.min((long) s.get("min_time_ms"), rt));
            s.put("max_time_ms", Math.max((long) s.get("max_time_ms"), rt));
            long ts = ((Number) r.getOrDefault("timestamp_ms", 0L)).longValue();
            if (ts > 0) {
                s.put("first_hit_ms", Math.min((long) s.get("first_hit_ms"), ts));
                s.put("last_hit_ms", Math.max((long) s.get("last_hit_ms"), ts));
            }
            int stCode = ((Number) r.getOrDefault("status", 200)).intValue();
            if (stCode >= 400) {
                s.put("error_count", (int) s.get("error_count") + 1);
                if (stCode >= 500) s.put("server_error_count", (int) s.get("server_error_count") + 1);
                else s.put("client_error_count", (int) s.get("client_error_count") + 1);
            }
            if (rt > 5000) {
                s.put("slow_request_count", (int) s.get("slow_request_count") + 1);
            }
            @SuppressWarnings("unchecked")
            Map<String, Integer> sc = (Map<String, Integer>) s.get("status_codes");
            sc.merge(String.valueOf(r.getOrDefault("status", 200)), 1, Integer::sum);
            String thread = (String) r.get("thread");
            if (thread != null) {
                @SuppressWarnings("unchecked")
                Set<String> tset = (Set<String>) s.get("threads");
                tset.add(thread);
                if (ts > 0) {
                    @SuppressWarnings("unchecked")
                    Map<String, Long> tlh = (Map<String, Long>) s.get("thread_last_hit");
                    tlh.merge(thread, ts, Math::max);
                }
            }
        }

        return byEndpoint.values().stream()
                .peek(s -> {
                    int hits = (int) s.get("hit_count");
                    s.put("avg_time_ms", hits > 0 ? Math.round((long) s.get("total_time_ms") * 100.0 / hits) / 100.0 : 0);
                    if ((long) s.get("min_time_ms") == Long.MAX_VALUE) s.put("min_time_ms", 0L);
                    @SuppressWarnings("unchecked")
                    Set<String> tset = (Set<String>) s.get("threads");
                    s.put("threads", new ArrayList<>(tset));
                    long firstMs = (long) s.get("first_hit_ms");
                    long lastMs = (long) s.get("last_hit_ms");
                    s.put("first_hit", firstMs == Long.MAX_VALUE ? null : java.time.Instant.ofEpochMilli(firstMs).toString());
                    s.put("last_hit", lastMs == 0 ? null : java.time.Instant.ofEpochMilli(lastMs).toString());
                    if (firstMs == Long.MAX_VALUE) s.put("first_hit_ms", 0L);
                })
                .sorted((a, b) -> Long.compare((long) b.get("max_time_ms"), (long) a.get("max_time_ms")))
                .collect(Collectors.toList());
    }

    private Map<String, Object> analyzeGc(List<Map<String, Object>> gcEvents) {
        if (gcEvents.isEmpty()) return Map.of("provided", false, "total_pauses", 0);

        double maxPause = 0;
        double totalPause = 0;
        int fullGcCount = 0;
        List<Map<String, Object>> longPauses = new ArrayList<>();

        for (Map<String, Object> e : gcEvents) {
            double pause = ((Number) e.getOrDefault("pause_ms", 0)).doubleValue();
            totalPause += pause;
            maxPause = Math.max(maxPause, pause);
            if (Boolean.TRUE.equals(e.get("full_gc"))) fullGcCount++;
            if (pause > 200) {
                longPauses.add(e);
            }
        }

        Map<String, Object> analysis = new LinkedHashMap<>();
        analysis.put("provided", true);
        analysis.put("total_pauses", gcEvents.size());
        analysis.put("full_gc_count", fullGcCount);
        analysis.put("max_pause_ms", Math.round(maxPause * 100.0) / 100.0);
        analysis.put("total_pause_ms", Math.round(totalPause * 100.0) / 100.0);
        analysis.put("avg_pause_ms", gcEvents.isEmpty() ? 0 :
                Math.round(totalPause / gcEvents.size() * 100.0) / 100.0);
        analysis.put("long_pauses", longPauses);
        analysis.put("pauses", gcEvents);
        return analysis;
    }

    private Map<String, Object> analyzeJdbcSpy(List<Map<String, Object>> queries) {
        if (queries.isEmpty()) return Map.of("provided", false, "total_queries", 0);

        Map<String, Integer> queryCountByDs = new LinkedHashMap<>();
        Map<String, Integer> sqlFrequency = new LinkedHashMap<>();
        // Slow queries collapsed by SQL text: one entry per unique query holding its
        // occurrence count and the MAX exec time (worst case), so a repeated slow query
        // shows as a single row instead of dozens of near-identical lines.
        Map<String, Map<String, Object>> slowBySql = new LinkedHashMap<>();
        int slowOccurrences = 0;
        List<Map<String, Object>> inflightQueries = new ArrayList<>();
        double totalExecMs = 0;
        double maxExecMs = 0;
        int completedCount = 0;

        for (Map<String, Object> q : queries) {
            String ds = (String) q.getOrDefault("datasource", "unknown");
            queryCountByDs.merge(ds, 1, Integer::sum);

            String sql = (String) q.getOrDefault("sql", "");
            String sqlNorm = sql.length() > 80 ? sql.substring(0, 80) : sql;
            sqlFrequency.merge(sqlNorm, 1, Integer::sum);

            if (Boolean.TRUE.equals(q.get("inflight"))) {
                inflightQueries.add(q);
                continue;
            }

            double execMs = ((Number) q.getOrDefault("exec_time_ms", 0.0)).doubleValue();
            if (execMs >= 0) {
                totalExecMs += execMs;
                maxExecMs = Math.max(maxExecMs, execMs);
                completedCount++;
            }
            if (execMs > 100) {
                slowOccurrences++;
                Map<String, Object> agg = slowBySql.get(sql);
                if (agg == null) {
                    agg = new LinkedHashMap<>();
                    agg.put("sql", sql);
                    agg.put("datasource", q.getOrDefault("datasource", ""));
                    agg.put("thread", q.getOrDefault("thread", ""));
                    agg.put("exec_time_ms", execMs);   // running max
                    agg.put("execute_type", q.getOrDefault("execute_type", ""));
                    agg.put("count", 1);
                    slowBySql.put(sql, agg);
                } else {
                    agg.put("count", ((Number) agg.get("count")).intValue() + 1);
                    double prevMax = ((Number) agg.get("exec_time_ms")).doubleValue();
                    if (execMs > prevMax) {
                        // Keep the worst occurrence's context (max time, its ds/thread/type).
                        agg.put("exec_time_ms", execMs);
                        agg.put("datasource", q.getOrDefault("datasource", ""));
                        agg.put("thread", q.getOrDefault("thread", ""));
                        agg.put("execute_type", q.getOrDefault("execute_type", ""));
                    }
                }
            }
        }

        List<Map<String, Object>> slowQueries = new ArrayList<>(slowBySql.values());
        slowQueries.sort((a, b) -> Double.compare(
                ((Number) b.getOrDefault("exec_time_ms", 0.0)).doubleValue(),
                ((Number) a.getOrDefault("exec_time_ms", 0.0)).doubleValue()));

        // Return a fuller ranked list; the UI (rca.html) shows 10 at a time with Show More/Less.
        List<Map<String, Object>> topSql = sqlFrequency.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                .limit(100)
                .map(e -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("sql", e.getKey());
                    m.put("count", e.getValue());
                    return m;
                })
                .collect(Collectors.toList());

        Map<String, Object> analysis = new LinkedHashMap<>();
        analysis.put("provided", true);
        analysis.put("total_queries", queries.size());
        analysis.put("completed_queries", completedCount);
        analysis.put("inflight_queries", inflightQueries.size());
        analysis.put("slow_queries_count", slowOccurrences);
        analysis.put("slow_queries_unique", slowQueries.size());
        analysis.put("avg_exec_ms", completedCount > 0 ? Math.round(totalExecMs / completedCount * 100.0) / 100.0 : 0);
        analysis.put("max_exec_ms", Math.round(maxExecMs * 100.0) / 100.0);
        analysis.put("queries_by_datasource", queryCountByDs);
        analysis.put("top_sql", topSql);
        analysis.put("slow_queries", slowQueries.subList(0, Math.min(30, slowQueries.size())));
        analysis.put("inflight", inflightQueries);
        return analysis;
    }

    private Map<String, Object> analyzeTx(List<Map<String, Object>> txEntries, List<Map<String, Object>> spyQueries) {
        if (txEntries.isEmpty()) return Map.of("provided", false, "total", 0);

        int committed = 0, aborted = 0, inflight = 0;
        double totalDuration = 0, maxDuration = 0;
        List<Map<String, Object>> slowTxs = new ArrayList<>();

        for (Map<String, Object> tx : txEntries) {
            String outcome = (String) tx.getOrDefault("outcome", "");
            if ("COMMITTED".equals(outcome)) committed++;
            else if ("ABORTED".equals(outcome)) aborted++;
            else if ("IN_FLIGHT".equals(outcome)) inflight++;

            double dur = ((Number) tx.getOrDefault("duration_ms", 0.0)).doubleValue();
            totalDuration += dur;
            maxDuration = Math.max(maxDuration, dur);
            if (dur > 1000) slowTxs.add(tx);
        }

        int total = committed + aborted;
        double abortRate = total > 0 ? Math.round((double) aborted / total * 10000) / 100.0 : 0;

        slowTxs.sort((a, b) -> Double.compare(
                ((Number) b.getOrDefault("duration_ms", 0.0)).doubleValue(),
                ((Number) a.getOrDefault("duration_ms", 0.0)).doubleValue()));

        // Correlate transactions with spy queries by thread+time
        if (!spyQueries.isEmpty()) {
            Map<String, List<Map<String, Object>>> threadQueries = new LinkedHashMap<>();
            for (Map<String, Object> q : spyQueries) {
                String thread = (String) q.get("thread");
                if (thread != null) {
                    threadQueries.computeIfAbsent(thread, k -> new ArrayList<>()).add(q);
                }
            }
            for (Map<String, Object> tx : txEntries) {
                String thread = (String) tx.get("thread");
                String begin = (String) tx.get("begin_time");
                String end = (String) tx.get("end_time");
                if (thread == null || begin == null) continue;
                if (end == null) end = begin;

                List<Map<String, Object>> tqs = threadQueries.getOrDefault(thread, List.of());
                List<Map<String, Object>> matched = new ArrayList<>();
                for (Map<String, Object> q : tqs) {
                    String qTs = (String) q.getOrDefault("timestamp", "");
                    if (!qTs.isEmpty() && qTs.compareTo(begin) >= 0 && qTs.compareTo(end) <= 0) {
                        Map<String, Object> mq = new LinkedHashMap<>();
                        mq.put("sql", q.get("sql"));
                        mq.put("datasource", q.get("datasource"));
                        mq.put("exec_time_ms", q.get("exec_time_ms"));
                        matched.add(mq);
                    }
                }
                tx.put("query_count", matched.size());
                tx.put("queries", matched.subList(0, Math.min(10, matched.size())));
            }
        }

        Map<String, Object> analysis = new LinkedHashMap<>();
        analysis.put("provided", true);
        analysis.put("total", txEntries.size());
        analysis.put("committed", committed);
        analysis.put("aborted", aborted);
        analysis.put("inflight", inflight);
        analysis.put("abort_rate_percent", abortRate);
        analysis.put("avg_duration_ms", txEntries.isEmpty() ? 0 :
                Math.round(totalDuration / txEntries.size() * 100.0) / 100.0);
        analysis.put("max_duration_ms", Math.round(maxDuration * 100.0) / 100.0);
        analysis.put("slow_transactions", slowTxs.subList(0, Math.min(30, slowTxs.size())));
        analysis.put("transactions", txEntries.subList(0, Math.min(200, txEntries.size())));
        return analysis;
    }

    // ═══════════════════════════════════════════════════════════════════
    // Correlation methods
    // ═══════════════════════════════════════════════════════════════════

    /** Slack (ms) added to each side of a request's in-flight window when linking a logged error to it. */
    private static final long ERROR_ACCESS_SLACK_MS = 2000;

    private List<Map<String, Object>> correlateErrors(
            List<Map<String, Object>> logEntries,
            List<Map<String, Object>> requests,
            List<Map<String, Object>> threads) {

        List<Map<String, Object>> errors = logEntries.stream()
                .filter(e -> "ERROR".equals(e.get("level")) || "FATAL".equals(e.get("level")))
                .collect(Collectors.toList());
        if (errors.isEmpty()) return List.of();

        Map<String, Map<String, Object>> threadLookup = new LinkedHashMap<>();
        for (Map<String, Object> t : threads) {
            threadLookup.put((String) t.get("thread_name"), t);
        }

        List<Map<String, Object>> correlated = new ArrayList<>();
        for (Map<String, Object> err : errors) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("timestamp", err.get("timestamp"));
            entry.put("level", err.get("level"));
            entry.put("thread", err.get("thread"));
            entry.put("logger", err.get("logger"));
            entry.put("message", truncate((String) err.get("message"), 300));
            entry.put("exception_class", err.get("exception_class"));

            @SuppressWarnings("unchecked")
            List<String> st = (List<String>) err.getOrDefault("stacktrace", List.of());
            entry.put("stacktrace_depth", st.size());
            entry.put("stacktrace_top", st.subList(0, Math.min(5, st.size())));

            String errThread = (String) err.get("thread");
            long errTsMs = ((Number) err.getOrDefault("timestamp_ms", 0L)).longValue();

            // Find the access-log request on the same worker thread whose in-flight window
            // contains (or is nearest to) the moment the error was logged. Undertow renders
            // %t at request COMPLETION, so the request ran over [completion - responseTime,
            // completion]; an error is logged somewhere inside that window. Matching on the
            // window (± slack) instead of |completion - errTime| correctly links errors that
            // fire early in a long-running request, which plain proximity would miss.
            Map<String, Object> relatedReq = null;
            long bestGap = Long.MAX_VALUE;
            if (errThread != null && errTsMs > 0) {
                for (Map<String, Object> req : requests) {
                    String reqThread = (String) req.get("thread");
                    if (!errThread.equals(reqThread)) continue;
                    long completion = ((Number) req.getOrDefault("timestamp_ms", 0L)).longValue();
                    if (completion <= 0) continue;
                    long rt = ((Number) req.getOrDefault("response_time_ms", 0L)).longValue();
                    long reqStart = completion - Math.max(rt, 0L);
                    if (errTsMs < reqStart - ERROR_ACCESS_SLACK_MS
                            || errTsMs > completion + ERROR_ACCESS_SLACK_MS) continue;
                    // Prefer the request whose window is tightest around the error moment.
                    long gap = errTsMs < reqStart ? reqStart - errTsMs
                            : errTsMs > completion ? errTsMs - completion : 0L;
                    if (gap < bestGap) {
                        bestGap = gap;
                        relatedReq = Map.of(
                                "method", req.getOrDefault("method", ""),
                                "uri", req.getOrDefault("uri", ""),
                                "status", req.getOrDefault("status", 0),
                                "response_time_ms", req.getOrDefault("response_time_ms", 0));
                        if (gap == 0L) break;
                    }
                }
            }
            entry.put("related_request", relatedReq);
            // Flat fields consumed directly by the Error Correlation table in rca.html.
            if (relatedReq != null) {
                String method = String.valueOf(relatedReq.getOrDefault("method", "")).trim();
                String uri = String.valueOf(relatedReq.getOrDefault("uri", "")).trim();
                entry.put("related_api", (method.isEmpty() ? "" : method + " ") + uri);
                entry.put("related_status", relatedReq.get("status"));
                entry.put("response_time_ms", relatedReq.get("response_time_ms"));
            }

            // Find thread state in dump
            if (errThread != null && threadLookup.containsKey(errThread)) {
                Map<String, Object> t = threadLookup.get(errThread);
                entry.put("thread_state_in_dump", t.get("thread_state"));
                @SuppressWarnings("unchecked")
                List<String> tStack = (List<String>) t.getOrDefault("stack_trace", List.of());
                entry.put("thread_top_frame", tStack.isEmpty() ? null : tStack.get(0));
            }

            correlated.add(entry);
        }

        return correlated.subList(0, Math.min(200, correlated.size()));
    }

    /** Tolerance (ms) around a request's in-flight window when matching the dump capture moment. */
    private static final long DUMP_MATCH_TOLERANCE_MS = 5000;

    private void enrichWithThreadData(
            List<Map<String, Object>> slowApis,
            List<Map<String, Object>> threads,
            List<Map<String, Object>> requests,
            long dumpMs) {
        if (threads.isEmpty() || slowApis.isEmpty()) return;

        Map<String, Map<String, Object>> threadLookup = new LinkedHashMap<>();
        for (Map<String, Object> t : threads) {
            threadLookup.put((String) t.get("thread_name"), t);
        }

        // Worker threads (e.g. "default task-1") are pooled and reused across many
        // requests over time, so matching a dump thread to a slow API purely by name is
        // wrong — the same name served many endpoints at different moments. Instead,
        // attribute each dump thread to the ONE request that was in-flight at the exact
        // moment the dump was captured: request window [start, start+responseTime]
        // (± tolerance) must contain the dump timestamp. If the dump has no timestamp we
        // cannot verify the timeframe, so we make no name-based match at all.
        //
        // inflightEndpointByThread: dump thread name -> endpoint key ("METHOD uri") it was serving.
        Map<String, String> inflightEndpointByThread = new HashMap<>();
        Map<String, Long> inflightHitByThread = new HashMap<>();
        Map<String, Long> bestDistByThread = new HashMap<>();
        if (dumpMs > 0) {
            for (Map<String, Object> req : requests) {
                String tn = (String) req.get("thread");
                if (tn == null || tn.isBlank()) continue;
                if (!threadLookup.containsKey(tn)) continue;   // only threads actually in the dump
                // Wall-clock comparison (see parseDumpTimeMs): both dumpMs and the request
                // window use naive/local time, so a timezone offset cannot cause a false match.
                long tsLocal = ((Number) req.getOrDefault("timestamp_local_ms", 0L)).longValue();
                if (tsLocal <= 0) continue;
                long rt = Math.max(((Number) req.getOrDefault("response_time_ms", 0)).longValue(), 0);
                // Undertow writes the access-log entry when the request COMPLETES, so %t is the
                // completion time and the request was in-flight during [ts-rt, ts]. Other servers
                // log the start time ([ts, ts+rt]). Accept the union [ts-rt, ts+rt] (± tolerance) so
                // a dump captured any time during the request matches — this is the fix for slow
                // in-progress requests (e.g. long uploads) showing "no thread active".
                long lo = tsLocal - rt, hi = tsLocal + rt;
                long dist = dumpMs < lo ? lo - dumpMs : (dumpMs > hi ? dumpMs - hi : 0);
                if (dist > DUMP_MATCH_TOLERANCE_MS) continue;  // request not in-flight at capture time
                Long prev = bestDistByThread.get(tn);
                if (prev == null || dist < prev) {
                    bestDistByThread.put(tn, dist);
                    String key = req.getOrDefault("method", "?") + " "
                            + normalizeUri((String) req.getOrDefault("uri", "/"));
                    inflightEndpointByThread.put(tn, key);
                    // Display the real absolute hit time, not the wall-clock proxy.
                    inflightHitByThread.put(tn, ((Number) req.getOrDefault("timestamp_ms", 0L)).longValue());
                }
            }
        }

        for (Map<String, Object> api : slowApis) {
            String apiKey = api.get("method") + " " + api.get("uri");
            api.put("thread_dump_provided", true);
            api.put("dump_time_known", dumpMs > 0);
            Map<String, Integer> threadStates = new LinkedHashMap<>();
            List<Map<String, Object>> blockedDetails = new ArrayList<>();
            // Exact thread(s) from the dump that were serving THIS endpoint when the dump was captured.
            List<Map<String, Object>> matchedThreads = new ArrayList<>();

            for (Map.Entry<String, String> e : inflightEndpointByThread.entrySet()) {
                if (!apiKey.equals(e.getValue())) continue;
                String tn = e.getKey();
                Map<String, Object> t = threadLookup.get(tn);
                if (t == null) continue;
                Long hitMs = inflightHitByThread.get(tn);

                String state = (String) t.getOrDefault("thread_state", "UNKNOWN");
                threadStates.merge(state, 1, Integer::sum);

                Map<String, Object> mt = new LinkedHashMap<>();
                mt.put("thread", tn);
                mt.put("in_dump", true);
                mt.put("last_hit_ms", hitMs != null ? hitMs : 0L);
                mt.put("last_hit", hitMs != null ? java.time.Instant.ofEpochMilli(hitMs).toString() : null);
                mt.put("thread_state", state);
                mt.put("cpu_ms", t.getOrDefault("cpu_ms", -1.0));
                mt.put("elapsed_s", t.getOrDefault("elapsed_s", -1.0));
                mt.put("lock_name", t.get("lock_name"));
                mt.put("lock_owner", t.get("lock_owner"));
                @SuppressWarnings("unchecked")
                List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());
                mt.put("top_frame", stack.isEmpty() ? "" : stack.get(0));
                if ("BLOCKED".equals(state)) {
                    Object lockName = t.get("lock_name");
                    Object lockOwner = t.get("lock_owner");
                    blockedDetails.add(Map.of(
                            "thread", tn,
                            "lock_name", lockName != null ? lockName : "",
                            "lock_owner", lockOwner != null ? lockOwner : ""));
                }
                matchedThreads.add(mt);
            }
            // Most-recent in-flight request first.
            matchedThreads.sort((a, b) -> Long.compare(
                    ((Number) b.getOrDefault("last_hit_ms", 0L)).longValue(),
                    ((Number) a.getOrDefault("last_hit_ms", 0L)).longValue()));

            api.put("thread_states", threadStates);
            api.put("blocked_details", blockedDetails);
            api.put("matched_threads", matchedThreads);
            api.put("matched_thread_count", matchedThreads.size());
        }
    }

    private void enrichWithGcImpact(
            List<Map<String, Object>> slowApis,
            List<Map<String, Object>> gcEvents,
            List<Map<String, Object>> requests) {
        if (gcEvents.isEmpty() || requests.isEmpty()) {
            for (Map<String, Object> api : slowApis) {
                api.put("gc_impact", false);
                api.put("gc_overlap_count", 0);
            }
            return;
        }

        // For each slow API endpoint, check if any of its slow requests overlap with GC pauses
        for (Map<String, Object> api : slowApis) {
            String method = (String) api.get("method");
            String uri = (String) api.get("uri");
            int gcOverlapCount = 0;

            for (Map<String, Object> req : requests) {
                if (!method.equals(req.get("method"))) continue;
                String reqUri = normalizeUri((String) req.getOrDefault("uri", "/"));
                if (!uri.equals(reqUri)) continue;

                long rt = ((Number) req.getOrDefault("response_time_ms", 0)).longValue();
                if (rt < 1000) continue;

                long reqTsMs = ((Number) req.getOrDefault("timestamp_ms", 0L)).longValue();
                if (reqTsMs == 0) continue;
                long reqEndMs = reqTsMs + rt;

                for (Map<String, Object> gc : gcEvents) {
                    long gcTsMs = ((Number) gc.getOrDefault("timestamp_ms", 0L)).longValue();
                    double pauseMs = ((Number) gc.getOrDefault("pause_ms", 0)).doubleValue();
                    if (gcTsMs == 0) continue;
                    long gcEndMs = gcTsMs + (long) pauseMs;
                    if (gcTsMs <= reqEndMs && gcEndMs >= reqTsMs) {
                        gcOverlapCount++;
                        break;
                    }
                }
            }
            api.put("gc_impact", gcOverlapCount > 0);
            api.put("gc_overlap_count", gcOverlapCount);
        }
    }

    private void enrichWithServerLogErrors(
            List<Map<String, Object>> slowApis,
            List<Map<String, Object>> logEntries,
            List<Map<String, Object>> requests) {
        if (logEntries.isEmpty() || slowApis.isEmpty()) return;

        List<Map<String, Object>> errors = logEntries.stream()
                .filter(e -> "ERROR".equals(e.get("level")) || "FATAL".equals(e.get("level")))
                .collect(Collectors.toList());
        if (errors.isEmpty()) return;

        final long TOL = 1000; // ms slack around each request window

        for (Map<String, Object> api : slowApis) {
            String apiMethod = (String) api.get("method");
            String apiUri = (String) api.get("uri");

            // Build the in-flight windows for THIS endpoint, grouped by the thread that served it.
            // An error only belongs to this API if it fired on the same thread WHILE a request
            // to this endpoint was in flight — thread name alone is not enough, because a worker
            // thread (e.g. "default task-6") serves many endpoints over its life.
            Map<String, List<long[]>> windowsByThread = new HashMap<>();
            for (Map<String, Object> req : requests) {
                if (!Objects.equals(apiMethod, req.get("method"))) continue;
                if (!Objects.equals(apiUri, normalizeUri((String) req.getOrDefault("uri", "/")))) continue;
                String th = (String) req.get("thread");
                if (th == null) continue;
                long s = ((Number) req.getOrDefault("timestamp_local_ms", 0L)).longValue();
                if (s <= 0) continue;
                long rt = ((Number) req.getOrDefault("response_time_ms", 0)).longValue();
                windowsByThread.computeIfAbsent(th, k -> new ArrayList<>())
                        .add(new long[]{s - TOL, s + rt + TOL});
            }
            if (windowsByThread.isEmpty()) { api.put("server_log_errors", List.of()); continue; }

            Set<String> relatedErrors = new LinkedHashSet<>(); // dedup identical messages
            for (Map<String, Object> err : errors) {
                String errThread = (String) err.get("thread");
                if (errThread == null) continue;
                List<long[]> windows = windowsByThread.get(errThread);
                if (windows == null) continue;
                long ems = wallMs((String) err.get("timestamp"));
                if (ems <= 0) continue;
                boolean inWindow = windows.stream().anyMatch(w -> ems >= w[0] && ems <= w[1]);
                if (!inWindow) continue;
                String exClass = (String) err.get("exception_class");
                String msg = truncate((String) err.get("message"), 120);
                relatedErrors.add(exClass != null ? exClass + ": " + msg : msg);
            }
            List<String> list = new ArrayList<>(relatedErrors);
            api.put("server_log_errors", list.subList(0, Math.min(10, list.size())));
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Timeline
    // ═══════════════════════════════════════════════════════════════════

    private List<Map<String, Object>> buildTimeline(
            List<Map<String, Object>> requests,
            List<Map<String, Object>> logEntries,
            List<Map<String, Object>> gcEvents) {

        List<Map<String, Object>> events = new ArrayList<>();

        for (Map<String, Object> r : requests) {
            long rt = ((Number) r.getOrDefault("response_time_ms", 0)).longValue();
            int status = ((Number) r.getOrDefault("status", 200)).intValue();
            if (rt > 5000 || status >= 500) {
                Map<String, Object> ev = new LinkedHashMap<>();
                ev.put("timestamp", r.get("timestamp"));
                ev.put("timestamp_ms", r.getOrDefault("timestamp_ms", 0L));
                ev.put("type", rt > 5000 ? "slow_request" : "error_request");
                ev.put("severity", rt > 30000 || status >= 500 ? "CRITICAL" : "WARNING");
                ev.put("detail", r.get("method") + " " + r.get("uri") + " → "
                        + status + " (" + rt + "ms)"
                        + (r.get("thread") != null ? " [" + r.get("thread") + "]" : ""));
                events.add(ev);
            }
        }

        for (Map<String, Object> e : logEntries) {
            String level = (String) e.get("level");
            if ("ERROR".equals(level) || "FATAL".equals(level)) {
                Map<String, Object> ev = new LinkedHashMap<>();
                ev.put("timestamp", e.get("timestamp"));
                ev.put("timestamp_ms", e.getOrDefault("timestamp_ms", 0L));
                ev.put("type", "server_error");
                ev.put("severity", "FATAL".equals(level) ? "CRITICAL" : "WARNING");
                String msg = truncate((String) e.get("message"), 150);
                String exCls = (String) e.get("exception_class");
                ev.put("detail", (exCls != null ? exCls + ": " : "") + msg
                        + (e.get("thread") != null ? " [" + e.get("thread") + "]" : ""));
                events.add(ev);
            }
        }

        for (Map<String, Object> g : gcEvents) {
            double pause = ((Number) g.getOrDefault("pause_ms", 0)).doubleValue();
            if (pause > 200) {
                Map<String, Object> ev = new LinkedHashMap<>();
                ev.put("timestamp", g.get("timestamp"));
                ev.put("timestamp_ms", g.getOrDefault("timestamp_ms", 0L));
                ev.put("type", "gc_pause");
                ev.put("severity", pause > 1000 ? "CRITICAL" : pause > 500 ? "WARNING" : "INFO");
                ev.put("detail", g.get("type") + " — "
                        + String.format("%.1f", pause) + "ms"
                        + (g.containsKey("heap_before_mb") ?
                        " (" + g.get("heap_before_mb") + "M→" + g.get("heap_after_mb") + "M)" : ""));
                events.add(ev);
            }
        }

        events.sort((a, b) -> {
            long ta = ((Number) a.getOrDefault("timestamp_ms", 0L)).longValue();
            long tb = ((Number) b.getOrDefault("timestamp_ms", 0L)).longValue();
            if (ta != tb) return Long.compare(tb, ta);
            return 0;
        });
        return events;
    }

    // ═══════════════════════════════════════════════════════════════════
    // Finding generation
    // ═══════════════════════════════════════════════════════════════════

    private List<Map<String, Object>> generateFindings(
            List<Map<String, Object>> threads,
            List<Map<String, Object>> requests,
            List<Map<String, Object>> logEntries,
            List<Map<String, Object>> gcEvents,
            Map<String, Object> threadAnalysis,
            List<Map<String, Object>> slowApis,
            Map<String, Object> gcAnalysis,
            Map<String, Object> jdbcSpyAnalysis,
            Map<String, Object> txAnalysis,
            String threadDumpTime) {

        List<Map<String, Object>> findings = new ArrayList<>();
        Map<String, Object> dumpSnapshot = snapshotRange(threadDumpTime);

        // ── Thread contention findings ──────────────────────────────
        if (Boolean.TRUE.equals(threadAnalysis.get("provided"))) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> contentionGroups =
                    (List<Map<String, Object>>) threadAnalysis.getOrDefault("contention_groups", List.of());

            for (Map<String, Object> cg : contentionGroups) {
                int waiters = (int) cg.get("waiter_count");
                String severity = waiters > 5 ? "CRITICAL" : waiters > 2 ? "WARNING" : "INFO";
                String lockClass = (String) cg.getOrDefault("lock_class", "unknown");
                String owner = (String) cg.get("owner_thread");

                List<Map<String, Object>> evidence = new ArrayList<>();
                evidence.add(Map.of("source", "thread_dump",
                        "detail", waiters + " threads blocked on " + lockClass
                                + (owner != null ? ", owned by " + owner : "")));

                findings.add(withTime(buildFinding(severity, "Thread Contention",
                        waiters + " threads blocked on " + shortClass(lockClass),
                        waiters + " threads are blocked waiting for lock on " + lockClass
                                + "." + (owner != null ? " The lock is held by thread \"" + owner
                                + "\". This contention can cause request timeouts and degraded throughput." : ""),
                        evidence,
                        "Review the lock holder's stack trace to understand why it's holding the lock. "
                                + "Consider reducing lock scope, using concurrent data structures, or adding connection pool capacity."),
                        dumpSnapshot));
            }

            Object blockedObj = threadAnalysis.getOrDefault("blocked_threads", List.of());
            int blockedCount;
            if (blockedObj instanceof Number) {
                blockedCount = ((Number) blockedObj).intValue();
            } else if (blockedObj instanceof List) {
                blockedCount = ((List<?>) blockedObj).size();
            } else {
                blockedCount = 0;
            }

            // Detect high ratio of non-daemon WAITING threads
            @SuppressWarnings("unchecked")
            Map<String, Integer> stateSummary = (Map<String, Integer>)
                    threadAnalysis.getOrDefault("state_summary", Map.of());
            int totalThreads = (int) threadAnalysis.getOrDefault("total", 0);
            int waitingCount = stateSummary.getOrDefault("WAITING", 0)
                    + stateSummary.getOrDefault("TIMED_WAITING", 0);
            if (totalThreads > 20 && waitingCount > totalThreads * 0.8) {
                findings.add(withTime(buildFinding("WARNING", "High Thread Wait Ratio",
                        waitingCount + "/" + totalThreads + " threads in WAITING state",
                        waitingCount + " out of " + totalThreads + " threads are in WAITING or TIMED_WAITING state. "
                                + "This may indicate thread pool exhaustion with idle threads awaiting work, "
                                + "or excessive use of blocking synchronization.",
                        List.of(Map.of("source", "thread_dump",
                                "detail", "WAITING: " + stateSummary.getOrDefault("WAITING", 0)
                                        + ", TIMED_WAITING: " + stateSummary.getOrDefault("TIMED_WAITING", 0))),
                        "Check if thread pools are properly sized. Review for connection pool leaks or misconfigured timeouts."),
                        dumpSnapshot));
            }

            // ── Datasource / Connection Pool exhaustion ────────────
            List<Map<String, Object>> dbWaiters = new ArrayList<>();
            List<Map<String, Object>> poolWaiters = new ArrayList<>();
            for (Map<String, Object> t : threads) {
                String state = (String) t.getOrDefault("thread_state", "");
                if (!"BLOCKED".equals(state) && !"WAITING".equals(state) && !"TIMED_WAITING".equals(state)) continue;
                @SuppressWarnings("unchecked")
                List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());
                String stackStr = String.join(" ", stack).toLowerCase();
                if (stackStr.contains("connectionpool") || stackStr.contains("datasource")
                        || stackStr.contains("getconnection") || stackStr.contains("pooledconnection")
                        || stackStr.contains("ironjacamar") || stackStr.contains("jca.core.connectionmanager")) {
                    dbWaiters.add(t);
                } else if (stackStr.contains("semaphore") || stackStr.contains("queue.take")
                        || stackStr.contains("blockingqueue") || stackStr.contains("threadpoolexecutor")) {
                    poolWaiters.add(t);
                }
            }
            if (dbWaiters.size() >= 2) {
                String dbSeverity = dbWaiters.size() > 5 ? "CRITICAL" : "WARNING";
                List<String> threadNames = dbWaiters.stream()
                        .map(t -> (String) t.get("thread_name")).limit(10).collect(Collectors.toList());
                findings.add(withTime(buildFinding(dbSeverity, "Connection Pool Exhaustion",
                        dbWaiters.size() + " threads waiting for DB connections",
                        dbWaiters.size() + " threads are waiting for database connections from the connection pool. "
                                + "This indicates the pool is exhausted — all connections are in use and new requests must wait. "
                                + "Threads: " + String.join(", ", threadNames.subList(0, Math.min(5, threadNames.size()))),
                        List.of(Map.of("source", "thread_dump",
                                "detail", dbWaiters.size() + " threads blocked on connection pool acquire")),
                        "Increase datasource max-pool-size, check for connection leaks (missing close() calls), "
                                + "review slow queries that hold connections too long. "
                                + "CLI: /subsystem=datasources/data-source=<name>:write-attribute(name=max-pool-size,value=<N>)"),
                        dumpSnapshot));
            }
            if (poolWaiters.size() >= 3) {
                findings.add(withTime(buildFinding("WARNING", "Thread Pool Saturation",
                        poolWaiters.size() + " threads waiting on pool/queue resources",
                        poolWaiters.size() + " threads are waiting on semaphores or blocking queues, "
                                + "suggesting thread pool saturation or resource throttling.",
                        List.of(Map.of("source", "thread_dump",
                                "detail", poolWaiters.size() + " threads in pool/queue wait")),
                        "Review thread pool sizing (io-worker task-max-threads, ejb3 thread-pool max-threads). "
                                + "Check if upstream load exceeds server capacity."),
                        dumpSnapshot));
            }

            // ── Deadlock detection ─────────────────────────────────
            Map<String, String> lockOwnership = new LinkedHashMap<>();
            Map<String, String> threadWaitsOn = new LinkedHashMap<>();
            for (Map<String, Object> t : threads) {
                String name = (String) t.get("thread_name");
                String waitLock = (String) t.get("lock_name");
                if (waitLock != null) threadWaitsOn.put(name, waitLock);
                @SuppressWarnings("unchecked")
                List<String> monitors = (List<String>) t.getOrDefault("locked_monitors", List.of());
                for (String mon : monitors) lockOwnership.put(mon, name);
            }
            Set<String> deadlockThreads = new LinkedHashSet<>();
            for (var entry : threadWaitsOn.entrySet()) {
                String threadA = entry.getKey();
                String lockA = entry.getValue();
                String ownerA = lockOwnership.get(lockA);
                if (ownerA != null && !ownerA.equals(threadA)) {
                    String lockB = threadWaitsOn.get(ownerA);
                    if (lockB != null) {
                        String ownerB = lockOwnership.get(lockB);
                        if (threadA.equals(ownerB)) {
                            deadlockThreads.add(threadA);
                            deadlockThreads.add(ownerA);
                        }
                    }
                }
            }
            if (!deadlockThreads.isEmpty()) {
                findings.add(withTime(buildFinding("CRITICAL", "Deadlock Detected",
                        deadlockThreads.size() + " threads in deadlock",
                        "Circular lock dependency detected between threads: "
                                + String.join(", ", deadlockThreads)
                                + ". These threads will never release their locks, causing permanent hang.",
                        List.of(Map.of("source", "thread_dump",
                                "detail", "Deadlocked threads: " + String.join(", ", deadlockThreads))),
                        "Restart the application to clear the deadlock. Fix the code to acquire locks in a consistent order."),
                        dumpSnapshot));
            }
        }

        // ── Slow API findings ───────────────────────────────────────
        for (Map<String, Object> api : slowApis) {
            long maxTime = (long) api.getOrDefault("max_time_ms", 0L);
            double avgTime = ((Number) api.getOrDefault("avg_time_ms", 0)).doubleValue();
            int slowReqs = (int) api.getOrDefault("slow_request_count", 0);
            int errorCount = (int) api.getOrDefault("error_count", 0);
            int clientErrors = (int) api.getOrDefault("client_error_count", 0); // 4xx
            int serverErrors = (int) api.getOrDefault("server_error_count", 0); // 5xx
            int hits = (int) api.getOrDefault("hit_count", 0);

            String endpoint = api.get("method") + " " + api.get("uri");
            String apiMethod = (String) api.get("method");
            String apiUri = (String) api.get("uri");

            boolean isSlow = maxTime >= 1000;
            boolean hasServerErrors = serverErrors > 0;
            boolean onlyClientErrors = !isSlow && !hasServerErrors && clientErrors > 0;

            // Nothing noteworthy: fast, no errors of any kind.
            if (!isSlow && errorCount == 0) continue;

            // Time range: when this endpoint's slow/error requests occurred in the access log
            List<String> apiTimes = new ArrayList<>();
            for (Map<String, Object> req : requests) {
                if (!Objects.equals(apiMethod, req.get("method"))) continue;
                if (!apiUri.equals(normalizeUri((String) req.getOrDefault("uri", "/")))) continue;
                long rt = ((Number) req.getOrDefault("response_time_ms", 0)).longValue();
                int st = ((Number) req.getOrDefault("status", 200)).intValue();
                if (rt > 1000 || st >= 400) {
                    Object ts = req.get("timestamp");
                    if (ts instanceof String s) apiTimes.add(s);
                }
            }
            Map<String, Object> apiRange = tsRange(apiTimes);

            // ── Fast endpoint with only client (4xx) errors → NOT a Slow API. ──
            // 4xx means the client sent a bad/unauthorized/not-found request; the server
            // handled it correctly and quickly. Surface it as an HTTP client-error pattern,
            // not a critical performance problem.
            if (onlyClientErrors) {
                String topCode = dominantStatus(api, 400, 499);
                boolean auth = "401".equals(topCode) || "403".equals(topCode);
                boolean notFound = "404".equals(topCode);
                // Auth failures and high-volume 4xx are worth a WARNING; a few 404s are INFO.
                String sev = (auth || clientErrors >= 10) ? "WARNING" : "INFO";
                String kind = notFound ? "not found (404)"
                        : auth ? "unauthorized/forbidden (" + topCode + ")"
                        : "client error (" + topCode + ")";
                String rec = notFound
                        ? "The endpoint does not exist or the URL/method is wrong. Check for a client calling a stale path, "
                          + "a missing deployment/route, or a scanner/health-check probing " + apiUri + "."
                        : auth
                        ? "Requests are being rejected by authentication/authorization. Verify credentials, tokens/session expiry, "
                          + "or a misconfigured security constraint. Repeated 401/403 can also indicate credential-stuffing."
                        : "The client is sending requests the server rejects with " + topCode
                          + ". Validate the request payload/parameters and client contract.";
                findings.add(withTime(buildFinding(sev, "HTTP Client Errors",
                        endpoint + " (" + clientErrors + "× " + topCode + ")",
                        "Endpoint " + endpoint + " returned " + clientErrors + " client error(s) out of "
                                + hits + " request(s), all handled quickly (max " + maxTime + "ms). "
                                + "Dominant status: " + kind + ". This is a client-side issue, not a server slowdown or fault.",
                        List.of(Map.of("source", "access_log",
                                "detail", String.format("%d/%d requests were %s, max=%dms, avg=%.0fms",
                                        clientErrors, hits, topCode, maxTime, avgTime))),
                        rec),
                        apiRange));
                continue;
            }

            // ── Genuine Slow API or server-error (5xx) finding. ──
            String severity;
            if (maxTime > 30000 || (serverErrors > 0 && serverErrors > hits * 0.5)) severity = "CRITICAL";
            else if (maxTime > 5000 || avgTime > 1000 || serverErrors > 0) severity = "WARNING";
            else severity = "INFO";

            List<Map<String, Object>> evidence = new ArrayList<>();
            evidence.add(Map.of("source", "access_log",
                    "detail", String.format("max=%dms, avg=%.0fms, %d slow/%d total, %d server(5xx)/%d client(4xx) errors",
                            maxTime, avgTime, slowReqs, hits, serverErrors, clientErrors)));

            @SuppressWarnings("unchecked")
            List<String> logErrors = (List<String>) api.getOrDefault("server_log_errors", List.of());
            if (!logErrors.isEmpty()) {
                evidence.add(Map.of("source", "server_log",
                        "detail", logErrors.size() + " error(s): " + logErrors.get(0)));
            }
            if (Boolean.TRUE.equals(api.get("gc_impact"))) {
                evidence.add(Map.of("source", "gc_log",
                        "detail", "GC pauses overlapped with " + api.get("gc_overlap_count") + " slow requests"));
            }
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> blockedDetails =
                    (List<Map<String, Object>>) api.getOrDefault("blocked_details", List.of());
            if (!blockedDetails.isEmpty()) {
                evidence.add(Map.of("source", "thread_dump",
                        "detail", blockedDetails.size() + " threads BLOCKED while handling this endpoint"));
            }

            StringBuilder rec = new StringBuilder();
            if (!blockedDetails.isEmpty())
                rec.append("Threads handling this endpoint are blocked — investigate lock contention. ");
            if (serverErrors > 0)
                rec.append(serverErrors + " request(s) failed with 5xx — check server.log for the exception behind these. ");
            if (!logErrors.isEmpty())
                rec.append("Check server.log errors associated with this endpoint. ");
            if (Boolean.TRUE.equals(api.get("gc_impact")))
                rec.append("GC pauses contribute to latency — review heap sizing and GC tuning. ");
            if (avgTime > 1000)
                rec.append("Average response time exceeds 1s — profile the endpoint for DB queries, external calls, or CPU-intensive logic.");

            String title = isSlow ? endpoint + " (max " + maxTime + "ms)"
                    : endpoint + " (" + serverErrors + "× 5xx)";
            String category = isSlow ? "Slow API" : "Server Errors (5xx)";
            findings.add(withTime(buildFinding(severity, category,
                    title,
                    "Endpoint " + endpoint + " has " + slowReqs + " slow requests (>5s) out of "
                            + hits + " total. Max response time: " + maxTime + "ms, avg: "
                            + String.format("%.0f", avgTime) + "ms."
                            + (serverErrors > 0 ? " " + serverErrors + " request(s) returned 5xx server errors." : "")
                            + (clientErrors > 0 ? " (" + clientErrors + " additional 4xx client error(s).)" : ""),
                    evidence,
                    rec.toString().strip()),
                    apiRange));
        }

        // ── Repeated slow endpoint pattern ─────────────────────────
        long totalSlowApis = slowApis.stream()
                .filter(a -> ((Number) a.getOrDefault("max_time_ms", 0L)).longValue() > 5000).count();
        if (totalSlowApis > 3) {
            boolean allHaveBlockedThreads = slowApis.stream()
                    .filter(a -> ((Number) a.getOrDefault("max_time_ms", 0L)).longValue() > 5000)
                    .allMatch(a -> {
                        @SuppressWarnings("unchecked")
                        List<?> bd = (List<?>) a.getOrDefault("blocked_details", List.of());
                        return !bd.isEmpty();
                    });
            if (allHaveBlockedThreads) {
                List<String> allSlowTimes = new ArrayList<>();
                for (Map<String, Object> req : requests) {
                    if (((Number) req.getOrDefault("response_time_ms", 0)).longValue() > 5000) {
                        Object ts = req.get("timestamp");
                        if (ts instanceof String s) allSlowTimes.add(s);
                    }
                }
                findings.add(withTime(buildFinding("CRITICAL", "System-Wide Contention",
                        totalSlowApis + " endpoints affected by thread blocking",
                        "Multiple API endpoints (" + totalSlowApis + ") are slow AND have blocked threads. "
                                + "This suggests a system-wide resource bottleneck (connection pool, shared lock, or overloaded subsystem) "
                                + "rather than individual endpoint issues.",
                        List.of(Map.of("source", "access_log + thread_dump",
                                "detail", totalSlowApis + " slow endpoints, all with BLOCKED threads")),
                        "Focus on the shared resource causing contention across endpoints. "
                                + "Common causes: undersized connection pool, global synchronization, or external dependency timeout."),
                        tsRange(allSlowTimes)));
            }
        }

        // ── GC findings ─────────────────────────────────────────────
        if (Boolean.TRUE.equals(gcAnalysis.get("provided"))) {
            double maxPause = ((Number) gcAnalysis.getOrDefault("max_pause_ms", 0)).doubleValue();
            int fullGcCount = ((Number) gcAnalysis.getOrDefault("full_gc_count", 0)).intValue();
            double totalPause = ((Number) gcAnalysis.getOrDefault("total_pause_ms", 0)).doubleValue();
            int totalPauses = ((Number) gcAnalysis.getOrDefault("total_pauses", 0)).intValue();

            if (maxPause > 500 || fullGcCount > 0) {
                String severity = maxPause > 2000 || fullGcCount > 3 ? "CRITICAL"
                        : maxPause > 500 || fullGcCount > 0 ? "WARNING" : "INFO";
                List<String> gcTimes = new ArrayList<>();
                for (Map<String, Object> g : gcEvents) {
                    if (((Number) g.getOrDefault("pause_ms", 0)).doubleValue() > 200) {
                        Object ts = g.get("timestamp");
                        if (ts instanceof String s) gcTimes.add(s);
                    }
                }
                Map<String, Object> gcRange = tsRange(gcTimes);
                findings.add(withTime(buildFinding(severity, "GC Pressure",
                        (fullGcCount > 0 ? fullGcCount + " Full GCs, " : "")
                                + "max pause " + String.format("%.0f", maxPause) + "ms",
                        totalPauses + " GC pauses totaling " + String.format("%.0f", totalPause) + "ms. "
                                + "Max single pause: " + String.format("%.1f", maxPause) + "ms. "
                                + fullGcCount + " Full GC events."
                                + (maxPause > 1000 ? " Pauses over 1 second cause visible application stalls." : ""),
                        List.of(Map.of("source", "gc_log",
                                "detail", String.format("%d pauses, %d Full GCs, max=%.1fms, total=%.0fms",
                                        totalPauses, fullGcCount, maxPause, totalPause))),
                        fullGcCount > 0
                                ? "Full GCs indicate heap pressure. Increase -Xmx, review for memory leaks, or tune GC parameters."
                                : "Review GC log for patterns. Consider adjusting -XX:MaxGCPauseMillis or switching to ZGC/Shenandoah for lower latency."),
                        gcRange));
            }
        }

        // ── Server log error pattern findings ───────────────────────
        if (!logEntries.isEmpty()) {
            Map<String, Integer> exceptionCounts = new LinkedHashMap<>();
            Map<String, String> exceptionSamples = new LinkedHashMap<>();
            Map<String, List<String>> exceptionTimes = new LinkedHashMap<>();
            for (Map<String, Object> e : logEntries) {
                if (!"ERROR".equals(e.get("level")) && !"FATAL".equals(e.get("level"))) continue;
                String exCls = (String) e.get("exception_class");
                if (exCls == null) exCls = "Error (no exception)";
                exceptionCounts.merge(exCls, 1, Integer::sum);
                exceptionSamples.putIfAbsent(exCls, truncate((String) e.get("message"), 150));
                Object ts = e.get("timestamp");
                if (ts instanceof String s) {
                    exceptionTimes.computeIfAbsent(exCls, k -> new ArrayList<>()).add(s);
                }
            }

            List<Map.Entry<String, Integer>> sortedExceptions = exceptionCounts.entrySet().stream()
                    .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                    .collect(Collectors.toList());

            for (var entry : sortedExceptions) {
                if (entry.getValue() < 2) continue;
                String severity = entry.getValue() > 20 ? "CRITICAL"
                        : entry.getValue() > 5 ? "WARNING" : "INFO";

                findings.add(withTime(buildFinding(severity, "Error Pattern",
                        entry.getKey() + " (" + entry.getValue() + " occurrences)",
                        "Exception " + entry.getKey() + " occurred " + entry.getValue()
                                + " times in server.log. Sample: " + exceptionSamples.get(entry.getKey()),
                        List.of(Map.of("source", "server_log",
                                "detail", entry.getValue() + " occurrences of " + entry.getKey())),
                        "Investigate the root cause of " + entry.getKey()
                                + ". Check if it correlates with specific API endpoints or resource exhaustion."),
                        tsRange(exceptionTimes.getOrDefault(entry.getKey(), List.of()))));
            }
        }

        // ── JDBC Spy findings ────────────────────────────────────────
        if (Boolean.TRUE.equals(jdbcSpyAnalysis.get("provided"))) {
            int slowQCount = ((Number) jdbcSpyAnalysis.getOrDefault("slow_queries_count", 0)).intValue();
            double maxExec = ((Number) jdbcSpyAnalysis.getOrDefault("max_exec_ms", 0)).doubleValue();
            int inflightCount = ((Number) jdbcSpyAnalysis.getOrDefault("inflight_queries", 0)).intValue();

            if (slowQCount > 0) {
                String severity = maxExec > 5000 || slowQCount > 20 ? "CRITICAL"
                        : maxExec > 1000 || slowQCount > 5 ? "WARNING" : "INFO";
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> slowQs = (List<Map<String, Object>>)
                        jdbcSpyAnalysis.getOrDefault("slow_queries", List.of());
                String topSql = slowQs.isEmpty() ? "" : truncate((String) slowQs.get(0).getOrDefault("sql", ""), 100);

                findings.add(withTime(buildFinding(severity, "Slow JDBC Queries",
                        slowQCount + " queries >100ms (max " + String.format("%.0f", maxExec) + "ms)",
                        slowQCount + " JDBC queries exceeded 100ms execution time. "
                                + "Slowest query: " + topSql + " at " + String.format("%.0f", maxExec) + "ms. "
                                + "Slow queries can cause connection pool exhaustion and request timeouts.",
                        List.of(Map.of("source", "jdbc_spy_log",
                                "detail", slowQCount + " slow queries, max=" + String.format("%.0f", maxExec) + "ms")),
                        "Review slow queries for missing indexes. Use EXPLAIN to check query plans. "
                                + "Consider adding query timeout configurations."),
                        tsRangeFrom(slowQs, "timestamp")));
            }

            if (inflightCount > 0) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> inflightQs = (List<Map<String, Object>>)
                        jdbcSpyAnalysis.getOrDefault("inflight", List.of());
                findings.add(withTime(buildFinding(inflightCount > 3 ? "WARNING" : "INFO", "Inflight JDBC Queries",
                        inflightCount + " queries still executing",
                        inflightCount + " JDBC queries were executing at the time the log was captured. "
                                + "These may represent long-running or stuck queries.",
                        List.of(Map.of("source", "jdbc_spy_log",
                                "detail", inflightCount + " queries without close()")),
                        "Check for queries without proper resource cleanup. Review connection pool blocking-timeout-wait-millis."),
                        tsRangeFrom(inflightQs, "timestamp")));
            }
        }

        // ── Transaction findings ────────────────────────────────────
        if (Boolean.TRUE.equals(txAnalysis.get("provided"))) {
            double abortRate = ((Number) txAnalysis.getOrDefault("abort_rate_percent", 0)).doubleValue();
            int abortedCount = ((Number) txAnalysis.getOrDefault("aborted", 0)).intValue();
            int txInflight = ((Number) txAnalysis.getOrDefault("inflight", 0)).intValue();
            double maxDuration = ((Number) txAnalysis.getOrDefault("max_duration_ms", 0)).doubleValue();

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> txList = (List<Map<String, Object>>)
                    txAnalysis.getOrDefault("transactions", List.of());
            List<String> abortedTimes = new ArrayList<>();
            List<String> inflightTimes = new ArrayList<>();
            for (Map<String, Object> tx : txList) {
                String outcome = (String) tx.getOrDefault("outcome", "");
                Object bt = tx.get("begin_time");
                if (!(bt instanceof String s)) continue;
                if ("ABORTED".equals(outcome)) abortedTimes.add(s);
                else if ("IN_FLIGHT".equals(outcome)) inflightTimes.add(s);
            }

            if (abortRate > 5 || abortedCount > 3) {
                String severity = abortRate > 20 || abortedCount > 10 ? "CRITICAL"
                        : abortRate > 10 || abortedCount > 5 ? "WARNING" : "INFO";
                findings.add(withTime(buildFinding(severity, "High Transaction Abort Rate",
                        String.format("%.1f%% abort rate (%d aborted)", abortRate, abortedCount),
                        "Transaction abort rate of " + String.format("%.1f%%", abortRate)
                                + " (" + abortedCount + " aborted transactions). "
                                + "High abort rates indicate application errors, timeout issues, or resource contention.",
                        List.of(Map.of("source", "tx_log",
                                "detail", abortedCount + " aborted, abort rate=" + String.format("%.1f%%", abortRate))),
                        "Check for application exceptions within transactions. Review transaction timeout settings "
                                + "(default-timeout in transactions subsystem). Investigate if datasource issues cause rollbacks."),
                        tsRange(abortedTimes)));
            }

            if (maxDuration > 5000) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> slowTxs = (List<Map<String, Object>>)
                        txAnalysis.getOrDefault("slow_transactions", List.of());
                String severity = maxDuration > 30000 ? "CRITICAL" : "WARNING";
                findings.add(withTime(buildFinding(severity, "Slow Transactions",
                        slowTxs.size() + " transactions >1s (max " + String.format("%.0f", maxDuration) + "ms)",
                        slowTxs.size() + " transactions exceeded 1 second. "
                                + "Longest transaction: " + String.format("%.0f", maxDuration) + "ms. "
                                + "Long transactions hold locks and connections, blocking other operations.",
                        List.of(Map.of("source", "tx_log",
                                "detail", slowTxs.size() + " slow txs, max=" + String.format("%.0f", maxDuration) + "ms")),
                        "Profile slow transactions to identify expensive operations. "
                                + "Check for N+1 query patterns, large batch operations, or external service calls within TX boundaries."),
                        tsRangeFrom(slowTxs, "begin_time")));
            }

            if (txInflight > 3) {
                findings.add(withTime(buildFinding("WARNING", "Inflight Transactions",
                        txInflight + " transactions still in progress",
                        txInflight + " transactions were still in progress at the time of capture. "
                                + "This may indicate stuck transactions or very long-running operations.",
                        List.of(Map.of("source", "tx_log",
                                "detail", txInflight + " inflight transactions")),
                        "Check for stuck threads holding open transactions. Review transaction timeouts."),
                        tsRange(inflightTimes)));
            }
        }

        // ── Exception-to-endpoint correlation ──────────────────────
        if (!logEntries.isEmpty() && !requests.isEmpty()) {
            Map<String, Set<String>> threadToEndpoints = new LinkedHashMap<>();
            for (Map<String, Object> req : requests) {
                String thread = (String) req.get("thread");
                String uri = normalizeUri((String) req.getOrDefault("uri", "/"));
                String method = (String) req.getOrDefault("method", "?");
                if (thread != null) {
                    threadToEndpoints.computeIfAbsent(thread, k -> new LinkedHashSet<>())
                            .add(method + " " + uri);
                }
            }
            Map<String, Set<String>> exceptionEndpoints = new LinkedHashMap<>();
            for (Map<String, Object> e : logEntries) {
                if (!"ERROR".equals(e.get("level")) && !"FATAL".equals(e.get("level"))) continue;
                String exCls = (String) e.get("exception_class");
                if (exCls == null) continue;
                String thread = (String) e.get("thread");
                if (thread != null && threadToEndpoints.containsKey(thread)) {
                    exceptionEndpoints.computeIfAbsent(exCls, k -> new LinkedHashSet<>())
                            .addAll(threadToEndpoints.get(thread));
                }
            }
            for (var exEntry : exceptionEndpoints.entrySet()) {
                if (exEntry.getValue().size() > 1) {
                    List<String> exTimes = new ArrayList<>();
                    for (Map<String, Object> e : logEntries) {
                        if (!"ERROR".equals(e.get("level")) && !"FATAL".equals(e.get("level"))) continue;
                        if (exEntry.getKey().equals(e.get("exception_class"))) {
                            Object ts = e.get("timestamp");
                            if (ts instanceof String s) exTimes.add(s);
                        }
                    }
                    findings.add(withTime(buildFinding("WARNING", "Cross-Endpoint Exception",
                            exEntry.getKey() + " affects " + exEntry.getValue().size() + " endpoints",
                            "Exception " + exEntry.getKey() + " occurs across multiple endpoints: "
                                    + String.join(", ", exEntry.getValue())
                                    + ". This suggests a shared dependency or infrastructure issue rather than endpoint-specific bugs.",
                            List.of(Map.of("source", "server_log + access_log",
                                    "detail", "Affects endpoints: " + String.join(", ", exEntry.getValue()))),
                            "Investigate the shared dependency (database, external service, shared component) "
                                    + "that " + exEntry.getKey() + " originates from."),
                            tsRange(exTimes)));
                }
            }
        }

        return findings;
    }

    // ═══════════════════════════════════════════════════════════════════
    // Cross-file correlation — stitch one request's story across every log.
    //
    // The join key is the WORKER THREAD NAME (e.g. "default task-4"), which
    // appears in the access log, jdbc-spy log, transaction log, server.log,
    // and the thread dump — combined with a TIME WINDOW around the request.
    // For each slow/failed request we gather the JDBC queries, transactions,
    // server-log errors, GC pauses and thread stack that belong to it, so the
    // Findings tab can show the full picture instead of six disconnected facts.
    // ═══════════════════════════════════════════════════════════════════

    // Wall-clock epoch millis for a "yyyy-MM-dd HH:mm:ss,SSS" (or with 'T'/'.') timestamp,
    // treated as UTC so it lines up with access-log timestamp_local_ms and parseDumpTimeMs.
    private long wallMs(String ts) {
        if (ts == null || ts.isBlank()) return -1;
        String t = ts.strip().replace('T', ' ').replace(',', '.');
        // Drop anything past the millis (zone offset etc.)
        for (DateTimeFormatter fmt : List.of(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))) {
            try {
                return LocalDateTime.parse(t.substring(0, Math.min(t.length(),
                                fmt.toString().contains("SSS") ? 23 : 19)), fmt)
                        .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            } catch (Exception ignored) {}
        }
        return -1;
    }

    private List<Map<String, Object>> correlateRequestTraces(
            List<Map<String, Object>> requests,
            List<Map<String, Object>> spyQueries,
            List<Map<String, Object>> txEntries,
            List<Map<String, Object>> logEntries,
            List<Map<String, Object>> gcEvents,
            Map<String, Map<String, Object>> threadStacks,
            String threadDumpTime) {

        List<Map<String, Object>> findings = new ArrayList<>();
        if (requests.isEmpty()) return findings;

        final long TOL = 1000; // ms slack on each side of the request window

        // Candidate requests: slow (>1s) or server error (>=500), highest impact first.
        List<Map<String, Object>> candidates = requests.stream()
                .filter(r -> r.get("thread") != null)
                .filter(r -> ((Number) r.getOrDefault("response_time_ms", 0)).longValue() >= 1000
                        || ((Number) r.getOrDefault("status", 200)).intValue() >= 500)
                .sorted((a, b) -> Long.compare(
                        ((Number) b.getOrDefault("response_time_ms", 0)).longValue(),
                        ((Number) a.getOrDefault("response_time_ms", 0)).longValue()))
                .limit(12)
                .collect(Collectors.toList());

        for (Map<String, Object> req : candidates) {
            String thread = (String) req.get("thread");
            long rt = ((Number) req.getOrDefault("response_time_ms", 0)).longValue();
            int status = ((Number) req.getOrDefault("status", 200)).intValue();
            long start = ((Number) req.getOrDefault("timestamp_local_ms", 0L)).longValue();
            long lo = (start > 0 ? start - TOL : Long.MIN_VALUE);
            long hi = (start > 0 ? start + rt + TOL : Long.MAX_VALUE);

            // ── JDBC queries on this thread during the window ──────────
            List<Map<String, Object>> qList = new ArrayList<>();
            double dbTotal = 0;
            double dbSlowest = 0;
            for (Map<String, Object> q : spyQueries) {
                if (!thread.equals(q.get("thread"))) continue;
                long qms = wallMs((String) q.get("timestamp"));
                if (start > 0 && qms > 0 && (qms < lo || qms > hi)) continue;
                double ex = ((Number) q.getOrDefault("exec_time_ms", 0)).doubleValue();
                if (ex > 0) { dbTotal += ex; dbSlowest = Math.max(dbSlowest, ex); }
                Map<String, Object> qc = new LinkedHashMap<>();
                qc.put("sql", truncate((String) q.getOrDefault("sql", ""), 160));
                qc.put("exec_time_ms", ex);
                qc.put("datasource", q.get("datasource"));
                qc.put("timestamp", q.get("timestamp"));
                qc.put("inflight", q.getOrDefault("inflight", false));
                qList.add(qc);
            }

            // ── Transactions on this thread during the window ──────────
            List<Map<String, Object>> txList = new ArrayList<>();
            for (Map<String, Object> tx : txEntries) {
                if (!thread.equals(tx.get("thread"))) continue;
                long bms = wallMs((String) tx.get("begin_time"));
                if (start > 0 && bms > 0 && (bms < lo || bms > hi)) continue;
                Map<String, Object> tc = new LinkedHashMap<>();
                tc.put("tx_id", tx.get("tx_id"));
                tc.put("outcome", tx.getOrDefault("outcome", "UNKNOWN"));
                tc.put("duration_ms", tx.getOrDefault("duration_ms", 0.0));
                tc.put("begin_time", tx.get("begin_time"));
                txList.add(tc);
            }

            // ── Server-log errors/warnings on this thread during window ─
            List<Map<String, Object>> errList = new ArrayList<>();
            for (Map<String, Object> e : logEntries) {
                if (!thread.equals(e.get("thread"))) continue;
                String lvl = (String) e.getOrDefault("level", "");
                if (!"ERROR".equals(lvl) && !"FATAL".equals(lvl) && !"WARN".equals(lvl)) continue;
                long ems = wallMs((String) e.get("timestamp"));
                if (start > 0 && ems > 0 && (ems < lo || ems > hi)) continue;
                Map<String, Object> ec = new LinkedHashMap<>();
                ec.put("level", lvl);
                ec.put("exception_class", e.get("exception_class"));
                ec.put("message", truncate((String) e.getOrDefault("message", ""), 160));
                ec.put("timestamp", e.get("timestamp"));
                errList.add(ec);
            }

            // ── GC pauses overlapping the window ───────────────────────
            int gcOverlap = 0;
            double gcPauseMs = 0;
            for (Map<String, Object> g : gcEvents) {
                if (start <= 0) break;
                long gms = wallMs((String) g.get("timestamp"));
                if (gms <= 0 || gms < lo || gms > hi) continue;
                double p = ((Number) g.getOrDefault("pause_ms", 0)).doubleValue();
                if (p > 0) { gcOverlap++; gcPauseMs += p; }
            }

            // ── Thread stack from the selected dump snapshot ───────────
            // The dump is a single moment; its stack only reflects THIS request if the
            // dump was captured while the request was still in flight.
            Map<String, Object> stackInfo = null;
            Map<String, Object> st = threadStacks.get(thread);
            if (st != null) {
                @SuppressWarnings("unchecked")
                List<String> frames = (List<String>) st.getOrDefault("stack_trace", List.of());
                long dumpMs = wallMs(threadDumpTime);
                boolean during = start > 0 && dumpMs > 0 && dumpMs >= lo && dumpMs <= hi;
                stackInfo = new LinkedHashMap<>();
                stackInfo.put("thread_name", thread);
                stackInfo.put("thread_state", st.getOrDefault("thread_state", "UNKNOWN"));
                stackInfo.put("top_frames", frames.subList(0, Math.min(6, frames.size())));
                stackInfo.put("during_request", during);
                stackInfo.put("dump_time", threadDumpTime);
            }

            boolean hasRelation = !qList.isEmpty() || !txList.isEmpty()
                    || !errList.isEmpty() || gcOverlap > 0 || stackInfo != null;
            if (!hasRelation) continue; // nothing to correlate → Slow API finding already covers it

            String endpoint = req.get("method") + " " + req.getOrDefault("uri", "/");

            // Severity: 5xx or >30s critical; >5s warning; else info.
            String severity = (status >= 500 || rt > 30000) ? "CRITICAL"
                    : (rt > 5000 || !errList.isEmpty()) ? "WARNING" : "INFO";

            // Build the human narrative + a "prime suspect".
            StringBuilder desc = new StringBuilder();
            desc.append("Request ").append(endpoint).append(" took ").append(rt).append("ms");
            if (status >= 400) desc.append(" and returned HTTP ").append(status);
            desc.append(", handled by thread \"").append(thread).append("\". ");

            String suspect = null;
            if (dbTotal > 0) {
                long pct = rt > 0 ? Math.round(dbTotal / rt * 100) : 0;
                desc.append(String.format("%d JDBC quer%s ran on this thread totaling %.0fms",
                        qList.size(), qList.size() == 1 ? "y" : "ies", dbTotal));
                if (rt > 0) desc.append(String.format(" (~%d%% of the request time)", pct));
                desc.append(". ");
                if (pct >= 50) suspect = "Database — queries account for ~" + pct + "% of the response time";
            }
            if (!txList.isEmpty()) {
                long aborted = txList.stream().filter(t -> "ABORTED".equals(t.get("outcome"))).count();
                desc.append(txList.size()).append(" transaction(s) on this thread");
                if (aborted > 0) { desc.append(" (").append(aborted).append(" ABORTED)"); if (suspect == null) suspect = "Transaction rollback"; }
                desc.append(". ");
            }
            if (!errList.isEmpty()) {
                Map<String, Object> e0 = errList.get(0);
                desc.append(errList.size()).append(" server-log error(s) on this thread; first: ")
                        .append(e0.get("exception_class") != null ? e0.get("exception_class") : e0.get("level"))
                        .append(". ");
                if (suspect == null) suspect = "Exception: " + (e0.get("exception_class") != null ? e0.get("exception_class") : "see server.log");
            }
            if (gcOverlap > 0) {
                desc.append(String.format("%d GC pause(s) (%.0fms) overlapped this request. ", gcOverlap, gcPauseMs));
                if (suspect == null && gcPauseMs > rt * 0.3) suspect = "GC pauses during the request";
            }
            if (stackInfo != null) {
                desc.append("Thread was ").append(stackInfo.get("thread_state"))
                        .append(" in the thread dump.");
            }
            if (suspect == null) suspect = "Application logic on thread " + thread;

            List<Map<String, Object>> evidence = new ArrayList<>();
            evidence.add(Map.of("source", "access_log",
                    "detail", endpoint + " → " + status + " in " + rt + "ms (thread " + thread + ")"));
            if (!qList.isEmpty()) evidence.add(Map.of("source", "jdbc_spy_log",
                    "detail", String.format("%d queries, %.0fms DB time, slowest %.0fms", qList.size(), dbTotal, dbSlowest)));
            if (!txList.isEmpty()) evidence.add(Map.of("source", "tx_log",
                    "detail", txList.size() + " transaction(s): " + txList.stream()
                            .map(t -> String.valueOf(t.get("outcome"))).distinct().collect(Collectors.joining(", "))));
            if (!errList.isEmpty()) evidence.add(Map.of("source", "server_log",
                    "detail", errList.size() + " error(s), e.g. " + errList.get(0).get("message")));
            if (gcOverlap > 0) evidence.add(Map.of("source", "gc_log",
                    "detail", String.format("%d GC pause(s), %.0fms during request", gcOverlap, gcPauseMs)));
            if (stackInfo != null) evidence.add(Map.of("source", "thread_dump",
                    "detail", "Thread \"" + thread + "\" state=" + stackInfo.get("thread_state")));

            // Recommendation targeted at the prime suspect.
            String rec;
            if (suspect.startsWith("Database")) {
                rec = "The database is the dominant cost. Review the slowest SQL below for missing indexes (EXPLAIN), "
                        + "N+1 patterns, or lock waits. Consider a query timeout and connection-pool sizing.";
            } else if (suspect.startsWith("Transaction")) {
                rec = "A transaction on this thread rolled back. Inspect the exception within the TX boundary and datasource health.";
            } else if (suspect.startsWith("Exception")) {
                rec = "An exception was logged on the same thread while serving this request. Fix the root cause in server.log; it is the likely failure driver.";
            } else if (suspect.startsWith("GC")) {
                rec = "GC pauses stalled this request. Review heap sizing (-Xmx) and GC tuning; correlate with the GC Pressure finding.";
            } else {
                rec = "No DB/GC/exception dominated — profile the endpoint's application code on thread " + thread + " (CPU, external calls, locks).";
            }

            Map<String, Object> finding = buildFinding(severity, "Correlated Request Trace",
                    endpoint + " (" + rt + "ms" + (status >= 400 ? ", HTTP " + status : "") + ")",
                    desc.toString().strip(), evidence, rec);
            finding.put("prime_suspect", suspect);

            // Structured trace for the UI to render the end-to-end chain.
            Map<String, Object> trace = new LinkedHashMap<>();
            Map<String, Object> reqInfo = new LinkedHashMap<>();
            reqInfo.put("method", req.get("method"));
            reqInfo.put("uri", req.get("uri"));
            reqInfo.put("status", status);
            reqInfo.put("response_time_ms", rt);
            reqInfo.put("timestamp", req.get("timestamp"));
            // Wall-clock time matching the other sources (jdbc/tx/gc/dump are all local).
            if (start > 0) {
                reqInfo.put("time_local", java.time.Instant.ofEpochMilli(start)
                        .atZone(java.time.ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            }
            reqInfo.put("thread", thread);
            trace.put("request", reqInfo);
            Map<String, Object> jdbc = new LinkedHashMap<>();
            jdbc.put("count", qList.size());
            jdbc.put("total_ms", Math.round(dbTotal));
            jdbc.put("slowest_ms", Math.round(dbSlowest));
            qList.sort((a, b) -> Double.compare(
                    ((Number) b.getOrDefault("exec_time_ms", 0)).doubleValue(),
                    ((Number) a.getOrDefault("exec_time_ms", 0)).doubleValue()));
            jdbc.put("queries", qList.subList(0, Math.min(8, qList.size())));
            trace.put("jdbc", jdbc);
            trace.put("transactions", txList);
            trace.put("errors", errList);
            Map<String, Object> gc = new LinkedHashMap<>();
            gc.put("overlap_count", gcOverlap);
            gc.put("total_pause_ms", Math.round(gcPauseMs));
            trace.put("gc", gc);
            trace.put("thread_stack", stackInfo);
            finding.put("trace", trace);

            Map<String, Object> range = new LinkedHashMap<>();
            if (start > 0) {
                range.put("start_ms", lo);
                range.put("end_ms", hi);
            }
            findings.add(withTime(finding, range));
        }

        return findings;
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

    // Collapse findings describing the same issue into one entry with an occurrence count.
    // Two findings are "the same" when their category matches and their titles are equal
    // once variable numbers (ms, counts, timestamps) are masked — so
    // "POST /api/files/upload (120036ms)" and "(120031ms)" merge into one.
    private List<Map<String, Object>> dedupeFindings(List<Map<String, Object>> findings) {
        Map<String, Map<String, Object>> byKey = new LinkedHashMap<>();
        for (Map<String, Object> f : findings) {
            String cat = String.valueOf(f.getOrDefault("category", ""));
            String title = String.valueOf(f.getOrDefault("title", ""));
            String norm = title.replaceAll("\\d+", "#"); // mask numbers so only the shape matters
            String key = cat + "||" + norm;

            Map<String, Object> rep = byKey.get(key);
            if (rep == null) {
                f.put("occurrences", 1);
                byKey.put(key, f);
            } else {
                int occ = ((Number) rep.getOrDefault("occurrences", 1)).intValue() + 1;
                // If the incoming duplicate carries a more informative trace (a thread stack
                // captured while its request was in flight), promote it to representative.
                if (hasDuringRequestStack(f) && !hasDuringRequestStack(rep)) {
                    f.put("occurrences", occ);
                    mergeTimeRange(f, rep);
                    byKey.put(key, f);
                } else {
                    rep.put("occurrences", occ);
                    // Widen the "when in the logs" range to span every occurrence.
                    mergeTimeRange(rep, f);
                }
            }
        }
        return new ArrayList<>(byKey.values());
    }

    @SuppressWarnings("unchecked")
    private boolean hasDuringRequestStack(Map<String, Object> f) {
        Object t = f.get("trace");
        if (!(t instanceof Map)) return false;
        Object s = ((Map<String, Object>) t).get("thread_stack");
        return s instanceof Map && Boolean.TRUE.equals(((Map<String, Object>) s).get("during_request"));
    }

    // Merge finding {@code src}'s time_range into {@code dst}'s, widening first/last and summing counts.
    @SuppressWarnings("unchecked")
    private void mergeTimeRange(Map<String, Object> dst, Map<String, Object> src) {
        Object dr = dst.get("time_range"), sr = src.get("time_range");
        if (!(sr instanceof Map)) return;
        if (!(dr instanceof Map)) { dst.put("time_range", sr); return; }
        Map<String, Object> d = (Map<String, Object>) dr;
        Map<String, Object> s = (Map<String, Object>) sr;
        String df = (String) d.get("first"), sf = (String) s.get("first");
        String dl = (String) d.get("last"), sl = (String) s.get("last");
        if (sf != null && (df == null || sf.compareTo(df) < 0)) d.put("first", sf);
        if (sl != null && (dl == null || sl.compareTo(dl) > 0)) d.put("last", sl);
        int dc = ((Number) d.getOrDefault("count", 0)).intValue();
        int sc = ((Number) s.getOrDefault("count", 0)).intValue();
        if (dc + sc > 0) d.put("count", dc + sc);
    }

    private Map<String, Object> buildFinding(String severity, String category,
                                              String title, String description,
                                              List<Map<String, Object>> evidence,
                                              String recommendation) {
        Map<String, Object> finding = new LinkedHashMap<>();
        finding.put("severity", severity);
        finding.put("category", category);
        finding.put("title", title);
        finding.put("description", description);
        finding.put("evidence", evidence);
        finding.put("recommendation", recommendation);
        return finding;
    }

    // ── Timestamp correlation helpers ───────────────────────────────────
    // Thread dumps usually start with a header line like "2024-01-15 10:30:00".
    private static final Pattern DUMP_TIME_RE = Pattern.compile(
            "(\\d{4}-\\d{2}-\\d{2})[ T](\\d{2}:\\d{2}:\\d{2})");

    /**
     * Parse the thread-dump capture time to a zone-agnostic wall-clock epoch (naive time
     * treated as UTC), or 0 if unknown. Compared against a request's {@code timestamp_local_ms}
     * so the dump (no zone) and the access log (own offset) are matched on wall-clock time —
     * both are written in the server's local time, so this is offset-independent.
     */
    private long parseDumpTimeMs(String threadDumpText) {
        String t = parseThreadDumpTime(threadDumpText);
        if (t == null) return 0L;
        for (DateTimeFormatter fmt : List.of(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))) {
            try {
                return LocalDateTime.parse(t.strip(), fmt)
                        .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            } catch (Exception ignored) {}
        }
        return 0L;
    }

    /** Parse the capture time from the first few lines of a thread dump, if present. */
    private String parseThreadDumpTime(String text) {
        if (text == null || text.isBlank()) return null;
        String[] lines = text.split("\n");
        for (int i = 0; i < Math.min(6, lines.length); i++) {
            Matcher m = DUMP_TIME_RE.matcher(lines[i]);
            if (m.find()) return m.group(1) + " " + m.group(2);
        }
        return null;
    }

    /** Build a {first,last,count} time range from a list of timestamp strings. */
    /** Most frequent HTTP status code within [lo, hi] for a slow-API entry; "" if none. */
    @SuppressWarnings("unchecked")
    private String dominantStatus(Map<String, Object> api, int lo, int hi) {
        Map<String, Integer> sc = (Map<String, Integer>) api.get("status_codes");
        if (sc == null) return "";
        String best = "";
        int bestCount = -1;
        for (Map.Entry<String, Integer> e : sc.entrySet()) {
            int code;
            try {
                code = Integer.parseInt(e.getKey().trim());
            } catch (NumberFormatException ex) {
                continue;
            }
            if (code < lo || code > hi) continue;
            if (e.getValue() > bestCount) {
                bestCount = e.getValue();
                best = e.getKey();
            }
        }
        return best;
    }

    private Map<String, Object> tsRange(List<String> timestamps) {
        List<String> ts = timestamps.stream()
                .filter(t -> t != null && !t.isBlank())
                .sorted()
                .collect(Collectors.toList());
        if (ts.isEmpty()) return null;
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("first", ts.get(0));
        r.put("last", ts.get(ts.size() - 1));
        r.put("count", ts.size());
        return r;
    }

    /** Extract timestamps from a list of event maps under the given key and build a range. */
    private Map<String, Object> tsRangeFrom(List<Map<String, Object>> items, String key) {
        if (items == null || items.isEmpty()) return null;
        List<String> ts = new ArrayList<>();
        for (Map<String, Object> it : items) {
            Object v = it.get(key);
            if (v instanceof String s && !s.isBlank()) ts.add(s);
        }
        return tsRange(ts);
    }

    /** A single-point range representing a thread-dump snapshot moment. */
    private Map<String, Object> snapshotRange(String t) {
        if (t == null || t.isBlank()) return null;
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("snapshot", t);
        return r;
    }

    /** Attach a time range to a finding (no-op if range is null) and return it. */
    private Map<String, Object> withTime(Map<String, Object> finding, Map<String, Object> range) {
        if (range != null && !range.isEmpty()) finding.put("time_range", range);
        return finding;
    }

    private String normalizeUri(String uri) {
        // Replace numeric path segments and UUIDs with placeholders
        return uri.replaceAll("/\\d+", "/{id}")
                .replaceAll("/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "/{uuid}")
                .replaceAll("\\?.*", "");
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }

    private String shortClass(String fqcn) {
        if (fqcn == null) return "unknown";
        int dot = fqcn.lastIndexOf('.');
        return dot >= 0 ? fqcn.substring(dot + 1) : fqcn;
    }

    private String inlineStateToJava(String s) {
        s = s.toLowerCase().trim();
        if ("runnable".equals(s)) return "RUNNABLE";
        if (s.contains("blocked") || s.contains("monitor entry")) return "BLOCKED";
        if (s.contains("waiting") || s.contains("sleeping")) return "WAITING";
        return "UNKNOWN";
    }

    private int severityOrder(String severity) {
        return switch (severity) {
            case "CRITICAL" -> 0;
            case "WARNING" -> 1;
            case "INFO" -> 2;
            default -> 3;
        };
    }

    private static int safeParseInt(String s, int defaultVal) {
        try { return Integer.parseInt(s); }
        catch (Exception e) { return defaultVal; }
    }

    private static long safeParseLong(String s, long defaultVal) {
        try { return Long.parseLong(s); }
        catch (Exception e) { return defaultVal; }
    }

    // ═══════════════════════════════════════════════════════════════════
    // AI-powered RCA endpoint
    // ═══════════════════════════════════════════════════════════════════

    @POST
    @Path("/rca/ai-analyze")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response aiAnalyze(Map<String, Object> input) {
        LOG.info("AI-powered RCA analysis requested");
        if (input == null) input = Map.of();
        String threadDumpText = (String) input.get("thread_dump");
        String serverLogText = (String) input.get("server_log");
        String accessLogText = (String) input.get("access_log");
        String gcLogText = (String) input.get("gc_log");

        List<String> filesProvided = new ArrayList<>();

        // Analyze one snapshot (default latest) from a multi-dump file, same as /rca/analyze.
        List<Map<String, Object>> dumpSnapshots = splitThreadDumps(threadDumpText);
        int selectedDumpIndex = pickDumpIndex(input.get("dump_index"), dumpSnapshots.size());
        if (!dumpSnapshots.isEmpty()) {
            threadDumpText = (String) dumpSnapshots.get(selectedDumpIndex).get("text");
        }

        List<Map<String, Object>> threads = List.of();
        if (threadDumpText != null && !threadDumpText.isBlank()) {
            threads = parseThreadDump(threadDumpText);
            filesProvided.add("thread_dump (" + threads.size() + " threads)");
        }

        List<Map<String, Object>> requests = List.of();
        if (accessLogText != null && !accessLogText.isBlank()) {
            requests = parseAccessLog(accessLogText);
            filesProvided.add("access_log (" + requests.size() + " requests)");
        }

        List<Map<String, Object>> logEntries = List.of();
        if (serverLogText != null && !serverLogText.isBlank()) {
            logEntries = parseServerLog(serverLogText);
            filesProvided.add("server_log (" + logEntries.size() + " entries)");
        }

        List<Map<String, Object>> gcEvents = List.of();
        if (gcLogText != null && !gcLogText.isBlank()) {
            gcEvents = parseGcLog(gcLogText);
            filesProvided.add("gc_log (" + gcEvents.size() + " events)");
        }

        if (filesProvided.isEmpty()) {
            return Response.status(400)
                    .entity("No log data provided. Upload at least one file.")
                    .build();
        }

        try {
            Map<String, Object> threadAnalysis = analyzeThreads(threads);
            List<Map<String, Object>> slowApis = analyzeSlowApis(requests);
            List<Map<String, Object>> errorCorrelation = correlateErrors(logEntries, requests, threads);
            Map<String, Object> gcAnalysis = analyzeGc(gcEvents);

            enrichWithThreadData(slowApis, threads, requests, parseDumpTimeMs(threadDumpText));
            enrichWithGcImpact(slowApis, gcEvents, requests);
            enrichWithServerLogErrors(slowApis, logEntries, requests);

            threadAnalysis.put("source", "uploaded_thread_dump");
            threadAnalysis.put("dump_provided", threadDumpText != null && !threadDumpText.isBlank());
            threadAnalysis.put("dump_time", parseThreadDumpTime(threadDumpText));

            List<Map<String, Object>> findings = generateFindings(
                    threads, requests, logEntries, gcEvents,
                    threadAnalysis, slowApis, gcAnalysis,
                    Map.of(), Map.of(), parseThreadDumpTime(threadDumpText));
            Map<String, Map<String, Object>> aiThreadStacks = buildThreadStacks(threads);
            findings.addAll(correlateRequestTraces(
                    requests, List.of(), List.of(), logEntries, gcEvents,
                    aiThreadStacks, parseThreadDumpTime(threadDumpText)));
            findings.sort((a, b) -> severityOrder((String) a.get("severity")) - severityOrder((String) b.get("severity")));
            findings = dedupeFindings(findings);

            Map<String, Object> analysisData = new LinkedHashMap<>();
            analysisData.put("files_analyzed", filesProvided);
            analysisData.put("thread_analysis", threadAnalysis);
            analysisData.put("thread_stacks", aiThreadStacks);
            analysisData.put("slow_apis", slowApis.subList(0, Math.min(30, slowApis.size())));
            analysisData.put("error_correlation", errorCorrelation.subList(0, Math.min(50, errorCorrelation.size())));
            analysisData.put("gc_analysis", gcAnalysis);
            analysisData.put("findings", findings);

            // Include top exception classes from server log
            if (!logEntries.isEmpty()) {
                Map<String, Integer> exceptionCounts = new LinkedHashMap<>();
                for (Map<String, Object> e : logEntries) {
                    if (!"ERROR".equals(e.get("level")) && !"FATAL".equals(e.get("level"))) continue;
                    String exCls = (String) e.get("exception_class");
                    if (exCls != null) exceptionCounts.merge(exCls, 1, Integer::sum);
                }
                analysisData.put("exception_summary", exceptionCounts);

                long errorCount = logEntries.stream()
                        .filter(e -> "ERROR".equals(e.get("level")) || "FATAL".equals(e.get("level"))).count();
                long warnCount = logEntries.stream()
                        .filter(e -> "WARN".equals(e.get("level"))).count();
                analysisData.put("log_level_summary", Map.of(
                        "ERROR_FATAL", errorCount, "WARN", warnCount, "total_entries", logEntries.size()));
            }

            // Include thread state distribution and key blocked thread info
            if (!threads.isEmpty()) {
                List<Map<String, Object>> blockedSummary = new ArrayList<>();
                for (Map<String, Object> t : threads) {
                    if ("BLOCKED".equals(t.get("thread_state"))) {
                        Map<String, Object> bt = new LinkedHashMap<>();
                        bt.put("thread_name", t.get("thread_name"));
                        bt.put("lock_name", t.get("lock_name"));
                        bt.put("lock_owner", t.get("lock_owner"));
                        @SuppressWarnings("unchecked")
                        List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());
                        bt.put("top_frames", stack.subList(0, Math.min(5, stack.size())));
                        blockedSummary.add(bt);
                    }
                }
                analysisData.put("blocked_thread_details", blockedSummary);

                // Classify threads by category
                Map<String, Integer> threadCategories = new LinkedHashMap<>();
                for (Map<String, Object> t : threads) {
                    String name = (String) t.getOrDefault("thread_name", "");
                    String category = classifyThread(name);
                    threadCategories.merge(category, 1, Integer::sum);
                }
                analysisData.put("thread_categories", threadCategories);
            }

            // Request distribution stats
            if (!requests.isEmpty()) {
                long total = requests.size();
                long errors = requests.stream()
                        .filter(r -> ((Number) r.getOrDefault("status", 200)).intValue() >= 400).count();
                long slow1s = requests.stream()
                        .filter(r -> ((Number) r.getOrDefault("response_time_ms", 0)).longValue() > 1000).count();
                long slow5s = requests.stream()
                        .filter(r -> ((Number) r.getOrDefault("response_time_ms", 0)).longValue() > 5000).count();
                long slow30s = requests.stream()
                        .filter(r -> ((Number) r.getOrDefault("response_time_ms", 0)).longValue() > 30000).count();
                analysisData.put("request_stats", Map.of(
                        "total", total, "errors", errors,
                        "slow_1s", slow1s, "slow_5s", slow5s, "slow_30s", slow30s));
            }

            String systemPrompt = "You are an expert JBoss EAP Root Cause Analysis specialist. "
                    + "You are given parsed and correlated data from multiple log sources (thread dump, server.log, access.log, GC log). "
                    + "Provide a thorough root cause analysis report.\n\n"
                    + "Structure your response as:\n"
                    + "## Executive Summary\n"
                    + "A 2-3 sentence overview of the most critical issues found.\n\n"
                    + "## Critical Findings\n"
                    + "List the most severe issues, each with:\n"
                    + "- What was found (specific metrics, thread names, endpoints)\n"
                    + "- Why it matters (impact on application behavior)\n"
                    + "- Root cause hypothesis\n\n"
                    + "## Thread Analysis\n"
                    + "- Thread state distribution and what it means\n"
                    + "- Blocked/contended threads and the locks involved\n"
                    + "- Thread pool saturation indicators\n"
                    + "- Deadlock or livelock patterns\n\n"
                    + "## API Performance\n"
                    + "- Slowest endpoints with response time breakdown\n"
                    + "- Error rate by endpoint\n"
                    + "- Correlation between slow APIs and thread contention or GC pauses\n\n"
                    + "## GC & Memory Impact\n"
                    + "- GC pause frequency and duration\n"
                    + "- Full GC events and their impact\n"
                    + "- Memory pressure indicators\n\n"
                    + "## Error Patterns\n"
                    + "- Top exceptions and their frequency\n"
                    + "- Exception correlation with specific threads or endpoints\n"
                    + "- Cascading failure patterns\n\n"
                    + "## Root Cause Chain\n"
                    + "Describe the most likely root cause chain: initial trigger -> cascading effects -> user-visible impact.\n\n"
                    + "## Recommendations\n"
                    + "A brief prioritized overview of the fixes, most impactful first "
                    + "(immediate actions, short-term fixes, long-term improvements). Keep this short — "
                    + "the detailed step-by-step fixes go in the Resolution section below.\n\n"
                    + "## Resolution\n"
                    + "This is the most important section — make it detailed and directly actionable.\n"
                    + "Provide a SEPARATE, detailed resolution for EACH critical and major (high-severity) "
                    + "issue you identified above — do not collapse them into one. Order the issues by "
                    + "severity, most critical first. For each issue use this structure:\n"
                    + "### <Issue name> (Severity: CRITICAL/MAJOR)\n"
                    + "- **Root cause:** one line stating the underlying cause.\n"
                    + "- **Impact:** what breaks / degrades if left unresolved.\n"
                    + "- **Fix steps:** a numbered, ordered list of concrete steps. For each step give "
                    + "the EXACT action — the full `jboss-cli.sh` command (with the real attribute/path), "
                    + "the specific standalone.xml/domain.xml element and value to change, the JVM flag to "
                    + "add, or the code/query change to make — so an operator can copy-paste and apply it. "
                    + "Reference the actual thread names, endpoint paths, datasource names, exception "
                    + "classes, and SQL from the data.\n"
                    + "- **Verification:** how to confirm the fix worked (metric to re-check or command to run).\n\n"
                    + "Use `inline code` for every command, path, flag, and identifier so it renders "
                    + "clearly. If no critical or major issue was found, state that no action is needed and "
                    + "list the key metrics to keep watching. Do not make up data — only reference what is "
                    + "in the provided analysis.\n\n"
                    + "Be specific with thread names, endpoint paths, exception classes, and metrics. "
                    + "Do not make up data — only reference what is in the provided analysis.";

            String result = ai.chat(systemPrompt, mapper.writeValueAsString(analysisData));
            return Response.ok(result).build();
        } catch (AnthropicClient.RateLimitException e) {
            return Response.status(429).entity(e.getMessage()).build();
        } catch (Exception e) {
            LOG.errorf(e, "AI RCA analysis failed");
            return Response.status(500)
                    .entity("AI RCA analysis failed: " + e.getMessage())
                    .build();
        }
    }

    private String classifyThread(String name) {
        if (name == null) return "other";
        String lower = name.toLowerCase();
        if (lower.contains("http") || lower.contains("default task") || lower.contains("request"))
            return "http-worker";
        if (lower.contains("ejb") || lower.contains("async"))
            return "ejb-async";
        if (lower.contains("jca") || lower.contains("workmanager"))
            return "jca-worker";
        if (lower.contains("batch") || lower.contains("scheduler") || lower.contains("timer"))
            return "batch-scheduler";
        if (lower.contains("gc") || lower.contains("finalizer") || lower.contains("reference"))
            return "gc-internal";
        if (lower.contains("acceptor") || lower.contains("xnio") || lower.contains("io"))
            return "io-subsystem";
        if (lower.contains("jmx") || lower.contains("management"))
            return "management";
        if (lower.contains("deployment") || lower.contains("msc"))
            return "deployment-msc";
        if (lower.contains("transaction") || lower.contains("arjuna"))
            return "transaction";
        return "other";
    }
}
