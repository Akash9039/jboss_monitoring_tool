package com.rh.monitor.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.resource.WebResource;
import com.rh.monitor.util.TimeUtils;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@ApplicationScoped
@Startup
public class HistoryTracker {

    private static final Logger LOG = Logger.getLogger(HistoryTracker.class);
    private static final int MAX_HISTORY = 200;
    private static final int MAX_BLOCKED_LOG = 500;
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    @Inject
    JBossManagementClient client;

    @Inject
    com.rh.monitor.config.JBossConfig config;

    private static final int MAX_DEADLOCK_EVENTS = 100;
    private static final int MAX_API_EVENTS = 200;
    private static final int MAX_TX_EVENTS = 200;
    private static final String EVENT_LOG_FILE = "thread_events.log";
    private static final String API_EVENT_LOG_FILE = "api_events.log";
    private static final String TX_EVENT_LOG_FILE = "transaction_events.log";
    private static final String TX_JSONL_FILE = "transaction_events.jsonl";
    private static final String API_JSONL_FILE = "api_events.jsonl";
    private static final String THREAD_SNAPSHOTS_JSONL_FILE = "thread_snapshots.jsonl";
    private static final String BLOCKED_JSONL_FILE = "thread_blocked.jsonl";
    private static final String DEADLOCK_JSONL_FILE = "deadlock_events.jsonl";

    // Thread history state
    private static final ConcurrentLinkedDeque<Map<String, Object>> threadHistorySnapshots = new ConcurrentLinkedDeque<>();
    private static final ConcurrentLinkedDeque<Map<String, Object>> blockedThreadLog = new ConcurrentLinkedDeque<>();
    private static final ConcurrentHashMap<Long, Map<String, Object>> threadStateTracker = new ConcurrentHashMap<>();

    // Deadlock tracking — current active deadlocks and event history
    private static final ConcurrentHashMap<Long, Map<String, Object>> activeDeadlocks = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedDeque<Map<String, Object>> deadlockEventLog = new ConcurrentLinkedDeque<>();

    // API event tracking
    private static final ConcurrentLinkedDeque<Map<String, Object>> apiEventLog = new ConcurrentLinkedDeque<>();
    private static volatile long prevTotalRequests = -1;
    private static volatile long prevTotalErrors = -1;
    private static volatile long prevMaxProcessingTimeMs = 0;

    // Transaction event tracking
    private static final ConcurrentLinkedDeque<Map<String, Object>> txEventLog = new ConcurrentLinkedDeque<>();
    private static volatile int prevCommitted = -1;
    private static volatile int prevAborted = -1;
    private static volatile int prevTimedOut = -1;

    private static volatile long lastPollTimeMs = 0;
    private static volatile boolean initialized = false;

    @PostConstruct
    void init() {
        if (initialized) return;
        initialized = true;
        LOG.info("Loading event history from log files...");

        loadJsonlEvents(TX_JSONL_FILE, txEventLog, MAX_TX_EVENTS);
        if (txEventLog.isEmpty()) {
            parseTxTextLog();
        }
        restoreTxPrevCounters();

        loadJsonlEvents(API_JSONL_FILE, apiEventLog, MAX_API_EVENTS);
        if (apiEventLog.isEmpty()) {
            parseApiTextLog();
        }
        restoreApiPrevCounters();

        loadJsonlEvents(THREAD_SNAPSHOTS_JSONL_FILE, threadHistorySnapshots, MAX_HISTORY);
        loadJsonlEvents(BLOCKED_JSONL_FILE, blockedThreadLog, MAX_BLOCKED_LOG);
        loadJsonlEvents(DEADLOCK_JSONL_FILE, deadlockEventLog, MAX_DEADLOCK_EVENTS);

        LOG.infof("Loaded history: %d tx events, %d api events, %d thread snapshots, %d blocked, %d deadlock",
                txEventLog.size(), apiEventLog.size(), threadHistorySnapshots.size(),
                blockedThreadLog.size(), deadlockEventLog.size());
    }

    private void restoreTxPrevCounters() {
        for (var it = txEventLog.descendingIterator(); it.hasNext();) {
            Map<String, Object> e = it.next();
            String event = (String) e.getOrDefault("event", "");
            if ("BASELINE".equals(event) || "POLL".equals(event)) {
                int c = txAbsCounter(e, "committed", "total_committed");
                if (c >= 0) {
                    prevCommitted = c;
                    prevAborted = txAbsCounter(e, "aborted", "total_aborted");
                    prevTimedOut = txAbsCounter(e, "timed_out", "total_timed_out");
                    if (prevAborted < 0) prevAborted = 0;
                    if (prevTimedOut < 0) prevTimedOut = 0;
                    LOG.infof("Restored tx prev counters: committed=%d, aborted=%d, timedOut=%d",
                            prevCommitted, prevAborted, prevTimedOut);
                    return;
                }
            }
        }
    }

    private static int txAbsCounter(Map<String, Object> e, String key, String altKey) {
        if (e.containsKey(key)) return toInt(e.get(key));
        if (e.containsKey(altKey)) return toInt(e.get(altKey));
        return -1;
    }

    private void restoreApiPrevCounters() {
        for (var it = apiEventLog.descendingIterator(); it.hasNext();) {
            Map<String, Object> e = it.next();
            String event = (String) e.getOrDefault("event", "");
            if ("BASELINE".equals(event) || "POLL".equals(event)) {
                if (e.containsKey("total_requests")) {
                    prevTotalRequests = toLong(e.get("total_requests"));
                    prevTotalErrors = toLong(e.get("total_errors"));
                    prevMaxProcessingTimeMs = toLong(e.get("max_processing_time_ms"));
                    LOG.infof("Restored api prev counters: requests=%d, errors=%d, maxProc=%d",
                            prevTotalRequests, prevTotalErrors, prevMaxProcessingTimeMs);
                    return;
                }
            }
        }
    }

    private void loadJsonlEvents(String filename, Deque<Map<String, Object>> deque, int maxSize) {
        Path path = getLogDir().resolve(filename);
        if (!Files.isRegularFile(path)) return;
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                try {
                    Map<String, Object> event = jsonMapper.readValue(line,
                            new TypeReference<LinkedHashMap<String, Object>>() {});
                    deque.addLast(event);
                    while (deque.size() > maxSize) deque.pollFirst();
                } catch (Exception ignored) {}
            }
        } catch (IOException e) {
            LOG.debugf("Failed to load %s: %s", filename, e.getMessage());
        }
    }

    private void appendJsonlEvent(String filename, Map<String, Object> event) {
        try {
            String json = jsonMapper.writeValueAsString(event);
            Files.writeString(getLogDir().resolve(filename), json + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOG.debugf("Failed to write to %s: %s", filename, e.getMessage());
        }
    }

    private void parseTxTextLog() {
        Path path = getLogDir().resolve(TX_EVENT_LOG_FILE);
        if (!Files.isRegularFile(path)) return;
        try {
            List<String> lines = Files.readAllLines(path);
            int start = Math.max(0, lines.size() - MAX_TX_EVENTS);
            for (int i = start; i < lines.size(); i++) {
                Map<String, Object> event = parseTxTextLine(lines.get(i));
                if (event != null) {
                    txEventLog.addLast(event);
                    while (txEventLog.size() > MAX_TX_EVENTS) txEventLog.pollFirst();
                }
            }
        } catch (IOException e) {
            LOG.debugf("Failed to parse tx text log: %s", e.getMessage());
        }
    }

    private static final Map<String, String> TEXT_KEY_MAP = Map.of(
            "timedout", "timed_out",
            "abortrate", "abort_rate_percent",
            "newcommitted", "new_committed",
            "newaborted", "new_aborted",
            "newtimedout", "new_timed_out",
            "maxprocessing", "max_processing_time_ms",
            "totalrequests", "total_requests",
            "totalerrors", "total_errors",
            "newrequests", "new_requests",
            "newerrors", "new_errors"
    );

    private Map<String, Object> parseTxTextLine(String line) {
        if (line == null || line.isBlank()) return null;
        try {
            Map<String, Object> event = new LinkedHashMap<>();
            String type = line.substring(line.indexOf('[') + 1, line.indexOf(']'));
            event.put("event", type);
            String rest = line.substring(line.indexOf(']') + 2).trim();
            String[] parts = rest.split("\\|");
            if (parts.length > 0) event.put("timestamp", parts[0].trim());
            for (int j = 1; j < parts.length; j++) {
                String part = parts[j].trim();
                int colon = part.indexOf(':');
                if (colon < 0) continue;
                String rawKey = part.substring(0, colon).trim().toLowerCase().replace(" ", "_");
                String key = TEXT_KEY_MAP.getOrDefault(rawKey, rawKey);
                String val = part.substring(colon + 1).trim().replaceAll("[%ms]", "").trim();
                try {
                    if (val.contains(".")) event.put(key, Double.parseDouble(val));
                    else event.put(key, Long.parseLong(val));
                } catch (NumberFormatException e2) {
                    event.put(key, val);
                }
            }
            return event;
        } catch (Exception e) {
            return null;
        }
    }

    private void parseApiTextLog() {
        Path path = getLogDir().resolve(API_EVENT_LOG_FILE);
        if (!Files.isRegularFile(path)) return;
        try {
            List<String> lines = Files.readAllLines(path);
            int start = Math.max(0, lines.size() - MAX_API_EVENTS);
            for (int i = start; i < lines.size(); i++) {
                Map<String, Object> event = parseTxTextLine(lines.get(i));
                if (event != null) {
                    apiEventLog.addLast(event);
                    while (apiEventLog.size() > MAX_API_EVENTS) apiEventLog.pollFirst();
                }
            }
        } catch (IOException e) {
            LOG.debugf("Failed to parse api text log: %s", e.getMessage());
        }
    }

    private static int toInt(Object val) {
        if (val instanceof Number) return ((Number) val).intValue();
        if (val instanceof String) {
            try { return Integer.parseInt((String) val); } catch (NumberFormatException e) { return 0; }
        }
        return 0;
    }

    private static long toLong(Object val) {
        if (val instanceof Number) return ((Number) val).longValue();
        if (val instanceof String) {
            try { return Long.parseLong((String) val); } catch (NumberFormatException e) { return 0; }
        }
        return 0;
    }

    public static Deque<Map<String, Object>> getThreadHistorySnapshots() { return threadHistorySnapshots; }
    public static Deque<Map<String, Object>> getBlockedThreadLog() { return blockedThreadLog; }
    public static Map<Long, Map<String, Object>> getThreadStateTracker() { return threadStateTracker; }
    public static Map<Long, Map<String, Object>> getActiveDeadlocks() { return activeDeadlocks; }
    public static Deque<Map<String, Object>> getDeadlockEventLog() { return deadlockEventLog; }
    public static Deque<Map<String, Object>> getApiEventLog() { return apiEventLog; }
    public static Deque<Map<String, Object>> getTxEventLog() { return txEventLog; }

    private static volatile long lastSessionPollTimeMs = 0;

    public void refreshNow() {
        long now = System.currentTimeMillis();
        if (now - lastPollTimeMs < 5000) return;
        lastPollTimeMs = now;
        String ts = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        pollThreadStates(ts);
        pollDeadlocks(ts);
    }

    public void refreshSessionsNow() {
        long now = System.currentTimeMillis();
        if (now - lastSessionPollTimeMs < 5000) return;
        lastSessionPollTimeMs = now;
        pollIndividualSessions();
    }

    @Scheduled(every = "30s", delayed = "10s")
    void pollSessionAndWebHistory() {
        try {
            String ts = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            pollSessions(ts);
            pollWebStats(ts);
            pollIndividualSessions();
            pollApiEvents(ts);
            pollTransactionEvents(ts);
        } catch (Exception e) {
            LOG.debugf("History poll error: %s", e.getMessage());
        }
    }

    @Scheduled(every = "30s", delayed = "15s")
    void pollThreadHistory() {
        try {
            lastPollTimeMs = System.currentTimeMillis();
            String ts = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            pollThreadStates(ts);
            pollDeadlocks(ts);
        } catch (Exception e) {
            LOG.debugf("Thread history poll error: %s", e.getMessage());
        }
    }

    private void pollSessions(String ts) {
        try {
            List<String[]> domainServers = client.getRunningDomainServers();
            if (!domainServers.isEmpty()) {
                for (String[] hs : domainServers) {
                    pollSessionsForServer(ts, List.of("host", hs[0], "server", hs[1]), hs[0] + "/" + hs[1]);
                }
                return;
            }
            pollSessionsForServer(ts, List.of(), null);
        } catch (Exception e) {
            LOG.debugf("Session poll error: %s", e.getMessage());
        }
    }

    private void pollSessionsForServer(String ts, List<String> prefix, String serverLabel) {
        try {
            JsonNode root = prefix.isEmpty()
                    ? client.readResource(List.of(), true, false)
                    : client.readResourceDirect(prefix, true, false);
            if (!"success".equals(root.path("outcome").asText())) return;

            JsonNode deployments = root.path("result").path("deployment");
            if (deployments.isMissingNode() || !deployments.isObject()) return;

            Iterator<String> depNames = deployments.fieldNames();
            while (depNames.hasNext()) {
                String depName = depNames.next();
                try {
                    List<String> depAddr = new ArrayList<>(prefix);
                    depAddr.addAll(List.of("deployment", depName, "subsystem", "undertow"));
                    JsonNode depUt = prefix.isEmpty()
                            ? client.readResource(List.of("deployment", depName, "subsystem", "undertow"), true, true)
                            : client.readResourceDirect(depAddr, true, true);
                    if (!"success".equals(depUt.path("outcome").asText())) continue;
                    JsonNode d = depUt.get("result");

                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("timestamp", ts);
                    if (serverLabel != null) entry.put("server_name", serverLabel);
                    entry.put("deployment", depName);
                    entry.put("active_sessions", d.path("active-sessions").asInt(0));
                    entry.put("expired_sessions", d.path("expired-sessions").asInt(0));
                    entry.put("sessions_created", d.path("sessions-created").asInt(0));
                    entry.put("rejected_sessions", d.path("rejected-sessions").asInt(0));
                    entry.put("session_avg_alive_time", d.path("session-avg-alive-time").asInt(0) * 1000);
                    entry.put("session_max_alive_time", d.path("session-max-alive-time").asInt(0) * 1000);
                    WebResource.addSessionHistory(entry);
                } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            LOG.debugf("Session poll error for %s: %s", serverLabel, e.getMessage());
        }
    }

    private void pollWebStats(String ts) {
        try {
            long totalReq = 0, totalErr = 0, totalBytes = 0, maxProc = 0;
            List<Map<String, Object>> allListeners = new ArrayList<>();

            List<String[]> domainServers = client.getRunningDomainServers();
            if (!domainServers.isEmpty()) {
                for (String[] hs : domainServers) {
                    long[] stats = collectWebStatsFromServer(
                            List.of("host", hs[0], "server", hs[1]), true, allListeners);
                    totalReq += stats[0]; totalErr += stats[1];
                    totalBytes += stats[2]; maxProc = Math.max(maxProc, stats[3]);
                }
            } else {
                long[] stats = collectWebStatsFromServer(List.of(), false, allListeners);
                totalReq = stats[0]; totalErr = stats[1];
                totalBytes = stats[2]; maxProc = stats[3];
            }

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("timestamp", ts);
            entry.put("total_requests", totalReq);
            entry.put("total_errors", totalErr);
            entry.put("total_bytes_sent", totalBytes);
            entry.put("max_processing_time_ms", maxProc);
            entry.put("listeners", allListeners);
            WebResource.addWebStatsHistory(entry);
        } catch (Exception e) {
            LOG.debugf("Web stats poll error: %s", e.getMessage());
        }
    }

    private long[] collectWebStatsFromServer(List<String> prefix, boolean useDirect,
                                              List<Map<String, Object>> listeners) {
        long totalReq = 0, totalErr = 0, totalBytes = 0, maxProc = 0;
        try {
            List<String> utAddr = new ArrayList<>(prefix);
            utAddr.addAll(List.of("subsystem", "undertow"));
            JsonNode utResult = useDirect
                    ? client.readResourceDirect(utAddr, true, false)
                    : client.readResource(List.of("subsystem", "undertow"), true, false);
            if (!"success".equals(utResult.path("outcome").asText())) return new long[]{0, 0, 0, 0};

            JsonNode servers = utResult.path("result").path("server");
            if (!servers.isObject()) return new long[]{0, 0, 0, 0};

            Iterator<String> serverNames = servers.fieldNames();
            while (serverNames.hasNext()) {
                String serverName = serverNames.next();
                try {
                    List<String> srvAddr = new ArrayList<>(prefix);
                    srvAddr.addAll(List.of("subsystem", "undertow", "server", serverName));
                    JsonNode srv = useDirect
                            ? client.readResourceDirect(srvAddr, true, true)
                            : client.readResource(List.of("subsystem", "undertow", "server", serverName), true, true);
                    if (!"success".equals(srv.path("outcome").asText())) continue;
                    JsonNode srvResult = srv.get("result");

                    for (String lt : List.of("http-listener", "https-listener", "ajp-listener")) {
                        JsonNode listenerNode = srvResult.path(lt);
                        if (!listenerNode.isObject()) continue;
                        Iterator<Map.Entry<String, JsonNode>> fields = listenerNode.fields();
                        while (fields.hasNext()) {
                            Map.Entry<String, JsonNode> e = fields.next();
                            JsonNode ld = e.getValue();
                            if (!ld.isObject()) continue;
                            long rc = ld.path("request-count").asLong(0);
                            long ec = ld.path("error-count").asLong(0);
                            long bs = ld.path("bytes-sent").asLong(0);
                            long mp = ld.path("max-processing-time").asLong(0) / 1_000_000;
                            totalReq += rc; totalErr += ec; totalBytes += bs;
                            maxProc = Math.max(maxProc, mp);
                            listeners.add(Map.of("name", e.getKey(), "type", lt,
                                    "request_count", rc, "error_count", ec));
                        }
                    }
                } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}
        return new long[]{totalReq, totalErr, totalBytes, maxProc};
    }

    private void pollIndividualSessions() {
        try {
            Set<String> activeSids = new HashSet<>();
            long nowMs = Instant.now().toEpochMilli();

            List<String[]> domainServers = client.getRunningDomainServers();
            if (!domainServers.isEmpty()) {
                for (String[] hs : domainServers) {
                    pollIndividualSessionsForServer(
                            List.of("host", hs[0], "server", hs[1]),
                            hs[0] + "/" + hs[1], activeSids, nowMs);
                }
            } else {
                pollIndividualSessionsForServer(List.of(), null, activeSids, nowMs);
            }

            for (String sid : new ArrayList<>(WebResource.individualSessionTracker.keySet())) {
                Map<String, Object> tracker = WebResource.individualSessionTracker.get(sid);
                if (!activeSids.contains(sid) && "active".equals(tracker.get("status"))) {
                    tracker.put("status", "expired");
                    tracker.put("end_time_ms", nowMs);
                    WebResource.addEndedSession(new LinkedHashMap<>(tracker));
                    WebResource.individualSessionTracker.remove(sid);
                }
            }
        } catch (Exception e) {
            LOG.debugf("Individual session poll error: %s", e.getMessage());
        }
    }

    private void pollIndividualSessionsForServer(List<String> prefix, String serverLabel,
                                                  Set<String> activeSids, long nowMs) {
        try {
            JsonNode root = prefix.isEmpty()
                    ? client.readResource(List.of(), true, false)
                    : client.readResourceDirect(prefix, true, false);
            if (!"success".equals(root.path("outcome").asText())) return;

            JsonNode deployments = root.path("result").path("deployment");
            if (!deployments.isObject()) return;

            Iterator<String> depNames = deployments.fieldNames();
            while (depNames.hasNext()) {
                String depName = depNames.next();
                try {
                    List<String> addr = new ArrayList<>(prefix);
                    addr.addAll(List.of("deployment", depName, "subsystem", "undertow"));
                    JsonNode listResult = client.mgmtRequest(Map.of(
                            "operation", "list-sessions",
                            "address", addr));
                    if (!"success".equals(listResult.path("outcome").asText())) continue;

                    JsonNode sessions = listResult.get("result");
                    if (sessions == null || !sessions.isArray()) continue;

                    for (JsonNode sidNode : sessions) {
                        String sid = sidNode.asText();
                        activeSids.add(sid);
                        if (!WebResource.individualSessionTracker.containsKey(sid)) {
                            Map<String, Object> tracker = new LinkedHashMap<>();
                            tracker.put("session_id", sid);
                            if (serverLabel != null) tracker.put("server_name", serverLabel);
                            tracker.put("deployment", depName);
                            tracker.put("start_time_ms", nowMs);
                            tracker.put("last_accessed_ms", nowMs);
                            tracker.put("end_time_ms", null);
                            tracker.put("status", "active");
                            WebResource.individualSessionTracker.put(sid, tracker);
                        } else {
                            WebResource.individualSessionTracker.get(sid).put("status", "active");
                        }
                    }
                } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            LOG.debugf("Individual session poll error for %s: %s", serverLabel, e.getMessage());
        }
    }

    private void pollThreadStates(String ts) {
        try {
            List<String[]> domainServers = client.getRunningDomainServers();
            if (domainServers != null && !domainServers.isEmpty()) {
                pollThreadStatesForDomain(domainServers, ts);
                return;
            }
        } catch (Exception ignored) {}

        pollThreadStatesForServer(null, ts);
    }

    private void pollThreadStatesForDomain(List<String[]> domainServers, String ts) {
        int total = 0, runnable = 0, blocked = 0, waiting = 0, timedWaiting = 0;
        Set<Long> allLiveCompoundIds = new HashSet<>();

        for (String[] ds : domainServers) {
            String serverLabel = ds[0] + "/" + ds[1];
            long serverHash = serverLabel.hashCode() * 1_000_000_000L;
            List<String> addr = List.of("host", ds[0], "server", ds[1],
                    "core-service", "platform-mbean", "type", "threading");
            try {
                JsonNode result = client.readResourceDirect(addr, true, false);
                if (!"success".equals(result.path("outcome").asText())) continue;
                total += result.path("result").path("thread-count").asInt(0);
            } catch (Exception e) {
                LOG.debugf("Thread stats poll failed for %s: %s", serverLabel, e.getMessage());
                continue;
            }

            try {
                Map<String, Object> op = new LinkedHashMap<>();
                op.put("operation", "dump-all-threads");
                op.put("address", addr);
                op.put("locked-monitors", false);
                op.put("locked-synchronizers", false);
                JsonNode dumpResult = client.mgmtRequest(op);
                if ("success".equals(dumpResult.path("outcome").asText())) {
                    JsonNode threads = dumpResult.get("result");
                    if (threads != null && threads.isArray()) {
                        for (JsonNode t : threads) {
                            String state = t.path("thread-state").asText("").toUpperCase();
                            switch (state) {
                                case "RUNNABLE": runnable++; break;
                                case "BLOCKED": blocked++; break;
                                case "WAITING": waiting++; break;
                                case "TIMED_WAITING": timedWaiting++; break;
                            }

                            long tid = t.path("thread-id").asLong(0);
                            if (tid <= 0) continue;
                            long compoundId = serverHash + tid;
                            allLiveCompoundIds.add(compoundId);
                            String threadName = t.path("thread-name").asText("unknown");
                            Map<String, Object> prev = threadStateTracker.get(compoundId);
                            String prevState = prev != null ? (String) prev.getOrDefault("current_state", "") : "";

                            List<String> stackLines = new ArrayList<>();
                            JsonNode stNode = t.path("stack-trace");
                            if (stNode.isArray()) {
                                for (JsonNode frame : stNode) {
                                    String cls = frame.path("class-name").asText("");
                                    String method = frame.path("method-name").asText("");
                                    String file = frame.path("file-name").asText("");
                                    int line = frame.path("line-number").asInt(-1);
                                    String loc = file.isEmpty() ? "Unknown Source" : (line >= 0 ? file + ":" + line : file);
                                    stackLines.add(cls + "." + method + "(" + loc + ")");
                                }
                            }

                            String lockName = t.path("lock-name").isNull() ? null : t.path("lock-name").asText();
                            String lockOwner = t.path("lock-owner-name").isNull() ? null : t.path("lock-owner-name").asText();
                            long blockedCount = t.path("blocked-count").asLong(0);

                            if (!state.equals(prevState)) {
                                Map<String, Object> tracker = new LinkedHashMap<>();
                                tracker.put("thread_id", compoundId);
                                tracker.put("thread_name", threadName);
                                tracker.put("server_name", serverLabel);
                                tracker.put("current_state", state);
                                tracker.put("state_since", ts);
                                tracker.put("stack_trace", stackLines);
                                tracker.put("lock_name", lockName);
                                tracker.put("lock_owner_name", lockOwner);
                                tracker.put("blocked_count", blockedCount);
                                threadStateTracker.put(compoundId, tracker);

                                if ("BLOCKED".equals(state)) {
                                    String topFrame = stackLines.isEmpty() ? "" : topFrameSummary(stackLines.get(0));
                                    Map<String, Object> event = new LinkedHashMap<>();
                                    event.put("timestamp", ts);
                                    event.put("thread_id", compoundId);
                                    event.put("thread_name", threadName);
                                    event.put("server_name", serverLabel);
                                    event.put("lock_name", lockName);
                                    event.put("lock_owner_name", lockOwner);
                                    event.put("blocked_count", blockedCount);
                                    event.put("top_frame", topFrame);
                                    persistEvent(blockedThreadLog, event, MAX_BLOCKED_LOG, BLOCKED_JSONL_FILE);
                                    appendToEventLog("[BLOCKED] " + ts + " | Server: " + serverLabel
                                            + " | Thread: " + threadName
                                            + " | Lock: " + lockName + " | Owner: " + lockOwner
                                            + " | BlockedCount: " + blockedCount + " | TopFrame: " + topFrame);
                                }
                            } else {
                                Map<String, Object> existing = threadStateTracker.get(compoundId);
                                if (existing != null) {
                                    @SuppressWarnings("unchecked")
                                    List<String> prevStack = (List<String>) existing.get("stack_trace");
                                    if (hasActivityChanged(prevStack, stackLines)) {
                                        existing.put("state_since", ts);
                                    }
                                    existing.put("stack_trace", stackLines);
                                    existing.put("lock_name", lockName);
                                    existing.put("lock_owner_name", lockOwner);
                                    existing.put("blocked_count", blockedCount);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.debugf("Thread dump poll failed for %s: %s", serverLabel, e.getMessage());
            }
        }

        threadStateTracker.keySet().removeIf(id -> !allLiveCompoundIds.contains(id));

        if (total > 0) {
            Map<String, Object> snapshot = new LinkedHashMap<>();
            snapshot.put("timestamp", ts);
            snapshot.put("total", total);
            snapshot.put("runnable", runnable);
            snapshot.put("blocked", blocked);
            snapshot.put("waiting", waiting);
            snapshot.put("timed_waiting", timedWaiting);
            persistEvent(threadHistorySnapshots, snapshot, MAX_HISTORY, THREAD_SNAPSHOTS_JSONL_FILE);
        }
    }

    private void pollThreadStatesForServer(List<String> serverPrefix, String ts) {
        try {
            JsonNode result;
            if (serverPrefix != null) {
                List<String> addr = new ArrayList<>(serverPrefix);
                addr.addAll(List.of("core-service", "platform-mbean", "type", "threading"));
                result = client.readResourceDirect(addr, true, false);
            } else {
                result = client.readResource(
                        List.of("core-service", "platform-mbean", "type", "threading"));
            }
            if (!"success".equals(result.path("outcome").asText())) return;

            JsonNode data = result.get("result");
            int total = data.path("thread-count").asInt(0);

            int runnable = 0, blocked = 0, waiting = 0, timedWaiting = 0;

            try {
                List<String> threadingAddr = serverPrefix != null
                        ? new ArrayList<>(serverPrefix) {{ addAll(List.of("core-service", "platform-mbean", "type", "threading")); }}
                        : List.of("core-service", "platform-mbean", "type", "threading");
                Map<String, Object> op = new LinkedHashMap<>();
                op.put("operation", "dump-all-threads");
                op.put("address", threadingAddr);
                op.put("locked-monitors", false);
                op.put("locked-synchronizers", false);
                JsonNode dumpResult = client.mgmtRequest(op);
                if ("success".equals(dumpResult.path("outcome").asText())) {
                    JsonNode threads = dumpResult.get("result");
                    if (threads != null && threads.isArray()) {
                        Set<Long> liveThreadIds = new HashSet<>();
                        for (JsonNode t : threads) {
                            String state = t.path("thread-state").asText("").toUpperCase();
                            switch (state) {
                                case "RUNNABLE": runnable++; break;
                                case "BLOCKED": blocked++; break;
                                case "WAITING": waiting++; break;
                                case "TIMED_WAITING": timedWaiting++; break;
                            }

                            long tid = t.path("thread-id").asLong(0);
                            if (tid > 0) {
                                liveThreadIds.add(tid);
                                String threadName = t.path("thread-name").asText("unknown");
                                Map<String, Object> prev = threadStateTracker.get(tid);
                                String prevState = prev != null ? (String) prev.getOrDefault("current_state", "") : "";

                                List<String> stackLines = new ArrayList<>();
                                JsonNode stNode = t.path("stack-trace");
                                if (stNode.isArray()) {
                                    for (JsonNode frame : stNode) {
                                        String cls = frame.path("class-name").asText("");
                                        String method = frame.path("method-name").asText("");
                                        String file = frame.path("file-name").asText("");
                                        int line = frame.path("line-number").asInt(-1);
                                        String loc = file.isEmpty() ? "Unknown Source" : (line >= 0 ? file + ":" + line : file);
                                        stackLines.add(cls + "." + method + "(" + loc + ")");
                                    }
                                }

                                String lockName = t.path("lock-name").isNull() ? null : t.path("lock-name").asText();
                                String lockOwner = t.path("lock-owner-name").isNull() ? null : t.path("lock-owner-name").asText();
                                long blockedCount = t.path("blocked-count").asLong(0);

                                if (!state.equals(prevState)) {
                                    Map<String, Object> tracker = new LinkedHashMap<>();
                                    tracker.put("thread_id", tid);
                                    tracker.put("thread_name", threadName);
                                    tracker.put("current_state", state);
                                    tracker.put("state_since", ts);
                                    tracker.put("stack_trace", stackLines);
                                    tracker.put("lock_name", lockName);
                                    tracker.put("lock_owner_name", lockOwner);
                                    tracker.put("blocked_count", blockedCount);
                                    threadStateTracker.put(tid, tracker);

                                    if ("BLOCKED".equals(state)) {
                                        String topFrame = stackLines.isEmpty() ? "" : topFrameSummary(stackLines.get(0));
                                        Map<String, Object> event = new LinkedHashMap<>();
                                        event.put("timestamp", ts);
                                        event.put("thread_id", tid);
                                        event.put("thread_name", threadName);
                                        event.put("lock_name", lockName);
                                        event.put("lock_owner_name", lockOwner);
                                        event.put("blocked_count", blockedCount);
                                        event.put("top_frame", topFrame);
                                        persistEvent(blockedThreadLog, event, MAX_BLOCKED_LOG, BLOCKED_JSONL_FILE);
                                        appendToEventLog("[BLOCKED] " + ts + " | Thread: " + threadName
                                                + " | Lock: " + lockName + " | Owner: " + lockOwner
                                                + " | BlockedCount: " + blockedCount + " | TopFrame: " + topFrame);
                                    }
                                } else {
                                    Map<String, Object> existing = threadStateTracker.get(tid);
                                    if (existing != null) {
                                        @SuppressWarnings("unchecked")
                                        List<String> prevStack = (List<String>) existing.get("stack_trace");
                                        if (hasActivityChanged(prevStack, stackLines)) {
                                            existing.put("state_since", ts);
                                        }
                                        existing.put("stack_trace", stackLines);
                                        existing.put("lock_name", lockName);
                                        existing.put("lock_owner_name", lockOwner);
                                        existing.put("blocked_count", blockedCount);
                                    }
                                }
                            }
                        }
                        threadStateTracker.keySet().removeIf(id -> !liveThreadIds.contains(id));
                    }
                }
            } catch (Exception e) {
                LOG.debugf("Thread dump for state breakdown failed: %s", e.getMessage());
            }

            Map<String, Object> snapshot = new LinkedHashMap<>();
            snapshot.put("timestamp", ts);
            snapshot.put("total", total);
            snapshot.put("runnable", runnable);
            snapshot.put("blocked", blocked);
            snapshot.put("waiting", waiting);
            snapshot.put("timed_waiting", timedWaiting);
            persistEvent(threadHistorySnapshots, snapshot, MAX_HISTORY, THREAD_SNAPSHOTS_JSONL_FILE);
        } catch (Exception e) {
            LOG.debugf("Thread state poll error: %s", e.getMessage());
        }
    }

    private void pollDeadlocks(String ts) {
        Set<Long> currentDeadlockIds = new HashSet<>();
        try {
            List<String[]> domainServers = client.getRunningDomainServers();
            if (domainServers != null && !domainServers.isEmpty()) {
                for (String[] ds : domainServers) {
                    String serverLabel = ds[0] + "/" + ds[1];
                    long serverHash = serverLabel.hashCode() * 1_000_000_000L;
                    List<String> addr = List.of("host", ds[0], "server", ds[1],
                            "core-service", "platform-mbean", "type", "threading");
                    pollDeadlocksForServer(addr, ts, serverHash, currentDeadlockIds);
                }
                resolveDeadlocks(ts, currentDeadlockIds);
                return;
            }
        } catch (Exception ignored) {}

        List<String> addr = List.of("core-service", "platform-mbean", "type", "threading");
        pollDeadlocksForServer(addr, ts, 0, currentDeadlockIds);
        resolveDeadlocks(ts, currentDeadlockIds);
    }

    private void pollDeadlocksForServer(List<String> threadingAddr, String ts,
                                         long idOffset, Set<Long> currentDeadlockIds) {
        try {
            Map<String, Object> findOp = new LinkedHashMap<>();
            findOp.put("operation", "find-deadlocked-threads");
            findOp.put("address", threadingAddr);
            JsonNode findResult = client.mgmtRequest(findOp);

            if (!"success".equals(findResult.path("outcome").asText())) return;
            JsonNode idsNode = findResult.get("result");
            if (idsNode == null || idsNode.isNull() || !idsNode.isArray() || idsNode.isEmpty()) return;

            Set<Long> deadlockedIds = new HashSet<>();
            for (JsonNode id : idsNode) {
                long val = id.asLong(0);
                if (val > 0) deadlockedIds.add(val);
            }
            if (deadlockedIds.isEmpty()) return;

            Map<String, Object> dumpOp = new LinkedHashMap<>();
            dumpOp.put("operation", "dump-all-threads");
            dumpOp.put("address", threadingAddr);
            dumpOp.put("locked-monitors", true);
            dumpOp.put("locked-synchronizers", true);
            JsonNode dumpResult = client.mgmtRequest(dumpOp);

            if (!"success".equals(dumpResult.path("outcome").asText())) return;
            JsonNode threads = dumpResult.get("result");
            if (threads == null || !threads.isArray()) return;

            for (JsonNode t : threads) {
                long rawTid = t.path("thread-id").asLong(0);
                if (rawTid <= 0 || !deadlockedIds.contains(rawTid)) continue;

                long tid = rawTid + idOffset;
                String name = t.path("thread-name").asText("unknown");
                String state = t.path("thread-state").asText("BLOCKED").toUpperCase();
                String lockName = t.path("lock-name").isNull() ? null : t.path("lock-name").asText();
                String lockOwner = t.path("lock-owner-name").isNull() ? null : t.path("lock-owner-name").asText();
                long lockOwnerId = t.path("lock-owner-id").asLong(-1);
                long blockedCount = t.path("blocked-count").asLong(0);

                String topFrame = "";
                JsonNode stNode = t.path("stack-trace");
                if (stNode.isArray() && !stNode.isEmpty()) {
                    JsonNode frame = stNode.get(0);
                    String cls = frame.path("class-name").asText("");
                    String method = frame.path("method-name").asText("");
                    String file = frame.path("file-name").asText("");
                    int line = frame.path("line-number").asInt(-1);
                    String loc = file.isEmpty() ? "Unknown Source" : (line >= 0 ? file + ":" + line : file);
                    topFrame = cls + "." + method + "(" + loc + ")";
                }

                currentDeadlockIds.add(tid);

                if (!activeDeadlocks.containsKey(tid)) {
                    Map<String, Object> dl = new LinkedHashMap<>();
                    dl.put("thread_id", tid);
                    dl.put("thread_name", name);
                    dl.put("state", state);
                    dl.put("lock_name", lockName);
                    dl.put("lock_owner_name", lockOwner);
                    dl.put("lock_owner_id", lockOwnerId);
                    dl.put("blocked_count", blockedCount);
                    dl.put("detected_at", ts);
                    dl.put("top_frame", topFrame);
                    activeDeadlocks.put(tid, dl);

                    Map<String, Object> event = new LinkedHashMap<>(dl);
                    event.put("event", "DETECTED");
                    event.put("timestamp", ts);
                    persistEvent(deadlockEventLog, event, MAX_DEADLOCK_EVENTS, DEADLOCK_JSONL_FILE);
                    appendToEventLog("[DEADLOCK DETECTED] " + ts + " | Thread: " + name
                            + " | State: " + state
                            + " | Lock: " + lockName
                            + " | Owner: " + lockOwner
                            + " | TopFrame: " + topFrame);
                    LOG.infof("Deadlock detected in JBoss: %s (id=%d)", name, tid);
                } else {
                    Map<String, Object> existing = activeDeadlocks.get(tid);
                    existing.put("lock_name", lockName);
                    existing.put("lock_owner_name", lockOwner);
                    existing.put("state", state);
                }
            }
        } catch (Exception e) {
            LOG.debugf("JBoss deadlock poll error: %s", e.getMessage());
        }
    }

    private void resolveDeadlocks(String ts, Set<Long> currentDeadlockIds) {
        for (Long prevId : new ArrayList<>(activeDeadlocks.keySet())) {
            if (!currentDeadlockIds.contains(prevId)) {
                Map<String, Object> resolved = activeDeadlocks.remove(prevId);
                if (resolved != null) {
                    Map<String, Object> event = new LinkedHashMap<>(resolved);
                    event.put("event", "RESOLVED");
                    event.put("timestamp", ts);
                    event.put("resolved_at", ts);
                    persistEvent(deadlockEventLog, event, MAX_DEADLOCK_EVENTS, DEADLOCK_JSONL_FILE);
                    appendToEventLog("[DEADLOCK RESOLVED] " + ts + " | Thread: " + resolved.get("thread_name")
                            + " | Was locked since: " + resolved.get("detected_at"));
                    LOG.infof("Deadlock resolved: %s (id=%d)", resolved.get("thread_name"), prevId);
                }
            }
        }
    }

    private void pollApiEvents(String ts) {
        try {
            long totalReq = 0, totalErr = 0, maxProcMs = 0;
            List<Map<String, Object>> listenerStats = new ArrayList<>();

            List<String[]> domainServers = client.getRunningDomainServers();
            if (!domainServers.isEmpty()) {
                for (String[] hs : domainServers) {
                    long[] stats = collectApiListenerStats(
                            List.of("host", hs[0], "server", hs[1]), true, listenerStats);
                    totalReq += stats[0]; totalErr += stats[1];
                    maxProcMs = Math.max(maxProcMs, stats[2]);
                }
            } else {
                long[] stats = collectApiListenerStats(List.of(), false, listenerStats);
                totalReq = stats[0]; totalErr = stats[1]; maxProcMs = stats[2];
            }

            if (prevTotalRequests < 0) {
                prevTotalRequests = totalReq;
                prevTotalErrors = totalErr;
                prevMaxProcessingTimeMs = maxProcMs;
                Map<String, Object> event = new LinkedHashMap<>();
                event.put("timestamp", ts);
                event.put("event", "BASELINE");
                event.put("total_requests", totalReq);
                event.put("total_errors", totalErr);
                event.put("max_processing_time_ms", maxProcMs);
                event.put("listeners", listenerStats);
                persistEvent(apiEventLog, event, MAX_API_EVENTS, API_JSONL_FILE);
                appendToLogFile(API_EVENT_LOG_FILE, "[BASELINE] " + ts
                        + " | Requests: " + totalReq + " | Errors: " + totalErr
                        + " | MaxProcessing: " + maxProcMs + "ms");
                return;
            }

            long newRequests = totalReq - prevTotalRequests;
            long newErrors = totalErr - prevTotalErrors;

            if (newRequests > 0 || newErrors > 0) {
                Map<String, Object> event = new LinkedHashMap<>();
                event.put("timestamp", ts);
                event.put("event", "POLL");
                event.put("new_requests", newRequests);
                event.put("new_errors", newErrors);
                event.put("total_requests", totalReq);
                event.put("total_errors", totalErr);
                event.put("max_processing_time_ms", maxProcMs);
                event.put("error_rate_percent", totalReq > 0 ? Math.round(totalErr * 10000.0 / totalReq) / 100.0 : 0);
                event.put("listeners", listenerStats);
                persistEvent(apiEventLog, event, MAX_API_EVENTS, API_JSONL_FILE);

                appendToLogFile(API_EVENT_LOG_FILE, "[POLL] " + ts
                        + " | NewRequests: " + newRequests + " | NewErrors: " + newErrors
                        + " | TotalRequests: " + totalReq + " | TotalErrors: " + totalErr
                        + " | MaxProcessing: " + maxProcMs + "ms");
            }

            if (newErrors > 0) {
                Map<String, Object> errorEvent = new LinkedHashMap<>();
                errorEvent.put("timestamp", ts);
                errorEvent.put("event", "ERRORS_DETECTED");
                errorEvent.put("new_errors", newErrors);
                errorEvent.put("total_errors", totalErr);
                errorEvent.put("error_rate_percent", totalReq > 0 ? Math.round(totalErr * 10000.0 / totalReq) / 100.0 : 0);
                persistEvent(apiEventLog, errorEvent, MAX_API_EVENTS, API_JSONL_FILE);
                appendToLogFile(API_EVENT_LOG_FILE, "[ERRORS_DETECTED] " + ts
                        + " | NewErrors: " + newErrors + " | TotalErrors: " + totalErr);
                LOG.warnf("API errors detected: %d new errors (total: %d)", newErrors, totalErr);
            }

            if (maxProcMs > prevMaxProcessingTimeMs && maxProcMs > 5000) {
                Map<String, Object> slowEvent = new LinkedHashMap<>();
                slowEvent.put("timestamp", ts);
                slowEvent.put("event", "SLOW_REQUEST");
                slowEvent.put("max_processing_time_ms", maxProcMs);
                slowEvent.put("previous_max_ms", prevMaxProcessingTimeMs);
                persistEvent(apiEventLog, slowEvent, MAX_API_EVENTS, API_JSONL_FILE);
                appendToLogFile(API_EVENT_LOG_FILE, "[SLOW_REQUEST] " + ts
                        + " | MaxProcessing: " + maxProcMs + "ms (prev: " + prevMaxProcessingTimeMs + "ms)");
                LOG.warnf("Slow API request detected: max processing time %dms", maxProcMs);
            }

            prevTotalRequests = totalReq;
            prevTotalErrors = totalErr;
            prevMaxProcessingTimeMs = maxProcMs;
        } catch (Exception e) {
            LOG.debugf("API event poll error: %s", e.getMessage());
        }
    }

    private long[] collectApiListenerStats(List<String> prefix, boolean useDirect,
                                               List<Map<String, Object>> listenerStats) {
        long totalReq = 0, totalErr = 0, maxProcMs = 0;
        try {
            List<String> utAddr = new ArrayList<>(prefix);
            utAddr.addAll(List.of("subsystem", "undertow"));
            JsonNode utResult = useDirect
                    ? client.readResourceDirect(utAddr, true, false)
                    : client.readResource(List.of("subsystem", "undertow"), true, false);
            if (!"success".equals(utResult.path("outcome").asText())) return new long[]{0, 0, 0};

            JsonNode servers = utResult.path("result").path("server");
            if (!servers.isObject()) return new long[]{0, 0, 0};

            Iterator<String> serverNames = servers.fieldNames();
            while (serverNames.hasNext()) {
                String serverName = serverNames.next();
                try {
                    List<String> srvAddr = new ArrayList<>(prefix);
                    srvAddr.addAll(List.of("subsystem", "undertow", "server", serverName));
                    JsonNode srv = useDirect
                            ? client.readResourceDirect(srvAddr, true, true)
                            : client.readResource(List.of("subsystem", "undertow", "server", serverName), true, true);
                    if (!"success".equals(srv.path("outcome").asText())) continue;
                    JsonNode srvResult = srv.get("result");

                    for (String lt : List.of("http-listener", "https-listener", "ajp-listener")) {
                        JsonNode listenerNode = srvResult.path(lt);
                        if (!listenerNode.isObject()) continue;
                        Iterator<Map.Entry<String, JsonNode>> fields = listenerNode.fields();
                        while (fields.hasNext()) {
                            Map.Entry<String, JsonNode> e = fields.next();
                            JsonNode ld = e.getValue();
                            if (!ld.isObject()) continue;
                            long rc = ld.path("request-count").asLong(0);
                            long ec = ld.path("error-count").asLong(0);
                            long mp = ld.path("max-processing-time").asLong(0) / 1_000_000;
                            totalReq += rc; totalErr += ec;
                            maxProcMs = Math.max(maxProcMs, mp);
                            listenerStats.add(Map.of("listener", e.getKey(), "type", lt,
                                    "requests", rc, "errors", ec, "max_processing_ms", mp));
                        }
                    }
                } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}
        return new long[]{totalReq, totalErr, maxProcMs};
    }

    private void pollTransactionEvents(String ts) {
        try {
            int committed = 0, aborted = 0, timedOut = 0, inflight = 0;

            List<String[]> domainServers = client.getRunningDomainServers();
            if (!domainServers.isEmpty()) {
                for (String[] hs : domainServers) {
                    try {
                        JsonNode r = client.readResourceDirect(
                                List.of("host", hs[0], "server", hs[1], "subsystem", "transactions"),
                                true, false);
                        if (!"success".equals(r.path("outcome").asText())) continue;
                        JsonNode d = r.get("result");
                        committed += d.path("number-of-committed-transactions").asInt(0);
                        aborted += d.path("number-of-aborted-transactions").asInt(0);
                        timedOut += d.path("number-of-timed-out-transactions").asInt(0);
                        inflight += d.path("number-of-inflight-transactions").asInt(0);
                    } catch (Exception ignored) {}
                }
            } else {
                JsonNode result = client.readResource(List.of("subsystem", "transactions"));
                if (!"success".equals(result.path("outcome").asText())) return;
                JsonNode data = result.get("result");
                committed = data.path("number-of-committed-transactions").asInt(0);
                aborted = data.path("number-of-aborted-transactions").asInt(0);
                timedOut = data.path("number-of-timed-out-transactions").asInt(0);
                inflight = data.path("number-of-inflight-transactions").asInt(0);
            }
            int total = committed + aborted;
            double abortRate = total > 0 ? Math.round(aborted * 10000.0 / total) / 100.0 : 0;

            if (prevCommitted < 0) {
                prevCommitted = committed;
                prevAborted = aborted;
                prevTimedOut = timedOut;
                Map<String, Object> event = new LinkedHashMap<>();
                event.put("timestamp", ts);
                event.put("event", "BASELINE");
                event.put("committed", committed);
                event.put("aborted", aborted);
                event.put("timed_out", timedOut);
                event.put("inflight", inflight);
                event.put("abort_rate_percent", abortRate);
                persistEvent(txEventLog, event, MAX_TX_EVENTS, TX_JSONL_FILE);
                appendToLogFile(TX_EVENT_LOG_FILE, "[BASELINE] " + ts
                        + " | Committed: " + committed + " | Aborted: " + aborted
                        + " | TimedOut: " + timedOut + " | Inflight: " + inflight
                        + " | AbortRate: " + abortRate + "%");
                return;
            }

            if (committed < prevCommitted || aborted < prevAborted) {
                LOG.infof("JBoss counter reset detected (prev committed=%d, now=%d). Re-baselining.",
                        prevCommitted, committed);
                prevCommitted = 0;
                prevAborted = 0;
                prevTimedOut = 0;
                Map<String, Object> resetEvent = new LinkedHashMap<>();
                resetEvent.put("timestamp", ts);
                resetEvent.put("event", "BASELINE");
                resetEvent.put("committed", committed);
                resetEvent.put("aborted", aborted);
                resetEvent.put("timed_out", timedOut);
                resetEvent.put("inflight", inflight);
                resetEvent.put("abort_rate_percent", abortRate);
                persistEvent(txEventLog, resetEvent, MAX_TX_EVENTS, TX_JSONL_FILE);
                appendToLogFile(TX_EVENT_LOG_FILE, "[BASELINE] " + ts
                        + " | Committed: " + committed + " | Aborted: " + aborted
                        + " | TimedOut: " + timedOut + " | Inflight: " + inflight
                        + " | AbortRate: " + abortRate + "%");
            }

            int newCommitted = committed - prevCommitted;
            int newAborted = aborted - prevAborted;
            int newTimedOut = timedOut - prevTimedOut;

            if (newCommitted > 0 || newAborted > 0 || newTimedOut > 0) {
                Map<String, Object> event = new LinkedHashMap<>();
                event.put("timestamp", ts);
                event.put("event", "POLL");
                event.put("new_committed", newCommitted);
                event.put("new_aborted", newAborted);
                event.put("new_timed_out", newTimedOut);
                event.put("total_committed", committed);
                event.put("total_aborted", aborted);
                event.put("total_timed_out", timedOut);
                event.put("inflight", inflight);
                event.put("abort_rate_percent", abortRate);
                persistEvent(txEventLog, event, MAX_TX_EVENTS, TX_JSONL_FILE);

                appendToLogFile(TX_EVENT_LOG_FILE, "[POLL] " + ts
                        + " | NewCommitted: " + newCommitted + " | NewAborted: " + newAborted
                        + " | NewTimedOut: " + newTimedOut + " | Inflight: " + inflight
                        + " | AbortRate: " + abortRate + "%");
            }

            if (newAborted > 0) {
                Map<String, Object> abortEvent = new LinkedHashMap<>();
                abortEvent.put("timestamp", ts);
                abortEvent.put("event", "ABORTS_DETECTED");
                abortEvent.put("new_aborted", newAborted);
                abortEvent.put("total_aborted", aborted);
                abortEvent.put("abort_rate_percent", abortRate);
                persistEvent(txEventLog, abortEvent, MAX_TX_EVENTS, TX_JSONL_FILE);
                appendToLogFile(TX_EVENT_LOG_FILE, "[ABORTS_DETECTED] " + ts
                        + " | NewAborted: " + newAborted + " | TotalAborted: " + aborted
                        + " | AbortRate: " + abortRate + "%");
                LOG.warnf("Transaction aborts detected: %d new aborts (rate: %.2f%%)", newAborted, abortRate);
            }

            if (newTimedOut > 0) {
                Map<String, Object> timeoutEvent = new LinkedHashMap<>();
                timeoutEvent.put("timestamp", ts);
                timeoutEvent.put("event", "TIMEOUTS_DETECTED");
                timeoutEvent.put("new_timed_out", newTimedOut);
                timeoutEvent.put("total_timed_out", timedOut);
                persistEvent(txEventLog, timeoutEvent, MAX_TX_EVENTS, TX_JSONL_FILE);
                appendToLogFile(TX_EVENT_LOG_FILE, "[TIMEOUTS_DETECTED] " + ts
                        + " | NewTimedOut: " + newTimedOut + " | TotalTimedOut: " + timedOut);
                LOG.warnf("Transaction timeouts detected: %d new timeouts (total: %d)", newTimedOut, timedOut);
            }

            if (abortRate > 20) {
                Map<String, Object> criticalEvent = new LinkedHashMap<>();
                criticalEvent.put("timestamp", ts);
                criticalEvent.put("event", "HIGH_ABORT_RATE");
                criticalEvent.put("abort_rate_percent", abortRate);
                criticalEvent.put("total_aborted", aborted);
                criticalEvent.put("total_committed", committed);
                persistEvent(txEventLog, criticalEvent, MAX_TX_EVENTS, TX_JSONL_FILE);
                appendToLogFile(TX_EVENT_LOG_FILE, "[HIGH_ABORT_RATE] " + ts
                        + " | AbortRate: " + abortRate + "% | Aborted: " + aborted
                        + " | Committed: " + committed);
                LOG.errorf("High transaction abort rate: %.2f%%", abortRate);
            }

            prevCommitted = committed;
            prevAborted = aborted;
            prevTimedOut = timedOut;
        } catch (Exception e) {
            LOG.debugf("Transaction event poll error: %s", e.getMessage());
        }
    }

    private Path getLogDir() {
        Path dir = Paths.get(System.getProperty("user.dir"), "logs");
        try { Files.createDirectories(dir); } catch (IOException ignored) {}
        return dir;
    }

    private Path getEventLogPath() {
        return getLogDir().resolve(EVENT_LOG_FILE);
    }

    public Path getApiEventLogPath() {
        return getLogDir().resolve(API_EVENT_LOG_FILE);
    }

    public Path getTransactionEventLogPath() {
        return getLogDir().resolve(TX_EVENT_LOG_FILE);
    }

    private void appendToEventLog(String line) {
        appendToLogFile(EVENT_LOG_FILE, line);
    }

    private void appendToLogFile(String filename, String line) {
        try {
            Files.writeString(getLogDir().resolve(filename), line + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOG.debugf("Failed to write to %s: %s", filename, e.getMessage());
        }
    }

    private static <T> void addCapped(Deque<T> deque, T item, int max) {
        deque.addLast(item);
        while (deque.size() > max) {
            deque.pollFirst();
        }
    }

    private void persistEvent(Deque<Map<String, Object>> deque, Map<String, Object> event,
                               int max, String jsonlFile) {
        addCapped(deque, event, max);
        appendJsonlEvent(jsonlFile, event);
    }

    private static boolean hasActivityChanged(List<String> prevStack, List<String> newStack) {
        if (prevStack == null || newStack == null) return prevStack != newStack;
        if (prevStack.isEmpty() && newStack.isEmpty()) return false;
        if (prevStack.isEmpty() || newStack.isEmpty()) return true;
        int depth = Math.min(3, Math.min(prevStack.size(), newStack.size()));
        for (int i = 0; i < depth; i++) {
            String prev = extractMethod(prevStack.get(i));
            String curr = extractMethod(newStack.get(i));
            if (!prev.equals(curr)) return true;
        }
        return false;
    }

    private static String extractMethod(String frame) {
        if (frame == null) return "";
        int paren = frame.indexOf('(');
        return paren > 0 ? frame.substring(0, paren) : frame;
    }

    private static String topFrameSummary(String frame) {
        if (frame == null || frame.isEmpty()) return "";
        int paren = frame.indexOf('(');
        if (paren < 0) return frame;
        String methodPart = frame.substring(0, paren);
        int lastDot = methodPart.lastIndexOf('.');
        if (lastDot < 0) return frame;
        int secondLastDot = methodPart.lastIndexOf('.', lastDot - 1);
        String shortClass = secondLastDot >= 0 ? methodPart.substring(secondLastDot + 1) : methodPart;
        return shortClass + frame.substring(paren);
    }
}
