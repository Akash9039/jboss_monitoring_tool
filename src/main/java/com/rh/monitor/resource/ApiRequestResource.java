package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.config.JBossConfig;
import com.rh.monitor.config.ServerContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.rh.monitor.service.HistoryTracker;
import com.rh.monitor.util.DomainHelper;
import org.jboss.logging.Logger;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class ApiRequestResource {

    private static final Logger LOG = Logger.getLogger(ApiRequestResource.class);

    @Inject
    JBossManagementClient client;

    @Inject
    JBossConfig config;

    @Inject
    ThreadResource threadResource;

    @Inject
    ServerContext serverContext;

    @Inject
    HistoryTracker historyTracker;

    // Domain:     %h %l %u %t "%r" %s %b ...extras (Thread/SessionID/TimeTaken labeled)
    // Standalone: %h %t "%r" %s %D %b %I|%S
    private static final Pattern ACCESS_LOG_RE = Pattern.compile(
            "^(\\S+)\\s+(?:\\S+\\s+\\S+\\s+)?\\[([^\\]]+)\\]\\s+\"(\\S+)\\s+(\\S+)\\s+\\S+\"\\s+(\\d{3})\\s+(\\d+)(.*)$");
    // Standalone trailing: response_time_ms bytes thread|session
    // Bytes (%b) may be "-" when the response size is unknown (uploads, HTTP/2, chunked) —
    // accept it so these lines are still recognised as standalone (otherwise response time
    // and thread are lost and the byte count is mislabelled).
    private static final Pattern STANDALONE_TAIL_RE = Pattern.compile(
            "^\\s+(\\d+|-)\\s+(.+?)\\|(.*)$");

    private static final DateTimeFormatter ACCESS_LOG_DATE = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");

    private static final Set<String> IDLE_INDICATORS = Set.of(
            "unsafe.park", "object.wait", "locksupport.park",
            "selectorimpl.select", "epollselectorimpl", "kevent",
            "socketinputstream.read", "accept(", "conditionobject.await");

    private static final ConcurrentHashMap<Object, Map<String, Object>> activeRequestTracker = new ConcurrentHashMap<>();

    // ---- Servlet URL learning (thread + time-window correlation) --------------------------------
    // Annotation-mapped servlets (@WebServlet) expose "mappings => undefined" in JBoss management,
    // and the access log only holds COMPLETED requests — so an in-flight servlet request has no
    // directly-retrievable URL. We recover it the same way RCA correlates files: remember every
    // in-flight request we sample (thread + start time + handler), and when a completed access-log
    // entry lines up with one of those observations (same worker thread, same start instant) we
    // learn "this servlet class serves this URL" and show the full path next time.
    // recentObservations: key "threadName@startMs" -> {thread, deployment, servlet_class, start_ms, last_seen_ms}
    private static final ConcurrentHashMap<String, Map<String, Object>> recentObservations = new ConcurrentHashMap<>();
    // learnedServletUri: key "deployment::SimpleServletClass" -> [method, uri]
    private static final ConcurrentHashMap<String, String[]> learnedServletUri = new ConcurrentHashMap<>();
    private static final long OBSERVATION_TTL_MS = 15 * 60 * 1000L;
    // The tracker stamps start at first observation, which lags the real start by up to one poll
    // interval; allow a generous window so the completed-entry start still matches.
    private static final long CORRELATION_TOLERANCE_MS = 20_000L;

    @GET
    @Path("/access-log")
    public Map<String, Object> getAccessLog() {
        LOG.info("Fetching access log data");
        String jbossHome = config.getJbossHome();
        boolean isServerSelected = false;
        try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

        List<Map<String, Object>> entries;
        if (!isServerSelected && client.isDomainMode()) {
            entries = new ArrayList<>();
            Map<String, String> serverGroupMap = buildServerGroupMap();
            List<DomainHelper.ServerLog> domainLogs = DomainHelper.findAllDomainLogs(config.getAllDomainHomes(), "access_log.log");
            for (DomainHelper.ServerLog sl : domainLogs) {
                List<Map<String, Object>> parsed = parseAccessLogFromPath(sl.path, 5000);
                for (Map<String, Object> e : parsed) {
                    e.put("server_name", sl.serverName);
                    String group = serverGroupMap.get(sl.serverName);
                    if (group != null) e.put("server_group", group);
                }
                entries.addAll(parsed);
            }
            entries.sort((a, b) -> {
                String aTs = (String) a.getOrDefault("timestamp", "");
                String bTs = (String) b.getOrDefault("timestamp", "");
                return bTs.compareTo(aTs);
            });
        } else {
            entries = parseAccessLog(5000);
        }
        if (entries.isEmpty()) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("status", "ok");
            r.put("message", "No access log data found. Ensure access-log is enabled in JBoss undertow config.");
            r.put("requests", List.of());
            r.put("endpoints", List.of());
            r.put("deployments", List.of());
            return r;
        }

        // Filter to deployed apps
        Set<String> validDeployments = getValidDeployments();
        if (!validDeployments.isEmpty()) {
            entries = entries.stream()
                    .filter(e -> validDeployments.contains(((String) e.getOrDefault("deployment", "")).toLowerCase()))
                    .collect(Collectors.toList());
        }

        if (entries.isEmpty()) {
            return Map.of("status", "ok", "message", "No requests found for deployed applications.",
                    "requests", List.of(), "endpoints", List.of(), "deployments", List.of());
        }

        Map<String, Map<String, Object>> endpointStats = new LinkedHashMap<>();
        Map<String, Map<String, Object>> deployStats = new LinkedHashMap<>();

        for (Map<String, Object> e : entries) {
            String key = e.get("method") + " " + e.get("uri");
            endpointStats.computeIfAbsent(key, k -> {
                Map<String, Object> s = new LinkedHashMap<>();
                s.put("method", e.get("method"));
                s.put("uri", e.get("uri"));
                s.put("deployment", e.get("deployment"));
                s.put("hit_count", 0);
                s.put("total_time_ms", 0L);
                s.put("min_time_ms", Long.MAX_VALUE);
                s.put("max_time_ms", 0L);
                s.put("status_codes", new LinkedHashMap<String, Integer>());
                s.put("first_hit", e.get("timestamp"));
                s.put("last_hit", e.get("timestamp"));
                s.put("_servers", new LinkedHashSet<String>());
                return s;
            });
            Map<String, Object> s = endpointStats.get(key);
            s.put("hit_count", (int) s.get("hit_count") + 1);
            long rt = ((Number) e.get("response_time_ms")).longValue();
            s.put("total_time_ms", (long) s.get("total_time_ms") + rt);
            s.put("min_time_ms", Math.min((long) s.get("min_time_ms"), rt));
            s.put("max_time_ms", Math.max((long) s.get("max_time_ms"), rt));
            @SuppressWarnings("unchecked")
            Map<String, Integer> sc = (Map<String, Integer>) s.get("status_codes");
            sc.merge(String.valueOf(e.get("status")), 1, Integer::sum);
            s.put("last_hit", e.get("timestamp"));
            if (e.get("server_name") != null) {
                @SuppressWarnings("unchecked")
                Set<String> srvs = (Set<String>) s.get("_servers");
                String srvLabel = (String) e.get("server_name");
                if (e.get("server_group") != null) srvLabel += " (" + e.get("server_group") + ")";
                srvs.add(srvLabel);
            }

            String dep = (String) e.get("deployment");
            deployStats.computeIfAbsent(dep, k -> {
                Map<String, Object> ds = new LinkedHashMap<>();
                ds.put("deployment", dep);
                ds.put("total_requests", 0);
                ds.put("total_time_ms", 0L);
                ds.put("error_count", 0);
                ds.put("endpoints", new HashSet<String>());
                return ds;
            });
            Map<String, Object> ds = deployStats.get(dep);
            ds.put("total_requests", (int) ds.get("total_requests") + 1);
            ds.put("total_time_ms", (long) ds.get("total_time_ms") + rt);
            @SuppressWarnings("unchecked")
            Set<String> eps = (Set<String>) ds.get("endpoints");
            eps.add((String) e.get("uri"));
            if (((Number) e.get("status")).intValue() >= 400) {
                ds.put("error_count", (int) ds.get("error_count") + 1);
            }
        }

        List<Map<String, Object>> endpoints = endpointStats.values().stream()
                .sorted((a, b) -> Long.compare((long) b.get("max_time_ms"), (long) a.get("max_time_ms")))
                .peek(s -> {
                    int hits = (int) s.get("hit_count");
                    s.put("avg_time_ms", hits > 0 ? Math.round((long) s.get("total_time_ms") * 100.0 / hits) / 100.0 : 0);
                    if ((long) s.get("min_time_ms") == Long.MAX_VALUE) s.put("min_time_ms", 0L);
                    @SuppressWarnings("unchecked")
                    Set<String> srvs = (Set<String>) s.remove("_servers");
                    if (srvs != null && !srvs.isEmpty()) {
                        s.put("servers", new ArrayList<>(srvs));
                    }
                })
                .collect(Collectors.toList());

        List<Map<String, Object>> deployments = deployStats.values().stream()
                .sorted((a, b) -> Integer.compare((int) b.get("total_requests"), (int) a.get("total_requests")))
                .peek(ds -> {
                    int total = (int) ds.get("total_requests");
                    ds.put("avg_time_ms", total > 0 ? Math.round((long) ds.get("total_time_ms") * 100.0 / total) / 100.0 : 0);
                    @SuppressWarnings("unchecked")
                    Set<String> eps = (Set<String>) ds.get("endpoints");
                    ds.put("endpoint_count", eps.size());
                    ds.remove("endpoints");
                })
                .collect(Collectors.toList());

        int size = entries.size();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("total_requests", size);
        result.put("requests", entries.subList(Math.max(0, size - 500), size));
        result.put("endpoints", endpoints);
        result.put("deployments", deployments);
        result.put("event_log_file", historyTracker.getApiEventLogPath().toString());
        return result;
    }

    @GET
    @Path("/api-event-log")
    @Produces(MediaType.TEXT_PLAIN)
    public String getApiEventLog() {
        LOG.info("Fetching API event log");
        java.nio.file.Path logPath = historyTracker.getApiEventLogPath();
        if (!Files.exists(logPath)) return "No API events recorded yet. File: " + logPath;
        try {
            return Files.readString(logPath);
        } catch (IOException e) {
            return "Error reading API event log: " + e.getMessage();
        }
    }

    @GET
    @Path("/api-events")
    public Map<String, Object> getApiEvents() {
        LOG.info("Fetching API events data");
        return Map.of("status", "ok",
                "events", new ArrayList<>(HistoryTracker.getApiEventLog()),
                "event_log_file", historyTracker.getApiEventLogPath().toString());
    }

    @GET
    @Path("/active-api-requests")
    public Map<String, Object> getActiveApiRequests() {
        LOG.info("Fetching active API requests");
        try {
            Map<String, String> threadServerName = new LinkedHashMap<>();
            List<Map<String, Object>> allThreads;
            try {
                allThreads = collectActiveThreads(threadServerName);
            } catch (DumpErrorException de) {
                return de.error;
            }

            // Stamp any newly-seen in-flight requests (without pruning — the background poller
            // owns pruning against a full-server view). The tracker is kept warm continuously by
            // the @Scheduled poller, so a request already running when this page is opened is
            // timed from when it actually started being processed, not from the page load.
            stampActiveRequests(allThreads, false);

            List<Map<String, Object>> accessEntries = parseRecentAccessEntries();
            learnServletUris(accessEntries);
            Map<String, Map<String, Object>> threadLastRequest = buildThreadToLastRequestMap(accessEntries);
            Map<String, List<String[]>> deploymentUris = buildDeploymentUriIndex(accessEntries);

            String nowIso = java.time.Instant.now().toString();
            long nowMs = System.currentTimeMillis();
            List<Map<String, Object>> activeRequests = new ArrayList<>();

            for (Map<String, Object> t : allThreads) {
                String name = (String) t.getOrDefault("thread_name", "");
                String state = (String) t.getOrDefault("thread_state", "");
                @SuppressWarnings("unchecked")
                List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());
                Object tid = t.get("thread_id");
                String serverName = (String) t.getOrDefault("_server_name", "");

                if (!isProcessingRequest(name, state, stack)) continue;

                String tidKey = serverName.isEmpty() ? String.valueOf(tid) : serverName + "::" + tid;

                // The access log only records COMPLETED requests, so a pooled worker thread's last
                // logged entry belongs to a PREVIOUS request — never the one in flight now. Trust
                // the LIVE STACK for the handler identity and only fall back to the access log when
                // the stack shows no application frame.
                Map<String, Object> lastReq = threadLastRequest.getOrDefault(name, Map.of());
                String logMethod = (String) lastReq.getOrDefault("method", "");
                String logUri = (String) lastReq.getOrDefault("uri", "");
                String logDeployment = (String) lastReq.getOrDefault("deployment", "");
                String clientIp = (String) lastReq.getOrDefault("client_ip", "");

                String method, uri, deployment, servletClass;
                String[] handler = extractHandlerFromStack(stack);
                if (handler != null) {
                    boolean isServlet = "true".equals(handler[4]);
                    String javaMethod = handler[2];
                    servletClass = (javaMethod == null || javaMethod.isEmpty())
                            ? handler[1] : handler[1] + "." + javaMethod;
                    // Deployment/context-root: the access log gives the real name (e.g.
                    // "department-drive"); a package guess would only yield "department".
                    deployment = !logDeployment.isEmpty() ? logDeployment : guessDeploymentFromStack(stack);
                    method = handler[3] != null ? handler[3]
                            : (!logMethod.isEmpty() ? logMethod : guessMethodFromStack(stack));
                    if (isServlet) {
                        // A concrete servlet on the stack: the logged URI (which may be a totally
                        // different completed request — e.g. a JAX-RS download) does NOT describe
                        // what is running, and annotation-mapped servlets have no URL in JBoss
                        // management. Recover the real path: (1) a URL learned by correlating a
                        // past completed request of this servlet, (2) a name-hint match against the
                        // deployment's known URLs, else (3) a wildcard under the context root.
                        String learned = null;
                        String[] lu = deployment.isEmpty() ? null
                                : learnedServletUri.get(deployment + "::" + handler[1]);
                        if (lu != null) {
                            learned = lu[1];
                            if (lu[0] != null && !lu[0].isEmpty()) method = lu[0];
                        }
                        if (learned == null) {
                            learned = guessUriByServletName(handler[1], deployment, method, deploymentUris);
                        }
                        uri = learned != null ? learned
                                : (deployment.isEmpty() ? "/**" : "/" + deployment + "/**");
                    } else {
                        // JAX-RS/other handler: keep the logged URI only when it is from the same
                        // deployment (best-effort real path).
                        boolean logUsable = !logUri.isEmpty()
                                && !deployment.isEmpty() && deployment.equals(logDeployment);
                        uri = logUsable ? logUri
                                : (deployment.isEmpty() ? "/**" : "/" + deployment + "/**");
                    }
                } else {
                    // No application frame on the stack — fall back to the access log entirely.
                    servletClass = extractServletClass(stack);
                    method = logMethod.isEmpty() ? guessMethodFromStack(stack) : logMethod;
                    deployment = logDeployment.isEmpty() ? guessDeploymentFromStack(stack) : logDeployment;
                    uri = logUri;
                }

                // Read timing from the tracker so it reflects the real elapsed processing time
                // since the request was first observed (by the poller or an earlier poll),
                // independent of when this endpoint was hit.
                Map<String, Object> prev = activeRequestTracker.get(tidKey);
                String startTime;
                double elapsedS;
                if (prev != null) {
                    startTime = (String) prev.get("start_time");
                    long startMs = (long) prev.getOrDefault("start_ms", nowMs);
                    elapsedS = (nowMs - startMs) / 1000.0;
                } else {
                    startTime = nowIso;
                    elapsedS = 0.0;
                }

                String reqStatus;
                if ("BLOCKED".equals(state)) reqStatus = "BLOCKED";
                else if (elapsedS > 60) reqStatus = "STUCK";
                else if (elapsedS > 15) reqStatus = "SLOW";
                else reqStatus = "RUNNING";

                Map<String, Object> req = new LinkedHashMap<>();
                req.put("thread_name", name);
                req.put("thread_id", tid);
                req.put("thread_state", state);
                req.put("start_time", startTime);
                req.put("elapsed_seconds", Math.round(elapsedS * 10.0) / 10.0);
                req.put("method", method.isEmpty() ? "?" : method);
                req.put("uri", uri.isEmpty() ? (deployment.isEmpty() ? "/**" : "/" + deployment + "/**") : uri);
                req.put("deployment", deployment);
                req.put("status", reqStatus);
                req.put("client_ip", clientIp);
                req.put("servlet_class", servletClass);
                req.put("lock_name", t.get("lock_name"));
                req.put("lock_owner_name", t.get("lock_owner_name"));
                req.put("stack_trace", stack.subList(0, Math.min(5, stack.size())));
                String srvLabel = threadServerName.get(serverName + "::" + name);
                if (srvLabel != null) req.put("server_name", srvLabel);
                activeRequests.add(req);
            }

            activeRequests.sort((a, b) -> {
                int so = reqStatusOrder((String) a.get("status")) - reqStatusOrder((String) b.get("status"));
                if (so != 0) return so;
                return Double.compare((double) b.get("elapsed_seconds"), (double) a.get("elapsed_seconds"));
            });

            Map<String, Integer> statusCounts = new LinkedHashMap<>();
            for (Map<String, Object> r : activeRequests) {
                statusCounts.merge((String) r.get("status"), 1, Integer::sum);
            }

            int stuck = statusCounts.getOrDefault("STUCK", 0);
            int blocked = statusCounts.getOrDefault("BLOCKED", 0);
            int slow = statusCounts.getOrDefault("SLOW", 0);
            int running = statusCounts.getOrDefault("RUNNING", 0);
            String health = "HEALTHY";
            if (stuck > 0 || blocked > 3) health = "CRITICAL";
            else if (blocked > 0 || slow > 5) health = "WARNING";
            else if (slow > 0) health = "MODERATE";

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("status", "ok");
            result.put("active_requests", activeRequests);
            result.put("total_active", activeRequests.size());
            result.put("status_summary", statusCounts);
            result.put("stuck_count", stuck);
            result.put("blocked_count", blocked);
            result.put("slow_count", slow);
            result.put("running_count", running);
            result.put("health", health);
            return result;
        } catch (Exception e) {
            LOG.errorf(e, "Failed to get active API requests");
            return Map.of("error", "Failed to get active API requests: " + e.getMessage());
        }
    }

    /** Thrown by {@link #collectActiveThreads} when the standalone thread dump returns an error
     *  map, so the HTTP endpoint can surface it while the background poller quietly skips. */
    private static class DumpErrorException extends RuntimeException {
        final Map<String, Object> error;
        DumpErrorException(Map<String, Object> error) { this.error = error; }
    }

    /**
     * Collects all threads — standalone, a selected domain server, or every running domain
     * server — filling {@code threadServerName} with per-thread display labels. Shared by the
     * HTTP endpoint and the background tracker poller. Safe to call off the request thread:
     * {@code serverContext} (request-scoped) access is guarded, so with no active request it
     * falls back to the unscoped (all-servers / standalone) view.
     */
    private List<Map<String, Object>> collectActiveThreads(Map<String, String> threadServerName) {
        boolean isServerSelected = false;
        try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}
        boolean isDomain = !isServerSelected && client.isDomainMode();

        List<Map<String, Object>> allThreads = new ArrayList<>();

        if (isDomain) {
            Map<String, String> serverGroupMap = buildServerGroupMap();
            List<String[]> domainServers = client.getRunningDomainServers();
            // Fan the per-server dump-all-threads out across the CLI connection pool
            // (getDomainServerThreads -> mgmtRequest is safe off the request thread), then
            // merge results sequentially so the shared maps aren't touched concurrently.
            List<Map<String, Object>> collected = domainServers.parallelStream().map(hs -> {
                Map<String, Object> out = new HashMap<>();
                try {
                    List<Map<String, Object>> serverThreads = getDomainServerThreads(hs[0], hs[1]);
                    String group = serverGroupMap.getOrDefault(hs[1], "");
                    String label = group.isEmpty() ? hs[0] + "/" + hs[1] : hs[0] + "/" + hs[1] + " (" + group + ")";
                    out.put("threads", serverThreads);
                    out.put("label", label);
                    out.put("server", hs[1]);
                } catch (Exception e) {
                    LOG.debugf("Failed thread dump for %s/%s: %s", hs[0], hs[1], e.getMessage());
                }
                return out;
            }).toList();
            for (Map<String, Object> c : collected) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> serverThreads = (List<Map<String, Object>>) c.get("threads");
                if (serverThreads == null) continue;
                String label = (String) c.get("label");
                String server = (String) c.get("server");
                for (Map<String, Object> t : serverThreads) {
                    threadServerName.put(server + "::" + t.get("thread_name"), label);
                }
                allThreads.addAll(serverThreads);
            }
        } else {
            Map<String, Object> dump = threadResource.getThreadDump(500);
            if (dump.containsKey("error")) throw new DumpErrorException(dump);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> threads = (List<Map<String, Object>>) dump.get("threads");
            if (threads != null) {
                if (isServerSelected) {
                    String selServer = serverContext.getServer();
                    String selLabel = serverContext.getHost() + "/" + selServer;
                    for (Map<String, Object> t : threads) {
                        t.put("_server_name", selServer);
                        threadServerName.put(selServer + "::" + t.get("thread_name"), selLabel);
                    }
                }
                allThreads.addAll(threads);
            }
        }
        return allThreads;
    }

    /**
     * Records the first-observed start time for each thread that is actively processing a
     * request (keyed by server::thread-id), and re-stamps when the same thread starts a new
     * request (its top stack frame changes). This is what makes "Processing Time" count from
     * when the request actually started rather than from when the page was opened.
     *
     * <p>Synchronized so the HTTP endpoint and the background poller never corrupt the tracker.
     * Only the background poll passes {@code prune=true}: it sees every server's threads, so it
     * can safely drop entries for requests that have finished. The endpoint passes
     * {@code prune=false} because a server-scoped call only sees a subset of threads and must
     * not evict other servers' live entries.
     */
    private synchronized void stampActiveRequests(List<Map<String, Object>> allThreads, boolean prune) {
        long nowMs = System.currentTimeMillis();
        String nowIso = java.time.Instant.now().toString();
        Set<Object> seenTids = new HashSet<>();
        for (Map<String, Object> t : allThreads) {
            String name = (String) t.getOrDefault("thread_name", "");
            String state = (String) t.getOrDefault("thread_state", "");
            @SuppressWarnings("unchecked")
            List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());
            if (!isProcessingRequest(name, state, stack)) continue;
            Object tid = t.get("thread_id");
            String serverName = (String) t.getOrDefault("_server_name", "");
            String tidKey = serverName.isEmpty() ? String.valueOf(tid) : serverName + "::" + tid;
            seenTids.add(tidKey);
            String topFrame = stack.isEmpty() ? "" : stack.get(0);
            Map<String, Object> prev = activeRequestTracker.get(tidKey);
            // New request on this thread, or the same thread now serving a different request
            // (top frame changed) → (re)stamp the start time. Otherwise keep the original start.
            long startMs;
            if (prev == null || !topFrame.equals(prev.get("top_frame"))) {
                Map<String, Object> tracker = new HashMap<>();
                tracker.put("start_time", nowIso);
                tracker.put("top_frame", topFrame);
                tracker.put("start_ms", nowMs);
                activeRequestTracker.put(tidKey, tracker);
                startMs = nowMs;
            } else {
                startMs = (long) prev.getOrDefault("start_ms", nowMs);
            }

            // Remember this in-flight servlet so we can later learn its URL from the access log
            // (a completed entry on the same thread that started at the same instant).
            String[] handler = extractHandlerFromStack(stack);
            if (handler != null && "true".equals(handler[4])) {
                String obsKey = name + "@" + startMs;
                Map<String, Object> obs = recentObservations.computeIfAbsent(obsKey, k -> new HashMap<>());
                obs.put("thread", name);
                obs.put("servlet_class", handler[1]);
                obs.put("start_ms", startMs);
                obs.put("last_seen_ms", nowMs);
            }
        }
        if (prune) activeRequestTracker.keySet().removeIf(k -> !seenTids.contains(k));
        recentObservations.values().removeIf(o ->
                nowMs - (long) ((Number) o.getOrDefault("last_seen_ms", 0L)).longValue() > OBSERVATION_TTL_MS);
    }

    /**
     * Keeps the active-request tracker warm even when nobody has the API Data page open, so the
     * moment the page is loaded each in-flight request already shows its true elapsed time
     * (accurate to within one poll interval). Runs unscoped (all domain servers / standalone)
     * and quietly skips a cycle if JBoss is unreachable or busy.
     */
    @io.quarkus.scheduler.Scheduled(every = "{monitor.active-requests.poll-interval:5s}",
            delayed = "5s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void pollActiveRequests() {
        try {
            List<Map<String, Object>> threads = collectActiveThreads(new LinkedHashMap<>());
            stampActiveRequests(threads, true);
            // Correlate freshly-completed requests with in-flight observations so servlet URLs are
            // learned continuously, even when nobody has the API Data page open.
            try {
                learnServletUris(parseRecentAccessEntries());
            } catch (Exception ignored) {
                // access log unreadable this cycle — observations stay cached for the next attempt.
            }
        } catch (DumpErrorException de) {
            // JBoss dump unavailable this cycle — leave existing entries as-is.
        } catch (Exception e) {
            LOG.debugf("Active-request poll skipped: %s", e.getMessage());
        }
    }

    private List<Map<String, Object>> getDomainServerThreads(String host, String server) {
        List<Map<String, Object>> threads = new ArrayList<>();
        try {
            List<String> addr = List.of("host", host, "server", server,
                    "core-service", "platform-mbean", "type", "threading");
            Map<String, Object> op = new LinkedHashMap<>();
            op.put("operation", "dump-all-threads");
            op.put("address", addr);
            op.put("locked-monitors", true);
            op.put("locked-synchronizers", true);
            JsonNode result = client.mgmtRequest(op);
            if ("success".equals(result.path("outcome").asText())) {
                JsonNode raw = result.get("result");
                if (raw != null && raw.isArray()) {
                    for (JsonNode t : raw) {
                        Map<String, Object> info = parseMgmtThreadInfo(t);
                        info.put("_server_name", server);
                        threads.add(info);
                    }
                }
            }
        } catch (Exception e) {
            LOG.debugf("getDomainServerThreads %s/%s failed: %s", host, server, e.getMessage());
        }
        return threads;
    }

    private Map<String, Object> parseMgmtThreadInfo(JsonNode t) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("thread_id", t.path("thread-id").asLong());
        info.put("thread_name", t.path("thread-name").asText("unknown"));
        info.put("thread_state", t.path("thread-state").asText("unknown"));
        info.put("lock_name", t.path("lock-name").isNull() ? null : t.path("lock-name").asText());
        info.put("lock_owner_name", t.path("lock-owner-name").isNull() ? null : t.path("lock-owner-name").asText());
        List<String> stackTrace = new ArrayList<>();
        JsonNode frames = t.path("stack-trace");
        if (frames.isArray()) {
            int limit = Math.min(frames.size(), 200);
            for (int i = 0; i < limit; i++) {
                JsonNode frame = frames.get(i);
                String cls = frame.path("class-name").asText("");
                String method = frame.path("method-name").asText("");
                String file = frame.path("file-name").asText("");
                int line = frame.path("line-number").asInt(-1);
                stackTrace.add(cls + "." + method + "(" + file + ":" + line + ")");
            }
        }
        info.put("stack_trace", stackTrace);
        return info;
    }

    private boolean isProcessingRequest(String threadName, String state, List<String> stack) {
        String nameLower = threadName.toLowerCase();
        boolean isWorkerThread = nameLower.contains("task-") || nameLower.contains("http")
                || nameLower.contains("undertow") || nameLower.contains("xnio")
                || nameLower.contains("i/o-");
        if (!isWorkerThread) return false;

        String stackStr = String.join(" ", stack).toLowerCase();
        boolean hasRequestStack = stackStr.contains("servlet") || stackStr.contains("resteasy")
                || stackStr.contains("jakarta.ws.rs") || stackStr.contains("javax.ws.rs")
                || stackStr.contains("dofilter") || stackStr.contains("httpservlet")
                || stackStr.contains("handlerequest") || stackStr.contains("processrequest");
        if (!hasRequestStack) return false;

        for (String indicator : IDLE_INDICATORS) {
            if (stackStr.contains(indicator)) return false;
        }
        return true;
    }

    /**
     * Identifies the application handler actually executing on a worker thread, from its live
     * stack. This is authoritative for an in-flight request — unlike the access log, which only
     * contains COMPLETED requests, so a pooled worker thread's last logged entry is a PREVIOUS
     * request (the cause of an upload being mislabelled with an earlier download's URL).
     *
     * @return {@code [fqcn, simpleClass, javaMethod, httpMethod|null, isServlet("true"/"false")]}
     *         or {@code null} if no application frame is found.
     */
    private String[] extractHandlerFromStack(List<String> stack) {
        // 1) A concrete application servlet: <appClass>.doGet/doPost/doPut/doDelete/service(...)
        for (String frame : stack) {
            String httpMethod;
            if (frame.contains(".doGet(")) httpMethod = "GET";
            else if (frame.contains(".doPost(")) httpMethod = "POST";
            else if (frame.contains(".doPut(")) httpMethod = "PUT";
            else if (frame.contains(".doDelete(")) httpMethod = "DELETE";
            else if (frame.contains(".service(")) httpMethod = null;
            else continue;
            String clsMethod = frame.split("\\(")[0];          // com.x.FooServlet.doPost
            int lastDot = clsMethod.lastIndexOf('.');
            if (lastDot <= 0) continue;
            String fqcn = clsMethod.substring(0, lastDot);
            if (isFrameworkClass(fqcn)) continue;              // skip jakarta HttpServlet base etc.
            String javaMethod = clsMethod.substring(lastDot + 1);
            return new String[]{ fqcn, simpleClassName(fqcn), javaMethod, httpMethod, "true" };
        }
        // 2) A JAX-RS resource / endpoint / controller (application class, not framework).
        for (String frame : stack) {
            String fl = frame.toLowerCase();
            if ((fl.contains("resource") || fl.contains("endpoint") || fl.contains("controller"))
                    && !isFrameworkClass(frame)) {
                String clsMethod = frame.split("\\(")[0];
                int lastDot = clsMethod.lastIndexOf('.');
                if (lastDot <= 0) continue;
                String fqcn = clsMethod.substring(0, lastDot);
                String javaMethod = clsMethod.substring(lastDot + 1);
                return new String[]{ fqcn, simpleClassName(fqcn), javaMethod, null, "false" };
            }
        }
        return null;
    }

    private static boolean isFrameworkClass(String s) {
        String fl = s.toLowerCase();
        return fl.startsWith("java.") || fl.startsWith("javax.") || fl.startsWith("jakarta.")
                || fl.startsWith("sun.") || fl.startsWith("jdk.") || fl.contains("jboss")
                || fl.contains("undertow") || fl.contains("wildfly") || fl.contains("xnio")
                || fl.contains("resteasy") || fl.contains("weld") || fl.contains("arjuna")
                || fl.contains("hibernate") || fl.contains("smallrye");
    }

    private static String simpleClassName(String fqcn) {
        int d = fqcn.lastIndexOf('.');
        return d >= 0 ? fqcn.substring(d + 1) : fqcn;
    }

    private String extractServletClass(List<String> stack) {
        for (String frame : stack) {
            if (frame.contains(".doGet(") || frame.contains(".doPost(") || frame.contains(".doPut(")
                    || frame.contains(".doDelete(") || frame.contains(".service(")) {
                String cls = frame.split("\\(")[0];
                int dot = cls.lastIndexOf('.');
                return dot > 0 ? cls.substring(cls.lastIndexOf('.', dot - 1) + 1, dot) : cls;
            }
        }
        for (String frame : stack) {
            String fl = frame.toLowerCase();
            if ((fl.contains("resource") || fl.contains("endpoint") || fl.contains("controller"))
                    && !fl.contains("jboss") && !fl.contains("undertow") && !fl.contains("jakarta")
                    && !fl.contains("javax") && !fl.contains("resteasy")) {
                String cls = frame.split("\\(")[0];
                int dot = cls.lastIndexOf('.');
                return dot > 0 ? cls.substring(cls.lastIndexOf('.', dot - 1) + 1, dot) : cls;
            }
        }
        return "";
    }

    private String guessMethodFromStack(List<String> stack) {
        for (String frame : stack) {
            if (frame.contains(".doGet(")) return "GET";
            if (frame.contains(".doPost(")) return "POST";
            if (frame.contains(".doPut(")) return "PUT";
            if (frame.contains(".doDelete(")) return "DELETE";
        }
        return "";
    }

    private String guessDeploymentFromStack(List<String> stack) {
        for (String frame : stack) {
            String fl = frame.toLowerCase();
            if (fl.startsWith("java.") || fl.startsWith("javax.") || fl.startsWith("jakarta.")
                    || fl.startsWith("sun.") || fl.startsWith("jdk.") || fl.contains("jboss")
                    || fl.contains("undertow") || fl.contains("wildfly") || fl.contains("xnio")
                    || fl.contains("resteasy") || fl.contains("weld") || fl.contains("arjuna"))
                continue;
            String[] parts = frame.split("\\.");
            if (parts.length >= 3 && List.of("com", "org", "net", "io").contains(parts[0])) {
                return parts[1];
            }
        }
        return "";
    }

    /** Parses the most recent access-log entries (domain-aware, serverContext-safe for the poller). */
    private List<Map<String, Object>> parseRecentAccessEntries() {
        boolean isServerSelected = false;
        try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

        if (!isServerSelected && client.isDomainMode()) {
            List<Map<String, Object>> entries = new ArrayList<>();
            List<DomainHelper.ServerLog> domainLogs = DomainHelper.findAllDomainLogs(config.getAllDomainHomes(), "access_log.log");
            for (DomainHelper.ServerLog sl : domainLogs) {
                entries.addAll(parseAccessLogFromPath(sl.path, 2000));
            }
            return entries;
        }
        return parseAccessLog(2000);
    }

    private Map<String, Map<String, Object>> buildThreadToLastRequestMap(List<Map<String, Object>> entries) {
        Map<String, Map<String, Object>> threadMap = new LinkedHashMap<>();
        for (int i = entries.size() - 1; i >= 0; i--) {
            Map<String, Object> e = entries.get(i);
            String thread = (String) e.getOrDefault("thread", "");
            if (!thread.isEmpty() && !threadMap.containsKey(thread)) {
                threadMap.put(thread, Map.of(
                        "method", e.getOrDefault("method", ""),
                        "uri", e.getOrDefault("uri", ""),
                        "deployment", e.getOrDefault("deployment", ""),
                        "client_ip", e.getOrDefault("remote", "")));
            }
        }
        return threadMap;
    }

    /**
     * Learns "servlet class -> URL" by correlating completed access-log entries with in-flight
     * observations recorded by {@link #stampActiveRequests}: a completed request on worker thread T
     * that started at instant S is the same request we sampled in-flight on T at ~S, so its logged
     * URL belongs to the servlet class we saw on T's stack then. Lets an in-flight annotation-mapped
     * servlet (whose URL is not in JBoss management) show its real path once one such request has
     * completed and been logged.
     */
    private void learnServletUris(List<Map<String, Object>> entries) {
        if (recentObservations.isEmpty()) return;
        for (Map<String, Object> e : entries) {
            String thread = (String) e.getOrDefault("thread", "");
            if (thread.isEmpty()) continue;
            long completeMs = ((Number) e.getOrDefault("timestamp_local_ms", 0L)).longValue();
            long rt = ((Number) e.getOrDefault("response_time_ms", 0)).longValue();
            if (completeMs <= 0 || rt <= 0) continue;
            long entryStartMs = completeMs - rt;
            String method = (String) e.getOrDefault("method", "");
            String uri = (String) e.getOrDefault("uri", "");
            String deployment = (String) e.getOrDefault("deployment", "");
            if (uri.isEmpty() || deployment.isEmpty()) continue;
            for (Map<String, Object> obs : recentObservations.values()) {
                if (!thread.equals(obs.get("thread"))) continue;
                long obsStart = ((Number) obs.getOrDefault("start_ms", 0L)).longValue();
                if (Math.abs(obsStart - entryStartMs) > CORRELATION_TOLERANCE_MS) continue;
                String servletClass = (String) obs.get("servlet_class");
                if (servletClass == null || servletClass.isEmpty()) continue;
                learnedServletUri.put(deployment + "::" + servletClass, new String[]{ method, uri });
            }
        }
    }

    /** deployment -> distinct [method, uri] seen in the access log (for name-hint URL guessing). */
    private Map<String, List<String[]>> buildDeploymentUriIndex(List<Map<String, Object>> entries) {
        Map<String, List<String[]>> index = new LinkedHashMap<>();
        Set<String> seen = new HashSet<>();
        for (Map<String, Object> e : entries) {
            String deployment = (String) e.getOrDefault("deployment", "");
            String method = (String) e.getOrDefault("method", "");
            String uri = (String) e.getOrDefault("uri", "");
            if (deployment.isEmpty() || uri.isEmpty()) continue;
            if (!seen.add(deployment + " " + method + " " + uri)) continue;
            index.computeIfAbsent(deployment, k -> new ArrayList<>()).add(new String[]{ method, uri });
        }
        return index;
    }

    /**
     * Best-effort immediate URL for an in-flight servlet before correlation has learned it: among
     * access-log URLs for the same deployment and HTTP method, pick the one whose path contains a
     * distinctive token of the servlet's class name (e.g. FileUploadServlet -> ".../upload"). Only
     * returns a match when exactly one URL qualifies, to avoid guessing wrong.
     */
    private String guessUriByServletName(String simpleClass, String deployment, String method,
                                         Map<String, List<String[]>> deploymentUris) {
        List<String[]> uris = deploymentUris.get(deployment);
        if (uris == null || simpleClass == null) return null;
        // Split the class name into words on camel-case boundaries FIRST (while case is intact),
        // then lowercase — e.g. "FileUploadServlet" -> ["file","upload"] (the "servlet"/"controller"
        // suffix is dropped as a non-distinctive word).
        List<String> tokens = new ArrayList<>();
        for (String tok : simpleClass.split("(?<=[a-z0-9])(?=[A-Z])|[^a-zA-Z0-9]+")) {
            String t = tok.toLowerCase();
            if (t.length() >= 4 && !t.equals("servlet") && !t.equals("controller")
                    && !t.equals("resource") && !t.equals("endpoint") && !t.equals("http")) {
                tokens.add(t);
            }
        }
        if (tokens.isEmpty()) return null;
        String match = null;
        for (String[] mu : uris) {
            if (!method.isEmpty() && !method.equalsIgnoreCase(mu[0])) continue;
            String uriLower = mu[1].toLowerCase();
            boolean hit = tokens.stream().anyMatch(uriLower::contains);
            if (hit) {
                if (match != null && !match.equals(mu[1])) return null; // ambiguous → give up
                match = mu[1];
            }
        }
        return match;
    }

    private static int reqStatusOrder(String status) {
        return switch (status) {
            case "STUCK" -> 0;
            case "BLOCKED" -> 1;
            case "SLOW" -> 2;
            case "RUNNING" -> 3;
            default -> 4;
        };
    }

    public Map<String, String> buildSessionToThreadMap() {
        List<Map<String, Object>> entries = parseAccessLog(5000);
        Map<String, String> map = new LinkedHashMap<>();
        for (Map<String, Object> e : entries) {
            String sessionId = (String) e.get("session_id");
            String thread = (String) e.get("thread");
            if (sessionId != null && thread != null) {
                map.put(sessionId, thread);
            }
        }
        return map;
    }

    private Set<String> getValidDeployments() {
        Set<String> valid = new HashSet<>();
        try {
            boolean isServerSelected = false;
            try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

            if (client.isDomainMode()) {
                if (isServerSelected) {
                    // Domain mode (specific server): find this server's group, read its deployments
                    String group = findServerGroup(serverContext.getHost(), serverContext.getServer());
                    if (group != null) {
                        try {
                            JsonNode sgRes = client.readResourceDirect(
                                    List.of("server-group", group), false, false);
                            JsonNode deps = sgRes.path("result").path("deployment");
                            if (deps.isObject()) {
                                deps.fieldNames().forEachRemaining(d ->
                                    valid.add(d.replace(".war", "").replace(".ear", "").toLowerCase()));
                            }
                        } catch (Exception ignored) {}
                    }
                } else {
                    // Domain mode (All Servers): read deployments from each server-group
                    JsonNode sgResult = client.readChildrenNames(List.of(), "server-group");
                    JsonNode sgNames = sgResult.path("result");
                    if (sgNames.isArray()) {
                        for (JsonNode sgNode : sgNames) {
                            String sg = sgNode.asText();
                            try {
                                JsonNode sgRes = client.readResourceDirect(
                                        List.of("server-group", sg), false, false);
                                JsonNode deps = sgRes.path("result").path("deployment");
                                if (deps.isObject()) {
                                    deps.fieldNames().forEachRemaining(d ->
                                        valid.add(d.replace(".war", "").replace(".ear", "").toLowerCase()));
                                }
                            } catch (Exception ignored) {}
                        }
                    }
                }
            } else {
                // Standalone
                JsonNode depResult = client.readResource(List.of(), true, false, false);
                if ("success".equals(depResult.path("outcome").asText())) {
                    JsonNode deps = depResult.path("result").path("deployment");
                    deps.fieldNames().forEachRemaining(d ->
                        valid.add(d.replace(".war", "").replace(".ear", "").toLowerCase()));
                }
            }
        } catch (Exception e) {
            LOG.debugf("getValidDeployments failed: %s", e.getMessage());
        }
        return valid;
    }

    private String findServerGroup(String host, String server) {
        try {
            Map<String, Object> op = new LinkedHashMap<>();
            op.put("operation", "read-resource");
            op.put("address", List.of("host", host, "server-config", server));
            op.put("include-runtime", false);
            op.put("recursive", false);
            JsonNode sc = client.mgmtRequest(op);
            if ("success".equals(sc.path("outcome").asText())) {
                return sc.path("result").path("group").asText(null);
            }
        } catch (Exception ignored) {}
        return null;
    }

    private List<Map<String, Object>> parseAccessLog(int maxLines) {
        String logPath = findAccessLog();
        if (logPath == null) return List.of();
        return parseAccessLogFromPath(logPath, maxLines);
    }

    private static final Pattern THREAD_RE = Pattern.compile("Thread:\\s+\"([^\"]+)\"");
    private static final Pattern SESSION_RE = Pattern.compile("SessionID:\\s+(\\S+)");
    private static final Pattern TIME_TAKEN_RE = Pattern.compile("TimeTaken:\\s+([\\d.]+)");

    private List<Map<String, Object>> parseAccessLogFromPath(String logPath, int maxLines) {
        List<Map<String, Object>> entries = new ArrayList<>();
        try {
            List<String> allLines = Files.readAllLines(java.nio.file.Path.of(logPath));
            List<String> lines = allLines.subList(Math.max(0, allLines.size() - maxLines), allLines.size());
            for (String line : lines) {
                Matcher m = ACCESS_LOG_RE.matcher(line.strip());
                if (!m.matches()) continue;
                String remote = m.group(1);
                String tsRaw = m.group(2);
                String method = m.group(3);
                String uri = m.group(4);
                int status = Integer.parseInt(m.group(5));
                long firstNum = Long.parseLong(m.group(6));
                String remainder = m.group(7);

                String ts;
                try {
                    ts = ZonedDateTime.parse(tsRaw, ACCESS_LOG_DATE).toInstant().toString();
                } catch (Exception e) {
                    ts = tsRaw;
                }

                String[] pathParts = uri.split("/");
                String deployment = pathParts.length > 1 ? pathParts[1] : "/";

                long responseTimeMs = 0;
                long bytes = 0;
                String threadName = null;
                String sessionId = null;

                Matcher stm = remainder != null ? STANDALONE_TAIL_RE.matcher(remainder) : null;
                if (stm != null && stm.matches()) {
                    // Standalone: %s %D %b %I|%S → firstNum=responseTime, tail=bytes thread|session
                    responseTimeMs = firstNum;
                    bytes = "-".equals(stm.group(1)) ? 0 : Long.parseLong(stm.group(1));
                    threadName = stm.group(2).strip();
                    String sid = stm.group(3);
                    if (sid != null && !sid.isBlank() && !"-".equals(sid)) sessionId = sid;
                } else {
                    // Domain: %s %b ... Thread: "X" SessionID: X TimeTaken: X.XXX
                    bytes = firstNum;
                    if (remainder != null && !remainder.isBlank()) {
                        Matcher ttm = TIME_TAKEN_RE.matcher(remainder);
                        if (ttm.find()) {
                            responseTimeMs = Math.round(Double.parseDouble(ttm.group(1)) * 1000);
                        }
                        Matcher thm = THREAD_RE.matcher(remainder);
                        if (thm.find()) {
                            threadName = thm.group(1);
                        }
                        Matcher sm = SESSION_RE.matcher(remainder);
                        if (sm.find()) {
                            String sid = sm.group(1);
                            if (!"-".equals(sid)) sessionId = sid;
                        }
                    }
                }

                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("remote", remote);
                entry.put("timestamp", ts);
                entry.put("method", method);
                entry.put("uri", uri);
                entry.put("deployment", deployment);
                entry.put("status", status);
                entry.put("response_time_ms", responseTimeMs);
                entry.put("bytes", bytes);

                if (threadName != null && !threadName.isBlank()) {
                    entry.put("thread", threadName);
                }
                if (sessionId != null && !sessionId.isBlank()) {
                    entry.put("session_id", sessionId);
                }
                entries.add(entry);
            }
        } catch (Exception ignored) {}
        return entries;
    }

    private String findAccessLog() {
        String path = findLogFile("access_log.log");
        if (path == null) path = findLogFile("access.log");
        return path;
    }

    private String findLogFile(String filename) {
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
                    File serversDir = new File(home, "domain/servers");
                    File[] dirs = serversDir.listFiles(File::isDirectory);
                    if (dirs != null) {
                        for (File dir : dirs) {
                            candidates.add(new File(dir, "log/" + filename).getPath());
                        }
                    }
                }
            }
        }
        String jbossHome = config.getJbossHome();
        if (!isDomain && jbossHome != null) {
            candidates.add(jbossHome + "/standalone/log/" + filename);
        }
        for (String c : candidates) {
            if (new File(c).isFile()) return c;
        }
        return null;
    }

    private Map<String, String> buildServerGroupMap() {
        Map<String, String> map = new LinkedHashMap<>();
        try {
            List<String[]> domainServers = client.getDomainServers();
            for (String[] hs : domainServers) {
                if (map.containsKey(hs[1])) continue;
                try {
                    Map<String, Object> op = new LinkedHashMap<>();
                    op.put("operation", "read-resource");
                    op.put("address", List.of("host", hs[0], "server-config", hs[1]));
                    op.put("include-runtime", false);
                    op.put("recursive", false);
                    JsonNode sc = client.mgmtRequest(op);
                    if ("success".equals(sc.path("outcome").asText())) {
                        String group = sc.path("result").path("group").asText(null);
                        if (group != null) map.put(hs[1], group);
                    }
                } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}
        return map;
    }
}
