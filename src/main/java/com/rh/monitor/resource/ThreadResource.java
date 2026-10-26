package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.config.JBossConfig;
import com.rh.monitor.config.ServerContext;
import com.rh.monitor.util.TimeUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.jboss.logging.Logger;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class ThreadResource {

    private static final Logger LOG = Logger.getLogger(ThreadResource.class);

    @Inject
    JBossManagementClient client;

    @Inject
    JBossConfig config;

    @Inject
    ServerContext serverContext;

    @Inject
    com.rh.monitor.service.HistoryTracker historyTracker;

    // ── GET /api/threads ─────────────────────────────────────────────────
    @GET
    @Path("/threads")
    public Map<String, Object> getThreadStats() {
        LOG.info("Fetching thread statistics");
        try {
            if (serverContext == null || !serverContext.hasContext()) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (domainServers != null && !domainServers.isEmpty()) {
                    return getDomainThreadStats(domainServers);
                }
            }
            JsonNode result = client.readResource(List.of("core-service", "platform-mbean", "type", "threading"));
            if (!"success".equals(result.path("outcome").asText())) {
                return Map.of("error", result.path("failure-description").asText("Unknown error"));
            }
            JsonNode data = result.get("result");
            int threadCount = data.path("thread-count").asInt(0);
            int peak = data.path("peak-thread-count").asInt(0);
            int daemon = data.path("daemon-thread-count").asInt(0);

            String health = "HEALTHY";
            if (threadCount > 500) health = "CRITICAL";
            else if (threadCount > 300) health = "WARNING";
            else if (threadCount > 200) health = "MODERATE";

            Map<String, Object> threads = new LinkedHashMap<>();
            threads.put("thread_count", threadCount);
            threads.put("peak_thread_count", peak);
            threads.put("daemon_thread_count", daemon);
            threads.put("total_started_thread_count", data.path("total-started-thread-count").asInt(0));
            threads.put("current_thread_cpu_time_ns", data.path("current-thread-cpu-time").asLong(0));
            threads.put("current_thread_user_time_ns", data.path("current-thread-user-time").asLong(0));
            threads.put("thread_contention_monitoring_enabled", data.path("thread-contention-monitoring-enabled").asBoolean(false));
            threads.put("thread_cpu_time_enabled", data.path("thread-cpu-time-enabled").asBoolean(false));
            threads.put("health", health);

            try {
                Map<String, Object> deadlockOp = new LinkedHashMap<>();
                deadlockOp.put("operation", "find-deadlocked-threads");
                deadlockOp.put("address", List.of("core-service", "platform-mbean", "type", "threading"));
                JsonNode deadlockResult = client.mgmtRequest(deadlockOp);
                if ("success".equals(deadlockResult.path("outcome").asText())) {
                    JsonNode deadlocked = deadlockResult.get("result");
                    boolean hasDeadlocks = deadlocked != null && !deadlocked.isNull() && deadlocked.size() > 0;
                    threads.put("deadlocked_threads", hasDeadlocks ? deadlocked : List.of());
                    threads.put("has_deadlocks", hasDeadlocks);
                    if (hasDeadlocks) threads.put("health", "CRITICAL");
                }
            } catch (Exception e) {
                threads.put("deadlocked_threads", List.of());
                threads.put("has_deadlocks", false);
            }

            return Map.of("status", "ok", "threads", threads);
        } catch (Exception e) {
            return Map.of("error", "Failed to get thread stats: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private Map<String, Object> getDomainThreadStats(List<String[]> domainServers) throws Exception {
        int totalThreadCount = 0, totalPeak = 0, totalDaemon = 0, totalStarted = 0;
        long totalCpuTime = 0, totalUserTime = 0;
        boolean contentionMonitoring = false, cpuTimeEnabled = false;
        boolean anyDeadlocks = false;
        List<Object> allDeadlocked = new ArrayList<>();

        // One wildcard read returns every server's threading mbean, replacing the per-server loop.
        Map<String, JsonNode> byServer = client.readWildcardAcrossServers(
                List.of("core-service", "platform-mbean", "type", "threading"), true, false);
        for (JsonNode item : byServer.values()) {
            JsonNode data = item.get("result");
            totalThreadCount += data.path("thread-count").asInt(0);
            totalPeak += data.path("peak-thread-count").asInt(0);
            totalDaemon += data.path("daemon-thread-count").asInt(0);
            totalStarted += data.path("total-started-thread-count").asInt(0);
            totalCpuTime += data.path("current-thread-cpu-time").asLong(0);
            totalUserTime += data.path("current-thread-user-time").asLong(0);
            if (data.path("thread-contention-monitoring-enabled").asBoolean(false)) contentionMonitoring = true;
            if (data.path("thread-cpu-time-enabled").asBoolean(false)) cpuTimeEnabled = true;
        }

        // find-deadlocked-threads is an operation (not a wildcard-able read), so still per-server.
        for (String[] ds : domainServers) {
            String host = ds[0], server = ds[1];
            List<String> addr = List.of("host", host, "server", server,
                    "core-service", "platform-mbean", "type", "threading");
            try {
                Map<String, Object> deadlockOp = new LinkedHashMap<>();
                deadlockOp.put("operation", "find-deadlocked-threads");
                deadlockOp.put("address", addr);
                JsonNode deadlockResult = client.mgmtRequest(deadlockOp);
                if ("success".equals(deadlockResult.path("outcome").asText())) {
                    JsonNode deadlocked = deadlockResult.get("result");
                    if (deadlocked != null && !deadlocked.isNull() && deadlocked.size() > 0) {
                        anyDeadlocks = true;
                        for (JsonNode dl : deadlocked) allDeadlocked.add(dl);
                    }
                }
            } catch (Exception ignored) {}
        }

        String health = "HEALTHY";
        if (totalThreadCount > 500) health = "CRITICAL";
        else if (totalThreadCount > 300) health = "WARNING";
        else if (totalThreadCount > 200) health = "MODERATE";
        if (anyDeadlocks) health = "CRITICAL";

        Map<String, Object> threads = new LinkedHashMap<>();
        threads.put("thread_count", totalThreadCount);
        threads.put("peak_thread_count", totalPeak);
        threads.put("daemon_thread_count", totalDaemon);
        threads.put("total_started_thread_count", totalStarted);
        threads.put("current_thread_cpu_time_ns", totalCpuTime);
        threads.put("current_thread_user_time_ns", totalUserTime);
        threads.put("thread_contention_monitoring_enabled", contentionMonitoring);
        threads.put("thread_cpu_time_enabled", cpuTimeEnabled);
        threads.put("health", health);
        threads.put("deadlocked_threads", allDeadlocked);
        threads.put("has_deadlocks", anyDeadlocks);

        return Map.of("status", "ok", "threads", threads);
    }

    private Map<String, Object> getDomainThreadDump(List<String[]> domainServers, int limit) {
        List<Map<String, Object>> allThreads = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        // dump-all-threads is an operation (no wildcard address), so we still need one call
        // per server — but fan them out across the CLI connection pool instead of running
        // them serially. mgmtRequest builds its command from the explicit host/server address
        // and never touches the request-scoped ServerContext, so it is safe off-thread.
        List<Map<String, Object>> perServer = domainServers.parallelStream().map(ds -> {
            String host = ds[0], server = ds[1];
            String serverLabel = host + "/" + server;
            List<String> addr = List.of("host", host, "server", server,
                    "core-service", "platform-mbean", "type", "threading");
            List<Map<String, Object>> threads = new ArrayList<>();
            String error = null;
            try {
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
                            Map<String, Object> info = parseThreadInfo(t);
                            info.put("server_name", serverLabel);
                            threads.add(info);
                        }
                    }
                } else {
                    error = serverLabel + ": " + result.path("failure-description").asText("failed");
                }
            } catch (Exception e) {
                error = serverLabel + ": " + e.getMessage();
            }
            Map<String, Object> out = new HashMap<>();
            out.put("threads", threads);
            if (error != null) out.put("error", error);
            return out;
        }).toList();

        for (Map<String, Object> ps : perServer) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> t = (List<Map<String, Object>>) ps.get("threads");
            if (t != null) allThreads.addAll(t);
            Object err = ps.get("error");
            if (err != null) errors.add((String) err);
        }

        if (allThreads.isEmpty()) {
            return Map.of("error", "No threads found from domain servers", "methods_tried", errors);
        }

        Map<String, Object> resp = buildThreadDumpResponse(allThreads, limit);
        resp.put("method", "management-api (domain dump-all-threads)");
        if (!errors.isEmpty()) resp.put("partial_errors", errors);
        return resp;
    }

    // ── GET /api/thread-dump ─────────────────────────────────────────────
    @GET
    @Path("/thread-dump")
    public Map<String, Object> getThreadDump(@QueryParam("limit") @DefaultValue("50") int limit) {
        LOG.infof("Thread dump requested, limit=%d", limit);

        if (serverContext == null || !serverContext.hasContext()) {
            List<String[]> domainServers = client.getRunningDomainServers();
            if (domainServers != null && !domainServers.isEmpty()) {
                return getDomainThreadDump(domainServers, limit);
            }
        }

        // Domain mode: specific server — use management API with server-prefixed address
        if (serverContext != null && serverContext.hasContext()) {
            try {
                List<String> addr = List.of("host", serverContext.getHost(),
                        "server", serverContext.getServer(),
                        "core-service", "platform-mbean", "type", "threading");
                Map<String, Object> op = new LinkedHashMap<>();
                op.put("operation", "dump-all-threads");
                op.put("address", addr);
                op.put("locked-monitors", true);
                op.put("locked-synchronizers", true);
                JsonNode result = client.mgmtRequest(op);
                if ("success".equals(result.path("outcome").asText())) {
                    JsonNode raw = result.get("result");
                    if (raw != null && raw.isArray() && raw.size() > 0) {
                        String serverLabel = serverContext.getHost() + "/" + serverContext.getServer();
                        List<Map<String, Object>> rawThreads = new ArrayList<>();
                        for (JsonNode t : raw) {
                            Map<String, Object> info = parseThreadInfo(t);
                            info.put("server_name", serverLabel);
                            rawThreads.add(info);
                        }
                        Map<String, Object> resp = buildThreadDumpResponse(rawThreads, limit);
                        resp.put("method", "management-api (server dump-all-threads)");
                        return resp;
                    }
                }
            } catch (Exception e) {
                LOG.debugf("Domain server dump-all-threads failed for %s/%s: %s",
                        serverContext.getHost(), serverContext.getServer(), e.getMessage());
            }
        }

        List<String> errors = new ArrayList<>();

        // Method 1: Management API dump-all-threads (fastest via persistent CLI)
        try {
            Map<String, Object> op = new LinkedHashMap<>();
            op.put("operation", "dump-all-threads");
            op.put("address", List.of("core-service", "platform-mbean", "type", "threading"));
            op.put("locked-monitors", true);
            op.put("locked-synchronizers", true);
            JsonNode result = client.mgmtRequest(op);
            if ("success".equals(result.path("outcome").asText())) {
                JsonNode raw = result.get("result");
                if (raw != null && raw.isArray() && raw.size() > 0) {
                    List<Map<String, Object>> rawThreads = new ArrayList<>();
                    for (JsonNode t : raw) rawThreads.add(parseThreadInfo(t));
                    Map<String, Object> resp = buildThreadDumpResponse(rawThreads, limit);
                    resp.put("method", "management-api (dump-all-threads)");
                    return resp;
                }
            }
            errors.add("dump-all-threads: " + result.path("failure-description").asText("empty result"));
        } catch (Exception e) {
            errors.add("dump-all-threads: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }

        // Method 2: Management API get-thread-infos
        try {
            JsonNode idsResult = client.readAttribute(
                    List.of("core-service", "platform-mbean", "type", "threading"), "all-thread-ids");
            if ("success".equals(idsResult.path("outcome").asText())) {
                JsonNode ids = idsResult.get("result");
                if (ids != null && ids.isArray() && ids.size() > 0) {
                    List<Map<String, Object>> rawThreads = new ArrayList<>();
                    int batchSize = 50;
                    int max = Math.min(ids.size(), limit);
                    for (int i = 0; i < max; i += batchSize) {
                        List<Long> batch = new ArrayList<>();
                        for (int j = i; j < Math.min(i + batchSize, max); j++) {
                            batch.add(ids.get(j).asLong());
                        }
                        try {
                            Map<String, Object> bop = new LinkedHashMap<>();
                            bop.put("operation", "get-thread-infos");
                            bop.put("address", List.of("core-service", "platform-mbean", "type", "threading"));
                            bop.put("ids", batch);
                            bop.put("max-depth", 200);
                            JsonNode batchResult = client.mgmtRequest(bop);
                            if ("success".equals(batchResult.path("outcome").asText())) {
                                for (JsonNode t : batchResult.get("result")) {
                                    if (t != null && !t.isNull()) rawThreads.add(parseThreadInfo(t));
                                }
                            }
                        } catch (Exception ignored) {}
                    }
                    if (!rawThreads.isEmpty()) {
                        Map<String, Object> resp = buildThreadDumpResponse(rawThreads, limit);
                        resp.put("method", "management-api (get-thread-infos)");
                        return resp;
                    }
                }
            }
            errors.add("get-thread-infos: no threads found");
        } catch (Exception e) {
            errors.add("get-thread-infos: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }

        // Method 3: jstack (fallback — spawns external process)
        String pid = findJbossPid();
        if (pid != null) {
            String javaHome = findJavaHome();
            String jstackCmd = javaHome != null ? javaHome + "/bin/jstack" : "jstack";
            try {
                ProcessBuilder pb = new ProcessBuilder(jstackCmd, "-l", pid);
                pb.redirectErrorStream(false);
                Process proc = pb.start();
                String stdout = new String(proc.getInputStream().readAllBytes());
                String stderr = new String(proc.getErrorStream().readAllBytes());
                int exit = proc.waitFor();
                if (exit == 0 && !stdout.isBlank()) {
                    List<Map<String, Object>> rawThreads = parseJcmdThreadDump(stdout);
                    if (!rawThreads.isEmpty()) {
                        Map<String, Object> resp = buildThreadDumpResponse(rawThreads, limit);
                        resp.put("method", "jstack");
                        resp.put("pid", pid);
                        return resp;
                    }
                }
                errors.add("jstack: " + (stderr.isBlank() ? "no output" : stderr.trim()));
            } catch (Exception e) {
                errors.add("jstack: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }

            // Method 4: jcmd
            String jcmdCmd = javaHome != null ? javaHome + "/bin/jcmd" : "jcmd";
            try {
                ProcessBuilder pb = new ProcessBuilder(jcmdCmd, pid, "Thread.print", "-l");
                pb.redirectErrorStream(false);
                Process proc = pb.start();
                String stdout = new String(proc.getInputStream().readAllBytes());
                String stderr = new String(proc.getErrorStream().readAllBytes());
                int exit = proc.waitFor();
                if (exit == 0 && !stdout.isBlank()) {
                    List<Map<String, Object>> rawThreads = parseJcmdThreadDump(stdout);
                    if (!rawThreads.isEmpty()) {
                        Map<String, Object> resp = buildThreadDumpResponse(rawThreads, limit);
                        resp.put("method", "jcmd");
                        resp.put("pid", pid);
                        return resp;
                    }
                }
                errors.add("jcmd: " + (stderr.isBlank() ? "no output" : stderr.trim()));
            } catch (Exception e) {
                errors.add("jcmd: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        } else {
            errors.add("jstack/jcmd: JBoss process not found (tried jps -l and ps -ef)");
        }

        return Map.of("error", "Failed to get thread dump via all methods", "methods_tried", errors);
    }

    // ── GET /api/thread-pools ────────────────────────────────────────────
    @GET
    @Path("/thread-pools")
    public Map<String, Object> getThreadPools() {
        LOG.info("Fetching thread pool statistics");
        try {
            if (serverContext == null || !serverContext.hasContext()) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (domainServers != null && !domainServers.isEmpty()) {
                    // One wildcard read per subsystem (the domain controller fans out to every
                    // server in a single round-trip) instead of ~9 per-child CLI calls per
                    // server. Recursive so each server's whole subsystem tree comes back at once.
                    Map<String, JsonNode> ut = client.readWildcardAcrossServers(List.of("subsystem", "undertow"), true, true);
                    Map<String, JsonNode> io = client.readWildcardAcrossServers(List.of("subsystem", "io"), true, true);
                    Map<String, JsonNode> ejb = client.readWildcardAcrossServers(List.of("subsystem", "ejb3"), true, true);
                    Map<String, JsonNode> jca = client.readWildcardAcrossServers(List.of("subsystem", "jca"), true, true);
                    List<Map<String, Object>> allPools = new ArrayList<>();
                    for (String[] ds : domainServers) {
                        String serverLabel = ds[0] + "/" + ds[1];
                        allPools.addAll(collectPoolsFromResults(serverLabel,
                                ut.get(serverLabel), io.get(serverLabel),
                                ejb.get(serverLabel), jca.get(serverLabel)));
                    }
                    return Map.of("status", "ok", "pools", allPools);
                }
            }
            return Map.of("status", "ok", "pools", collectPools(null, null));
        } catch (Exception e) {
            return Map.of("error", "Failed to get thread pools: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private JsonNode readServerResource(List<String> serverPrefix, List<String> address,
                                         boolean includeRuntime, boolean recursive) throws Exception {
        if (serverPrefix != null) {
            List<String> fullAddress = new ArrayList<>(serverPrefix);
            fullAddress.addAll(address);
            return client.readResourceDirect(fullAddress, includeRuntime, recursive);
        }
        return client.readResource(address, includeRuntime, recursive);
    }

    private List<Map<String, Object>> collectPools(List<String> serverPrefix, String serverLabel) {
        // Fetch each subsystem ONCE, recursively, so all child pools come back in a single
        // read-resource per subsystem instead of a follow-up call per listener/worker/pool.
        JsonNode ut = null, io = null, ejb = null, jca = null;
        try { ut = readServerResource(serverPrefix, List.of("subsystem", "undertow"), true, true); } catch (Exception ignored) {}
        try { io = readServerResource(serverPrefix, List.of("subsystem", "io"), true, true); } catch (Exception ignored) {}
        try { ejb = readServerResource(serverPrefix, List.of("subsystem", "ejb3"), true, true); } catch (Exception ignored) {}
        try { jca = readServerResource(serverPrefix, List.of("subsystem", "jca"), true, true); } catch (Exception ignored) {}
        return collectPoolsFromResults(serverLabel, ut, io, ejb, jca);
    }

    /** {@code result} subtree of a recursive read-resource response, or null if it failed. */
    private JsonNode subsystemResult(JsonNode resp) {
        if (resp == null || !"success".equals(resp.path("outcome").asText())) return null;
        JsonNode r = resp.path("result");
        return (r == null || r.isMissingNode() || r.isNull()) ? null : r;
    }

    /**
     * Build the pool list for one server by walking pre-fetched recursive subsystem trees.
     * Each {@code *Resp} is a full read-resource response (the same shape whether it came from
     * a per-server read or a {@code /host=*&#47;server=*} wildcard item), so this is shared by
     * both the single-server and domain-aggregate paths.
     */
    private List<Map<String, Object>> collectPoolsFromResults(String serverLabel,
            JsonNode utResp, JsonNode ioResp, JsonNode ejbResp, JsonNode jcaResp) {
        List<Map<String, Object>> pools = new ArrayList<>();

        // Undertow listeners
        JsonNode ut = subsystemResult(utResp);
        if (ut != null) {
            JsonNode servers = ut.path("server");
            Iterator<String> serverNames = servers.fieldNames();
            while (serverNames.hasNext()) {
                String serverName = serverNames.next();
                JsonNode srv = servers.path(serverName);
                for (String lt : List.of("http-listener", "https-listener", "ajp-listener")) {
                    JsonNode listeners = srv.path(lt);
                    Iterator<String> lnames = listeners.fieldNames();
                    while (lnames.hasNext()) {
                        String lname = lnames.next();
                        JsonNode ld = listeners.get(lname);
                        if (ld != null && ld.isObject()) {
                            Map<String, Object> pool = new LinkedHashMap<>();
                            long reqCount = ld.path("request-count").asLong(0);
                            long totalProcMs = ld.path("processing-time").asLong(0) / 1_000_000;
                            pool.put("pool_name", serverName + "/" + lt + "/" + lname);
                            pool.put("type", "undertow-listener");
                            pool.put("request_count", reqCount);
                            pool.put("error_count", ld.path("error-count").asInt(0));
                            pool.put("bytes_sent", ld.path("bytes-sent").asLong(0));
                            pool.put("bytes_received", ld.path("bytes-received").asLong(0));
                            pool.put("processing_time_ms", totalProcMs);
                            pool.put("avg_processing_time_ms", reqCount > 0 ? totalProcMs / reqCount : 0);
                            pool.put("max_processing_time_ms", ld.path("max-processing-time").asLong(0) / 1_000_000);
                            if (serverLabel != null) pool.put("server_name", serverLabel);
                            pools.add(pool);
                        }
                    }
                }
            }
        }

        // IO workers
        JsonNode io = subsystemResult(ioResp);
        if (io != null) {
            JsonNode workers = io.path("worker");
            Iterator<String> wnames = workers.fieldNames();
            while (wnames.hasNext()) {
                String wname = wnames.next();
                JsonNode wd = workers.path(wname);
                Map<String, Object> pool = new LinkedHashMap<>();
                pool.put("pool_name", "io-worker/" + wname);
                pool.put("type", "io-worker");
                pool.put("io_threads", wd.path("io-threads").asInt(0));
                pool.put("task_keepalive", wd.path("task-keepalive").asInt(0));
                pool.put("task_max_threads", wd.path("task-max-threads").asInt(0));
                pool.put("stack_size", wd.path("stack-size").asLong(0));
                putIfPresent(pool, "active_count", wd, "busy-task-thread-count");
                putIfPresent(pool, "current_thread_count", wd, "task-thread-count");
                putIfPresentLong(pool, "completed_task_count", wd, "completed-task-count");
                putIfPresent(pool, "queue_size", wd, "queue-size");
                putIfPresent(pool, "rejected_count", wd, "rejected-count");
                if (serverLabel != null) pool.put("server_name", serverLabel);
                pools.add(pool);
            }
        }

        // EJB3 thread pools
        JsonNode ejb = subsystemResult(ejbResp);
        if (ejb != null) {
            JsonNode tpools = ejb.path("thread-pool");
            Iterator<String> pnames = tpools.fieldNames();
            while (pnames.hasNext()) {
                String pname = pnames.next();
                JsonNode pd = tpools.path(pname);
                Map<String, Object> pool = new LinkedHashMap<>();
                pool.put("pool_name", "ejb3/thread-pool/" + pname);
                pool.put("type", "ejb-thread-pool");
                pool.put("max_threads", pd.path("max-threads").asInt(0));
                pool.put("core_threads", pd.path("core-threads").asInt(0));
                putIfPresent(pool, "current_thread_count", pd, "current-thread-count");
                putIfPresent(pool, "largest_thread_count", pd, "largest-thread-count");
                putIfPresent(pool, "active_count", pd, "active-count");
                putIfPresentLong(pool, "completed_task_count", pd, "completed-task-count");
                putIfPresentLong(pool, "task_count", pd, "task-count");
                putIfPresent(pool, "queue_size", pd, "queue-size");
                putIfPresent(pool, "rejected_count", pd, "rejected-count");
                if (serverLabel != null) pool.put("server_name", serverLabel);
                pools.add(pool);
            }
        }

        // JCA workmanager thread pools
        JsonNode jca = subsystemResult(jcaResp);
        if (jca != null) {
            JsonNode workmanagers = jca.path("workmanager");
            Iterator<String> wmNames = workmanagers.fieldNames();
            while (wmNames.hasNext()) {
                String wmName = wmNames.next();
                JsonNode wm = workmanagers.path(wmName);
                for (String poolType : List.of("short-running-threads", "long-running-threads")) {
                    JsonNode ptMap = wm.path(poolType);
                    Iterator<String> ptNames = ptMap.fieldNames();
                    while (ptNames.hasNext()) {
                        JsonNode pd = ptMap.path(ptNames.next());
                        Map<String, Object> pool = new LinkedHashMap<>();
                        pool.put("pool_name", "jca/" + wmName + "/" + poolType);
                        pool.put("type", "jca-thread-pool");
                        pool.put("max_threads", pd.path("max-threads").asInt(0));
                        pool.put("core_threads", pd.path("core-threads").asInt(0));
                        putIfPresent(pool, "current_thread_count", pd, "current-thread-count");
                        putIfPresent(pool, "largest_thread_count", pd, "largest-thread-count");
                        putIfPresent(pool, "active_count", pd, "active-count");
                        putIfPresentLong(pool, "completed_task_count", pd, "completed-task-count");
                        putIfPresentLong(pool, "task_count", pd, "task-count");
                        putIfPresent(pool, "queue_size", pd, "queue-size");
                        putIfPresent(pool, "rejected_count", pd, "rejected-count");
                        if (serverLabel != null) pool.put("server_name", serverLabel);
                        pools.add(pool);
                    }
                }
            }
        }

        return pools;
    }

    // ── GET /api/thread-details ──────────────────────────────────────────
    @GET
    @Path("/thread-details")
    public Map<String, Object> getThreadDetails() {
        LOG.info("Fetching thread details with deadlock/blocked analysis");
        try {
            Map<String, Object> dump = getThreadDump(10000);
            if (dump.containsKey("error")) return dump;

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> threads = (List<Map<String, Object>>) dump.get("threads");
            List<Map<String, Object>> allDetails = new ArrayList<>();
            Map<String, Integer> categoryCounts = new LinkedHashMap<>();
            Map<String, Integer> stateCounts = new LinkedHashMap<>();
            int busyCount = 0;

            for (Map<String, Object> t : threads) {
                String state = (String) t.getOrDefault("thread_state", "");
                String threadName = (String) t.getOrDefault("thread_name", "");
                @SuppressWarnings("unchecked")
                List<String> stack = (List<String>) t.getOrDefault("stack_trace", List.of());

                String[] classified = classifyThread(threadName, stack);
                String category = classified[0];
                String description = classified[1];
                String topFrame = topFrameSummary(stack);

                Map<String, Object> detail = new LinkedHashMap<>();
                detail.put("thread_name", threadName);
                detail.put("thread_id", t.get("thread_id"));
                detail.put("state", state);
                detail.put("category", category);
                detail.put("task", description);
                detail.put("top_frame", topFrame);
                detail.put("lock_name", t.get("lock_name"));
                detail.put("lock_owner_name", t.get("lock_owner_name"));
                detail.put("blocked_count", t.getOrDefault("blocked_count", 0));
                detail.put("blocked_time", t.getOrDefault("blocked_time", -1));
                detail.put("waited_count", t.getOrDefault("waited_count", 0));
                detail.put("waited_time", t.getOrDefault("waited_time", -1));
                detail.put("suspended", t.getOrDefault("suspended", false));
                detail.put("in_native", t.getOrDefault("in_native", false));
                detail.put("stack_trace", stack);
                if (t.containsKey("server_name")) detail.put("server_name", t.get("server_name"));
                allDetails.add(detail);

                categoryCounts.merge(category, 1, Integer::sum);
                stateCounts.merge(state, 1, Integer::sum);
                if ("RUNNABLE".equals(state) || "BLOCKED".equals(state)) busyCount++;
            }

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("status", "ok");
            resp.put("total_threads", dump.getOrDefault("total_threads", 0));
            resp.put("busy_count", busyCount);
            resp.put("threads", allDetails);
            resp.put("category_summary", categoryCounts);
            resp.put("state_summary", dump.getOrDefault("state_summary", Map.of()));
            resp.put("dump_method", dump.getOrDefault("method", "unknown"));
            resp.put("dump_pid", dump.getOrDefault("pid", null));
            return resp;
        } catch (Exception e) {
            return Map.of("error", "Failed to get thread details: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    // ── GET /api/thread-history ────────────────────────────────────────
    @GET
    @Path("/thread-history")
    public Map<String, Object> getThreadHistory() {
        LOG.info("Fetching thread history");
        // Trigger a live poll if data is stale (>5s old) — makes Thread Details respond with fresh data
        historyTracker.refreshNow();

        var tracker = com.rh.monitor.service.HistoryTracker.getThreadStateTracker();
        var snapshots = com.rh.monitor.service.HistoryTracker.getThreadHistorySnapshots();
        var blockedLog = com.rh.monitor.service.HistoryTracker.getBlockedThreadLog();

        // Get deadlock data from HistoryTracker (polled every 30s, filters infrastructure threads)
        var currentDeadlocks = com.rh.monitor.service.HistoryTracker.getActiveDeadlocks();
        var deadlockEvents = com.rh.monitor.service.HistoryTracker.getDeadlockEventLog();
        Set<Long> deadlockedIds = currentDeadlocks.keySet();

        String filterServer = null;
        if (serverContext != null && serverContext.hasContext()) {
            filterServer = serverContext.getHost() + "/" + serverContext.getServer();
        }

        java.time.LocalDateTime now = java.time.LocalDateTime.now();
        List<Map<String, Object>> longRunning = new ArrayList<>();
        List<Map<String, Object>> allTracked = new ArrayList<>();
        List<Map<String, Object>> deadlockThreads = new ArrayList<>();
        Map<String, Integer> trackedStateSummary = new LinkedHashMap<>();
        Map<String, Integer> trackedCategorySummary = new LinkedHashMap<>();
        Set<Long> seenDeadlockIds = new HashSet<>();

        for (var entry : tracker.entrySet()) {
            var t = entry.getValue();

            if (filterServer != null) {
                String tServer = (String) t.get("server_name");
                if (tServer != null && !filterServer.equals(tServer)) continue;
            }

            String state = (String) t.getOrDefault("current_state", "");
            String stateSince = (String) t.getOrDefault("state_since", "");
            double durationS = 0;
            try {
                var since = java.time.LocalDateTime.parse(stateSince);
                durationS = java.time.Duration.between(since, now).toMillis() / 1000.0;
            } catch (Exception ignored) {}

            @SuppressWarnings("unchecked")
            List<String> stack = (List<String>) t.get("stack_trace");
            String threadName = (String) t.getOrDefault("thread_name", "");
            long threadId = ((Number) t.getOrDefault("thread_id", 0L)).longValue();
            boolean isDeadlocked = deadlockedIds.contains(threadId);

            String[] classification = classifyThread(threadName, stack);
            String category = isDeadlocked ? "Deadlock" : classification[0];

            String lockName = (String) t.get("lock_name");
            String lockOwnerName = (String) t.get("lock_owner_name");
            if (isDeadlocked) {
                Map<String, Object> dlInfo = currentDeadlocks.get(threadId);
                if (dlInfo != null) {
                    if (dlInfo.get("lock_name") != null) lockName = (String) dlInfo.get("lock_name");
                    if (dlInfo.get("lock_owner_name") != null) lockOwnerName = (String) dlInfo.get("lock_owner_name");
                    if (state.isEmpty()) state = (String) dlInfo.getOrDefault("state", "BLOCKED");
                    if (threadName.isEmpty()) threadName = (String) dlInfo.getOrDefault("thread_name", "");
                }
            }

            Map<String, Object> item = new LinkedHashMap<>();
            item.put("thread_id", threadId);
            item.put("thread_name", threadName);
            if (t.containsKey("server_name")) item.put("server_name", t.get("server_name"));
            item.put("state", state);
            item.put("start_time", stateSince);
            item.put("duration_seconds", Math.round(durationS * 10.0) / 10.0);
            item.put("category", category);
            item.put("top_frame", topFrameSummary(stack));
            item.put("lock_name", lockName);
            item.put("lock_owner_name", lockOwnerName);
            item.put("blocked_count", t.getOrDefault("blocked_count", 0L));
            item.put("deadlocked", isDeadlocked);
            allTracked.add(item);

            trackedStateSummary.merge(state, 1, Integer::sum);
            trackedCategorySummary.merge(category, 1, Integer::sum);

            if (isDeadlocked) {
                seenDeadlockIds.add(threadId);
                deadlockThreads.add(new LinkedHashMap<>(item));
            }

            if (("RUNNABLE".equals(state) || "BLOCKED".equals(state)) && t.get("ended") == null && durationS >= 30) {
                Map<String, Object> lr = new LinkedHashMap<>(item);
                longRunning.add(lr);
            }
        }

        // Add deadlocked threads not yet in tracker
        for (var dlEntry : currentDeadlocks.entrySet()) {
            if (seenDeadlockIds.contains(dlEntry.getKey())) continue;
            Map<String, Object> dl = dlEntry.getValue();
            if (filterServer != null) {
                String dlServer = (String) dl.get("server_name");
                if (dlServer != null && !filterServer.equals(dlServer)) continue;
            }
            String detectedAt = (String) dl.getOrDefault("detected_at", "");
            double durationS = 0;
            try {
                var since = java.time.LocalDateTime.parse(detectedAt);
                durationS = java.time.Duration.between(since, now).toMillis() / 1000.0;
            } catch (Exception ignored) {}

            Map<String, Object> item = new LinkedHashMap<>();
            item.put("thread_id", dlEntry.getKey());
            item.put("thread_name", dl.get("thread_name"));
            item.put("state", dl.getOrDefault("state", "BLOCKED"));
            item.put("start_time", detectedAt);
            item.put("duration_seconds", Math.round(durationS * 10.0) / 10.0);
            item.put("category", "Deadlock");
            item.put("top_frame", dl.getOrDefault("top_frame", ""));
            item.put("lock_name", dl.get("lock_name"));
            item.put("lock_owner_name", dl.get("lock_owner_name"));
            item.put("blocked_count", dl.getOrDefault("blocked_count", 0L));
            item.put("deadlocked", true);
            allTracked.add(item);
            deadlockThreads.add(new LinkedHashMap<>(item));
            trackedStateSummary.merge((String) dl.getOrDefault("state", "BLOCKED"), 1, Integer::sum);
            trackedCategorySummary.merge("Deadlock", 1, Integer::sum);
        }

        longRunning.sort((a, b) -> Double.compare(
                ((Number) b.getOrDefault("duration_seconds", 0)).doubleValue(),
                ((Number) a.getOrDefault("duration_seconds", 0)).doubleValue()));
        allTracked.sort((a, b) -> Double.compare(
                ((Number) b.getOrDefault("duration_seconds", 0)).doubleValue(),
                ((Number) a.getOrDefault("duration_seconds", 0)).doubleValue()));

        List<Map<String, Object>> blocked = new ArrayList<>();
        for (Map<String, Object> bl : blockedLog) {
            if (filterServer != null) {
                String blServer = (String) bl.get("server_name");
                if (blServer != null && !filterServer.equals(blServer)) continue;
            }
            blocked.add(bl);
        }
        if (blocked.size() > 100) blocked = blocked.subList(blocked.size() - 100, blocked.size());
        java.util.Collections.reverse(blocked);

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "ok");
        resp.put("all_tracked", allTracked);
        resp.put("all_tracked_count", allTracked.size());
        resp.put("tracked_state_summary", trackedStateSummary);
        resp.put("tracked_category_summary", trackedCategorySummary);
        resp.put("long_running", longRunning);
        resp.put("long_running_count", longRunning.size());
        resp.put("blocked_events", blocked);
        resp.put("blocked_event_count", blocked.size());
        resp.put("deadlocks", deadlockThreads);
        resp.put("deadlock_count", deadlockThreads.size());
        List<Map<String, Object>> dlEvents = new ArrayList<>();
        for (Map<String, Object> de : deadlockEvents) {
            if (filterServer != null) {
                String deServer = (String) de.get("server_name");
                if (deServer != null && !filterServer.equals(deServer)) continue;
            }
            dlEvents.add(de);
        }
        java.util.Collections.reverse(dlEvents);
        resp.put("deadlock_events", dlEvents);
        resp.put("deadlock_event_count", dlEvents.size());
        resp.put("total_tracked", allTracked.size());
        resp.put("snapshots", new ArrayList<>(snapshots));
        resp.put("snapshot_count", snapshots.size());

        resp.put("event_log_file", Paths.get(System.getProperty("user.dir"), "logs", "thread_events.log").toString());
        return resp;
    }

    // ── GET /api/thread-event-log ──────────────────────────────────────
    @GET
    @Path("/thread-event-log")
    @Produces(MediaType.TEXT_PLAIN)
    public String getThreadEventLog() {
        java.nio.file.Path logPath = Paths.get(System.getProperty("user.dir"), "logs", "thread_events.log");
        if (!Files.exists(logPath)) return "No events recorded yet. File: " + logPath;
        try {
            return Files.readString(logPath);
        } catch (IOException e) {
            return "Error reading event log: " + e.getMessage();
        }
    }

    // ── POST /api/capture-thread-dumps ───────────────────────────────────
    @POST
    @Path("/capture-thread-dumps")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> captureThreadDumps(Map<String, Object> body) {
        int count = body != null && body.containsKey("count") ? Integer.parseInt(body.get("count").toString()) : 10;
        int interval = body != null && body.containsKey("interval") ? Integer.parseInt(body.get("interval").toString()) : 20;

        String jbossHome = findJbossHome();
        if (jbossHome == null) {
            return Map.of("error", "JBOSS_HOME not set or not found. Set the JBOSS_HOME environment variable.");
        }

        String javaHome = findJavaHome();
        String jstackCmd = javaHome != null ? javaHome + "/bin/jstack" : "jstack";

        if (javaHome == null) {
            try {
                ProcessBuilder pb = new ProcessBuilder("which", "jstack");
                Process proc = pb.start();
                String out = new String(proc.getInputStream().readAllBytes()).trim();
                if (proc.waitFor() != 0 || out.isBlank()) {
                    return Map.of("error", "jstack not found. Set JAVA_HOME environment variable or ensure JDK bin is on PATH.",
                            "suggestion", "Install a JDK (not just JRE) and set JAVA_HOME=/path/to/jdk");
                }
            } catch (Exception e) {
                return Map.of("error", "jstack not found. Set JAVA_HOME environment variable.");
            }
        }

        String pid = findJbossPid();
        if (pid == null) {
            return Map.of("error", "JBoss process not found. Is JBoss EAP running?",
                    "suggestion", "Start JBoss and try again, or check with: jps -l or ps -ef | grep jboss");
        }

        java.nio.file.Path dumpDir = Paths.get(jbossHome, "thread_dump");
        try { Files.createDirectories(dumpDir); } catch (Exception ignored) {}

        String tsPrefix = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        java.nio.file.Path combinedFile = dumpDir.resolve("jstack_threaddump_" + tsPrefix + ".out");
        List<Map<String, Object>> dumpFiles = new ArrayList<>();
        List<java.nio.file.Path> individualFiles = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            if (i > 0) {
                try { Thread.sleep(interval * 1000L); } catch (InterruptedException ignored) {}
            }
            String dumpTs = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            try {
                ProcessBuilder pb = new ProcessBuilder(jstackCmd, "-l", pid);
                pb.redirectErrorStream(false);
                Process proc = pb.start();
                String stdout = new String(proc.getInputStream().readAllBytes());
                String stderr = new String(proc.getErrorStream().readAllBytes());
                boolean ok = proc.waitFor() == 0;

                if (!ok) {
                    String errMsg = stderr.isBlank() ? "jstack exited with code " + proc.exitValue() : stderr.trim();
                    dumpFiles.add(Map.of("index", i + 1, "error", errMsg, "timestamp", dumpTs));
                    continue;
                }

                int threadCount = 0;
                for (int ci = 0; ci < stdout.length() - 1; ci++) {
                    if (stdout.charAt(ci) == '"') threadCount++;
                }
                threadCount /= 2;

                // Append to combined file
                try (var writer = Files.newBufferedWriter(combinedFile, java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.APPEND)) {
                    writer.write("\n" + "=".repeat(80) + "\n");
                    writer.write("Thread Dump #" + (i + 1) + " — " + dumpTs + "\n");
                    writer.write("=".repeat(80) + "\n\n");
                    writer.write(stdout);
                    writer.write("\n\n");
                }

                // Individual file
                String indFilename = String.format("thread_dump_%s_%02d.txt", tsPrefix, i + 1);
                java.nio.file.Path indPath = dumpDir.resolve(indFilename);
                Files.writeString(indPath, stdout);
                individualFiles.add(indPath);

                Map<String, Object> df = new LinkedHashMap<>();
                df.put("index", i + 1);
                df.put("filename", indFilename);
                df.put("thread_count", threadCount);
                df.put("timestamp", dumpTs);
                dumpFiles.add(df);
            } catch (Exception e) {
                dumpFiles.add(Map.of("index", i + 1, "error", e.getClass().getSimpleName() + ": " + e.getMessage(), "timestamp", dumpTs));
            }
        }

        // Create zip
        String zipName = "thread_dumps_" + tsPrefix + ".zip";
        java.nio.file.Path zipPath = dumpDir.resolve(zipName);
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipPath.toFile()))) {
            if (Files.isRegularFile(combinedFile)) {
                zos.putNextEntry(new ZipEntry(combinedFile.getFileName().toString()));
                Files.copy(combinedFile, zos);
                zos.closeEntry();
            }
            for (java.nio.file.Path fp : individualFiles) {
                if (Files.isRegularFile(fp)) {
                    zos.putNextEntry(new ZipEntry(fp.getFileName().toString()));
                    Files.copy(fp, zos);
                    zos.closeEntry();
                }
            }
        } catch (Exception e) {
            return Map.of("error", "Failed to create zip: " + e.getMessage(), "dumps", dumpFiles);
        }

        int successful = (int) dumpFiles.stream().filter(d -> d.containsKey("filename")).count();
        int failed = (int) dumpFiles.stream().filter(d -> d.containsKey("error")).count();

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "ok");
        resp.put("zip_file", zipPath.toString());
        resp.put("zip_name", zipName);
        resp.put("combined_file", combinedFile.toString());
        resp.put("dump_dir", dumpDir.toString());
        resp.put("jboss_pid", pid);
        resp.put("java_home", javaHome != null ? javaHome : "(system PATH)");
        resp.put("jstack_cmd", jstackCmd);
        resp.put("count", count);
        resp.put("successful", successful);
        resp.put("failed", failed);
        resp.put("interval_seconds", interval);
        resp.put("dumps", dumpFiles);
        return resp;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Internal helpers
    // ═══════════════════════════════════════════════════════════════════════

    private void putIfPresent(Map<String, Object> pool, String key, JsonNode node, String attr) {
        JsonNode val = node.path(attr);
        if (!val.isMissingNode() && !val.isNull()) {
            pool.put(key, val.asInt(0));
        }
    }

    private void putIfPresentLong(Map<String, Object> pool, String key, JsonNode node, String attr) {
        JsonNode val = node.path(attr);
        if (!val.isMissingNode() && !val.isNull()) {
            pool.put(key, val.asLong(0));
        }
    }

    private Map<String, Object> buildThreadDumpResponse(List<Map<String, Object>> rawThreads, int maxThreads) {
        List<Map<String, Object>> threads = rawThreads.subList(0, Math.min(rawThreads.size(), maxThreads));
        Map<String, Integer> stateSummary = new LinkedHashMap<>();
        for (Map<String, Object> t : rawThreads) {
            String state = (String) t.getOrDefault("thread_state", "UNKNOWN");
            stateSummary.merge(state, 1, Integer::sum);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "ok");
        resp.put("total_threads", rawThreads.size());
        resp.put("returned_threads", threads.size());
        resp.put("state_summary", stateSummary);
        resp.put("threads", threads);
        return resp;
    }

    private Map<String, Object> parseThreadInfo(JsonNode t) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("thread_id", t.path("thread-id").asLong());
        info.put("thread_name", t.path("thread-name").asText("unknown"));
        info.put("thread_state", t.path("thread-state").asText("unknown"));
        info.put("suspended", t.path("suspended").asBoolean(false));
        info.put("in_native", t.path("in-native").asBoolean(false));
        info.put("blocked_count", t.path("blocked-count").asInt(0));
        info.put("blocked_time", t.path("blocked-time").asInt(-1));
        info.put("waited_count", t.path("waited-count").asInt(0));
        info.put("waited_time", t.path("waited-time").asInt(-1));
        info.put("lock_name", t.path("lock-name").isNull() ? null : t.path("lock-name").asText());
        info.put("lock_owner_id", t.path("lock-owner-id").asLong(-1));
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

    private static final Pattern THREAD_HEADER_RE = Pattern.compile("^\"(.+?)\"\\s+");
    private static final Pattern TID_RE = Pattern.compile("tid=(0x[\\da-fA-F]+)");
    private static final Pattern STATE_INLINE_RE = Pattern.compile(
            "\\b(runnable|sleeping|waiting\\s+on\\s+condition|in\\s+Object\\.wait|blocked\\s+on\\s+monitor\\s+entry|waiting\\s+for\\s+monitor\\s+entry)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern STATE_RE = Pattern.compile("^\\s+java\\.lang\\.Thread\\.State:\\s+(\\S+)");
    private static final Pattern FRAME_RE = Pattern.compile("^\\s+at\\s+([\\w.$]+)\\.([\\w$<>]+)\\(([^)]*)\\)");
    private static final Pattern LOCK_WAITING_RE = Pattern.compile("^\\s+-\\s+(?:waiting on|waiting to lock|parking to wait for)\\s+<([^>]+)>");
    private static final Pattern LOCK_LOCKED_RE = Pattern.compile("^\\s+-\\s+locked\\s+<([^>]+)>");
    private static final Pattern THREAD_NUM_RE = Pattern.compile("#(\\d+)");

    List<Map<String, Object>> parseJcmdThreadDump(String text) {
        List<Map<String, Object>> threads = new ArrayList<>();
        Map<String, Object> current = null;

        for (String line : text.split("\n")) {
            Matcher hm = THREAD_HEADER_RE.matcher(line);
            if (hm.find()) {
                String threadName = hm.group(1);

                Matcher tidM = TID_RE.matcher(line);
                long tid = tidM.find() ? Long.parseLong(tidM.group(1).substring(2), 16) : threads.size();

                String state = "UNKNOWN";
                Matcher stateM = STATE_INLINE_RE.matcher(line);
                if (stateM.find()) {
                    state = inlineStateToJava(stateM.group(1));
                }

                current = new LinkedHashMap<>();
                current.put("thread_name", threadName);
                current.put("thread_id", tid);
                current.put("thread_state", state);
                current.put("suspended", false);
                current.put("in_native", false);
                current.put("blocked_count", 0);
                current.put("blocked_time", -1);
                current.put("waited_count", 0);
                current.put("waited_time", -1);
                current.put("lock_name", null);
                current.put("lock_owner_name", null);
                current.put("stack_trace", new ArrayList<String>());
                threads.add(current);
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
                String cls = fm.group(1);
                String method = fm.group(2);
                String location = fm.group(3);
                String fileName = "";
                int lineNumber = -1;
                if (location.contains(":")) {
                    String[] parts = location.split(":");
                    fileName = parts[0];
                    try { lineNumber = Integer.parseInt(parts[1]); } catch (NumberFormatException ignored) {}
                } else if ("Native Method".equals(location)) {
                    fileName = "Native Method";
                } else {
                    fileName = location;
                }
                @SuppressWarnings("unchecked")
                List<String> stack = (List<String>) current.get("stack_trace");
                stack.add(cls + "." + method + "(" + fileName + ":" + lineNumber + ")");
                continue;
            }

            Matcher lm = LOCK_WAITING_RE.matcher(line);
            if (lm.find()) {
                current.put("lock_name", lm.group(1));
                continue;
            }

            Matcher lm2 = LOCK_LOCKED_RE.matcher(line);
            if (lm2.find()) {
                @SuppressWarnings("unchecked")
                List<String> monitors = (List<String>) current.computeIfAbsent("locked_monitors", k -> new ArrayList<String>());
                monitors.add(lm2.group(1));
            }
        }
        return threads;
    }

    private String inlineStateToJava(String s) {
        s = s.toLowerCase().trim();
        if ("runnable".equals(s)) return "RUNNABLE";
        if (s.contains("blocked") || s.contains("monitor entry")) return "BLOCKED";
        if (s.contains("waiting") || s.contains("sleeping")) return "WAITING";
        return "UNKNOWN";
    }

    String[] classifyThread(String threadName, List<String> stackTrace) {
        String nameLower = threadName.toLowerCase();
        String stackStr = stackTrace != null ? String.join(" ", stackTrace).toLowerCase() : "";

        boolean isServingRequest = stackStr.contains("servlet") || stackStr.contains("dofilter")
                || stackStr.contains("resteasy") || stackStr.contains("jakarta.ws.rs")
                || stackStr.contains("javax.ws.rs");
        if (nameLower.contains("task-")) {
            if (isServingRequest) return new String[]{"HTTP Request", "Handling HTTP/REST request"};
            return new String[]{"Task Worker", "Default task thread"};
        }
        if (nameLower.contains("i/o-") || nameLower.matches(".*\\bi/o\\b.*")) {
            if (isServingRequest) return new String[]{"HTTP Request", "Handling HTTP/REST request"};
            return new String[]{"IO Worker", "Default I/O thread"};
        }
        if (nameLower.contains("http") || nameLower.contains("undertow") || nameLower.contains("xnio")) {
            if (isServingRequest) return new String[]{"HTTP Request", "Handling HTTP/REST request"};
            return new String[]{"HTTP Worker", "Undertow I/O thread"};
        }
        if (stackStr.contains("jdbc") || stackStr.contains("datasource") || stackStr.contains("mysql") || stackStr.contains("postgresql"))
            return new String[]{"DB Query", "Executing database query"};
        if (stackStr.contains("hibernate") || stackStr.contains("jpa") || stackStr.contains("persistence"))
            return new String[]{"JPA/Hibernate", "ORM persistence operation"};
        if (nameLower.contains("ejb") || stackStr.contains("ejb"))
            return new String[]{"EJB Call", "Enterprise bean invocation"};
        if (stackStr.contains("transaction") || stackStr.contains("arjuna"))
            return new String[]{"Transaction", "JTA transaction processing"};
        if (stackStr.contains("weld") || stackStr.contains("cdi"))
            return new String[]{"CDI", "CDI bean processing"};
        if (nameLower.contains("jms") || nameLower.contains("messaging") || stackStr.contains("activemq"))
            return new String[]{"JMS", "Message processing"};
        if (nameLower.contains("batch"))
            return new String[]{"Batch Job", "Batch processing"};
        if (nameLower.contains("timer") || nameLower.contains("scheduler") || stackStr.contains("quartz"))
            return new String[]{"Scheduler", "Scheduled task execution"};
        if (nameLower.contains("deployer") || stackStr.contains("deploy"))
            return new String[]{"Deployment", "Deployment processing"};
        if (nameLower.contains("gc") || nameLower.contains("finalizer") || nameLower.contains("reference"))
            return new String[]{"GC/Finalizer", "Garbage collection"};
        if (nameLower.contains("management") || nameLower.contains("jmx"))
            return new String[]{"Management", "Management operation"};
        if (nameLower.contains("acceptor") || stackStr.contains("socket") || stackStr.contains("nio"))
            return new String[]{"Network I/O", "Socket/NIO operation"};
        if (nameLower.contains("idle") || nameLower.contains("pool") || nameLower.contains("worker"))
            return new String[]{"Pool Worker", "Thread pool worker"};
        return new String[]{"JBoss Internal", "Internal server thread"};
    }

    private String topFrameSummary(List<String> stackTrace) {
        if (stackTrace == null || stackTrace.isEmpty()) return "";
        String top = stackTrace.get(0);
        int parenIdx = top.lastIndexOf('(');
        if (parenIdx > 0) {
            String methodPart = top.substring(0, parenIdx);
            int dotIdx = methodPart.lastIndexOf('.');
            if (dotIdx > 0) {
                String clsPart = methodPart.substring(0, dotIdx);
                int clsDot = clsPart.lastIndexOf('.');
                String cls = clsDot > 0 ? clsPart.substring(clsDot + 1) : clsPart;
                return cls + "." + methodPart.substring(dotIdx + 1) + "()";
            }
        }
        int di = top.indexOf('(');
        if (di > 0) {
            String p = top.substring(0, di);
            int ld = p.lastIndexOf('.');
            return ld > 0 ? p.substring(ld + 1) : p;
        }
        return top;
    }

    // ── Process / file system helpers ────────────────────────────────────

    String findJbossPid() {
        String selfPid = String.valueOf(ProcessHandle.current().pid());
        String jbossHome = findJbossHome();

        // Primary: score candidates by their FULL command line.
        //
        // `jps -l` truncates every jboss-modules launch to the identical jar path
        // ("<home>/jboss-modules.jar"), so the app server, jboss-cli connections and
        // the process/host controllers all look the same — picking the first match
        // often grabs a jboss-cli JVM (~37 threads) instead of the real server
        // (~hundreds of threads). Read full command lines so we can tell them apart.
        String best = scoreJbossCandidates(listJavaProcesses(), selfPid, jbossHome);
        if (best != null) return best;

        // Fallback: jps -ml (includes the main class + args, e.g. org.jboss.as.standalone
        // vs org.jboss.as.cli) — still better than a bare jar-path match.
        try {
            ProcessBuilder pb = new ProcessBuilder("jps", "-ml");
            Process proc = pb.start();
            String out = new String(proc.getInputStream().readAllBytes());
            proc.waitFor();
            List<String[]> procs = new ArrayList<>();
            for (String line : out.split("\n")) {
                String trimmed = line.trim();
                int sp = trimmed.indexOf(' ');
                if (sp <= 0 || !trimmed.substring(0, sp).matches("\\d+")) continue;
                procs.add(new String[]{trimmed.substring(0, sp), trimmed.substring(sp + 1)});
            }
            best = scoreJbossCandidates(procs, selfPid, jbossHome);
            if (best != null) return best;
        } catch (Exception ignored) {}

        // Last resort: ps -ef (older heuristic).
        try {
            ProcessBuilder pb = new ProcessBuilder("ps", "-ef");
            Process proc = pb.start();
            String out = new String(proc.getInputStream().readAllBytes());
            proc.waitFor();
            for (String line : out.split("\n")) {
                String lower = line.toLowerCase();
                if ((lower.contains("jboss") || lower.contains("standalone") || lower.contains("wildfly"))
                        && lower.contains("java") && !lower.contains("grep")
                        && !isNonServerJbossJvm(lower)) {
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 2 && !parts[1].equals(selfPid)) return parts[1];
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    /** Read every process's PID + full (untruncated) command line via ps. */
    private List<String[]> listJavaProcesses() {
        List<String[]> procs = new ArrayList<>();
        try {
            // -ww = unlimited width (no truncation); command= = full argv, no header.
            ProcessBuilder pb = new ProcessBuilder("ps", "-ww", "-eo", "pid=,command=");
            Process proc = pb.start();
            String out = new String(proc.getInputStream().readAllBytes());
            proc.waitFor();
            for (String line : out.split("\n")) {
                String trimmed = line.strip();
                if (trimmed.isEmpty()) continue;
                int sp = trimmed.indexOf(' ');
                if (sp <= 0) continue;
                String pid = trimmed.substring(0, sp);
                if (!pid.matches("\\d+")) continue;
                procs.add(new String[]{pid, trimmed.substring(sp + 1)});
            }
        } catch (Exception ignored) {}
        return procs;
    }

    /** True if a jboss command line is a jboss-cli / process-controller JVM — never the app server. */
    private boolean isNonServerJbossJvm(String lowerCmd) {
        return lowerCmd.contains("org.jboss.as.cli")
                || lowerCmd.contains("jboss-cli")
                || lowerCmd.contains("--connect")
                || lowerCmd.contains("--controller=")
                || lowerCmd.contains("process-controller")
                || lowerCmd.contains("[process controller]");
    }

    /**
     * Pick the PID most likely to be the running JBoss/WildFly application server.
     * Prefers a standalone server, then a domain server; excludes jboss-cli and the
     * process controller (few threads, no application code); prefers a match on JBOSS_HOME.
     */
    private String scoreJbossCandidates(List<String[]> procs, String selfPid, String jbossHome) {
        String best = null;
        int bestScore = Integer.MIN_VALUE;
        String homeLower = jbossHome != null ? jbossHome.toLowerCase() : null;
        for (String[] p : procs) {
            String pid = p[0];
            String lower = p[1].toLowerCase();
            if (pid.equals(selfPid)) continue;
            if (lower.contains("jboss-monitor-tool")) continue;          // our own app
            boolean jbossish = lower.contains("jboss-modules") || lower.contains("org.jboss.as")
                    || lower.contains("wildfly") || lower.contains("jboss.server.base.dir");
            if (!jbossish) continue;
            if (isNonServerJbossJvm(lower)) continue;                    // jboss-cli / process controller

            int score = 0;
            if (lower.contains("org.jboss.as.standalone") || lower.contains("[standalone]")) score += 100;
            if (lower.contains("[server:") || lower.contains("org.jboss.as.server")) score += 90;
            if (lower.contains("jboss.server.base.dir") || lower.contains("standalone.xml")) score += 20;
            if (lower.contains("[host controller]") || lower.contains("host-controller")) score += 5; // last resort
            if (homeLower != null && lower.contains(homeLower)) score += 15;
            if (score == 0) score = 1;                                   // generic jboss-modules server-ish

            if (score > bestScore) { bestScore = score; best = pid; }
        }
        return best;
    }

    String findJbossHome() {
        String jbossHome = config != null ? config.getJbossHome() : System.getenv("JBOSS_HOME");
        if (jbossHome != null && !jbossHome.isBlank() && Files.isDirectory(Paths.get(jbossHome))) {
            return jbossHome;
        }
        String[] candidates = {
                System.getProperty("user.home") + "/Documents/Softwares/Claude/jboss/jboss-eap-8.1",
                System.getProperty("user.home") + "/Downloads/jboss/jboss-eap-8.1",
        };
        for (String c : candidates) {
            if (Files.isDirectory(Paths.get(c))) return c;
        }
        try {
            try (var stream = Files.newDirectoryStream(Paths.get(System.getProperty("user.home")), "*")) {
                for (java.nio.file.Path dir : stream) {
                    java.nio.file.Path eap = dir.resolve("jboss");
                    if (Files.isDirectory(eap)) {
                        try (var inner = Files.newDirectoryStream(eap, "jboss-eap-*")) {
                            for (java.nio.file.Path e : inner) {
                                if (Files.isDirectory(e)) return e.toString();
                            }
                        }
                    }
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    String findJavaHome() {
        String javaHome = System.getenv("JAVA_HOME");
        if (javaHome != null && !javaHome.isBlank() && Files.isDirectory(Paths.get(javaHome))) {
            return javaHome;
        }
        // Try which jstack
        try {
            ProcessBuilder pb = new ProcessBuilder("which", "jstack");
            Process proc = pb.start();
            String out = new String(proc.getInputStream().readAllBytes()).trim();
            if (proc.waitFor() == 0 && !out.isBlank()) {
                java.nio.file.Path real = Paths.get(out).toRealPath();
                return real.getParent().getParent().toString();
            }
        } catch (Exception ignored) {}
        // Common paths
        String[] candidates = {
                "/usr/lib/jvm/java",
                "/usr/lib/jvm/java-17",
                "/usr/lib/jvm/java-11",
                System.getProperty("user.home") + "/jdk",
        };
        for (String c : candidates) {
            if (Files.isDirectory(Paths.get(c)) && Files.isRegularFile(Paths.get(c, "bin", "jstack"))) {
                return c;
            }
        }
        // macOS
        try {
            java.nio.file.Path jvmDir = Paths.get("/Library/Java/JavaVirtualMachines");
            if (Files.isDirectory(jvmDir)) {
                try (var stream = Files.newDirectoryStream(jvmDir)) {
                    for (java.nio.file.Path d : stream) {
                        java.nio.file.Path home = d.resolve("Contents/Home");
                        if (Files.isRegularFile(home.resolve("bin/jstack"))) return home.toString();
                    }
                }
            }
        } catch (Exception ignored) {}
        return null;
    }
}
