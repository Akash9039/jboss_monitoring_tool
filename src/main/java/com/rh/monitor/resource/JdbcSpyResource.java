package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.config.JBossConfig;
import com.rh.monitor.config.ServerContext;
import com.rh.monitor.util.DomainHelper;
import com.rh.monitor.util.TimeUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class JdbcSpyResource {

    private static final Logger LOG = Logger.getLogger(JdbcSpyResource.class);

    @Inject
    JBossManagementClient client;

    @Inject
    JBossConfig config;

    @Inject
    ServerContext serverContext;

    private static final Pattern SPY_PATTERN = Pattern.compile(
            "^((?:\\d{4}-\\d{2}-\\d{2} )?\\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\w+\\s+\\[jboss\\.jdbc\\.spy\\]\\s+" +
                    "\\(([^)]+)\\)\\s+(java:jboss/datasources/\\S+)\\s+\\[(\\w+)\\]\\s+(.*)");

    @GET
    @Path("/jdbc-spy")
    public Map<String, Object> getJdbcSpy(@QueryParam("datasource") String datasource) {
        LOG.info("Fetching JDBC spy data");
        return buildSpyResponse(datasource);
    }

    @GET
    @Path("/datasources/{name}/jdbc-spy")
    public Map<String, Object> getJdbcSpyByDs(@PathParam("name") String name) {
        return buildSpyResponse(name);
    }

    @GET
    @Path("/pool-sessions")
    public Map<String, Object> getPoolSessions() {
        LOG.info("Fetching pool sessions");
        try {
            boolean isServerSelected = false;
            try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

            List<Map<String, Object>> allEntries = parseAllSpyLogs(null, 10000);
            SpyQueryResult sqr = buildSpyQueries(allEntries);
            List<Map<String, Object>> sessions = new ArrayList<>();

            if (!isServerSelected) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (!domainServers.isEmpty()) {
                    for (String[] hs : domainServers) {
                        try {
                            collectPoolSessionsForServer(
                                    List.of("host", hs[0], "server", hs[1]),
                                    hs[0] + "/" + hs[1],
                                    sqr, sessions, true);
                        } catch (Exception ignored) {}
                    }

                    boolean spyConfigured = findJdbcSpyLog() != null;
                    int totalInflight = sessions.stream().mapToInt(s -> (int) s.get("inflight_count")).sum();
                    int totalInUse = sessions.stream().mapToInt(s -> (int) s.get("in_use")).sum();

                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("status", "ok");
                    result.put("sessions", sessions);
                    result.put("total_in_use", totalInUse);
                    result.put("total_inflight", totalInflight);
                    result.put("spy_configured", spyConfigured);
                    return result;
                }
            }

            collectPoolSessionsForServer(List.of(), null, sqr, sessions, false);

            boolean spyConfigured = findJdbcSpyLog() != null;
            int totalInflight = sessions.stream().mapToInt(s -> (int) s.get("inflight_count")).sum();
            int totalInUse = sessions.stream().mapToInt(s -> (int) s.get("in_use")).sum();

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("status", "ok");
            result.put("sessions", sessions);
            result.put("total_in_use", totalInUse);
            result.put("total_inflight", totalInflight);
            result.put("spy_configured", spyConfigured);
            return result;
        } catch (Exception e) {
            return Map.of("error", "Failed to get pool sessions: " + e.getMessage());
        }
    }

    private void collectPoolSessionsForServer(List<String> addressPrefix, String serverLabel,
                                               SpyQueryResult sqr, List<Map<String, Object>> sessions,
                                               boolean useDirect) throws Exception {
        List<String> dsAddr = new ArrayList<>(addressPrefix);
        dsAddr.addAll(List.of("subsystem", "datasources"));
        JsonNode dsResult = useDirect
                ? client.readResourceDirect(dsAddr, true, false)
                : client.readResource(List.of("subsystem", "datasources"), true, false);
        if (!"success".equals(dsResult.path("outcome").asText())) return;

        JsonNode data = dsResult.get("result");
        for (String dsType : List.of("data-source", "xa-data-source")) {
            JsonNode dsMap = data.path(dsType);
            if (dsMap.isMissingNode()) continue;
            Iterator<String> names = dsMap.fieldNames();
            while (names.hasNext()) {
                String dsName = names.next();
                try {
                    List<String> detailAddr = new ArrayList<>(addressPrefix);
                    detailAddr.addAll(List.of("subsystem", "datasources", dsType, dsName));
                    JsonNode r = useDirect
                            ? client.readResourceDirect(detailAddr, true, true)
                            : client.readResource(List.of("subsystem", "datasources", dsType, dsName), true, true);
                    if (!"success".equals(r.path("outcome").asText())) continue;
                    JsonNode ds = r.get("result");
                    JsonNode pool = ds.path("statistics").path("pool");

                    int inUse = TimeUtils.safeInt(pool.path("InUseCount").asText("0"));
                    int active = TimeUtils.safeInt(pool.path("ActiveCount").asText("0"));
                    int idle = TimeUtils.safeInt(pool.path("IdleCount").asText("0"));
                    int maxSize = TimeUtils.safeInt(ds.path("max-pool-size").asText("20"));

                    List<Map<String, Object>> dsInflight = sqr.inflight.stream()
                            .filter(q -> dsName.equals(q.get("datasource")))
                            .collect(Collectors.toList());

                    Map<String, Object> session = new LinkedHashMap<>();
                    if (serverLabel != null) session.put("server_name", serverLabel);
                    session.put("datasource", dsName);
                    session.put("jndi", ds.path("jndi-name").asText(""));
                    session.put("driver", ds.path("driver-name").asText(""));
                    session.put("in_use", inUse);
                    session.put("active", active);
                    session.put("idle", idle);
                    session.put("max_size", maxSize);
                    session.put("inflight_queries", dsInflight);
                    session.put("inflight_count", dsInflight.size());
                    session.put("pool_utilization", maxSize > 0 ? Math.round(active * 1000.0 / maxSize) / 10.0 : 0);
                    session.put("avg_get_time_ms", TimeUtils.safeInt(pool.path("AverageGetTime").asText("0")));
                    session.put("avg_blocking_time_ms", TimeUtils.safeInt(pool.path("AverageBlockingTime").asText("0")));
                    session.put("wait_count", TimeUtils.safeInt(pool.path("WaitCount").asText("0")));
                    session.put("timed_out", TimeUtils.safeInt(pool.path("TimedOut").asText("0")));
                    sessions.add(session);
                } catch (Exception ignored) {}
            }
        }
    }

    @GET
    @Path("/spy-session-history")
    public Map<String, Object> getSpySessionHistory(@QueryParam("datasource") String datasource) {
        LOG.info("Fetching spy session history");
        List<Map<String, Object>> entries = parseAllSpyLogs(datasource, 10000);
        if (entries.isEmpty()) {
            String logPath = findJdbcSpyLog();
            if (logPath == null) {
                return Map.of("status", "not_configured", "message", "JDBC spy logging is not configured.", "sessions", List.of());
            }
            return Map.of("status", "ok", "sessions", List.of(), "message", "No spy entries found");
        }

        SpyQueryResult sqr = buildSpyQueries(entries);
        Set<String> dsList = entries.stream().map(e -> (String) e.get("datasource")).collect(Collectors.toSet());

        List<Map<String, Object>> sessions = new ArrayList<>();
        for (Map<String, Object> q : sqr.queries.subList(0, Math.min(500, sqr.queries.size()))) {
            Map<String, Object> session = new LinkedHashMap<>();
            if (q.containsKey("server_name")) session.put("server_name", q.get("server_name"));
            session.put("datasource", q.get("datasource"));
            session.put("thread", q.get("thread"));
            session.put("sql", q.get("sql"));
            session.put("params", q.getOrDefault("params", ""));
            session.put("timestamp", q.get("timestamp"));
            session.put("exec_time_ms", q.get("exec_time_ms"));
            session.put("session_time_ms", q.get("exec_time_ms"));
            session.put("execute_type", q.getOrDefault("execute_type", ""));
            sessions.add(session);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("sessions", sessions);
        result.put("total", sqr.queries.size());
        result.put("datasources_seen", new ArrayList<>(dsList));
        result.put("datasource_filter", datasource);
        return result;
    }

    private Map<String, Object> buildSpyResponse(String datasourceFilter) {
        List<Map<String, Object>> entries = parseAllSpyLogs(datasourceFilter, 10000);
        if (entries.isEmpty()) {
            String logPath = findJdbcSpyLog();
            if (logPath == null) {
                return Map.of("status", "not_configured",
                        "message", "No jdbc-spy.log found. Enable spy=true on the datasource, then give the "
                                + "jboss.jdbc.spy logger its OWN periodic-rotating-file-handler writing to "
                                + "jdbc-spy.log with use-parent-handlers=false (otherwise spy output is merged "
                                + "into server.log and cannot be found). See README section 2.3.4.",
                        "queries", List.of());
            }
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("status", "ok");
            r.put("message", "No spy entries found" + (datasourceFilter != null ? " for " + datasourceFilter : ""));
            r.put("queries", List.of());
            r.put("log_file", logPath);
            return r;
        }

        SpyQueryResult sqr = buildSpyQueries(entries);
        Set<String> dsList = entries.stream().map(e -> (String) e.get("datasource")).collect(Collectors.toSet());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("queries", sqr.queries.subList(0, Math.min(500, sqr.queries.size())));
        result.put("inflight", sqr.inflight);
        result.put("total_queries", sqr.queries.size());
        result.put("inflight_count", sqr.inflight.size());
        result.put("raw_entries", entries.size());
        result.put("datasources_seen", new ArrayList<>(dsList));
        result.put("datasource_filter", datasourceFilter);
        result.put("log_file", findJdbcSpyLog());
        return result;
    }

    private List<Map<String, Object>> parseAllSpyLogs(String datasourceFilter, int maxLines) {
        boolean isServerSelected = false;
        try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}
        String jbossHome = config != null ? config.getJbossHome() : System.getenv("JBOSS_HOME");

        if (!isServerSelected && client.isDomainMode()) {
            List<Map<String, Object>> all = new ArrayList<>();
            List<DomainHelper.ServerLog> domainLogs = DomainHelper.findAllDomainLogs(config.getDomainHomeMap(), "jdbc-spy.log");
            for (DomainHelper.ServerLog sl : domainLogs) {
                List<Map<String, Object>> parsed = parseJdbcSpyLogFromPath(sl.path, datasourceFilter, maxLines);
                for (Map<String, Object> e : parsed) e.put("server_name", sl.serverName);
                all.addAll(parsed);
            }
            return all;
        }
        return parseJdbcSpyLog(datasourceFilter, maxLines);
    }

    List<Map<String, Object>> parseJdbcSpyLog(String datasourceFilter, int maxLines) {
        String logPath = findJdbcSpyLog();
        if (logPath == null) return List.of();
        return parseJdbcSpyLogFromPath(logPath, datasourceFilter, maxLines);
    }

    private List<Map<String, Object>> parseJdbcSpyLogFromPath(String logPath, String datasourceFilter, int maxLines) {

        List<Map<String, Object>> entries = new ArrayList<>();
        try {
            List<String> allLines = Files.readAllLines(java.nio.file.Path.of(logPath));
            List<String> lines = allLines.subList(Math.max(0, allLines.size() - maxLines), allLines.size());
            for (String line : lines) {
                Matcher m = SPY_PATTERN.matcher(line.strip());
                if (!m.matches()) continue;
                String timestamp = m.group(1);
                String thread = m.group(2);
                String jndi = m.group(3);
                String objType = m.group(4);
                String operation = m.group(5).strip();
                String dsName = jndi.replace("java:jboss/datasources/", "");
                if (datasourceFilter != null && !dsName.equals(datasourceFilter)) continue;

                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("timestamp", timestamp);
                entry.put("thread", thread);
                entry.put("datasource", dsName);
                entry.put("jndi", jndi);
                entry.put("object_type", objType);
                entry.put("operation", operation);
                entries.add(entry);
            }
        } catch (Exception ignored) {}
        return entries;
    }

    SpyQueryResult buildSpyQueries(List<Map<String, Object>> entries) {
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
                if (e.containsKey("server_name")) p.put("server_name", e.get("server_name"));
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
                    if (q.containsKey("server_name")) query.put("server_name", q.get("server_name"));
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

        List<Map<String, Object>> inflight = new ArrayList<>();
        String nowStr = TimeUtils.nowSpyFormat();
        for (Map.Entry<String, Map<String, Object>> entry : pending.entrySet()) {
            Map<String, Object> q = entry.getValue();
            if (q.get("execute_time") != null) {
                double elapsedMs = TimeUtils.spyTimeDiff((String) q.get("execute_time"), nowStr);
                double sessionMs = TimeUtils.spyTimeDiff((String) q.get("prepare_time"), nowStr);
                @SuppressWarnings("unchecked")
                List<String> params = (List<String>) q.get("params");
                String paramsStr = String.join("; ", params.subList(0, Math.min(10, params.size())));
                if (params.size() > 10) paramsStr += " ... (+" + (params.size() - 10) + " more)";

                Map<String, Object> inf = new LinkedHashMap<>();
                if (q.containsKey("server_name")) inf.put("server_name", q.get("server_name"));
                inf.put("sql", q.get("sql"));
                inf.put("datasource", q.get("datasource"));
                inf.put("thread", q.get("thread"));
                inf.put("timestamp", q.get("execute_time"));
                inf.put("prepare_time", q.get("prepare_time"));
                inf.put("exec_time_ms", elapsedMs);
                inf.put("session_time_ms", sessionMs);
                inf.put("params", paramsStr);
                inf.put("execute_type", q.getOrDefault("execute_type", ""));
                inf.put("status", "executing");
                inflight.add(inf);
            }
        }

        queries.sort((a, b) -> ((String) b.get("timestamp")).compareTo((String) a.get("timestamp")));
        inflight.sort((a, b) -> Double.compare((double) b.get("exec_time_ms"), (double) a.get("exec_time_ms")));
        return new SpyQueryResult(queries, inflight);
    }

    String findJdbcSpyLog() {
        List<String> candidates = new ArrayList<>();
        boolean hasServerCtx = false;
        try { hasServerCtx = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

        boolean isDomain = client.isDomainMode();
        if (hasServerCtx && isDomain) {
            String hostHome = config.getHomeForHost(serverContext.getHost());
            candidates.add(hostHome + "/domain/servers/" + serverContext.getServer() + "/log/jdbc-spy.log");
            for (String home : config.getAllDomainHomes()) {
                String path = home + "/domain/servers/" + serverContext.getServer() + "/log/jdbc-spy.log";
                if (!candidates.contains(path)) candidates.add(path);
            }
        } else if (isDomain) {
            for (String home : config.getAllDomainHomes()) {
                if (DomainHelper.isDomainLayout(home)) {
                    File serversDir = new File(home, "domain/servers");
                    File[] dirs = serversDir.listFiles(File::isDirectory);
                    if (dirs != null) {
                        for (File dir : dirs) {
                            candidates.add(new File(dir, "log/jdbc-spy.log").getPath());
                        }
                    }
                }
            }
        }
        String jbossHome = config != null ? config.getJbossHome() : System.getenv("JBOSS_HOME");
        if (!isDomain && jbossHome != null) {
            candidates.add(jbossHome + "/standalone/log/jdbc-spy.log");
        }
        for (String c : candidates) {
            if (new File(c).isFile()) return c;
        }
        return null;
    }

    static class SpyQueryResult {
        final List<Map<String, Object>> queries;
        final List<Map<String, Object>> inflight;

        SpyQueryResult(List<Map<String, Object>> queries, List<Map<String, Object>> inflight) {
            this.queries = queries;
            this.inflight = inflight;
        }
    }
}
