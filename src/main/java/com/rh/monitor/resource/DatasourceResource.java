package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.config.ServerContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import org.jboss.logging.Logger;

import java.net.URI;
import java.sql.*;
import java.util.*;

import static com.rh.monitor.util.TimeUtils.safeInt;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class DatasourceResource {

    private static final Logger LOG = Logger.getLogger(DatasourceResource.class);

    @Inject
    JBossManagementClient client;

    @Inject
    ServerContext serverContext;

    @GET
    @Path("/datasources")
    public Map<String, Object> getDatasources() {
        LOG.info("Fetching datasource statistics");
        try {
            boolean isServerSelected = false;
            try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

            if (!isServerSelected) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (!domainServers.isEmpty()) {
                    List<Map<String, Object>> datasources = new ArrayList<>();
                    Set<String> seen = new HashSet<>();
                    for (String[] hs : domainServers) {
                        try {
                            collectDatasourcesForServer(
                                    List.of("host", hs[0], "server", hs[1]),
                                    hs[0] + "/" + hs[1], datasources, seen, true);
                        } catch (Exception ignored) {}
                    }
                    Map<String, Object> resp = new LinkedHashMap<>();
                    resp.put("status", "ok");
                    resp.put("datasources", datasources);
                    return resp;
                }
            }

            List<Map<String, Object>> datasources = new ArrayList<>();
            collectDatasourcesForServer(List.of(), null, datasources, new HashSet<>(), false);

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("status", "ok");
            resp.put("datasources", datasources);
            return resp;
        } catch (Exception e) {
            return Map.of("error", "Failed to get datasource stats: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void collectDatasourcesForServer(List<String> addressPrefix, String domainServer,
                                              List<Map<String, Object>> datasources, Set<String> seen,
                                              boolean useDirect) throws Exception {
        List<String> dsAddr = new ArrayList<>(addressPrefix);
        dsAddr.addAll(List.of("subsystem", "datasources"));
        JsonNode result = useDirect
                ? client.readResourceDirect(dsAddr, true, false)
                : client.readResource(List.of("subsystem", "datasources"), true, false);
        if (!"success".equals(result.path("outcome").asText())) return;

        JsonNode data = result.get("result");

        List<String> batchCmds = new ArrayList<>();
        List<String[]> batchKeys = new ArrayList<>();

        for (String dsType : List.of("data-source", "xa-data-source")) {
            JsonNode dsNode = data.path(dsType);
            if (!dsNode.isObject()) continue;
            Iterator<String> names = dsNode.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                String key = name + "|" + (domainServer != null ? domainServer : "");
                if (seen.contains(key)) continue;
                seen.add(key);
                List<String> detailAddr = useDirect ? new ArrayList<>(addressPrefix) : new ArrayList<>();
                if (!useDirect) {
                    detailAddr.addAll(List.of("subsystem", "datasources", dsType, name));
                } else {
                    detailAddr.addAll(List.of("subsystem", "datasources", dsType, name));
                }
                batchCmds.add(client.buildReadResourceCommand(
                        useDirect ? detailAddr : List.of("subsystem", "datasources", dsType, name),
                        true, true));
                batchKeys.add(new String[]{name, dsType});
            }
        }

        if (batchCmds.isEmpty()) return;

        try {
            List<JsonNode> results = client.executeBatch(batchCmds);
            for (int i = 0; i < Math.min(results.size(), batchKeys.size()); i++) {
                try {
                    if ("success".equals(results.get(i).path("outcome").asText())) {
                        Map<String, Object> stats = extractDsStats(batchKeys.get(i)[0],
                                results.get(i).get("result"), batchKeys.get(i)[1]);
                        if (domainServer != null) stats.put("server_name", domainServer);
                        datasources.add(stats);
                    }
                } catch (Exception ex) {
                    Map<String, Object> err = new LinkedHashMap<>();
                    err.put("name", batchKeys.get(i)[0]);
                    if (domainServer != null) err.put("server_name", domainServer);
                    err.put("error", ex.toString());
                    datasources.add(err);
                }
            }
        } catch (Exception ex) {
            for (String[] key : batchKeys) {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("name", key[0]);
                if (domainServer != null) err.put("server_name", domainServer);
                err.put("error", ex.toString());
                datasources.add(err);
            }
        }
    }

    @GET
    @Path("/datasources/{name}")
    public Map<String, Object> getDatasourceDetail(@PathParam("name") String name) {
        LOG.infof("Fetching datasource detail: %s", name);
        try {
            for (String dsType : List.of("data-source", "xa-data-source")) {
                try {
                    JsonNode result = readDatasourceResource(dsType, name, true, true);
                    if (result != null) {
                        JsonNode ds = result.get("result");
                        Map<String, Object> stats = extractDsStats(name, ds, dsType);

                        Map<String, Object> connProps = new LinkedHashMap<>();
                        connProps.put("flush_strategy", ds.path("flush-strategy").asText(""));
                        connProps.put("idle_timeout_minutes", nodeToObj(ds.path("idle-timeout-minutes")));
                        connProps.put("blocking_timeout_wait_millis", nodeToObj(ds.path("blocking-timeout-wait-millis")));
                        connProps.put("query_timeout", nodeToObj(ds.path("query-timeout")));
                        connProps.put("set_tx_query_timeout", ds.path("set-tx-query-timeout").asBoolean(false));
                        connProps.put("use_try_lock", nodeToObj(ds.path("use-try-lock")));
                        connProps.put("validate_on_match", ds.path("validate-on-match").asBoolean(false));
                        connProps.put("background_validation", ds.path("background-validation").asBoolean(false));
                        connProps.put("background_validation_millis", nodeToObj(ds.path("background-validation-millis")));
                        connProps.put("valid_connection_checker", ds.path("valid-connection-checker-class-name").asText(null));
                        connProps.put("exception_sorter", ds.path("exception-sorter-class-name").asText(null));
                        stats.put("connection_properties", connProps);

                        Map<String, Object> resp = new LinkedHashMap<>();
                        resp.put("status", "ok");
                        resp.put("datasource", stats);
                        return resp;
                    }
                } catch (Exception ignored) {}
            }
            return Map.of("error", "Datasource '" + name + "' not found");
        } catch (Exception e) {
            return Map.of("error", "Failed to get datasource details: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @POST
    @Path("/datasources/{name}/flush")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> flushPool(@PathParam("name") String name, Map<String, Object> body) {
        String flushType = body != null && body.containsKey("flush_type")
                ? body.get("flush_type").toString()
                : "flush-idle-connection-in-pool";

        List<String> validOps = List.of(
                "flush-idle-connection-in-pool",
                "flush-all-connection-in-pool",
                "flush-invalid-connection-in-pool",
                "flush-gracefully-connection-in-pool");

        LOG.infof("Flushing datasource pool: %s, type: %s", name, flushType);
        if (!validOps.contains(flushType)) {
            return Map.of("error", "Invalid flush_type. Must be one of: " + validOps);
        }

        try {
            for (String dsType : List.of("data-source", "xa-data-source")) {
                try {
                    Map<String, Object> op = new LinkedHashMap<>();
                    op.put("operation", flushType);
                    op.put("address", List.of("subsystem", "datasources", dsType, name));
                    JsonNode result = client.mgmtRequest(op);
                    if ("success".equals(result.path("outcome").asText())) {
                        Map<String, Object> resp = new LinkedHashMap<>();
                        resp.put("status", "ok");
                        resp.put("datasource", name);
                        resp.put("flush_type", flushType);
                        resp.put("message", "Successfully flushed " + name);
                        return resp;
                    }
                } catch (Exception ignored) {}
            }
            return Map.of("error", "Datasource '" + name + "' not found or flush failed");
        } catch (Exception e) {
            return Map.of("error", "Failed to flush pool: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @GET
    @Path("/datasources/{name}/queries")
    public Map<String, Object> getQueryPerformance(@PathParam("name") String name) {
        LOG.infof("Fetching query performance for datasource: %s", name);
        try {
            for (String dsType : List.of("data-source", "xa-data-source")) {
                try {
                    JsonNode dsResult = readDatasourceResource(dsType, name, true, true);
                    if (dsResult == null) continue;

                    JsonNode ds = dsResult.get("result");
                    JsonNode stats = ds.path("statistics");
                    JsonNode pool = stats.isObject() ? stats.path("pool") : client.getMapper().createObjectNode();
                    JsonNode jdbc = stats.isObject() ? stats.path("jdbc") : client.getMapper().createObjectNode();

                    int avgUsage = safeInt(nodeToObj(pool.path("AverageUsageTime")));
                    int maxUsage = safeInt(nodeToObj(pool.path("MaxUsageTime")));

                    String health = "HEALTHY";
                    if (maxUsage > 30000) health = "CRITICAL";
                    else if (maxUsage > 10000) health = "WARNING";
                    else if (avgUsage > 5000) health = "WARNING";

                    Map<String, Object> connUsage = new LinkedHashMap<>();
                    connUsage.put("average_usage_time_ms", avgUsage);
                    connUsage.put("max_usage_time_ms", maxUsage);
                    connUsage.put("total_usage_time_ms", safeInt(nodeToObj(pool.path("TotalUsageTime"))));
                    connUsage.put("description", "Time connections are held by application (indicates query duration)");

                    Map<String, Object> connPoolTime = new LinkedHashMap<>();
                    connPoolTime.put("average_pool_time_ms", safeInt(nodeToObj(pool.path("AveragePoolTime"))));
                    connPoolTime.put("max_pool_time_ms", safeInt(nodeToObj(pool.path("MaxPoolTime"))));
                    connPoolTime.put("total_pool_time_ms", safeInt(nodeToObj(pool.path("TotalPoolTime"))));
                    connPoolTime.put("description", "Time connections sit idle in pool before reuse");

                    Map<String, Object> connAcquisition = new LinkedHashMap<>();
                    connAcquisition.put("average_get_time_ms", safeInt(nodeToObj(pool.path("AverageGetTime"))));
                    connAcquisition.put("max_get_time_ms", safeInt(nodeToObj(pool.path("MaxGetTime"))));
                    connAcquisition.put("total_get_time_ms", safeInt(nodeToObj(pool.path("TotalGetTime"))));
                    connAcquisition.put("average_creation_time_ms", safeInt(nodeToObj(pool.path("AverageCreationTime"))));
                    connAcquisition.put("max_creation_time_ms", safeInt(nodeToObj(pool.path("MaxCreationTime"))));

                    Map<String, Object> blocking = new LinkedHashMap<>();
                    blocking.put("average_blocking_time_ms", safeInt(nodeToObj(pool.path("AverageBlockingTime"))));
                    blocking.put("max_blocking_time_ms", safeInt(nodeToObj(pool.path("MaxBlockingTime"))));
                    blocking.put("total_blocking_time_ms", safeInt(nodeToObj(pool.path("TotalBlockingTime"))));
                    blocking.put("blocking_failure_count", safeInt(nodeToObj(pool.path("BlockingFailureCount"))));
                    blocking.put("description", "Time threads blocked waiting for a connection (high = pool exhaustion from slow queries)");

                    Map<String, Object> waitStats = new LinkedHashMap<>();
                    waitStats.put("wait_count", safeInt(nodeToObj(pool.path("WaitCount"))));
                    waitStats.put("max_wait_count", safeInt(nodeToObj(pool.path("MaxWaitCount"))));
                    waitStats.put("max_wait_time_ms", safeInt(nodeToObj(pool.path("MaxWaitTime"))));
                    waitStats.put("timed_out", safeInt(nodeToObj(pool.path("TimedOut"))));

                    Map<String, Object> activeConns = new LinkedHashMap<>();
                    activeConns.put("in_use_count", safeInt(nodeToObj(pool.path("InUseCount"))));
                    activeConns.put("active_count", safeInt(nodeToObj(pool.path("ActiveCount"))));
                    activeConns.put("max_used_count", safeInt(nodeToObj(pool.path("MaxUsedCount"))));

                    Map<String, Object> prepStmts = new LinkedHashMap<>();
                    prepStmts.put("cache_access_count", safeInt(nodeToObj(jdbc.path("PreparedStatementCacheAccessCount"))));
                    prepStmts.put("cache_hit_count", safeInt(nodeToObj(jdbc.path("PreparedStatementCacheHitCount"))));
                    prepStmts.put("cache_miss_count", safeInt(nodeToObj(jdbc.path("PreparedStatementCacheMissCount"))));
                    prepStmts.put("cache_add_count", safeInt(nodeToObj(jdbc.path("PreparedStatementCacheAddCount"))));
                    prepStmts.put("cache_delete_count", safeInt(nodeToObj(jdbc.path("PreparedStatementCacheDeleteCount"))));
                    prepStmts.put("cache_current_size", safeInt(nodeToObj(jdbc.path("PreparedStatementCacheCurrentSize"))));

                    Map<String, Object> queryConfig = new LinkedHashMap<>();
                    queryConfig.put("query_timeout", nodeToObj(ds.path("query-timeout")));
                    queryConfig.put("set_tx_query_timeout", ds.path("set-tx-query-timeout").asBoolean(false));
                    queryConfig.put("blocking_timeout_wait_millis", nodeToObj(ds.path("blocking-timeout-wait-millis")));
                    queryConfig.put("idle_timeout_minutes", nodeToObj(ds.path("idle-timeout-minutes")));
                    queryConfig.put("validate_on_match", nodeToObj(ds.path("validate-on-match")));
                    queryConfig.put("background_validation", nodeToObj(ds.path("background-validation")));
                    queryConfig.put("check_valid_connection_sql", ds.path("check-valid-connection-sql").asText(null));

                    Map<String, Object> queryPerf = new LinkedHashMap<>();
                    queryPerf.put("connection_usage", connUsage);
                    queryPerf.put("connection_pool_time", connPoolTime);
                    queryPerf.put("connection_acquisition", connAcquisition);
                    queryPerf.put("blocking", blocking);
                    queryPerf.put("wait_stats", waitStats);
                    queryPerf.put("active_connections", activeConns);
                    queryPerf.put("prepared_statements", prepStmts);
                    queryPerf.put("query_config", queryConfig);
                    queryPerf.put("health", health);

                    Map<String, Object> resp = new LinkedHashMap<>();
                    resp.put("status", "ok");
                    resp.put("datasource", name);
                    resp.put("query_performance", queryPerf);
                    return resp;
                } catch (Exception ignored) {}
            }
            return Map.of("error", "Datasource '" + name + "' not found");
        } catch (Exception e) {
            return Map.of("error", "Failed to get query performance: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @GET
    @Path("/datasources/{name}/active-queries")
    public Map<String, Object> getActiveQueries(@PathParam("name") String name) {
        LOG.infof("Fetching active queries for datasource: %s", name);
        return runDbOperation(name, "get active sessions", (conn, dbType) -> {
            List<Map<String, Object>> sessions;
            if ("postgresql".equals(dbType)) {
                sessions = pgActiveSessions(conn);
            } else {
                sessions = mysqlActiveSessions(conn);
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("sessions", sessions);
            return result;
        });
    }

    @GET
    @Path("/datasources/{name}/query-history")
    public Map<String, Object> getQueryHistory(@PathParam("name") String name) {
        return runDbOperation(name, "get query history", (conn, dbType) -> {
            Map<String, Object> result = new LinkedHashMap<>();
            if ("postgresql".equals(dbType)) {
                boolean hasExt = false;
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")) {
                    hasExt = rs.next();
                } catch (Exception ignored) {}
                if (hasExt) {
                    result.put("queries", pgQueryHistory(conn));
                    result.put("source", "pg_stat_statements");
                    result.put("has_source", true);
                } else {
                    result.put("queries", List.of());
                    result.put("source", null);
                    result.put("has_source", false);
                }
            } else {
                List<Map<String, Object>> queries = mysqlQueryHistory(conn);
                result.put("queries", queries);
                result.put("source", queries.isEmpty() ? null : "performance_schema");
                result.put("has_source", !queries.isEmpty());
            }
            return result;
        });
    }

    @GET
    @Path("/datasources/{name}/session-history")
    public Map<String, Object> getSessionHistory(@PathParam("name") String name) {
        return runDbOperation(name, "get session history", (conn, dbType) -> {
            Map<String, Object> result = new LinkedHashMap<>();
            if ("postgresql".equals(dbType)) {
                List<Map<String, Object>> events = new ArrayList<>();
                boolean hasSource = false;
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("""
                             SELECT query AS sql_text, calls,
                                    ROUND(total_exec_time::numeric, 2) AS total_time_ms,
                                    ROUND(mean_exec_time::numeric, 2) AS avg_time_ms,
                                    ROUND(min_exec_time::numeric, 2) AS min_time_ms,
                                    ROUND(max_exec_time::numeric, 2) AS max_time_ms,
                                    rows, shared_blks_hit, shared_blks_read
                             FROM pg_stat_statements
                             WHERE query NOT LIKE '%%pg_stat_statements%%'
                             ORDER BY max_exec_time DESC LIMIT 500
                             """)) {
                    events = resultSetToList(rs);
                    hasSource = true;
                } catch (Exception ignored) {}
                result.put("events", events);
                result.put("source", hasSource ? "pg_stat_statements" : null);
                result.put("has_source", hasSource);
            } else {
                List<Map<String, Object>> events = new ArrayList<>();
                boolean hasSource = false;
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("""
                             SELECT SQL_TEXT AS sql_text, CURRENT_SCHEMA AS db_name,
                                    ROUND(TIMER_WAIT / 1000000000, 2) AS exec_time_ms,
                                    ROUND(LOCK_TIME / 1000000000, 2) AS lock_time_ms,
                                    ROWS_AFFECTED AS rows_affected, ROWS_EXAMINED AS rows_examined,
                                    ROWS_SENT AS rows_sent, CREATED_TMP_TABLES AS tmp_tables,
                                    NO_INDEX_USED AS no_index, ERRORS AS errors,
                                    WARNINGS AS warnings, EVENT_NAME AS event_name
                             FROM performance_schema.events_statements_history_long
                             WHERE SQL_TEXT IS NOT NULL
                               AND SQL_TEXT NOT LIKE '%%performance_schema%%'
                               AND SQL_TEXT NOT LIKE '%%INFORMATION_SCHEMA%%'
                             ORDER BY TIMER_START DESC LIMIT 500
                             """)) {
                    events = resultSetToList(rs);
                    hasSource = true;
                } catch (Exception ignored) {}
                result.put("events", events);
                result.put("source", hasSource ? "events_statements_history_long" : null);
                result.put("has_source", hasSource);
            }
            return result;
        });
    }

    // --- Helper methods ---

    private Map<String, Object> extractDsStats(String name, JsonNode ds, String dsType) {
        JsonNode stats = ds.path("statistics");
        JsonNode pool = stats.isObject() ? stats.path("pool") : client.getMapper().createObjectNode();
        JsonNode jdbc = stats.isObject() ? stats.path("jdbc") : client.getMapper().createObjectNode();

        int active = safeInt(nodeToObj(pool.path("ActiveCount")));
        int maxUsed = safeInt(nodeToObj(pool.path("MaxUsedCount")));
        int available = safeInt(nodeToObj(pool.path("AvailableCount")));
        int maxPool = safeInt(nodeToObj(ds.path("max-pool-size")));
        if (maxPool == 0) maxPool = 20;

        String health = "HEALTHY";
        if (maxPool > 0) {
            double usagePct = (double) active / maxPool * 100;
            if (usagePct > 90) health = "CRITICAL";
            else if (usagePct > 75) health = "WARNING";
            else if (usagePct > 50) health = "MODERATE";
        }

        String connUrl = ds.path("connection-url").isObject()
                ? ds.path("connection-url").path("EXPRESSION_VALUE").asText("")
                : ds.path("connection-url").asText("");

        Map<String, Object> poolMap = new LinkedHashMap<>();
        poolMap.put("min_pool_size", safeInt(nodeToObj(ds.path("min-pool-size"))));
        poolMap.put("max_pool_size", maxPool);
        poolMap.put("initial_pool_size", nodeToObj(ds.path("initial-pool-size")));
        poolMap.put("active_count", active);
        poolMap.put("available_count", available);
        poolMap.put("max_used_count", maxUsed);
        poolMap.put("created_count", safeInt(nodeToObj(pool.path("CreatedCount"))));
        poolMap.put("destroyed_count", safeInt(nodeToObj(pool.path("DestroyedCount"))));
        poolMap.put("in_use_count", safeInt(nodeToObj(pool.path("InUseCount"))));
        poolMap.put("idle_count", safeInt(nodeToObj(pool.path("IdleCount"))));
        poolMap.put("max_wait_count", safeInt(nodeToObj(pool.path("MaxWaitCount"))));
        poolMap.put("timed_out", safeInt(nodeToObj(pool.path("TimedOut"))));
        poolMap.put("total_blocking_time_ms", safeInt(nodeToObj(pool.path("TotalBlockingTime"))));
        poolMap.put("total_creation_time_ms", safeInt(nodeToObj(pool.path("TotalCreationTime"))));
        poolMap.put("total_get_time_ms", safeInt(nodeToObj(pool.path("TotalGetTime"))));
        poolMap.put("wait_count", safeInt(nodeToObj(pool.path("WaitCount"))));
        poolMap.put("average_blocking_time_ms", safeInt(nodeToObj(pool.path("AverageBlockingTime"))));
        poolMap.put("average_creation_time_ms", safeInt(nodeToObj(pool.path("AverageCreationTime"))));
        poolMap.put("average_get_time_ms", safeInt(nodeToObj(pool.path("AverageGetTime"))));

        Map<String, Object> jdbcMap = new LinkedHashMap<>();
        jdbcMap.put("prepared_statement_cache_access_count", safeInt(nodeToObj(jdbc.path("PreparedStatementCacheAccessCount"))));
        jdbcMap.put("prepared_statement_cache_hit_count", safeInt(nodeToObj(jdbc.path("PreparedStatementCacheHitCount"))));
        jdbcMap.put("prepared_statement_cache_miss_count", safeInt(nodeToObj(jdbc.path("PreparedStatementCacheMissCount"))));
        jdbcMap.put("prepared_statement_cache_current_size", safeInt(nodeToObj(jdbc.path("PreparedStatementCacheCurrentSize"))));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("name", name);
        result.put("type", dsType);
        result.put("jndi_name", ds.path("jndi-name").asText(""));
        result.put("driver_name", ds.path("driver-name").asText(""));
        result.put("connection_url", connUrl);
        result.put("enabled", ds.path("enabled").asBoolean(false));
        result.put("statistics_enabled", ds.path("statistics-enabled").asBoolean(false));
        result.put("pool", poolMap);
        result.put("jdbc", jdbcMap);
        result.put("health", health);
        return result;
    }

    // Reads a datasource resource, resolving its address for the current mode. When a
    // server is selected, resolveAddress() adds the host/server prefix. In domain mode
    // with no server selected (the aggregated view), the datasource lives under a running
    // server rather than the domain-controller root, so we search running servers and
    // return the first match. Returns the success response node, or null if not found.
    private JsonNode readDatasourceResource(String dsType, String name,
                                            boolean includeRuntime, boolean recursive) {
        List<String> sub = List.of("subsystem", "datasources", dsType, name);
        boolean serverSelected = false;
        try { serverSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}
        try {
            if (serverSelected || !client.isDomainMode()) {
                JsonNode r = client.readResource(sub, includeRuntime, recursive);
                return "success".equals(r.path("outcome").asText()) ? r : null;
            }
            for (String[] hs : client.getRunningDomainServers()) {
                List<String> addr = new ArrayList<>(List.of("host", hs[0], "server", hs[1]));
                addr.addAll(sub);
                try {
                    JsonNode r = client.readResourceDirect(addr, includeRuntime, recursive);
                    if ("success".equals(r.path("outcome").asText())) return r;
                } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            LOG.debugf("readDatasourceResource(%s/%s) failed: %s", dsType, name, e.getMessage());
        }
        return null;
    }

    private Map<String, Object> getDsConnectionInfo(String dsName) throws Exception {
        for (String dsType : List.of("data-source", "xa-data-source")) {
            try {
                JsonNode result = readDatasourceResource(dsType, dsName, true, false);
                if (result != null) {
                    JsonNode ds = result.get("result");
                    String connUrl = ds.path("connection-url").isObject()
                            ? ds.path("connection-url").path("EXPRESSION_VALUE").asText()
                            : ds.path("connection-url").asText();
                    String username = ds.path("user-name").isObject()
                            ? ds.path("user-name").path("EXPRESSION_VALUE").asText("")
                            : ds.path("user-name").asText("");
                    String password = ds.path("password").isObject()
                            ? ds.path("password").path("EXPRESSION_VALUE").asText("")
                            : ds.path("password").asText("");
                    if (username.isEmpty() || username.contains("${"))
                        username = System.getenv().getOrDefault("DB_USER", "");
                    if (password.isEmpty() || password.contains("${"))
                        password = System.getenv().getOrDefault("DB_PASSWORD", "");

                    Map<String, Object> info = new LinkedHashMap<>();
                    info.put("connection_url", connUrl);
                    info.put("username", username);
                    info.put("password", password);
                    info.put("datasource_name", dsName);
                    info.put("ds_type", dsType);
                    return info;
                }
            } catch (Exception ignored) {}
        }
        return Map.of("error", "Datasource '" + dsName + "' not found");
    }

    private String detectDbType(String jdbcUrl) {
        String url = jdbcUrl.toLowerCase();
        if (url.contains("postgresql")) return "postgresql";
        if (url.contains("mysql") || url.contains("mariadb")) return "mysql";
        if (url.contains("h2")) return "h2";
        if (url.contains("oracle")) return "oracle";
        if (url.contains("sqlserver") || url.contains("jtds")) return "sqlserver";
        return "unknown";
    }

    private Connection connectToDb(String jdbcUrl, String username, String password) throws SQLException {
        if (!jdbcUrl.startsWith("jdbc:")) {
            jdbcUrl = "jdbc:" + jdbcUrl;
        }
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("connectTimeout", "10");
        props.setProperty("loginTimeout", "10");
        return DriverManager.getConnection(jdbcUrl, props);
    }

    @FunctionalInterface
    private interface DbOperation {
        Map<String, Object> execute(Connection conn, String dbType) throws Exception;
    }

    private Map<String, Object> runDbOperation(String dsName, String operation, DbOperation fn) {
        try {
            Map<String, Object> dsConfig = getDsConnectionInfo(dsName);
            if (dsConfig.containsKey("error")) return dsConfig;

            String connUrl = (String) dsConfig.get("connection_url");
            String dbType = detectDbType(connUrl);

            if ("postgresql".equals(dbType) || "mysql".equals(dbType) || "mariadb".equals(dbType)) {
                String effectiveType = "mariadb".equals(dbType) ? "mysql" : dbType;
                try (Connection conn = connectToDb(connUrl, (String) dsConfig.get("username"), (String) dsConfig.get("password"))) {
                    Map<String, Object> result = fn.execute(conn, effectiveType);
                    Map<String, Object> resp = new LinkedHashMap<>();
                    resp.put("status", "ok");
                    resp.put("datasource", dsName);
                    resp.put("db_type", effectiveType);
                    resp.putAll(result);
                    return resp;
                }
            } else {
                return unsupportedDbResponse(dsName, dbType, operation);
            }
        } catch (Exception e) {
            String className = e.getClass().getSimpleName();
            if (e instanceof ClassNotFoundException || className.contains("ClassNotFound")) {
                String module = e.getMessage();
                return Map.of("error", "Database driver not installed: " + module);
            }
            return Map.of("error", "Failed to " + operation + ": " + className + ": " + e.getMessage());
        }
    }

    private Map<String, Object> unsupportedDbResponse(String dsName, String dbType, String operation) {
        String msg;
        String status;
        if ("h2".equals(dbType)) {
            msg = "H2 in-memory databases cannot be queried externally. Use JBoss connection pool metrics instead.";
            status = "limited";
        } else {
            msg = "Direct " + operation + " is not yet supported for '" + dbType + "' databases. Supported: PostgreSQL, MySQL/MariaDB.";
            status = "unsupported";
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", status);
        resp.put("datasource", dsName);
        resp.put("db_type", dbType);
        resp.put("message", msg);
        return resp;
    }

    private List<Map<String, Object>> pgActiveSessions(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("""
                     SELECT pid, usename, datname, state, query,
                            EXTRACT(EPOCH FROM (now() - query_start))::numeric(10,2) AS runtime_seconds,
                            query_start::text AS query_start,
                            wait_event_type, wait_event,
                            EXTRACT(EPOCH FROM (now() - xact_start))::numeric(10,2) AS transaction_seconds
                     FROM pg_stat_activity
                     WHERE pid != pg_backend_pid()
                       AND query NOT LIKE '%%pg_stat_activity%%'
                     ORDER BY runtime_seconds DESC NULLS LAST
                     LIMIT 50
                     """)) {
            return resultSetToList(rs);
        }
    }

    private List<Map<String, Object>> mysqlActiveSessions(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("""
                     SELECT ID AS pid, USER AS usename, HOST, DB AS datname,
                            COMMAND AS state, TIME AS runtime_seconds,
                            STATE AS wait_event, INFO AS query
                     FROM INFORMATION_SCHEMA.PROCESSLIST
                     WHERE INFO IS NOT NULL
                       AND INFO NOT LIKE '%%INFORMATION_SCHEMA.PROCESSLIST%%'
                     ORDER BY TIME DESC
                     LIMIT 50
                     """)) {
            return resultSetToList(rs);
        }
    }

    private List<Map<String, Object>> pgQueryHistory(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("""
                     SELECT query,
                            calls,
                            round((total_exec_time / 1000)::numeric, 4) AS total_time_sec,
                            round((mean_exec_time / 1000)::numeric, 4) AS avg_time_sec,
                            round((min_exec_time / 1000)::numeric, 4) AS min_time_sec,
                            round((max_exec_time / 1000)::numeric, 4) AS max_time_sec,
                            rows,
                            round((shared_blks_hit::numeric / NULLIF(shared_blks_hit + shared_blks_read, 0) * 100), 1) AS cache_hit_pct,
                            NULL::text AS first_executed,
                            NULL::text AS last_executed,
                            NULL::text AS sample_query
                     FROM pg_stat_statements
                     WHERE query NOT LIKE '%%pg_stat_statements%%'
                       AND query NOT LIKE '%%pg_stat_activity%%'
                     ORDER BY max_exec_time DESC
                     """)) {
            return resultSetToList(rs);
        }
    }

    private List<Map<String, Object>> mysqlQueryHistory(Connection conn) {
        List<Map<String, Object>> queries = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("""
                     SELECT DIGEST_TEXT AS query,
                            COUNT_STAR AS calls,
                            FIRST_SEEN AS first_executed,
                            LAST_SEEN AS last_executed,
                            QUERY_SAMPLE_TEXT AS sample_query,
                            ROUND(SUM_TIMER_WAIT / 1000000000000, 4) AS total_time_sec,
                            ROUND(AVG_TIMER_WAIT / 1000000000000, 4) AS avg_time_sec,
                            ROUND(MIN_TIMER_WAIT / 1000000000000, 4) AS min_time_sec,
                            ROUND(MAX_TIMER_WAIT / 1000000000000, 4) AS max_time_sec,
                            SUM_ROWS_EXAMINED AS rows_examined,
                            SUM_ROWS_SENT AS rows_sent
                     FROM performance_schema.events_statements_summary_by_digest
                     WHERE DIGEST_TEXT IS NOT NULL
                       AND DIGEST_TEXT NOT LIKE '%%INFORMATION_SCHEMA%%'
                       AND DIGEST_TEXT NOT LIKE '%%performance_schema%%'
                     ORDER BY MAX_TIMER_WAIT DESC
                     """)) {
            queries = resultSetToList(rs);
        } catch (Exception ignored) {}
        return queries;
    }

    private List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= colCount; i++) {
                String colName = meta.getColumnLabel(i).toLowerCase();
                Object val = rs.getObject(i);
                if (val instanceof java.math.BigDecimal bd) {
                    row.put(colName, bd.doubleValue());
                } else if (val instanceof java.sql.Timestamp ts) {
                    row.put(colName, ts.toString());
                } else if (val instanceof byte[] bytes) {
                    row.put(colName, new String(bytes));
                } else {
                    row.put(colName, val);
                }
            }
            rows.add(row);
        }
        return rows;
    }

    private Object nodeToObj(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) return null;
        if (node.isInt()) return node.intValue();
        if (node.isLong()) return node.longValue();
        if (node.isDouble() || node.isFloat()) return node.doubleValue();
        if (node.isBoolean()) return node.booleanValue();
        if (node.isObject() && node.has("EXPRESSION_VALUE")) return node.path("EXPRESSION_VALUE").asText();
        return node.asText();
    }
}
