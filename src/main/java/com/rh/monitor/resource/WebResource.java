package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.config.ServerContext;
import com.rh.monitor.service.HistoryTracker;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.jboss.logging.Logger;


@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class WebResource {

    private static final Logger LOG = Logger.getLogger(WebResource.class);

    private static final int MAX_HISTORY = 200;
    private static final int MAX_ENDED_SESSIONS = 500;

    public static final ConcurrentLinkedDeque<Map<String, Object>> sessionHistory = new ConcurrentLinkedDeque<>();
    public static final ConcurrentLinkedDeque<Map<String, Object>> webStatsHistory = new ConcurrentLinkedDeque<>();
    public static final ConcurrentHashMap<String, Map<String, Object>> individualSessionTracker = new ConcurrentHashMap<>();
    public static final ConcurrentLinkedDeque<Map<String, Object>> endedSessionsLog = new ConcurrentLinkedDeque<>();

    @Inject
    JBossManagementClient client;

    @Inject
    ServerContext serverContext;

    @Inject
    ApiRequestResource apiRequestResource;

    @Inject
    HistoryTracker historyTracker;

    @GET
    @Path("/web")
    public Map<String, Object> getWebStats() {
        LOG.info("Fetching web statistics");
        try {
            boolean isServerSelected = false;
            try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

            List<Map<String, Object>> listeners = new ArrayList<>();
            List<Map<String, Object>> deployments = new ArrayList<>();

            if (!isServerSelected) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (!domainServers.isEmpty()) {
                    for (String[] hs : domainServers) {
                        try {
                            collectWebStatsForServer(
                                    List.of("host", hs[0], "server", hs[1]),
                                    hs[0] + "/" + hs[1], listeners, deployments, true);
                        } catch (Exception ignored) {}
                    }
                    Map<String, Object> web = new LinkedHashMap<>();
                    web.put("listeners", listeners);
                    web.put("deployments", deployments);
                    return Map.of("status", "ok", "web", web);
                }
            }

            collectWebStatsForServer(List.of(), null, listeners, deployments, false);

            Map<String, Object> web = new LinkedHashMap<>();
            web.put("listeners", listeners);
            web.put("deployments", deployments);
            return Map.of("status", "ok", "web", web);
        } catch (Exception e) {
            return Map.of("error", "Failed to get web stats: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void collectWebStatsForServer(List<String> addressPrefix, String domainServer,
                                           List<Map<String, Object>> listeners,
                                           List<Map<String, Object>> deployments,
                                           boolean useDirect) throws Exception {
        // First batch: undertow + root resource
        List<String> utAddr = useDirect ? new ArrayList<>(addressPrefix) : new ArrayList<>();
        if (!useDirect) utAddr.addAll(List.of("subsystem", "undertow"));
        else { utAddr.addAll(List.of("subsystem", "undertow")); }

        List<String> rootAddr = useDirect ? new ArrayList<>(addressPrefix) : List.of();

        List<String> batch1 = new ArrayList<>();
        batch1.add(client.buildReadResourceCommand(
                useDirect ? utAddr : List.of("subsystem", "undertow"), true, true));
        batch1.add(client.buildReadResourceCommand(
                useDirect ? new ArrayList<>(addressPrefix) : List.of(), true, false));

        List<JsonNode> batch1Results = client.executeBatch(batch1);
        if (batch1Results.isEmpty()) return;

        // Parse undertow result for server names
        JsonNode result = batch1Results.get(0);
        if (!"success".equals(result.path("outcome").asText())) return;

        JsonNode ut = result.get("result");
        JsonNode serverNode = ut.path("server");
        List<String> srvNames = new ArrayList<>();
        if (serverNode.isObject()) {
            serverNode.fieldNames().forEachRemaining(srvNames::add);
        }

        // Parse root result for deployment names
        List<String> depNames = new ArrayList<>();
        if (batch1Results.size() > 1) {
            JsonNode root = batch1Results.get(1);
            if ("success".equals(root.path("outcome").asText())) {
                JsonNode depNode = root.path("result").path("deployment");
                if (depNode.isObject()) {
                    depNode.fieldNames().forEachRemaining(depNames::add);
                }
            }
        }

        // Second batch: all server details + all deployment undertow details
        List<String> batch2 = new ArrayList<>();
        for (String srvName : srvNames) {
            List<String> srvAddr = useDirect ? new ArrayList<>(addressPrefix) : new ArrayList<>();
            srvAddr.addAll(List.of("subsystem", "undertow", "server", srvName));
            batch2.add(client.buildReadResourceCommand(srvAddr, true, true));
        }
        int depStartIdx = batch2.size();
        for (String depName : depNames) {
            List<String> depAddr = useDirect ? new ArrayList<>(addressPrefix) : new ArrayList<>();
            depAddr.addAll(List.of("deployment", depName, "subsystem", "undertow"));
            batch2.add(client.buildReadResourceCommand(depAddr, true, true));
        }

        List<JsonNode> batch2Results = batch2.isEmpty() ? List.of() : client.executeBatch(batch2);

        // Process server/listener results
        for (int i = 0; i < srvNames.size() && i < batch2Results.size(); i++) {
            try {
                JsonNode srvResult = batch2Results.get(i);
                if ("success".equals(srvResult.path("outcome").asText())) {
                    JsonNode srv = srvResult.get("result");
                    for (String lt : List.of("http-listener", "https-listener", "ajp-listener")) {
                        JsonNode listenerNode = srv.path(lt);
                        if (listenerNode.isObject()) {
                            Iterator<Map.Entry<String, JsonNode>> fields = listenerNode.fields();
                            while (fields.hasNext()) {
                                Map.Entry<String, JsonNode> entry = fields.next();
                                if (entry.getValue().isObject()) {
                                    JsonNode ld = entry.getValue();
                                    Map<String, Object> lm = new LinkedHashMap<>();
                                    if (domainServer != null) lm.put("server_name", domainServer);
                                    lm.put("server", srvNames.get(i));
                                    lm.put("type", lt);
                                    lm.put("name", entry.getKey());
                                    lm.put("enabled", ld.path("enabled").asBoolean(true));
                                    lm.put("request_count", safeLong(ld.path("request-count")));
                                    lm.put("error_count", safeLong(ld.path("error-count")));
                                    lm.put("bytes_sent", safeLong(ld.path("bytes-sent")));
                                    lm.put("bytes_received", safeLong(ld.path("bytes-received")));
                                    lm.put("processing_time_ms", safeLong(ld.path("processing-time")) / 1_000_000);
                                    lm.put("max_processing_time_ms", safeLong(ld.path("max-processing-time")) / 1_000_000);
                                    listeners.add(lm);
                                }
                            }
                        }
                    }
                }
            } catch (Exception ignored) {}
        }

        // Process deployment undertow results
        for (int i = 0; i < depNames.size(); i++) {
            int resultIdx = depStartIdx + i;
            if (resultIdx >= batch2Results.size()) break;
            try {
                JsonNode depUt = batch2Results.get(resultIdx);
                if ("success".equals(depUt.path("outcome").asText())) {
                    JsonNode depData = depUt.get("result");
                    JsonNode servlets = depData.path("servlet");
                    if (servlets.isObject()) {
                        Iterator<Map.Entry<String, JsonNode>> fields = servlets.fields();
                        while (fields.hasNext()) {
                            Map.Entry<String, JsonNode> entry = fields.next();
                            if (entry.getValue().isObject()) {
                                JsonNode ctx = entry.getValue();
                                Map<String, Object> dm = new LinkedHashMap<>();
                                if (domainServer != null) dm.put("server_name", domainServer);
                                dm.put("deployment", depNames.get(i));
                                dm.put("servlet", entry.getKey());
                                dm.put("request_count", safeLong(ctx.path("request-count")));
                                dm.put("total_request_time_ms", safeLong(ctx.path("total-request-time")));
                                dm.put("min_request_time_ms", safeLong(ctx.path("min-request-time")));
                                dm.put("max_request_time_ms", safeLong(ctx.path("max-request-time")));
                                deployments.add(dm);
                            }
                        }
                    }
                }
            } catch (Exception ignored) {}
        }
    }

    @GET
    @Path("/sessions")
    public Map<String, Object> getHttpSessions() {
        LOG.info("Fetching HTTP sessions");
        try {
            boolean isServerSelected = false;
            try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

            List<Map<String, Object>> sessions = new ArrayList<>();

            if (!isServerSelected) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (!domainServers.isEmpty()) {
                    for (String[] hs : domainServers) {
                        try {
                            collectSessionsForServer(
                                    List.of("host", hs[0], "server", hs[1]),
                                    hs[0] + "/" + hs[1], sessions, true);
                        } catch (Exception ignored) {}
                    }
                    return Map.of("status", "ok", "sessions", sessions);
                }
            }

            collectSessionsForServer(List.of(), null, sessions, false);
            return Map.of("status", "ok", "sessions", sessions);
        } catch (Exception e) {
            return Map.of("error", "Failed to get HTTP sessions: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void collectSessionsForServer(List<String> addressPrefix, String domainServer,
                                           List<Map<String, Object>> sessions, boolean useDirect) throws Exception {
        List<String> rootAddr = useDirect ? new ArrayList<>(addressPrefix) : List.of();
        JsonNode root = useDirect
                ? client.readResourceDirect(new ArrayList<>(addressPrefix), true, false)
                : client.readResource(List.of());
        if (!"success".equals(root.path("outcome").asText())) return;

        JsonNode depNode = root.path("result").path("deployment");
        if (!depNode.isObject()) return;

        List<String> depNames = new ArrayList<>();
        depNode.fieldNames().forEachRemaining(depNames::add);
        if (depNames.isEmpty()) return;

        List<String> batchCmds = new ArrayList<>();
        for (String depName : depNames) {
            List<String> depAddr = useDirect ? new ArrayList<>(addressPrefix) : new ArrayList<>();
            depAddr.addAll(List.of("deployment", depName, "subsystem", "undertow"));
            batchCmds.add(client.buildReadResourceCommand(depAddr, true, true));
        }

        List<JsonNode> results = client.executeBatch(batchCmds);
        for (int i = 0; i < Math.min(results.size(), depNames.size()); i++) {
            try {
                JsonNode depUt = results.get(i);
                if ("success".equals(depUt.path("outcome").asText())) {
                    JsonNode dd = depUt.get("result");
                    Map<String, Object> sm = new LinkedHashMap<>();
                    if (domainServer != null) sm.put("server_name", domainServer);
                    sm.put("deployment", depNames.get(i));
                    sm.put("active_sessions", safeLong(dd.path("active-sessions")));
                    sm.put("expired_sessions", safeLong(dd.path("expired-sessions")));
                    sm.put("sessions_created", safeLong(dd.path("sessions-created")));
                    sm.put("max_active_sessions", safeLong(dd.path("max-active-sessions")));
                    sm.put("rejected_sessions", safeLong(dd.path("rejected-sessions")));
                    sm.put("session_avg_alive_time", safeLong(dd.path("session-avg-alive-time")) * 1000);
                    sm.put("session_max_alive_time", safeLong(dd.path("session-max-alive-time")) * 1000);
                    sm.put("duplicated_session_ids", safeLong(dd.path("duplicated-session-ids")));
                    sessions.add(sm);
                }
            } catch (Exception ignored) {}
        }
    }

    @GET
    @Path("/sessions/history")
    public Map<String, Object> getSessionHistory(@QueryParam("deployment") String deployment) {
        boolean isServerSelected = false;
        String serverFilter = null;
        try {
            isServerSelected = serverContext != null && serverContext.hasContext();
            if (isServerSelected) {
                serverFilter = serverContext.getHost() + "/" + serverContext.getServer();
            }
        } catch (Exception ignored) {}

        List<Map<String, Object>> entries = new ArrayList<>();
        for (Map<String, Object> e : sessionHistory) {
            if (deployment != null && !deployment.isBlank() && !deployment.equals(e.get("deployment"))) continue;
            if (serverFilter != null && !serverFilter.equals(e.get("server_name"))) continue;
            if (isServerSelected) {
                Map<String, Object> copy = new LinkedHashMap<>(e);
                copy.remove("server_name");
                entries.add(copy);
            } else {
                entries.add(e);
            }
        }
        return Map.of("status", "ok", "history", entries, "count", entries.size());
    }

    @GET
    @Path("/web/history")
    public Map<String, Object> getWebHistory() {
        List<Map<String, Object>> entries = new ArrayList<>(webStatsHistory);
        return Map.of("status", "ok", "history", entries, "count", entries.size());
    }

    @GET
    @Path("/session-details")
    public Map<String, Object> getSessionDetails(@QueryParam("deployment") String deployment) {
        LOG.infof("Fetching session details for deployment: %s", deployment);

        if (individualSessionTracker.isEmpty() && endedSessionsLog.isEmpty()) {
            historyTracker.refreshSessionsNow();
        }

        boolean isServerSelected = false;
        String serverFilter = null;
        try {
            isServerSelected = serverContext != null && serverContext.hasContext();
            if (isServerSelected) {
                serverFilter = serverContext.getHost() + "/" + serverContext.getServer();
            }
        } catch (Exception ignored) {}

        List<Map<String, Object>> active = new ArrayList<>();
        for (Map<String, Object> tracker : individualSessionTracker.values()) {
            if (deployment != null && !deployment.isBlank() && !deployment.equals(tracker.get("deployment"))) {
                continue;
            }
            if (serverFilter != null && !serverFilter.equals(tracker.get("server_name"))) {
                continue;
            }
            Map<String, Object> copy = new LinkedHashMap<>(tracker);
            if (isServerSelected) copy.remove("server_name");
            active.add(copy);
        }
        active.sort((a, b) -> {
            long aTime = a.get("start_time_ms") instanceof Number ? ((Number) a.get("start_time_ms")).longValue() : 0;
            long bTime = b.get("start_time_ms") instanceof Number ? ((Number) b.get("start_time_ms")).longValue() : 0;
            return Long.compare(bTime, aTime);
        });

        List<Map<String, Object>> ended = new ArrayList<>();
        for (Map<String, Object> s : endedSessionsLog) {
            if (deployment != null && !deployment.isBlank() && !deployment.equals(s.get("deployment"))) continue;
            if (serverFilter != null && !serverFilter.equals(s.get("server_name"))) continue;
            Map<String, Object> copy = new LinkedHashMap<>(s);
            if (isServerSelected) copy.remove("server_name");
            ended.add(copy);
        }
        Collections.reverse(ended);
        if (ended.size() > 200) ended = ended.subList(0, 200);

        try {
            Map<String, String> sessionThreadMap = apiRequestResource.buildSessionToThreadMap();
            for (Map<String, Object> s : active) {
                String sid = (String) s.get("session_id");
                if (sid != null && sessionThreadMap.containsKey(sid)) {
                    s.put("thread", sessionThreadMap.get(sid));
                }
            }
            for (Map<String, Object> s : ended) {
                String sid = (String) s.get("session_id");
                if (sid != null && sessionThreadMap.containsKey(sid)) {
                    s.put("thread", sessionThreadMap.get(sid));
                }
            }
        } catch (Exception ignored) {}

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "ok");
        resp.put("active_sessions", active);
        resp.put("ended_sessions", ended);
        resp.put("active_count", active.size());
        resp.put("ended_count", endedSessionsLog.size());
        return resp;
    }

    // Called by HistoryPoller to add session history entries
    public static void addSessionHistory(Map<String, Object> entry) {
        sessionHistory.addLast(entry);
        while (sessionHistory.size() > MAX_HISTORY) sessionHistory.pollFirst();
    }

    // Called by HistoryPoller to add web stats history entries
    public static void addWebStatsHistory(Map<String, Object> entry) {
        webStatsHistory.addLast(entry);
        while (webStatsHistory.size() > MAX_HISTORY) webStatsHistory.pollFirst();
    }

    // Called by HistoryPoller to track/expire individual sessions
    public static void addEndedSession(Map<String, Object> session) {
        endedSessionsLog.addLast(session);
        while (endedSessionsLog.size() > MAX_ENDED_SESSIONS) endedSessionsLog.pollFirst();
    }

    private long safeLong(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) return 0;
        return node.asLong(0);
    }
}
