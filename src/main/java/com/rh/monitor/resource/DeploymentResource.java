package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.config.ServerContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import org.jboss.logging.Logger;

import java.util.*;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class DeploymentResource {

    private static final Logger LOG = Logger.getLogger(DeploymentResource.class);

    @Inject
    JBossManagementClient client;

    @Inject
    ServerContext serverContext;

    @GET
    @Path("/deployments")
    public Map<String, Object> getDeploymentStatus() {
        LOG.info("Fetching deployment status");
        try {
            boolean isServerSelected = false;
            try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

            if (!isServerSelected) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (!domainServers.isEmpty()) {
                    return getDomainDeploymentStatus(domainServers);
                }
            }

            return getLocalDeploymentStatus();
        } catch (Exception e) {
            return Map.of("error", "Failed to get deployments: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private Map<String, Object> getDomainDeploymentStatus(List<String[]> domainServers) {
        Map<String, Map<String, Object>> deploymentMap = new LinkedHashMap<>();

        for (String[] hs : domainServers) {
            String serverLabel = hs[0] + "/" + hs[1];
            List<String> prefix = List.of("host", hs[0], "server", hs[1]);

            try {
                JsonNode root = client.readResourceDirect(prefix, true, false);
                if (!"success".equals(root.path("outcome").asText())) continue;

                JsonNode depNode = root.path("result").path("deployment");
                if (!depNode.isObject()) continue;

                Iterator<String> depNames = depNode.fieldNames();
                while (depNames.hasNext()) {
                    String depName = depNames.next();
                    try {
                        List<String> depAddr = new ArrayList<>(prefix);
                        depAddr.addAll(List.of("deployment", depName));
                        JsonNode depResult = client.readResourceDirect(depAddr, true, false);
                        if (!"success".equals(depResult.path("outcome").asText())) continue;

                        JsonNode dep = depResult.get("result");

                        Map<String, Object> dm = deploymentMap.get(depName);
                        if (dm == null) {
                            dm = new LinkedHashMap<>();
                            dm.put("name", depName);
                            dm.put("runtime_name", dep.path("runtime-name").asText(depName));
                            dm.put("enabled", dep.path("enabled").asBoolean(false));
                            dm.put("status", dep.path("status").asText("unknown"));
                            dm.put("persistent", dep.path("persistent").asBoolean(true));
                            dm.put("enabled_time", dep.path("enabled-time").isMissingNode() ? null : dep.path("enabled-time").asLong());
                            dm.put("disabled_time", dep.path("disabled-time").isMissingNode() ? null : dep.path("disabled-time").asLong());

                            List<Object> content = new ArrayList<>();
                            if (dep.has("content") && dep.get("content").isArray()) {
                                for (JsonNode c : dep.get("content")) {
                                    content.add(client.getMapper().convertValue(c, Map.class));
                                }
                            }
                            dm.put("content", content);

                            List<String> subsystems = new ArrayList<>();
                            JsonNode subsNode = dep.path("subsystem");
                            if (subsNode.isObject()) {
                                subsNode.fieldNames().forEachRemaining(subsystems::add);
                            }
                            dm.put("subsystems", subsystems);
                            dm.put("servers", new ArrayList<String>());
                            deploymentMap.put(depName, dm);
                        }

                        @SuppressWarnings("unchecked")
                        List<String> servers = (List<String>) dm.get("servers");
                        servers.add(serverLabel);

                        if (!dep.path("enabled").asBoolean(false)) {
                            dm.put("enabled", false);
                        }
                        String serverStatus = dep.path("status").asText("unknown");
                        if (!"OK".equals(serverStatus)) {
                            dm.put("status", serverStatus);
                        }
                    } catch (Exception ignored) {}
                }
            } catch (Exception ignored) {}
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "ok");
        resp.put("deployments", new ArrayList<>(deploymentMap.values()));
        resp.put("total", deploymentMap.size());
        return resp;
    }

    private Map<String, Object> getLocalDeploymentStatus() throws Exception {
        JsonNode result = client.readResource(List.of());
        if (!"success".equals(result.path("outcome").asText())) {
            return Map.of("error", result.path("failure-description").asText("Unknown error"));
        }

        List<Map<String, Object>> deployments = new ArrayList<>();
        JsonNode depNode = result.path("result").path("deployment");
        if (depNode.isObject()) {
            List<String> names = new ArrayList<>();
            depNode.fieldNames().forEachRemaining(names::add);

            if (!names.isEmpty()) {
                List<String> batchCmds = new ArrayList<>();
                for (String name : names) {
                    batchCmds.add(client.buildReadResourceCommand(
                            List.of("deployment", name), true, false));
                }
                List<JsonNode> results = client.executeBatch(batchCmds);
                for (int i = 0; i < Math.min(results.size(), names.size()); i++) {
                    String depName = names.get(i);
                    try {
                        if ("success".equals(results.get(i).path("outcome").asText())) {
                            JsonNode dep = results.get(i).get("result");
                            Map<String, Object> dm = new LinkedHashMap<>();
                            dm.put("name", depName);
                            dm.put("runtime_name", dep.path("runtime-name").asText(depName));
                            dm.put("enabled", dep.path("enabled").asBoolean(false));
                            dm.put("status", dep.path("status").asText("unknown"));
                            dm.put("persistent", dep.path("persistent").asBoolean(true));
                            dm.put("enabled_time", dep.path("enabled-time").isMissingNode() ? null : dep.path("enabled-time").asLong());
                            dm.put("disabled_time", dep.path("disabled-time").isMissingNode() ? null : dep.path("disabled-time").asLong());

                            List<Object> content = new ArrayList<>();
                            if (dep.has("content") && dep.get("content").isArray()) {
                                for (JsonNode c : dep.get("content")) {
                                    content.add(client.getMapper().convertValue(c, Map.class));
                                }
                            }
                            dm.put("content", content);

                            List<String> subsystems = new ArrayList<>();
                            JsonNode subsNode = dep.path("subsystem");
                            if (subsNode.isObject()) {
                                subsNode.fieldNames().forEachRemaining(subsystems::add);
                            }
                            dm.put("subsystems", subsystems);
                            deployments.add(dm);
                        }
                    } catch (Exception ex) {
                        deployments.add(Map.of("name", depName, "error", ex.getMessage()));
                    }
                }
            }
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "ok");
        resp.put("deployments", deployments);
        resp.put("total", deployments.size());
        return resp;
    }

    @GET
    @Path("/deployment-resources")
    public Map<String, Object> getDeploymentResources() {
        LOG.info("Fetching deployment resources");
        try {
            boolean isServerSelected = false;
            try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

            if (!isServerSelected) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (!domainServers.isEmpty()) {
                    return getDomainDeploymentResources(domainServers);
                }
            }

            return getLocalDeploymentResources();
        } catch (Exception e) {
            return Map.of("error", "Failed to get deployment resources: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private Map<String, Object> getDomainDeploymentResources(List<String[]> domainServers) {
        Map<String, Map<String, Object>> deploymentMap = new LinkedHashMap<>();

        for (String[] hs : domainServers) {
            String serverLabel = hs[0] + "/" + hs[1];
            List<String> prefix = List.of("host", hs[0], "server", hs[1]);

            try {
                JsonNode root = client.readResourceDirect(prefix, true, false);
                if (!"success".equals(root.path("outcome").asText())) continue;

                JsonNode depNode = root.path("result").path("deployment");
                if (!depNode.isObject()) continue;

                Iterator<String> depNames = depNode.fieldNames();
                while (depNames.hasNext()) {
                    String depName = depNames.next();
                    try {
                        List<String> depAddr = new ArrayList<>(prefix);
                        depAddr.addAll(List.of("deployment", depName));
                        JsonNode depResult = client.readResourceDirect(depAddr, true, true);
                        if (!"success".equals(depResult.path("outcome").asText())) continue;

                        JsonNode dep = depResult.get("result");

                        Map<String, Object> depInfo = deploymentMap.get(depName);
                        boolean isNew = (depInfo == null);
                        if (isNew) {
                            depInfo = new LinkedHashMap<>();
                            depInfo.put("name", depName);
                            depInfo.put("status", dep.path("status").asText("unknown"));
                            depInfo.put("enabled", dep.path("enabled").asBoolean(false));
                            depInfo.put("runtime_name", dep.path("runtime-name").asText(depName));
                            depInfo.put("servers", new ArrayList<String>());

                            JsonNode content = dep.path("content");
                            if (content.isArray() && content.size() > 0) {
                                depInfo.put("content_hash", content.get(0).path("hash").path("BYTES_VALUE").asText(""));
                            }

                            JsonNode subsystems = dep.path("subsystem");
                            List<String> subsList = new ArrayList<>();
                            if (subsystems.isObject()) {
                                subsystems.fieldNames().forEachRemaining(subsList::add);
                            }
                            depInfo.put("subsystems", subsList);

                            depInfo.put("active_sessions", 0);
                            depInfo.put("sessions_created", 0);
                            depInfo.put("expired_sessions", 0);
                            depInfo.put("highest_session_count", 0);
                            depInfo.put("context_root", "");
                            depInfo.put("servlet_count", 0);
                            depInfo.put("total_requests", 0);
                            depInfo.put("total_request_time_ms", 0);
                            depInfo.put("rest_endpoint_count", 0);
                            depInfo.put("rest_resource_count", 0);
                            depInfo.put("ejb_count", 0);
                            depInfo.put("jpa", new ArrayList<>());

                            deploymentMap.put(depName, depInfo);
                        }

                        @SuppressWarnings("unchecked")
                        List<String> servers = (List<String>) depInfo.get("servers");
                        servers.add(serverLabel);

                        if (!dep.path("enabled").asBoolean(false)) {
                            depInfo.put("enabled", false);
                        }
                        String serverStatus = dep.path("status").asText("unknown");
                        if (!"OK".equals(serverStatus)) {
                            depInfo.put("status", serverStatus);
                        }

                        JsonNode subsystems = dep.path("subsystem");
                        JsonNode ut = subsystems.path("undertow");
                        if (ut.isObject()) {
                            depInfo.put("active_sessions", getInt(depInfo, "active_sessions") + si(ut, "active-sessions"));
                            depInfo.put("sessions_created", getInt(depInfo, "sessions_created") + si(ut, "sessions-created"));
                            depInfo.put("expired_sessions", getInt(depInfo, "expired_sessions") + si(ut, "expired-sessions"));
                            int hsc = si(ut, "highest-session-count");
                            if (hsc > getInt(depInfo, "highest_session_count")) {
                                depInfo.put("highest_session_count", hsc);
                            }
                            String cr = ut.path("context-root").asText("");
                            if (!cr.isEmpty()) depInfo.put("context_root", cr);

                            JsonNode servlets = ut.path("servlet");
                            if (servlets.isObject()) {
                                Iterator<Map.Entry<String, JsonNode>> fields = servlets.fields();
                                while (fields.hasNext()) {
                                    Map.Entry<String, JsonNode> entry = fields.next();
                                    if (entry.getValue().isObject()) {
                                        depInfo.put("servlet_count", getInt(depInfo, "servlet_count") + 1);
                                        depInfo.put("total_requests", getInt(depInfo, "total_requests") + si(entry.getValue(), "request-count"));
                                        depInfo.put("total_request_time_ms", getInt(depInfo, "total_request_time_ms") + si(entry.getValue(), "total-request-time"));
                                    }
                                }
                            }
                        }

                        if (isNew) {
                            JsonNode jaxrs = subsystems.path("jaxrs");
                            if (jaxrs.isObject()) {
                                JsonNode restResources = jaxrs.path("rest-resource");
                                if (restResources.isObject()) {
                                    Iterator<Map.Entry<String, JsonNode>> fields = restResources.fields();
                                    while (fields.hasNext()) {
                                        Map.Entry<String, JsonNode> entry = fields.next();
                                        if (entry.getValue().isObject()) {
                                            depInfo.put("rest_resource_count", getInt(depInfo, "rest_resource_count") + 1);
                                            JsonNode paths = entry.getValue().path("rest-resource-paths");
                                            if (paths.isArray()) {
                                                depInfo.put("rest_endpoint_count", getInt(depInfo, "rest_endpoint_count") + paths.size());
                                            }
                                        }
                                    }
                                }
                            }

                            JsonNode ejb = subsystems.path("ejb3");
                            if (ejb.isObject()) {
                                int ejbCount = 0;
                                for (String beanType : List.of("stateless-session-bean", "stateful-session-bean", "singleton-bean", "message-driven-bean")) {
                                    JsonNode beans = ejb.path(beanType);
                                    if (beans.isObject()) ejbCount += beans.size();
                                }
                                depInfo.put("ejb_count", ejbCount);
                            }

                            JsonNode jpa = subsystems.path("jpa");
                            if (jpa.isObject()) {
                                JsonNode pus = jpa.path("hibernate-persistence-unit");
                                if (pus.isObject()) {
                                    @SuppressWarnings("unchecked")
                                    List<Map<String, Object>> jpaStats = (List<Map<String, Object>>) depInfo.get("jpa");
                                    Iterator<Map.Entry<String, JsonNode>> fields = pus.fields();
                                    while (fields.hasNext()) {
                                        Map.Entry<String, JsonNode> entry = fields.next();
                                        if (!entry.getValue().isObject()) continue;
                                        JsonNode puData = entry.getValue();
                                        String puName = entry.getKey();
                                        if (puName.contains("#")) puName = puName.substring(puName.lastIndexOf('#') + 1);

                                        int entityCount = 0;
                                        JsonNode entities = puData.path("entity");
                                        if (entities.isObject()) entityCount = entities.size();

                                        long cacheSize = 0;
                                        int cacheCount = 0;
                                        JsonNode entityCaches = puData.path("entity-cache");
                                        if (entityCaches.isObject()) {
                                            Iterator<Map.Entry<String, JsonNode>> ecFields = entityCaches.fields();
                                            while (ecFields.hasNext()) {
                                                JsonNode ecData = ecFields.next().getValue();
                                                if (ecData.isObject()) {
                                                    cacheSize += ecData.path("second-level-cache-size-in-memory").asLong(0);
                                                    cacheCount += ecData.path("second-level-cache-count-in-memory").asInt(0);
                                                }
                                            }
                                        }

                                        Map<String, Object> jps = new LinkedHashMap<>();
                                        jps.put("persistence_unit", puName);
                                        jps.put("entity_count", entityCount);
                                        jps.put("session_open_count", si(puData, "session-open-count"));
                                        jps.put("session_close_count", si(puData, "session-close-count"));
                                        jps.put("query_execution_count", si(puData, "query-execution-count"));
                                        jps.put("l2_cache_size_bytes", cacheSize);
                                        jps.put("l2_cache_entries", cacheCount);
                                        jps.put("connect_count", si(puData, "connect-count"));
                                        jpaStats.add(jps);
                                    }
                                }
                            }
                        }
                    } catch (Exception ignored) {}
                }
            } catch (Exception ignored) {}
        }

        List<Map<String, Object>> deployments = new ArrayList<>(deploymentMap.values());
        estimateMemory(deployments);

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "ok");
        resp.put("deployments", deployments);
        resp.put("total", deployments.size());
        return resp;
    }

    private Map<String, Object> getLocalDeploymentResources() throws Exception {
        JsonNode root = client.readResource(List.of());
        if (!"success".equals(root.path("outcome").asText())) {
            return Map.of("error", root.path("failure-description").asText("Unknown error"));
        }

        List<Map<String, Object>> deployments = new ArrayList<>();
        JsonNode depNode = root.path("result").path("deployment");
        if (!depNode.isObject()) {
            return Map.of("status", "ok", "deployments", deployments, "total", 0);
        }

        List<String> names = new ArrayList<>();
        depNode.fieldNames().forEachRemaining(names::add);

        List<String> batchCmds = new ArrayList<>();
        for (String name : names) {
            batchCmds.add(client.buildReadResourceCommand(
                    List.of("deployment", name), true, true));
        }
        List<JsonNode> batchResults = batchCmds.isEmpty() ? List.of() : client.executeBatch(batchCmds);

        for (int idx = 0; idx < names.size(); idx++) {
            String depName = names.get(idx);
            Map<String, Object> depInfo = new LinkedHashMap<>();
            depInfo.put("name", depName);

            try {
                JsonNode depResult = idx < batchResults.size() ? batchResults.get(idx) : null;
                if (depResult == null || !"success".equals(depResult.path("outcome").asText())) {
                    depInfo.put("error", "Could not read deployment");
                    deployments.add(depInfo);
                    continue;
                }

                JsonNode dep = depResult.get("result");
                depInfo.put("status", dep.path("status").asText("unknown"));
                depInfo.put("enabled", dep.path("enabled").asBoolean(false));
                depInfo.put("runtime_name", dep.path("runtime-name").asText(depName));

                JsonNode content = dep.path("content");
                if (content.isArray() && content.size() > 0) {
                    depInfo.put("content_hash", content.get(0).path("hash").path("BYTES_VALUE").asText(""));
                }

                JsonNode subsystems = dep.path("subsystem");
                List<String> subsList = new ArrayList<>();
                if (subsystems.isObject()) {
                    subsystems.fieldNames().forEachRemaining(subsList::add);
                }
                depInfo.put("subsystems", subsList);

                JsonNode ut = subsystems.path("undertow");
                if (ut.isObject()) {
                    depInfo.put("active_sessions", si(ut, "active-sessions"));
                    depInfo.put("sessions_created", si(ut, "sessions-created"));
                    depInfo.put("expired_sessions", si(ut, "expired-sessions"));
                    depInfo.put("highest_session_count", si(ut, "highest-session-count"));
                    depInfo.put("context_root", ut.path("context-root").asText(""));

                    JsonNode servlets = ut.path("servlet");
                    int servletCount = 0;
                    int totalRequests = 0;
                    int totalTime = 0;
                    if (servlets.isObject()) {
                        Iterator<Map.Entry<String, JsonNode>> fields = servlets.fields();
                        while (fields.hasNext()) {
                            Map.Entry<String, JsonNode> entry = fields.next();
                            if (entry.getValue().isObject()) {
                                servletCount++;
                                totalRequests += si(entry.getValue(), "request-count");
                                totalTime += si(entry.getValue(), "total-request-time");
                            }
                        }
                    }
                    depInfo.put("servlet_count", servletCount);
                    depInfo.put("total_requests", totalRequests);
                    depInfo.put("total_request_time_ms", totalTime);
                }

                JsonNode jpa = subsystems.path("jpa");
                if (jpa.isObject()) {
                    JsonNode pus = jpa.path("hibernate-persistence-unit");
                    List<Map<String, Object>> jpaStats = new ArrayList<>();
                    if (pus.isObject()) {
                        Iterator<Map.Entry<String, JsonNode>> fields = pus.fields();
                        while (fields.hasNext()) {
                            Map.Entry<String, JsonNode> entry = fields.next();
                            if (!entry.getValue().isObject()) continue;
                            JsonNode puData = entry.getValue();
                            String puName = entry.getKey();
                            if (puName.contains("#")) puName = puName.substring(puName.lastIndexOf('#') + 1);

                            int entityCount = 0;
                            JsonNode entities = puData.path("entity");
                            if (entities.isObject()) entityCount = entities.size();

                            long cacheSize = 0;
                            int cacheCount = 0;
                            JsonNode entityCaches = puData.path("entity-cache");
                            if (entityCaches.isObject()) {
                                Iterator<Map.Entry<String, JsonNode>> ecFields = entityCaches.fields();
                                while (ecFields.hasNext()) {
                                    JsonNode ecData = ecFields.next().getValue();
                                    if (ecData.isObject()) {
                                        cacheSize += ecData.path("second-level-cache-size-in-memory").asLong(0);
                                        cacheCount += ecData.path("second-level-cache-count-in-memory").asInt(0);
                                    }
                                }
                            }

                            Map<String, Object> jps = new LinkedHashMap<>();
                            jps.put("persistence_unit", puName);
                            jps.put("entity_count", entityCount);
                            jps.put("session_open_count", si(puData, "session-open-count"));
                            jps.put("session_close_count", si(puData, "session-close-count"));
                            jps.put("query_execution_count", si(puData, "query-execution-count"));
                            jps.put("l2_cache_size_bytes", cacheSize);
                            jps.put("l2_cache_entries", cacheCount);
                            jps.put("connect_count", si(puData, "connect-count"));
                            jpaStats.add(jps);
                        }
                    }
                    depInfo.put("jpa", jpaStats);
                }

                JsonNode jaxrs = subsystems.path("jaxrs");
                if (jaxrs.isObject()) {
                    JsonNode restResources = jaxrs.path("rest-resource");
                    int endpointCount = 0;
                    int resourceCount = 0;
                    if (restResources.isObject()) {
                        Iterator<Map.Entry<String, JsonNode>> fields = restResources.fields();
                        while (fields.hasNext()) {
                            Map.Entry<String, JsonNode> entry = fields.next();
                            if (entry.getValue().isObject()) {
                                resourceCount++;
                                JsonNode paths = entry.getValue().path("rest-resource-paths");
                                if (paths.isArray()) endpointCount += paths.size();
                            }
                        }
                    }
                    depInfo.put("rest_endpoint_count", endpointCount);
                    depInfo.put("rest_resource_count", resourceCount);
                }

                JsonNode ejb = subsystems.path("ejb3");
                if (ejb.isObject()) {
                    int ejbCount = 0;
                    for (String beanType : List.of("stateless-session-bean", "stateful-session-bean", "singleton-bean", "message-driven-bean")) {
                        JsonNode beans = ejb.path(beanType);
                        if (beans.isObject()) ejbCount += beans.size();
                    }
                    depInfo.put("ejb_count", ejbCount);
                }

            } catch (Exception ex) {
                depInfo.put("error", ex.getMessage());
            }

            deployments.add(depInfo);
        }

        estimateMemory(deployments);

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "ok");
        resp.put("deployments", deployments);
        resp.put("total", deployments.size());
        return resp;
    }

    private void estimateMemory(List<Map<String, Object>> deployments) {
        for (Map<String, Object> dep : deployments) {
            long est = 0;
            est += getInt(dep, "active_sessions") * 50L;
            est += getInt(dep, "servlet_count") * 200L;
            est += getInt(dep, "rest_endpoint_count") * 100L;
            est += getInt(dep, "ejb_count") * 300L;

            Object jpaObj = dep.get("jpa");
            if (jpaObj instanceof List<?> jpaList) {
                for (Object item : jpaList) {
                    if (item instanceof Map<?, ?> pu) {
                        est += getInt(pu, "entity_count") * 500L;
                        est += getLong(pu, "l2_cache_size_bytes") / 1024;
                        est += getInt(pu, "session_open_count") * 10L;
                    }
                }
            }

            est = Math.max(est, 256);
            dep.put("estimated_memory_kb", est);
        }
    }

    private int si(JsonNode parent, String field) {
        JsonNode n = parent.path(field);
        if (n.isMissingNode() || n.isNull()) return 0;
        return n.asInt(0);
    }

    private int getInt(Map<?, ?> map, String key) {
        Object v = map.get(key);
        if (v instanceof Number) return ((Number) v).intValue();
        return 0;
    }

    private long getLong(Map<?, ?> map, String key) {
        Object v = map.get(key);
        if (v instanceof Number) return ((Number) v).longValue();
        return 0;
    }
}
