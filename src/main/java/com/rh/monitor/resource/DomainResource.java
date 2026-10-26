package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.config.JBossConfig;
import com.rh.monitor.util.TimeUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import org.jboss.logging.Logger;

import java.util.*;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class DomainResource {

    private static final Logger LOG = Logger.getLogger(DomainResource.class);

    @Inject
    JBossManagementClient client;

    @Inject
    JBossConfig config;

    @GET
    @Path("/domain/mode")
    public Map<String, Object> getMode() {
        LOG.info("Detecting JBoss operating mode");
        client.clearDomainModeCache();
        try {
            JsonNode data = readRootResource();
            if (data == null) {
                return Map.of("error", "Cannot read root resource from JBoss management API");
            }
            String launchType = data.path("launch-type").asText("STANDALONE");
            String processType = data.path("process-type").asText("Server");

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("status", "ok");
            result.put("mode", launchType);
            result.put("process_type", processType);
            result.put("is_domain", "DOMAIN".equalsIgnoreCase(launchType));
            result.put("product_name", data.path("product-name").asText("unknown"));
            result.put("product_version", data.path("product-version").asText("unknown"));
            result.put("management_url", config.mgmtUrl());
            return result;
        } catch (Exception e) {
            return Map.of("error", "Failed to detect mode: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @GET
    @Path("/domain/topology")
    public Map<String, Object> getTopology() {
        LOG.info("Fetching domain topology");
        client.clearDomainModeCache();
        try {
            JsonNode rootData = readRootResource();
            if (rootData == null) {
                return Map.of("error", "Cannot read root resource from JBoss management API");
            }
            String launchType = rootData.path("launch-type").asText("STANDALONE");

            if (!"DOMAIN".equalsIgnoreCase(launchType)) {
                return buildStandaloneTopology(rootData);
            }

            return buildDomainTopology(rootData);
        } catch (Exception e) {
            return Map.of("error", "Failed to get topology: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private Map<String, Object> buildStandaloneTopology(JsonNode rootData) throws Exception {
        Map<String, Object> serverInfo = new LinkedHashMap<>();
        serverInfo.put("name", "standalone");
        serverInfo.put("host", "localhost");
        String serverState = rootData.path("server-state").asText("unknown");
        serverInfo.put("server_state", serverState);
        serverInfo.put("status", "running".equalsIgnoreCase(serverState) ? "STARTED" : "STOPPED");
        serverInfo.put("suspend_state", rootData.path("suspend-state").asText("unknown"));
        serverInfo.put("running_mode", rootData.path("running-mode").asText("unknown"));

        try {
            List<String> batchCmds = List.of(
                    client.buildReadResourceCommand(
                            List.of("core-service", "platform-mbean", "type", "runtime"), true, false),
                    client.buildReadResourceCommand(
                            List.of("core-service", "platform-mbean", "type", "memory"), true, false),
                    client.buildReadResourceCommand(
                            List.of("core-service", "platform-mbean", "type", "threading"), true, false));
            List<JsonNode> results = client.executeBatch(batchCmds);

            if (results.size() > 0 && "success".equals(results.get(0).path("outcome").asText())) {
                JsonNode rt = results.get(0).get("result");
                serverInfo.put("uptime_ms", rt.path("uptime").asLong(0));
                serverInfo.put("uptime_human", TimeUtils.msToHuman(rt.path("uptime").asLong(0)));
            }
            if (results.size() > 1 && "success".equals(results.get(1).path("outcome").asText())) {
                JsonNode heap = results.get(1).path("result").path("heap-memory-usage");
                long used = heap.path("used").asLong(0);
                long max = heap.path("max").asLong(1);
                serverInfo.put("heap_used_mb", round2(used / 1048576.0));
                serverInfo.put("heap_max_mb", round2(max / 1048576.0));
                serverInfo.put("heap_used_percent", round2(used * 100.0 / Math.max(max, 1)));
            }
            if (results.size() > 2 && "success".equals(results.get(2).path("outcome").asText())) {
                serverInfo.put("thread_count", results.get(2).path("result").path("thread-count").asInt(0));
            }
        } catch (Exception ignored) {}

        Map<String, Object> host = new LinkedHashMap<>();
        host.put("name", "localhost");
        host.put("host_state", "running");
        host.put("is_domain_controller", false);
        host.put("servers", List.of(serverInfo));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("mode", "STANDALONE");
        result.put("product_name", rootData.path("product-name").asText("unknown"));
        result.put("product_version", rootData.path("product-version").asText("unknown"));
        result.put("hosts", List.of(host));
        result.put("server_groups", List.of());
        result.put("total_hosts", 1);
        result.put("total_servers", 1);
        result.put("running_servers", 1);
        result.put("stopped_servers", 0);
        result.put("management_url", config.mgmtUrl());
        return result;
    }

    private Map<String, Object> buildDomainTopology(JsonNode rootData) throws Exception {
        List<Map<String, Object>> hosts = new ArrayList<>();
        int totalServers = 0;
        int runningServers = 0;
        int stoppedServers = 0;

        List<Map<String, Object>> serverGroups = new ArrayList<>();
        try {
            JsonNode sgResult = client.readChildrenNames(List.of(), "server-group");
            if ("success".equals(sgResult.path("outcome").asText())) {
                for (JsonNode sgName : sgResult.get("result")) {
                    String groupName = sgName.asText();
                    Map<String, Object> group = new LinkedHashMap<>();
                    group.put("name", groupName);
                    try {
                        JsonNode sgDetail = client.readResource(List.of("server-group", groupName), true, false, false);
                        if ("success".equals(sgDetail.path("outcome").asText())) {
                            JsonNode sgData = sgDetail.get("result");
                            group.put("profile", sgData.path("profile").asText("unknown"));
                            group.put("socket_binding_group", sgData.path("socket-binding-group").asText("unknown"));
                            group.put("socket_binding_port_offset", sgData.path("socket-binding-port-offset").asInt(0));
                        }
                    } catch (Exception ignored) {}
                    serverGroups.add(group);
                }
            }
        } catch (Exception ignored) {}

        JsonNode hostResult = client.readChildrenNames(List.of(), "host");
        if (!"success".equals(hostResult.path("outcome").asText())) {
            return Map.of("error", "Cannot read hosts: " + hostResult.path("failure-description").asText());
        }

        for (JsonNode hostNameNode : hostResult.get("result")) {
            String hostName = hostNameNode.asText();
            Map<String, Object> hostInfo = new LinkedHashMap<>();
            hostInfo.put("name", hostName);

            try {
                JsonNode hostDetail = client.readResource(List.of("host", hostName), true, false, false);
                if ("success".equals(hostDetail.path("outcome").asText())) {
                    JsonNode hd = hostDetail.get("result");
                    hostInfo.put("host_state", hd.path("host-state").asText("unknown"));
                    hostInfo.put("running_mode", hd.path("running-mode").asText("unknown"));
                    hostInfo.put("product_name", hd.path("product-name").asText(""));
                    hostInfo.put("product_version", hd.path("product-version").asText(""));
                    hostInfo.put("release_version", hd.path("release-version").asText(""));
                    boolean isDC = "DOMAIN".equalsIgnoreCase(hd.path("process-type").asText())
                            || hd.path("primary").asBoolean(false)
                            || hd.path("master").asBoolean(false)
                            || "master".equalsIgnoreCase(hostName);
                    hostInfo.put("is_domain_controller", isDC);
                    hostInfo.put("directory_grouping", hd.path("directory-grouping").asText("by-server"));
                }
            } catch (Exception e) {
                hostInfo.put("host_state", "unreachable");
                hostInfo.put("error", e.getMessage());
            }

            List<Map<String, Object>> servers = new ArrayList<>();
            try {
                JsonNode scResult = client.readChildrenNames(List.of("host", hostName), "server-config");
                if ("success".equals(scResult.path("outcome").asText())) {
                    for (JsonNode scNameNode : scResult.get("result")) {
                        String serverName = scNameNode.asText();
                        Map<String, Object> serverInfo = new LinkedHashMap<>();
                        serverInfo.put("name", serverName);
                        serverInfo.put("host", hostName);
                        totalServers++;

                        try {
                            JsonNode scDetail = client.readResource(
                                    List.of("host", hostName, "server-config", serverName), true, false, false);
                            if ("success".equals(scDetail.path("outcome").asText())) {
                                JsonNode sc = scDetail.get("result");
                                serverInfo.put("group", sc.path("group").asText("unknown"));
                                serverInfo.put("auto_start", sc.path("auto-start").asBoolean(true));
                                serverInfo.put("status", sc.path("status").asText("STOPPED"));
                                serverInfo.put("socket_binding_port_offset", sc.path("socket-binding-port-offset").asInt(0));
                                serverInfo.put("socket_binding_group", sc.path("socket-binding-group").asText(""));
                            }
                        } catch (Exception ignored) {}

                        String status = (String) serverInfo.getOrDefault("status", "STOPPED");
                        boolean isStarted = "STARTED".equalsIgnoreCase(status);

                        if (isStarted) {
                            runningServers++;
                            try {
                                JsonNode srvDetail = client.readResource(
                                        List.of("host", hostName, "server", serverName), true, false, false);
                                if ("success".equals(srvDetail.path("outcome").asText())) {
                                    JsonNode sd = srvDetail.get("result");
                                    serverInfo.put("server_state", sd.path("server-state").asText("unknown"));
                                    serverInfo.put("suspend_state", sd.path("suspend-state").asText("unknown"));
                                    serverInfo.put("running_mode", sd.path("running-mode").asText("unknown"));
                                    serverInfo.put("profile_name", sd.path("profile-name").asText(""));
                                }
                            } catch (Exception ignored) {
                                serverInfo.put("server_state", "unknown");
                            }

                            try {
                                JsonNode runtime = client.readResource(
                                        List.of("host", hostName, "server", serverName,
                                                "core-service", "platform-mbean", "type", "runtime"),
                                        true, false, false);
                                if ("success".equals(runtime.path("outcome").asText())) {
                                    JsonNode rt = runtime.get("result");
                                    long uptimeMs = rt.path("uptime").asLong(0);
                                    serverInfo.put("uptime_ms", uptimeMs);
                                    serverInfo.put("uptime_human", TimeUtils.msToHuman(uptimeMs));
                                }
                            } catch (Exception ignored) {}

                            try {
                                JsonNode mem = client.readResource(
                                        List.of("host", hostName, "server", serverName,
                                                "core-service", "platform-mbean", "type", "memory"),
                                        true, false, false);
                                if ("success".equals(mem.path("outcome").asText())) {
                                    JsonNode heap = mem.path("result").path("heap-memory-usage");
                                    long used = heap.path("used").asLong(0);
                                    long max = heap.path("max").asLong(1);
                                    serverInfo.put("heap_used_mb", round2(used / 1048576.0));
                                    serverInfo.put("heap_max_mb", round2(max / 1048576.0));
                                    serverInfo.put("heap_used_percent", round2(used * 100.0 / Math.max(max, 1)));
                                }
                            } catch (Exception ignored) {}

                            try {
                                JsonNode thr = client.readResource(
                                        List.of("host", hostName, "server", serverName,
                                                "core-service", "platform-mbean", "type", "threading"),
                                        true, false, false);
                                if ("success".equals(thr.path("outcome").asText())) {
                                    serverInfo.put("thread_count", thr.path("result").path("thread-count").asInt(0));
                                    serverInfo.put("peak_thread_count", thr.path("result").path("peak-thread-count").asInt(0));
                                }
                            } catch (Exception ignored) {}
                        } else {
                            stoppedServers++;
                            serverInfo.put("server_state", "stopped");
                        }

                        servers.add(serverInfo);
                    }
                }
            } catch (Exception e) {
                hostInfo.put("server_error", "Failed to read servers: " + e.getMessage());
            }

            hostInfo.put("servers", servers);
            hostInfo.put("server_count", servers.size());
            hosts.add(hostInfo);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("mode", "DOMAIN");
        result.put("product_name", rootData.path("product-name").asText("unknown"));
        result.put("product_version", rootData.path("product-version").asText("unknown"));
        result.put("hosts", hosts);
        result.put("server_groups", serverGroups);
        result.put("total_hosts", hosts.size());
        result.put("total_servers", totalServers);
        result.put("running_servers", runningServers);
        result.put("stopped_servers", stoppedServers);
        result.put("management_url", config.mgmtUrl());
        return result;
    }

    @GET
    @Path("/domain/server/{host}/{server}/status")
    public Map<String, Object> getServerDetail(@PathParam("host") String hostName,
                                                @PathParam("server") String serverName) {
        LOG.infof("Fetching server detail: host=%s, server=%s", hostName, serverName);
        try {
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("host", hostName);
            detail.put("server", serverName);

            JsonNode rootData = readRootResource();
            String launchType = rootData != null
                    ? rootData.path("launch-type").asText("STANDALONE") : "STANDALONE";
            boolean isDomain = "DOMAIN".equalsIgnoreCase(launchType);

            List<String> serverPrefix = isDomain
                    ? List.of("host", hostName, "server", serverName)
                    : List.of();

            if (isDomain) {
                try {
                    JsonNode sc = client.readResource(
                            List.of("host", hostName, "server-config", serverName), true, false, false);
                    if ("success".equals(sc.path("outcome").asText())) {
                        JsonNode scd = sc.get("result");
                        detail.put("group", scd.path("group").asText("unknown"));
                        detail.put("auto_start", scd.path("auto-start").asBoolean(true));
                        detail.put("status", scd.path("status").asText("STOPPED"));
                        detail.put("socket_binding_port_offset", scd.path("socket-binding-port-offset").asInt(0));
                    }
                } catch (Exception ignored) {}
            }

            try {
                JsonNode srvNode = isDomain
                        ? client.readResource(List.of("host", hostName, "server", serverName), true, false, false)
                        : client.readResource(List.of(), true, false, false);
                if ("success".equals(srvNode.path("outcome").asText())) {
                    JsonNode sd = srvNode.get("result");
                    detail.put("server_state", sd.path("server-state").asText("unknown"));
                    detail.put("suspend_state", sd.path("suspend-state").asText("unknown"));
                    detail.put("running_mode", sd.path("running-mode").asText("unknown"));
                    detail.put("product_name", sd.path("product-name").asText("unknown"));
                    detail.put("product_version", sd.path("product-version").asText("unknown"));
                }
            } catch (Exception e) {
                detail.put("server_state", "stopped");
                detail.put("error", e.getMessage());
                detail.put("status", "ok");
                return detail;
            }

            try {
                List<String> rtAddr = new ArrayList<>(serverPrefix);
                rtAddr.addAll(List.of("core-service", "platform-mbean", "type", "runtime"));
                JsonNode runtime = client.readResource(rtAddr);
                if ("success".equals(runtime.path("outcome").asText())) {
                    JsonNode rt = runtime.get("result");
                    long uptimeMs = rt.path("uptime").asLong(0);
                    detail.put("uptime_ms", uptimeMs);
                    detail.put("uptime_human", TimeUtils.msToHuman(uptimeMs));
                    detail.put("start_time", rt.path("start-time").asLong(0));
                    detail.put("vm_name", rt.path("vm-name").asText("unknown"));
                    detail.put("vm_version", rt.path("vm-version").asText("unknown"));
                    detail.put("vm_vendor", rt.path("vm-vendor").asText("unknown"));
                    List<String> inputArgs = new ArrayList<>();
                    JsonNode argsNode = rt.get("input-arguments");
                    if (argsNode != null && argsNode.isArray()) {
                        for (JsonNode arg : argsNode) inputArgs.add(arg.asText());
                    }
                    detail.put("input_arguments", inputArgs);
                }
            } catch (Exception ignored) {}

            try {
                List<String> memAddr = new ArrayList<>(serverPrefix);
                memAddr.addAll(List.of("core-service", "platform-mbean", "type", "memory"));
                JsonNode mem = client.readResource(memAddr);
                if ("success".equals(mem.path("outcome").asText())) {
                    JsonNode heap = mem.path("result").path("heap-memory-usage");
                    JsonNode nonHeap = mem.path("result").path("non-heap-memory-usage");
                    long used = heap.path("used").asLong(0);
                    long max = heap.path("max").asLong(1);

                    Map<String, Object> heapMap = new LinkedHashMap<>();
                    heapMap.put("used_mb", round2(used / 1048576.0));
                    heapMap.put("max_mb", round2(max / 1048576.0));
                    heapMap.put("committed_mb", round2(heap.path("committed").asLong(0) / 1048576.0));
                    heapMap.put("used_percent", round2(used * 100.0 / Math.max(max, 1)));
                    detail.put("heap", heapMap);

                    Map<String, Object> nhMap = new LinkedHashMap<>();
                    nhMap.put("used_mb", round2(nonHeap.path("used").asLong(0) / 1048576.0));
                    nhMap.put("committed_mb", round2(nonHeap.path("committed").asLong(0) / 1048576.0));
                    detail.put("non_heap", nhMap);
                }
            } catch (Exception ignored) {}

            try {
                List<String> thrAddr = new ArrayList<>(serverPrefix);
                thrAddr.addAll(List.of("core-service", "platform-mbean", "type", "threading"));
                JsonNode thr = client.readResource(thrAddr);
                if ("success".equals(thr.path("outcome").asText())) {
                    JsonNode td = thr.get("result");
                    Map<String, Object> threads = new LinkedHashMap<>();
                    threads.put("thread_count", td.path("thread-count").asInt(0));
                    threads.put("peak_thread_count", td.path("peak-thread-count").asInt(0));
                    threads.put("daemon_thread_count", td.path("daemon-thread-count").asInt(0));
                    threads.put("total_started", td.path("total-started-thread-count").asLong(0));
                    detail.put("threads", threads);
                }
            } catch (Exception ignored) {}

            try {
                List<String> osAddr = new ArrayList<>(serverPrefix);
                osAddr.addAll(List.of("core-service", "platform-mbean", "type", "operating-system"));
                JsonNode osResult = client.readResource(osAddr);
                if ("success".equals(osResult.path("outcome").asText())) {
                    JsonNode od = osResult.get("result");
                    Map<String, Object> os = new LinkedHashMap<>();
                    os.put("name", od.path("name").asText("unknown"));
                    os.put("arch", od.path("arch").asText("unknown"));
                    os.put("available_processors", od.path("available-processors").asInt(0));
                    if (od.has("process-cpu-load")) {
                        os.put("process_cpu_percent", round2(od.path("process-cpu-load").asDouble() * 100));
                    }
                    if (od.has("system-cpu-load")) {
                        os.put("system_cpu_percent", round2(od.path("system-cpu-load").asDouble() * 100));
                    }
                    detail.put("os", os);
                }
            } catch (Exception ignored) {}

            try {
                List<String> dsAddr = new ArrayList<>(serverPrefix);
                dsAddr.addAll(List.of("subsystem", "datasources"));
                JsonNode dsResult = client.readResource(dsAddr, true, false);
                if ("success".equals(dsResult.path("outcome").asText())) {
                    JsonNode dsData = dsResult.get("result");
                    List<Map<String, Object>> sources = new ArrayList<>();
                    for (String dsType : List.of("data-source", "xa-data-source")) {
                        JsonNode dsGroup = dsData.path(dsType);
                        if (dsGroup.isObject()) {
                            Iterator<String> names = dsGroup.fieldNames();
                            while (names.hasNext()) {
                                String dsName = names.next();
                                try {
                                    List<String> dsDetailAddr = new ArrayList<>(serverPrefix);
                                    dsDetailAddr.addAll(List.of("subsystem", "datasources", dsType, dsName));
                                    JsonNode dsDet = client.readResource(dsDetailAddr, true, true);
                                    if ("success".equals(dsDet.path("outcome").asText())) {
                                        JsonNode ds = dsDet.get("result");
                                        JsonNode pool = ds.path("statistics").path("pool");
                                        Map<String, Object> dsInfo = new LinkedHashMap<>();
                                        dsInfo.put("name", dsName);
                                        dsInfo.put("type", dsType.equals("xa-data-source") ? "XA" : "Non-XA");
                                        dsInfo.put("active_count", TimeUtils.safeInt(pool.path("ActiveCount").asText("0")));
                                        dsInfo.put("available_count", TimeUtils.safeInt(pool.path("AvailableCount").asText("0")));
                                        dsInfo.put("in_use_count", TimeUtils.safeInt(pool.path("InUseCount").asText("0")));
                                        dsInfo.put("max_pool_size", ds.path("max-pool-size").asInt(20));
                                        dsInfo.put("enabled", ds.path("enabled").asBoolean(false));
                                        sources.add(dsInfo);
                                    }
                                } catch (Exception ignored) {}
                            }
                        }
                    }
                    detail.put("datasources", sources);
                }
            } catch (Exception ignored) {}

            try {
                List<String> depAddr = isDomain
                        ? List.of("host", hostName, "server", serverName)
                        : List.of();
                JsonNode depRoot = client.readResource(depAddr);
                if ("success".equals(depRoot.path("outcome").asText())) {
                    JsonNode depNode = depRoot.path("result").path("deployment");
                    List<Map<String, Object>> deployments = new ArrayList<>();
                    if (depNode.isObject()) {
                        Iterator<String> depNames = depNode.fieldNames();
                        while (depNames.hasNext()) {
                            String depName = depNames.next();
                            Map<String, Object> dep = new LinkedHashMap<>();
                            dep.put("name", depName);
                            try {
                                List<String> dAddr = new ArrayList<>(serverPrefix);
                                dAddr.addAll(List.of("deployment", depName));
                                JsonNode dResult = client.readResource(dAddr);
                                if ("success".equals(dResult.path("outcome").asText())) {
                                    JsonNode dd = dResult.get("result");
                                    dep.put("runtime_name", dd.path("runtime-name").asText(depName));
                                    dep.put("enabled", dd.path("enabled").asBoolean(false));
                                    dep.put("status", dd.path("status").asText("unknown"));
                                }
                            } catch (Exception ignored) {}
                            deployments.add(dep);
                        }
                    }
                    detail.put("deployments", deployments);
                }
            } catch (Exception ignored) {}

            detail.put("status", "ok");
            return detail;
        } catch (Exception e) {
            return Map.of("error", "Failed to get server detail: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private JsonNode readRootResource() throws Exception {
        // Try without resolve-expressions first (avoids WFLYCTL0374 on some EAP versions)
        JsonNode root = client.readResource(List.of(), true, false, false);
        if ("success".equals(root.path("outcome").asText())) {
            return root.get("result");
        }
        // Fallback: try with resolve-expressions
        root = client.readResource(List.of(), true, false, true);
        if ("success".equals(root.path("outcome").asText())) {
            return root.get("result");
        }
        return null;
    }

    private static double round2(double val) {
        return Math.round(val * 100.0) / 100.0;
    }
}
