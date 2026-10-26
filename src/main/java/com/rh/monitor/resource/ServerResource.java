package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.config.ServerContext;
import com.rh.monitor.util.TimeUtils;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.jboss.logging.Logger;

import java.util.*;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class ServerResource {

    private static final Logger LOG = Logger.getLogger(ServerResource.class);

    @Inject
    JBossManagementClient client;

    @Inject
    ServerContext serverContext;

    @GET
    @Path("/server-status")
    public Map<String, Object> getServerStatus() {
        LOG.info("Fetching server status");
        try {
            List<String> batchCmds = List.of(
                    client.buildReadResourceCommand(List.of(), true, false),
                    client.buildReadResourceCommand(
                            List.of("core-service", "platform-mbean", "type", "runtime"), true, false));
            List<JsonNode> results = client.executeBatch(batchCmds);
            if (results.isEmpty() || !"success".equals(results.get(0).path("outcome").asText())) {
                String err = results.isEmpty() ? "No result" : results.get(0).path("failure-description").asText("Unknown error");
                return Map.of("error", err);
            }

            JsonNode data = results.get(0).get("result");
            Map<String, Object> server = new LinkedHashMap<>();
            server.put("product_name", data.path("product-name").asText("unknown"));
            server.put("product_version", data.path("product-version").asText("unknown"));
            server.put("release_version", data.path("release-version").asText("unknown"));
            server.put("release_codename", data.path("release-codename").asText("unknown"));
            server.put("launch_type", data.path("launch-type").asText("unknown"));
            server.put("process_type", data.path("process-type").asText("unknown"));
            server.put("management_major_version", data.path("management-major-version").asInt(0));
            server.put("management_minor_version", data.path("management-minor-version").asInt(0));

            boolean isDomain = "DOMAIN".equals(data.path("launch-type").asText(""));
            boolean noServerCtx = serverContext == null || !serverContext.hasContext();

            if (isDomain && noServerCtx) {
                server.put("server_state", "running");
                server.put("running_mode", "DOMAIN");
                server.put("suspend_state", "RUNNING");

                List<String[]> domainServers = client.getRunningDomainServers();
                if (domainServers != null && !domainServers.isEmpty()) {
                    server.put("domain_server_count", domainServers.size());
                    List<String> rtCmds = new ArrayList<>();
                    for (String[] ds : domainServers) {
                        List<String> rtAddr = List.of("host", ds[0], "server", ds[1],
                                "core-service", "platform-mbean", "type", "runtime");
                        rtCmds.add(client.buildReadResourceCommand(rtAddr, true, false));
                    }
                    List<JsonNode> rtResults = client.executeBatch(rtCmds);

                    List<Map<String, Object>> perServer = new ArrayList<>();
                    long maxUptime = 0;
                    for (int i = 0; i < domainServers.size(); i++) {
                        String[] ds = domainServers.get(i);
                        Map<String, Object> srvInfo = new LinkedHashMap<>();
                        srvInfo.put("server_name", ds[0] + "/" + ds[1]);
                        srvInfo.put("state", "running");
                        if (i < rtResults.size() && "success".equals(rtResults.get(i).path("outcome").asText())) {
                            JsonNode rtData = rtResults.get(i).get("result");
                            long upMs = rtData.path("uptime").asLong(0);
                            srvInfo.put("uptime_ms", upMs);
                            srvInfo.put("uptime_human", TimeUtils.msToHuman(upMs));
                            if (upMs > maxUptime) {
                                maxUptime = upMs;
                                server.put("uptime_ms", upMs);
                                server.put("uptime_human", TimeUtils.msToHuman(upMs));
                                server.put("start_time", rtData.path("start-time").asLong(0));
                                server.put("vm_name", rtData.path("vm-name").asText("unknown"));
                                server.put("vm_version", rtData.path("vm-version").asText("unknown"));
                                server.put("vm_vendor", rtData.path("vm-vendor").asText("unknown"));
                                server.put("spec_version", rtData.path("spec-version").asText("unknown"));
                            }
                        }
                        perServer.add(srvInfo);
                    }
                    server.put("per_server", perServer);
                }
            } else {
                server.put("server_state", data.path("server-state").asText("unknown"));
                server.put("running_mode", data.path("running-mode").asText("unknown"));
                server.put("suspend_state", data.path("suspend-state").asText("unknown"));

                if (results.size() > 1 && "success".equals(results.get(1).path("outcome").asText())) {
                    JsonNode rt = results.get(1).get("result");
                    long uptimeMs = rt.path("uptime").asLong(0);
                    server.put("uptime_ms", uptimeMs);
                    server.put("uptime_human", TimeUtils.msToHuman(uptimeMs));
                    server.put("start_time", rt.path("start-time").asLong(0));
                    server.put("vm_name", rt.path("vm-name").asText("unknown"));
                    server.put("vm_version", rt.path("vm-version").asText("unknown"));
                    server.put("vm_vendor", rt.path("vm-vendor").asText("unknown"));
                    server.put("spec_version", rt.path("spec-version").asText("unknown"));

                    List<String> inputArgs = new ArrayList<>();
                    JsonNode argsNode = rt.get("input-arguments");
                    if (argsNode != null && argsNode.isArray()) {
                        for (JsonNode arg : argsNode) inputArgs.add(arg.asText());
                    }
                    server.put("input_arguments", inputArgs);
                }
            }

            return Map.of("status", "ok", "server", server);
        } catch (Exception e) {
            return Map.of("error", "Failed to get server status: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @GET
    @Path("/system")
    public Map<String, Object> getSystemInfo() {
        LOG.info("Fetching system info");
        try {
            JsonNode osResult = null;

            if (serverContext == null || !serverContext.hasContext()) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (domainServers != null && !domainServers.isEmpty()) {
                    String[] ds = domainServers.get(0);
                    List<String> addr = List.of("host", ds[0], "server", ds[1],
                            "core-service", "platform-mbean", "type", "operating-system");
                    osResult = client.readResourceDirect(addr, true, false);
                }
            }

            if (osResult == null) {
                osResult = client.readResource(
                        List.of("core-service", "platform-mbean", "type", "operating-system"));
            }

            Map<String, Object> systemInfo = new LinkedHashMap<>();
            Map<String, Object> os;
            if ("success".equals(osResult.path("outcome").asText())) {
                os = parseOsData(osResult.get("result"));
            } else {
                os = new LinkedHashMap<>();
            }

            supplementWithLocalOs(os);
            systemInfo.put("os", os);

            return Map.of("status", "ok", "system", systemInfo);
        } catch (Exception e) {
            return Map.of("error", "Failed to get system info: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void supplementWithLocalOs(Map<String, Object> os) {
        try {
            java.lang.management.OperatingSystemMXBean osMxBean =
                    java.lang.management.ManagementFactory.getOperatingSystemMXBean();

            if (!os.containsKey("name") || "unknown".equals(os.get("name"))) {
                os.put("name", osMxBean.getName());
            }
            if (!os.containsKey("version") || "unknown".equals(os.get("version"))) {
                os.put("version", osMxBean.getVersion());
            }
            if (!os.containsKey("arch") || "unknown".equals(os.get("arch"))) {
                os.put("arch", osMxBean.getArch());
            }
            if (!os.containsKey("available_processors") || Integer.valueOf(0).equals(os.get("available_processors"))) {
                os.put("available_processors", osMxBean.getAvailableProcessors());
            }
            if (!os.containsKey("system_load_average")) {
                os.put("system_load_average", osMxBean.getSystemLoadAverage());
            }

            if (osMxBean instanceof com.sun.management.OperatingSystemMXBean sunOs) {
                if (!os.containsKey("process_cpu_load_percent")) {
                    os.put("process_cpu_load_percent", Math.round(sunOs.getProcessCpuLoad() * 10000.0) / 100.0);
                }
                if (!os.containsKey("system_cpu_load_percent")) {
                    os.put("system_cpu_load_percent", Math.round(sunOs.getCpuLoad() * 10000.0) / 100.0);
                }
                if (!os.containsKey("total_physical_memory_mb")) {
                    os.put("total_physical_memory_mb", Math.round(sunOs.getTotalMemorySize() / 1048576.0 * 100.0) / 100.0);
                }
                if (!os.containsKey("free_physical_memory_mb")) {
                    os.put("free_physical_memory_mb", Math.round(sunOs.getFreeMemorySize() / 1048576.0 * 100.0) / 100.0);
                }
                if (!os.containsKey("total_swap_mb")) {
                    os.put("total_swap_mb", Math.round(sunOs.getTotalSwapSpaceSize() / 1048576.0 * 100.0) / 100.0);
                }
                if (!os.containsKey("free_swap_mb")) {
                    os.put("free_swap_mb", Math.round(sunOs.getFreeSwapSpaceSize() / 1048576.0 * 100.0) / 100.0);
                }
            }
        } catch (Exception e) {
            LOG.debugf("Failed to supplement OS data from local JVM: %s", e.getMessage());
        }
    }

    private Map<String, Object> parseOsData(JsonNode osData) {
        Map<String, Object> os = new LinkedHashMap<>();
        os.put("name", osData.path("name").asText("unknown"));
        os.put("version", osData.path("version").asText("unknown"));
        os.put("arch", osData.path("arch").asText("unknown"));
        os.put("available_processors", osData.path("available-processors").asInt(0));
        os.put("system_load_average", osData.path("system-load-average").asDouble(-1));

        if (osData.has("process-cpu-load")) {
            os.put("process_cpu_load_percent", Math.round(osData.path("process-cpu-load").asDouble() * 10000.0) / 100.0);
        }
        if (osData.has("system-cpu-load")) {
            os.put("system_cpu_load_percent", Math.round(osData.path("system-cpu-load").asDouble() * 10000.0) / 100.0);
        }
        if (osData.has("total-physical-memory-size")) {
            os.put("total_physical_memory_mb", Math.round(osData.path("total-physical-memory-size").asLong() / 1048576.0 * 100.0) / 100.0);
        }
        if (osData.has("free-physical-memory-size")) {
            os.put("free_physical_memory_mb", Math.round(osData.path("free-physical-memory-size").asLong() / 1048576.0 * 100.0) / 100.0);
        }
        if (osData.has("total-swap-space-size")) {
            os.put("total_swap_mb", Math.round(osData.path("total-swap-space-size").asLong() / 1048576.0 * 100.0) / 100.0);
        }
        if (osData.has("free-swap-space-size")) {
            os.put("free_swap_mb", Math.round(osData.path("free-swap-space-size").asLong() / 1048576.0 * 100.0) / 100.0);
        }
        if (osData.has("committed-virtual-memory-size")) {
            os.put("committed_virtual_memory_mb", Math.round(osData.path("committed-virtual-memory-size").asLong() / 1048576.0 * 100.0) / 100.0);
        }
        return os;
    }

    @GET
    @Path("/health")
    public Map<String, Object> getHealthReport() {
        LOG.info("Generating health report");
        try {
            Map<String, Object> components = new LinkedHashMap<>();
            String overallHealth = "HEALTHY";

            boolean isDomain = false;
            List<String[]> domainServers = null;
            boolean noServerCtx = serverContext == null || !serverContext.hasContext();

            if (noServerCtx) {
                domainServers = client.getRunningDomainServers();
                isDomain = domainServers != null && !domainServers.isEmpty();
            }

            if (isDomain) {
                overallHealth = buildDomainHealth(domainServers, components);
            } else {
                overallHealth = buildStandaloneHealth(components);
            }

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "ok");
            response.put("overall_health", overallHealth);
            response.put("components", components);
            return response;
        } catch (Exception e) {
            return Map.of("error", "Failed to generate health report: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private String buildStandaloneHealth(Map<String, Object> components) {
        String overallHealth = "HEALTHY";
        try {
            List<String> cmds = new ArrayList<>();
            cmds.add(client.buildReadResourceCommand(List.of(), true, false));
            cmds.add(client.buildReadResourceCommand(
                    List.of("core-service", "platform-mbean", "type", "runtime"), true, false));
            cmds.add(client.buildReadResourceCommand(
                    List.of("core-service", "platform-mbean", "type", "memory"), true, false));
            cmds.add(client.buildReadResourceCommand(
                    List.of("core-service", "platform-mbean", "type", "threading"), true, false));
            cmds.add(client.buildOperationCommand(
                    List.of("core-service", "platform-mbean", "type", "threading"), "find-deadlocked-threads"));
            cmds.add(client.buildReadResourceCommand(
                    List.of("subsystem", "datasources"), true, false));
            cmds.add(client.buildReadResourceCommand(
                    List.of("subsystem", "transactions"), true, false));

            List<JsonNode> results = client.executeBatch(cmds);

            // 0: root resource (server state)
            if (results.size() > 0) {
                JsonNode rootResult = results.get(0);
                if ("success".equals(rootResult.path("outcome").asText())) {
                    JsonNode data = rootResult.get("result");
                    String state = data.path("server-state").asText("unknown");
                    String srvHealth = "running".equals(state) ? "HEALTHY" : "CRITICAL";
                    Map<String, Object> srvComp = new LinkedHashMap<>();
                    srvComp.put("state", state);
                    srvComp.put("health", srvHealth);
                    if ("CRITICAL".equals(srvHealth)) overallHealth = "CRITICAL";

                    // 1: runtime (uptime)
                    if (results.size() > 1 && "success".equals(results.get(1).path("outcome").asText())) {
                        JsonNode rt = results.get(1).get("result");
                        long uptimeMs = rt.path("uptime").asLong(0);
                        srvComp.put("uptime", TimeUtils.msToHuman(uptimeMs));
                    }
                    components.put("server", srvComp);
                }
            }

            // 2: memory
            if (results.size() > 2) {
                try {
                    JsonNode memResult = results.get(2);
                    if ("success".equals(memResult.path("outcome").asText())) {
                        JsonNode heap = memResult.path("result").path("heap-memory-usage");
                        long totalUsed = heap.path("used").asLong(0);
                        long totalMax = heap.path("max").asLong(1);
                        double usedMb = Math.round(totalUsed / 1048576.0 * 100.0) / 100.0;
                        double maxMb = Math.round(totalMax / 1048576.0 * 100.0) / 100.0;
                        double usedPct = Math.round(totalUsed * 10000.0 / Math.max(totalMax, 1)) / 100.0;
                        String memHealth = usedPct > 90 ? "CRITICAL" : usedPct > 75 ? "WARNING" : usedPct > 60 ? "MODERATE" : "HEALTHY";
                        Map<String, Object> memComp = new LinkedHashMap<>();
                        memComp.put("heap_used_mb", usedMb);
                        memComp.put("heap_max_mb", maxMb);
                        memComp.put("heap_used_percent", usedPct);
                        memComp.put("health", memHealth);
                        components.put("memory", memComp);
                        if ("CRITICAL".equals(memHealth)) overallHealth = "CRITICAL";
                        else if ("WARNING".equals(memHealth) && "HEALTHY".equals(overallHealth)) overallHealth = "WARNING";
                    }
                } catch (Exception e) {
                    components.put("memory", Map.of("error", e.getMessage(), "health", "UNKNOWN"));
                }
            }

            // 3: threading, 4: deadlocks
            if (results.size() > 3) {
                try {
                    JsonNode thrResult = results.get(3);
                    int threadCount = 0, peak = 0;
                    boolean hasDeadlocks = false;
                    if ("success".equals(thrResult.path("outcome").asText())) {
                        threadCount = thrResult.path("result").path("thread-count").asInt(0);
                        peak = thrResult.path("result").path("peak-thread-count").asInt(0);
                    }
                    if (results.size() > 4 && "success".equals(results.get(4).path("outcome").asText())) {
                        JsonNode dl = results.get(4).get("result");
                        hasDeadlocks = dl != null && !dl.isNull() && dl.isArray() && dl.size() > 0;
                    }
                    String thrHealth = threadCount > 500 ? "CRITICAL" : threadCount > 300 ? "WARNING" : threadCount > 200 ? "MODERATE" : "HEALTHY";
                    if (hasDeadlocks) thrHealth = "CRITICAL";
                    Map<String, Object> thrComp = new LinkedHashMap<>();
                    thrComp.put("thread_count", threadCount);
                    thrComp.put("peak_thread_count", peak);
                    thrComp.put("has_deadlocks", hasDeadlocks);
                    thrComp.put("health", thrHealth);
                    components.put("threads", thrComp);
                    if ("CRITICAL".equals(thrHealth)) overallHealth = "CRITICAL";
                    else if ("WARNING".equals(thrHealth) && !"CRITICAL".equals(overallHealth)) overallHealth = "WARNING";
                } catch (Exception e) {
                    components.put("threads", Map.of("error", e.getMessage(), "health", "UNKNOWN"));
                }
            }

            // 5: datasources
            if (results.size() > 5) {
                try {
                    overallHealth = processDatasourceHealth(results.get(5), List.of(), false, components, overallHealth);
                } catch (Exception e) {
                    components.put("datasources", Map.of("error", e.getMessage(), "health", "UNKNOWN"));
                }
            }

            // 6: transactions
            if (results.size() > 6) {
                try {
                    overallHealth = processTransactionHealth(results.get(6), components, overallHealth);
                } catch (Exception e) {
                    components.put("transactions", Map.of("error", e.getMessage(), "health", "UNKNOWN"));
                }
            }
        } catch (Exception e) {
            LOG.errorf(e, "Health batch failed");
        }
        return overallHealth;
    }

    private String buildDomainHealth(List<String[]> domainServers, Map<String, Object> components) {
        String overallHealth = "HEALTHY";

        // Server component
        Map<String, Object> srvComp = new LinkedHashMap<>();
        srvComp.put("state", "running");
        srvComp.put("health", "HEALTHY");
        srvComp.put("domain_server_count", domainServers.size());
        components.put("server", srvComp);

        long totalUsed = 0, totalMax = 0;
        int totalThreadCount = 0, totalPeak = 0;
        boolean hasDeadlocks = false;
        int committed = 0, aborted = 0, timedOut = 0;
        boolean hasTxData = false;

        // Fan the per-server health batches out across the CLI connection pool instead of
        // running them serially. executeBatch borrows its own pooled connection and builds
        // commands from explicit host/server addresses (no ServerContext), so it is safe
        // off the request thread. Results are merged sequentially below.
        List<List<JsonNode>> perServerResults = domainServers.parallelStream().map(ds -> {
            try {
                List<String> prefix = List.of("host", ds[0], "server", ds[1]);
                List<String> cmds = new ArrayList<>();
                List<String> rtAddr = new ArrayList<>(prefix);
                rtAddr.addAll(List.of("core-service", "platform-mbean", "type", "runtime"));
                cmds.add(client.buildReadResourceCommand(rtAddr, true, false));

                List<String> memAddr = new ArrayList<>(prefix);
                memAddr.addAll(List.of("core-service", "platform-mbean", "type", "memory"));
                cmds.add(client.buildReadResourceCommand(memAddr, true, false));

                List<String> thrAddr = new ArrayList<>(prefix);
                thrAddr.addAll(List.of("core-service", "platform-mbean", "type", "threading"));
                cmds.add(client.buildReadResourceCommand(thrAddr, true, false));
                cmds.add(client.buildOperationCommand(thrAddr, "find-deadlocked-threads"));

                List<String> txAddr = new ArrayList<>(prefix);
                txAddr.addAll(List.of("subsystem", "transactions"));
                cmds.add(client.buildReadResourceCommand(txAddr, true, false));

                return client.executeBatch(cmds);
            } catch (Exception e) {
                return java.util.Collections.<JsonNode>emptyList();
            }
        }).toList();

        for (List<JsonNode> results : perServerResults) {
            // runtime
            if (results.size() > 0 && "success".equals(results.get(0).path("outcome").asText())) {
                JsonNode rt = results.get(0).get("result");
                long upMs = rt.path("uptime").asLong(0);
                srvComp.put("uptime", TimeUtils.msToHuman(upMs));
            }
            // memory
            if (results.size() > 1 && "success".equals(results.get(1).path("outcome").asText())) {
                JsonNode heap = results.get(1).path("result").path("heap-memory-usage");
                totalUsed += heap.path("used").asLong(0);
                totalMax += heap.path("max").asLong(0);
            }
            // threading
            if (results.size() > 2 && "success".equals(results.get(2).path("outcome").asText())) {
                totalThreadCount += results.get(2).path("result").path("thread-count").asInt(0);
                totalPeak += results.get(2).path("result").path("peak-thread-count").asInt(0);
            }
            // deadlocks
            if (results.size() > 3 && "success".equals(results.get(3).path("outcome").asText())) {
                JsonNode dl = results.get(3).get("result");
                if (dl != null && !dl.isNull() && dl.isArray() && dl.size() > 0) hasDeadlocks = true;
            }
            // transactions
            if (results.size() > 4 && "success".equals(results.get(4).path("outcome").asText())) {
                JsonNode txData = results.get(4).get("result");
                committed += txData.path("number-of-committed-transactions").asInt(0);
                aborted += txData.path("number-of-aborted-transactions").asInt(0);
                timedOut += txData.path("number-of-timed-out-transactions").asInt(0);
                hasTxData = true;
            }
        }

        // Memory component
        if (totalMax > 0) {
            double usedMb = Math.round(totalUsed / 1048576.0 * 100.0) / 100.0;
            double maxMb = Math.round(totalMax / 1048576.0 * 100.0) / 100.0;
            double usedPct = Math.round(totalUsed * 10000.0 / Math.max(totalMax, 1)) / 100.0;
            String memHealth = usedPct > 90 ? "CRITICAL" : usedPct > 75 ? "WARNING" : usedPct > 60 ? "MODERATE" : "HEALTHY";
            Map<String, Object> memComp = new LinkedHashMap<>();
            memComp.put("heap_used_mb", usedMb);
            memComp.put("heap_max_mb", maxMb);
            memComp.put("heap_used_percent", usedPct);
            memComp.put("health", memHealth);
            components.put("memory", memComp);
            if ("CRITICAL".equals(memHealth)) overallHealth = "CRITICAL";
            else if ("WARNING".equals(memHealth) && "HEALTHY".equals(overallHealth)) overallHealth = "WARNING";
        }

        // Threads component
        int serverCount = domainServers.size();
        int thrPerServer = serverCount > 0 ? totalThreadCount / serverCount : totalThreadCount;
        String thrHealth = thrPerServer > 500 ? "CRITICAL" : thrPerServer > 300 ? "WARNING" : thrPerServer > 200 ? "MODERATE" : "HEALTHY";
        if (hasDeadlocks) thrHealth = "CRITICAL";
        Map<String, Object> thrComp = new LinkedHashMap<>();
        thrComp.put("thread_count", totalThreadCount);
        thrComp.put("peak_thread_count", totalPeak);
        thrComp.put("has_deadlocks", hasDeadlocks);
        thrComp.put("health", thrHealth);
        components.put("threads", thrComp);
        if ("CRITICAL".equals(thrHealth)) overallHealth = "CRITICAL";
        else if ("WARNING".equals(thrHealth) && !"CRITICAL".equals(overallHealth)) overallHealth = "WARNING";

        // Datasources (needs separate call per domain server to list names) — fanned out
        // across the CLI connection pool, then merged sequentially.
        try {
            List<Map<String, Object>> sources = new ArrayList<>();
            String worstDs = "HEALTHY";
            List<List<Map<String, Object>>> perServerDs = domainServers.parallelStream().map(ds -> {
                List<Map<String, Object>> srv = new ArrayList<>();
                List<String> prefix = List.of("host", ds[0], "server", ds[1]);
                try {
                    List<String> dsAddr = new ArrayList<>(prefix);
                    dsAddr.addAll(List.of("subsystem", "datasources"));
                    JsonNode dsResult = client.readResourceDirect(dsAddr, true, false);
                    if (!"success".equals(dsResult.path("outcome").asText())) return srv;

                    List<String> detailCmds = new ArrayList<>();
                    List<String[]> dsKeys = new ArrayList<>();
                    for (String dsType : List.of("data-source", "xa-data-source")) {
                        JsonNode dsGroup = dsResult.path("result").path(dsType);
                        if (!dsGroup.isObject()) continue;
                        Iterator<String> names = dsGroup.fieldNames();
                        while (names.hasNext()) {
                            String dsName = names.next();
                            List<String> detailAddr = new ArrayList<>(prefix);
                            detailAddr.addAll(List.of("subsystem", "datasources", dsType, dsName));
                            detailCmds.add(client.buildReadResourceCommand(detailAddr, true, true));
                            dsKeys.add(new String[]{dsName, dsType});
                        }
                    }
                    if (!detailCmds.isEmpty()) {
                        List<JsonNode> details = client.executeBatch(detailCmds);
                        for (int i = 0; i < Math.min(details.size(), dsKeys.size()); i++) {
                            if ("success".equals(details.get(i).path("outcome").asText())) {
                                JsonNode dsd = details.get(i).get("result");
                                JsonNode pool = dsd.path("statistics").path("pool");
                                int active = TimeUtils.safeInt(pool.path("ActiveCount").asText("0"));
                                int maxPool = dsd.path("max-pool-size").asInt(20);
                                double usagePct = maxPool > 0 ? (active * 100.0 / maxPool) : 0;
                                String dsHealth = usagePct > 90 ? "CRITICAL" : usagePct > 75 ? "WARNING" : usagePct > 50 ? "MODERATE" : "HEALTHY";
                                srv.add(Map.of("name", dsKeys.get(i)[0], "health", dsHealth, "active", active));
                            }
                        }
                    }
                } catch (Exception ignored) {}
                return srv;
            }).toList();
            for (List<Map<String, Object>> srv : perServerDs) {
                for (Map<String, Object> s : srv) {
                    sources.add(s);
                    String dsHealth = (String) s.get("health");
                    if ("CRITICAL".equals(dsHealth)) worstDs = "CRITICAL";
                    else if ("WARNING".equals(dsHealth) && !"CRITICAL".equals(worstDs)) worstDs = "WARNING";
                }
            }
            Map<String, Object> dsComp = new LinkedHashMap<>();
            dsComp.put("total", sources.size());
            dsComp.put("health", worstDs);
            dsComp.put("sources", sources);
            components.put("datasources", dsComp);
            if ("CRITICAL".equals(worstDs)) overallHealth = "CRITICAL";
            else if ("WARNING".equals(worstDs) && "HEALTHY".equals(overallHealth)) overallHealth = "WARNING";
        } catch (Exception e) {
            components.put("datasources", Map.of("error", e.getMessage(), "health", "UNKNOWN"));
        }

        // Transactions component
        if (hasTxData) {
            int total = committed + aborted;
            double abortRate = total > 0 ? Math.round(aborted * 10000.0 / total) / 100.0 : 0;
            String txHealth = (abortRate > 20 || timedOut > 10) ? "CRITICAL"
                    : (abortRate > 10 || timedOut > 5) ? "WARNING"
                    : abortRate > 5 ? "MODERATE" : "HEALTHY";
            Map<String, Object> txComp = new LinkedHashMap<>();
            txComp.put("committed", committed);
            txComp.put("aborted", aborted);
            txComp.put("timed_out", timedOut);
            txComp.put("abort_rate_percent", abortRate);
            txComp.put("health", txHealth);
            components.put("transactions", txComp);
            if ("CRITICAL".equals(txHealth)) overallHealth = "CRITICAL";
            else if ("WARNING".equals(txHealth) && "HEALTHY".equals(overallHealth)) overallHealth = "WARNING";
        }

        return overallHealth;
    }

    private String processDatasourceHealth(JsonNode dsResult, List<String> prefix, boolean useDirect,
                                            Map<String, Object> components, String overallHealth) throws Exception {
        List<Map<String, Object>> sources = new ArrayList<>();
        String worstDs = "HEALTHY";

        if ("success".equals(dsResult.path("outcome").asText())) {
            JsonNode dsData = dsResult.get("result");
            List<String> detailCmds = new ArrayList<>();
            List<String[]> dsKeys = new ArrayList<>();

            for (String dsType : List.of("data-source", "xa-data-source")) {
                JsonNode dsGroup = dsData.path(dsType);
                if (dsGroup.isObject()) {
                    Iterator<String> names = dsGroup.fieldNames();
                    while (names.hasNext()) {
                        String dsName = names.next();
                        List<String> addr = new ArrayList<>(prefix);
                        addr.addAll(List.of("subsystem", "datasources", dsType, dsName));
                        detailCmds.add(client.buildReadResourceCommand(addr, true, true));
                        dsKeys.add(new String[]{dsName, dsType});
                    }
                }
            }

            if (!detailCmds.isEmpty()) {
                List<JsonNode> details = client.executeBatch(detailCmds);
                for (int i = 0; i < Math.min(details.size(), dsKeys.size()); i++) {
                    if ("success".equals(details.get(i).path("outcome").asText())) {
                        JsonNode dsd = details.get(i).get("result");
                        JsonNode pool = dsd.path("statistics").path("pool");
                        int active = TimeUtils.safeInt(pool.path("ActiveCount").asText("0"));
                        int maxPool = dsd.path("max-pool-size").asInt(20);
                        double usagePct = maxPool > 0 ? (active * 100.0 / maxPool) : 0;
                        String dsHealth = usagePct > 90 ? "CRITICAL" : usagePct > 75 ? "WARNING" : usagePct > 50 ? "MODERATE" : "HEALTHY";
                        sources.add(Map.of("name", dsKeys.get(i)[0], "health", dsHealth, "active", active));
                        if ("CRITICAL".equals(dsHealth)) worstDs = "CRITICAL";
                        else if ("WARNING".equals(dsHealth) && !"CRITICAL".equals(worstDs)) worstDs = "WARNING";
                    }
                }
            }
        }

        Map<String, Object> dsComp = new LinkedHashMap<>();
        dsComp.put("total", sources.size());
        dsComp.put("health", worstDs);
        dsComp.put("sources", sources);
        components.put("datasources", dsComp);
        if ("CRITICAL".equals(worstDs)) overallHealth = "CRITICAL";
        else if ("WARNING".equals(worstDs) && "HEALTHY".equals(overallHealth)) overallHealth = "WARNING";
        return overallHealth;
    }

    private String processTransactionHealth(JsonNode txResult, Map<String, Object> components, String overallHealth) {
        if ("success".equals(txResult.path("outcome").asText())) {
            JsonNode txData = txResult.get("result");
            int committed = txData.path("number-of-committed-transactions").asInt(0);
            int aborted = txData.path("number-of-aborted-transactions").asInt(0);
            int timedOut = txData.path("number-of-timed-out-transactions").asInt(0);
            int total = committed + aborted;
            double abortRate = total > 0 ? Math.round(aborted * 10000.0 / total) / 100.0 : 0;
            String txHealth = (abortRate > 20 || timedOut > 10) ? "CRITICAL"
                    : (abortRate > 10 || timedOut > 5) ? "WARNING"
                    : abortRate > 5 ? "MODERATE" : "HEALTHY";
            Map<String, Object> txComp = new LinkedHashMap<>();
            txComp.put("committed", committed);
            txComp.put("aborted", aborted);
            txComp.put("timed_out", timedOut);
            txComp.put("abort_rate_percent", abortRate);
            txComp.put("health", txHealth);
            components.put("transactions", txComp);
            if ("CRITICAL".equals(txHealth)) overallHealth = "CRITICAL";
            else if ("WARNING".equals(txHealth) && "HEALTHY".equals(overallHealth)) overallHealth = "WARNING";
        }
        return overallHealth;
    }
}
