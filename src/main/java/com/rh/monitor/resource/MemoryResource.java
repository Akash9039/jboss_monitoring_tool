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
public class MemoryResource {

    private static final Logger LOG = Logger.getLogger(MemoryResource.class);

    @Inject
    JBossManagementClient client;

    @Inject
    ServerContext serverContext;

    @GET
    @Path("/memory")
    public Map<String, Object> getMemoryStats() {
        LOG.info("Fetching memory statistics");
        try {
            if (serverContext == null || !serverContext.hasContext()) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (domainServers != null && !domainServers.isEmpty()) {
                    return getDomainMemoryStats(domainServers);
                }
            }

            JsonNode result = client.readResource(
                    List.of("core-service", "platform-mbean", "type", "memory"));
            if (!"success".equals(result.path("outcome").asText())) {
                return Map.of("error", result.path("failure-description").asText("Unknown error"));
            }

            return Map.of("status", "ok", "memory", parseMemoryData(result.get("result")));
        } catch (Exception e) {
            return Map.of("error", "Failed to get memory stats: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private Map<String, Object> getDomainMemoryStats(List<String[]> domainServers) throws Exception {
        long heapInit = 0, heapUsed = 0, heapCommitted = 0, heapMax = 0;
        long nhInit = 0, nhUsed = 0, nhCommitted = 0, nhMax = 0;
        boolean nhMaxSet = false;
        int pendingFin = 0;
        List<Map<String, Object>> perServer = new ArrayList<>();

        // One wildcard read returns every server's memory mbean, replacing the per-server loop.
        Map<String, JsonNode> byServer = client.readWildcardAcrossServers(
                List.of("core-service", "platform-mbean", "type", "memory"), true, false);
        for (Map.Entry<String, JsonNode> entry : byServer.entrySet()) {
            String serverLabel = entry.getKey();
            try {
                JsonNode data = entry.getValue().get("result");
                JsonNode heap = data.path("heap-memory-usage");
                JsonNode nonHeap = data.path("non-heap-memory-usage");

                heapInit += heap.path("init").asLong(0);
                heapUsed += heap.path("used").asLong(0);
                heapCommitted += heap.path("committed").asLong(0);
                heapMax += heap.path("max").asLong(0);
                nhInit += nonHeap.path("init").asLong(0);
                nhUsed += nonHeap.path("used").asLong(0);
                nhCommitted += nonHeap.path("committed").asLong(0);
                long srvNhMax = nonHeap.path("max").asLong(-1);
                if (srvNhMax > 0) { nhMax += srvNhMax; nhMaxSet = true; }
                pendingFin += data.path("object-pending-finalization-count").asInt(0);

                Map<String, Object> srv = parseMemoryData(data);
                srv.put("server_name", serverLabel);
                perServer.add(srv);
            } catch (Exception e) {
                LOG.debugf("Memory stats failed for %s: %s", serverLabel, e.getMessage());
            }
        }

        double heapUsedPct = Math.round(heapUsed * 10000.0 / Math.max(heapMax, 1)) / 100.0;
        String health;
        if (heapUsedPct > 90) health = "CRITICAL";
        else if (heapUsedPct > 75) health = "WARNING";
        else if (heapUsedPct > 60) health = "MODERATE";
        else health = "HEALTHY";

        Map<String, Object> heapMap = new LinkedHashMap<>();
        heapMap.put("init_mb", round2(heapInit / 1048576.0));
        heapMap.put("used_mb", round2(heapUsed / 1048576.0));
        heapMap.put("committed_mb", round2(heapCommitted / 1048576.0));
        heapMap.put("max_mb", round2(heapMax / 1048576.0));
        heapMap.put("used_percent", heapUsedPct);

        Map<String, Object> nonHeapMap = new LinkedHashMap<>();
        nonHeapMap.put("init_mb", round2(nhInit / 1048576.0));
        nonHeapMap.put("used_mb", round2(nhUsed / 1048576.0));
        nonHeapMap.put("committed_mb", round2(nhCommitted / 1048576.0));
        nonHeapMap.put("max_mb", nhMaxSet ? round2(nhMax / 1048576.0) : -1);

        Map<String, Object> mem = new LinkedHashMap<>();
        mem.put("heap", heapMap);
        mem.put("non_heap", nonHeapMap);
        mem.put("object_pending_finalization", pendingFin);
        mem.put("health", health);
        mem.put("per_server", perServer);

        return Map.of("status", "ok", "memory", mem);
    }

    private Map<String, Object> parseMemoryData(JsonNode data) {
        JsonNode heap = data.path("heap-memory-usage");
        JsonNode nonHeap = data.path("non-heap-memory-usage");

        long heapUsed = heap.path("used").asLong(0);
        long heapMax = heap.path("max").asLong(1);
        double heapUsedPct = Math.round(heapUsed * 10000.0 / Math.max(heapMax, 1)) / 100.0;

        String health;
        if (heapUsedPct > 90) health = "CRITICAL";
        else if (heapUsedPct > 75) health = "WARNING";
        else if (heapUsedPct > 60) health = "MODERATE";
        else health = "HEALTHY";

        Map<String, Object> heapMap = new LinkedHashMap<>();
        heapMap.put("init_mb", round2(heap.path("init").asLong(0) / 1048576.0));
        heapMap.put("used_mb", round2(heapUsed / 1048576.0));
        heapMap.put("committed_mb", round2(heap.path("committed").asLong(0) / 1048576.0));
        heapMap.put("max_mb", round2(heapMax / 1048576.0));
        heapMap.put("used_percent", heapUsedPct);

        long nhMax = nonHeap.path("max").asLong(-1);
        Map<String, Object> nonHeapMap = new LinkedHashMap<>();
        nonHeapMap.put("init_mb", round2(nonHeap.path("init").asLong(0) / 1048576.0));
        nonHeapMap.put("used_mb", round2(nonHeap.path("used").asLong(0) / 1048576.0));
        nonHeapMap.put("committed_mb", round2(nonHeap.path("committed").asLong(0) / 1048576.0));
        nonHeapMap.put("max_mb", nhMax > 0 ? round2(nhMax / 1048576.0) : -1);

        Map<String, Object> mem = new LinkedHashMap<>();
        mem.put("heap", heapMap);
        mem.put("non_heap", nonHeapMap);
        mem.put("object_pending_finalization", data.path("object-pending-finalization-count").asInt(0));
        mem.put("health", health);
        return mem;
    }

    @GET
    @Path("/memory-pools")
    public Map<String, Object> getMemoryPools() {
        LOG.info("Fetching memory pools");
        try {
            if (serverContext == null || !serverContext.hasContext()) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (domainServers != null && !domainServers.isEmpty()) {
                    List<Map<String, Object>> allPools = new ArrayList<>();
                    Map<String, JsonNode> byServer = client.readWildcardAcrossServers(
                            List.of("core-service", "platform-mbean", "type", "memory-pool"), true, true);
                    for (Map.Entry<String, JsonNode> entry : byServer.entrySet()) {
                        try {
                            allPools.addAll(parsePoolsFromResult(entry.getValue(), entry.getKey()));
                        } catch (Exception e) {
                            LOG.debugf("Memory pools failed for %s: %s", entry.getKey(), e.getMessage());
                        }
                    }
                    return Map.of("status", "ok", "pools", allPools);
                }
            }

            JsonNode result = client.readResource(
                    List.of("core-service", "platform-mbean", "type", "memory-pool"), true, true);
            if (!"success".equals(result.path("outcome").asText())) {
                return Map.of("error", result.path("failure-description").asText("Unknown error"));
            }
            return Map.of("status", "ok", "pools", parsePoolsFromResult(result, null));
        } catch (Exception e) {
            return Map.of("error", "Failed to get memory pools: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private List<Map<String, Object>> parsePoolsFromResult(JsonNode result, String serverLabel) {
        List<Map<String, Object>> pools = new ArrayList<>();
        JsonNode nameNode = result.path("result").path("name");
        if (nameNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = nameNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String poolName = entry.getKey();
                JsonNode poolData = entry.getValue();
                if (!poolData.isObject()) continue;

                JsonNode usage = poolData.path("usage");
                JsonNode peak = poolData.path("peak-usage");

                long usageMax = usage.path("max").asLong(-1);
                long peakMax = peak.path("max").asLong(-1);

                Map<String, Object> usageMap = new LinkedHashMap<>();
                usageMap.put("used_mb", round2(usage.path("used").asLong(0) / 1048576.0));
                usageMap.put("committed_mb", round2(usage.path("committed").asLong(0) / 1048576.0));
                usageMap.put("max_mb", usageMax > 0 ? round2(usageMax / 1048576.0) : -1);

                Map<String, Object> peakMap = new LinkedHashMap<>();
                peakMap.put("used_mb", round2(peak.path("used").asLong(0) / 1048576.0));
                peakMap.put("max_mb", peakMax > 0 ? round2(peakMax / 1048576.0) : -1);

                Map<String, Object> pool = new LinkedHashMap<>();
                pool.put("name", poolName);
                pool.put("type", poolData.path("type").asText("unknown"));
                pool.put("usage", usageMap);
                pool.put("peak_usage", peakMap);
                pool.put("valid", poolData.path("valid").asBoolean(true));
                if (serverLabel != null) pool.put("server_name", serverLabel);
                pools.add(pool);
            }
        }
        return pools;
    }

    @GET
    @Path("/gc")
    public Map<String, Object> getGcStats() {
        LOG.info("Fetching GC statistics");
        try {
            if (serverContext == null || !serverContext.hasContext()) {
                List<String[]> domainServers = client.getRunningDomainServers();
                if (domainServers != null && !domainServers.isEmpty()) {
                    List<Map<String, Object>> allCollectors = new ArrayList<>();
                    Map<String, JsonNode> byServer = client.readWildcardAcrossServers(
                            List.of("core-service", "platform-mbean", "type", "garbage-collector"), true, true);
                    for (Map.Entry<String, JsonNode> entry : byServer.entrySet()) {
                        try {
                            allCollectors.addAll(parseGcFromResult(entry.getValue(), entry.getKey()));
                        } catch (Exception e) {
                            LOG.debugf("GC stats failed for %s: %s", entry.getKey(), e.getMessage());
                        }
                    }
                    return Map.of("status", "ok", "garbage_collectors", allCollectors);
                }
            }

            JsonNode result = client.readResource(
                    List.of("core-service", "platform-mbean", "type", "garbage-collector"), true, true);
            if (!"success".equals(result.path("outcome").asText())) {
                return Map.of("error", result.path("failure-description").asText("Unknown error"));
            }
            return Map.of("status", "ok", "garbage_collectors", parseGcFromResult(result, null));
        } catch (Exception e) {
            return Map.of("error", "Failed to get GC stats: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private List<Map<String, Object>> parseGcFromResult(JsonNode result, String serverLabel) {
        List<Map<String, Object>> collectors = new ArrayList<>();
        JsonNode nameNode = result.path("result").path("name");
        if (nameNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = nameNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String gcName = entry.getKey();
                JsonNode gcData = entry.getValue();
                if (!gcData.isObject()) continue;

                long collectionTime = gcData.path("collection-time").asLong(0);

                List<String> poolNames = new ArrayList<>();
                JsonNode mpn = gcData.get("memory-pool-names");
                if (mpn != null && mpn.isArray()) {
                    for (JsonNode p : mpn) poolNames.add(p.asText());
                }

                Map<String, Object> gc = new LinkedHashMap<>();
                gc.put("name", gcName);
                gc.put("collection_count", gcData.path("collection-count").asInt(0));
                gc.put("collection_time_ms", collectionTime);
                gc.put("collection_time_human", TimeUtils.msToHuman(collectionTime));
                gc.put("memory_pool_names", poolNames);
                gc.put("valid", gcData.path("valid").asBoolean(true));
                if (serverLabel != null) gc.put("server_name", serverLabel);
                collectors.add(gc);
            }
        }
        return collectors;
    }

    private static double round2(double val) {
        return Math.round(val * 100.0) / 100.0;
    }
}
