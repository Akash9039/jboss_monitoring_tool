package com.rh.monitor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.rh.monitor.client.JBossManagementClient;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;

@ApplicationScoped
@Startup
public class StatisticsEnabler {

    private static final Logger LOG = Logger.getLogger(StatisticsEnabler.class);

    @Inject
    JBossManagementClient client;

    @PostConstruct
    void enableStatistics() {
        try {
            List<String[]> domainServers = client.getRunningDomainServers();
            if (!domainServers.isEmpty()) {
                enableDomainStatistics();
            } else {
                enableStandaloneStatistics();
            }
        } catch (Exception e) {
            LOG.debugf("Statistics enabler deferred: %s", e.getMessage());
        }
    }

    private void enableDomainStatistics() {
        try {
            JsonNode profilesResult = client.readChildrenNames(List.of(), "profile");
            JsonNode profileNames = profilesResult.path("result");
            if (!profileNames.isArray()) return;

            for (JsonNode pn : profileNames) {
                String profile = pn.asText();
                List<String> profileAddr = List.of("profile", profile);

                enableAttribute(concat(profileAddr, List.of("subsystem", "undertow")),
                        "statistics-enabled", "undertow/" + profile);

                enableAttribute(concat(profileAddr, List.of("subsystem", "transactions")),
                        "statistics-enabled", "transactions/" + profile);

                enableDatasourceStats(concat(profileAddr, List.of("subsystem", "datasources")),
                        profile);
            }
        } catch (Exception e) {
            LOG.debugf("Domain statistics enablement error: %s", e.getMessage());
        }
    }

    private void enableStandaloneStatistics() {
        enableAttribute(List.of("subsystem", "undertow"), "statistics-enabled", "undertow");
        enableAttribute(List.of("subsystem", "transactions"), "statistics-enabled", "transactions");
        enableDatasourceStats(List.of("subsystem", "datasources"), "standalone");
    }

    private void enableDatasourceStats(List<String> dsSubsystemAddr, String label) {
        try {
            for (String dsType : List.of("data-source", "xa-data-source")) {
                JsonNode names = client.mgmtRequest(Map.of(
                        "operation", "read-children-names",
                        "address", dsSubsystemAddr,
                        "child-type", dsType));
                if (!"success".equals(names.path("outcome").asText())) continue;
                JsonNode result = names.path("result");
                if (!result.isArray()) continue;
                for (JsonNode dsNode : result) {
                    String dsName = dsNode.asText();
                    List<String> addr = concat(dsSubsystemAddr, List.of(dsType, dsName));
                    enableAttribute(addr, "statistics-enabled", label + "/" + dsName);
                }
            }
        } catch (Exception e) {
            LOG.debugf("Datasource statistics enablement error for %s: %s", label, e.getMessage());
        }
    }

    private void enableAttribute(List<String> address, String attrName, String label) {
        try {
            JsonNode readResult = client.mgmtRequest(Map.of(
                    "operation", "read-attribute",
                    "address", address,
                    "name", attrName));
            if (!"success".equals(readResult.path("outcome").asText())) return;

            JsonNode val = readResult.path("result");
            boolean enabled = val.isBoolean() && val.asBoolean();
            if (val.isObject() && val.has("EXPRESSION_VALUE")) {
                enabled = false;
            }

            if (!enabled) {
                JsonNode writeResult = client.mgmtRequest(Map.of(
                        "operation", "write-attribute",
                        "address", address,
                        "name", attrName,
                        "value", true));
                if ("success".equals(writeResult.path("outcome").asText())) {
                    LOG.infof("Enabled %s statistics on %s", label, attrName);
                }
            }
        } catch (Exception e) {
            LOG.debugf("Failed to enable %s on %s: %s", attrName, label, e.getMessage());
        }
    }

    private static List<String> concat(List<String> a, List<String> b) {
        var list = new java.util.ArrayList<>(a);
        list.addAll(b);
        return list;
    }
}
