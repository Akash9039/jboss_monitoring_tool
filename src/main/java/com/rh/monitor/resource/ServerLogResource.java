package com.rh.monitor.resource;

import com.rh.monitor.client.JBossManagementClient;
import com.rh.monitor.config.JBossConfig;
import com.rh.monitor.config.ServerContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import com.rh.monitor.util.DomainHelper;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import org.jboss.logging.Logger;

import java.util.regex.Pattern;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class ServerLogResource {

    private static final Logger LOG = Logger.getLogger(ServerLogResource.class);

    @Inject
    JBossManagementClient client;

    @Inject
    JBossConfig config;

    @Inject
    ServerContext serverContext;

    private static final Pattern SERVER_LOG_RE = Pattern.compile(
            "^((?:\\d{4}-\\d{2}-\\d{2} )?\\d{2}:\\d{2}:\\d{2},\\d{3})\\s+(\\w+)\\s+\\[([^\\]]+)\\]\\s+\\(([^)]+)\\)\\s+(.*)");

    private static final Pattern JBOSS_CODE_RE = Pattern.compile("[A-Z]{2,}[0-9]{4,}");

    @GET
    @Path("/server-logs")
    public Map<String, Object> getServerLogs(@QueryParam("level") String level,
                                              @QueryParam("limit") Integer limit) {
        LOG.infof("Fetching server logs, level filter: %s", level);
        List<String> levels;
        if (level != null && !level.isBlank()) {
            levels = List.of(level.toUpperCase());
        } else {
            levels = List.of("WARN", "ERROR", "FATAL");
        }
        return getServerLogEntries(levels, limit != null ? limit : 1000);
    }

    @GET
    @Path("/server-logs/all")
    public Map<String, Object> getServerLogsAll(@QueryParam("level") String level,
                                                 @QueryParam("limit") Integer limit) {
        LOG.infof("Fetching all server logs, level filter: %s", level);
        List<String> levels;
        if (level != null && !level.isBlank()) {
            levels = List.of(level.toUpperCase());
        } else {
            levels = List.of("INFO", "WARN", "ERROR", "FATAL");
        }
        return getServerLogEntries(levels, limit != null ? limit : 1000);
    }

    private Map<String, Object> getServerLogEntries(List<String> levels, int maxEntries) {
        String jbossHome = config != null ? config.getJbossHome() : System.getenv("JBOSS_HOME");
        boolean isServerSelected = false;
        try { isServerSelected = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

        List<Map<String, Object>> allEntries;
        String logPath;

        if (!isServerSelected && client.isDomainMode()) {
            List<DomainHelper.ServerLog> domainLogs = DomainHelper.findAllDomainLogs(config.getAllDomainHomes(), "server.log");
            if (domainLogs.isEmpty()) {
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("status", "not_found");
                r.put("message", "No server.log found in domain/servers/. Check JBOSS_HOME.");
                r.put("entries", List.of());
                r.put("total", 0);
                r.put("level_summary", Map.of());
                r.put("kcs_suggestions", List.of());
                return r;
            }
            allEntries = new ArrayList<>();
            List<String> logFiles = new ArrayList<>();
            for (DomainHelper.ServerLog sl : domainLogs) {
                List<Map<String, Object>> parsed = parseServerLog(sl.path, levels);
                for (Map<String, Object> e : parsed) e.put("server_name", sl.serverName);
                allEntries.addAll(parsed);
                logFiles.add(sl.path);
            }
            allEntries.sort((a, b) -> {
                String aTs = (String) a.getOrDefault("timestamp", "");
                String bTs = (String) b.getOrDefault("timestamp", "");
                return bTs.compareTo(aTs);
            });
            logPath = String.join(", ", logFiles);
        } else {
            logPath = findServerLog();
            if (logPath == null) {
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("status", "not_found");
                r.put("message", "server.log not found. Set JBOSS_HOME or check JBoss log directory.");
                r.put("entries", List.of());
                r.put("total", 0);
                r.put("level_summary", Map.of());
                r.put("kcs_suggestions", List.of());
                return r;
            }
            allEntries = parseServerLog(logPath, levels);
        }

        Map<String, Integer> levelSummary = new LinkedHashMap<>();
        for (Map<String, Object> e : allEntries) {
            String lvl = (String) e.get("level");
            levelSummary.merge(lvl, 1, Integer::sum);
        }

        Map<String, Map<String, Object>> codeTracker = new LinkedHashMap<>();
        for (Map<String, Object> e : allEntries) {
            String code = (String) e.getOrDefault("jboss_code", "");
            if (!code.isEmpty()) {
                codeTracker.computeIfAbsent(code, k -> {
                    Map<String, Object> info = new LinkedHashMap<>();
                    info.put("code", k);
                    info.put("message", ((String) e.get("message")).substring(0, Math.min(120, ((String) e.get("message")).length())));
                    info.put("count", 0);
                    info.put("level", e.get("level"));
                    info.put("_servers", new LinkedHashMap<String, Integer>());
                    return info;
                });
                codeTracker.get(code).merge("count", 1, (a, b) -> (int) a + (int) b);
                String srv = (String) e.get("server_name");
                if (srv != null) {
                    @SuppressWarnings("unchecked")
                    Map<String, Integer> srvMap = (Map<String, Integer>) codeTracker.get(code).get("_servers");
                    srvMap.merge(srv, 1, Integer::sum);
                }
            }
        }

        List<Map<String, Object>> kcsSuggestions = new ArrayList<>();
        codeTracker.entrySet().stream()
                .sorted((a, b) -> Integer.compare((int) b.getValue().get("count"), (int) a.getValue().get("count")))
                .forEach(entry -> {
                    Map<String, Object> info = entry.getValue();
                    Map<String, Object> suggestion = new LinkedHashMap<>();
                    suggestion.put("code", info.get("code"));
                    suggestion.put("message", info.get("message"));
                    suggestion.put("count", info.get("count"));
                    suggestion.put("level", info.get("level"));
                    suggestion.put("search_url", "https://access.redhat.com/search/#/knowledgebase?q=" + info.get("code") + "&p=1&rows=10");
                    suggestion.put("doc_url", "https://access.redhat.com/solutions?q=" + info.get("code"));
                    @SuppressWarnings("unchecked")
                    Map<String, Integer> srvMap = (Map<String, Integer>) info.get("_servers");
                    if (srvMap != null && !srvMap.isEmpty()) {
                        List<Map<String, Object>> servers = new ArrayList<>();
                        srvMap.forEach((srv, cnt) -> {
                            Map<String, Object> s = new LinkedHashMap<>();
                            s.put("name", srv);
                            s.put("count", cnt);
                            servers.add(s);
                        });
                        servers.sort((a2, b2) -> Integer.compare((int) b2.get("count"), (int) a2.get("count")));
                        suggestion.put("servers", servers);
                    }
                    kcsSuggestions.add(suggestion);
                });

        List<Map<String, Object>> trimmed = allEntries.subList(0, Math.min(maxEntries, allEntries.size()));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("entries", trimmed);
        result.put("total", allEntries.size());
        result.put("returned", trimmed.size());
        result.put("level_summary", levelSummary);
        result.put("log_file", logPath);
        result.put("kcs_suggestions", kcsSuggestions);
        return result;
    }

    private List<Map<String, Object>> parseServerLog(String logPath, List<String> levels) {
        Set<String> levelsUpper = new HashSet<>(levels);
        List<Map<String, Object>> entries = new ArrayList<>();
        try {
            List<String> allLines = Files.readAllLines(java.nio.file.Path.of(logPath));

            Map<String, Object> current = null;
            for (String line : allLines) {
                Matcher m = SERVER_LOG_RE.matcher(line.stripTrailing());
                if (m.matches()) {
                    if (current != null && levelsUpper.contains(current.get("level"))) {
                        entries.add(current);
                    }
                    String timestamp = m.group(1);
                    String lvl = m.group(2).toUpperCase();
                    String logger = m.group(3);
                    String thread = m.group(4);
                    String message = m.group(5).strip();

                    Matcher codeMatcher = JBOSS_CODE_RE.matcher(message);
                    String jbossCode = codeMatcher.find() ? codeMatcher.group() : "";

                    current = new LinkedHashMap<>();
                    current.put("timestamp", timestamp);
                    current.put("level", lvl);
                    current.put("logger", logger);
                    current.put("thread", thread);
                    current.put("message", message);
                    current.put("stacktrace", new ArrayList<String>());
                    current.put("jboss_code", jbossCode);
                } else if (current != null) {
                    @SuppressWarnings("unchecked")
                    List<String> st = (List<String>) current.get("stacktrace");
                    st.add(line.stripTrailing());
                }
            }
            if (current != null && levelsUpper.contains(current.get("level"))) {
                entries.add(current);
            }
        } catch (Exception e) {
            // log parse failed
        }
        Collections.reverse(entries);
        return entries;
    }

    private String findServerLog() {
        List<String> candidates = new ArrayList<>();
        boolean hasServerCtx = false;
        try { hasServerCtx = serverContext != null && serverContext.hasContext(); } catch (Exception ignored) {}

        boolean isDomain = client.isDomainMode();
        if (hasServerCtx && isDomain) {
            String hostHome = config.getHomeForHost(serverContext.getHost());
            candidates.add(hostHome + "/domain/servers/" + serverContext.getServer() + "/log/server.log");
            for (String home : config.getAllDomainHomes()) {
                String path = home + "/domain/servers/" + serverContext.getServer() + "/log/server.log";
                if (!candidates.contains(path)) candidates.add(path);
            }
        } else if (isDomain) {
            for (String home : config.getAllDomainHomes()) {
                if (DomainHelper.isDomainLayout(home)) {
                    File serversDir = new File(home, "domain/servers");
                    File[] dirs = serversDir.listFiles(File::isDirectory);
                    if (dirs != null) {
                        for (File dir : dirs) {
                            candidates.add(new File(dir, "log/server.log").getPath());
                        }
                    }
                }
            }
        }
        String jbossHome = config != null ? config.getJbossHome() : System.getenv("JBOSS_HOME");
        if (!isDomain && jbossHome != null) {
            candidates.add(jbossHome + "/standalone/log/server.log");
        }
        for (String c : candidates) {
            if (new File(c).isFile()) return c;
        }
        return findByGlob("server.log");
    }

    private String findByGlob(String filename) {
        String home = System.getProperty("user.home");
        String[] patterns = {
                home + "/Documents/Softwares/Claude/jboss",
                home + "/Downloads/jboss"
        };
        for (String base : patterns) {
            File dir = new File(base);
            if (dir.isDirectory()) {
                String found = searchRecursive(dir, filename, 5);
                if (found != null) return found;
            }
        }
        return null;
    }

    private String searchRecursive(File dir, String filename, int depth) {
        if (depth <= 0) return null;
        File[] files = dir.listFiles();
        if (files == null) return null;
        for (File f : files) {
            if (f.isFile() && f.getName().equals(filename)) return f.getAbsolutePath();
        }
        for (File f : files) {
            if (f.isDirectory()) {
                String found = searchRecursive(f, filename, depth - 1);
                if (found != null) return found;
            }
        }
        return null;
    }
}
