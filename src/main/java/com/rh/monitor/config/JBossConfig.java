package com.rh.monitor.config;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class JBossConfig {

    private static final Logger LOG = Logger.getLogger(JBossConfig.class);

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String protocol;
    private final String jbossHome;
    private final Map<String, String> domainHomes;

    public JBossConfig() {
        Map<String, String> fileProps = loadSetEnvFile();
        this.host = resolve(fileProps, "JBOSS_HOST", "localhost");
        this.port = Integer.parseInt(resolve(fileProps, "JBOSS_MGMT_PORT", "9990"));
        this.user = resolve(fileProps, "JBOSS_USER", "admin");
        this.password = resolve(fileProps, "JBOSS_PASSWORD", "admin");
        this.protocol = resolve(fileProps, "JBOSS_PROTOCOL", "http");
        this.jbossHome = resolve(fileProps, "JBOSS_HOME", null);
        this.domainHomes = parseDomainHomes(resolve(fileProps, "JBOSS_DOMAIN_HOMES", null));
        LOG.infof("JBoss config: %s://%s:%d (user=%s, domainHomes=%s, source=%s)",
                protocol, host, port, user, domainHomes, fileProps.isEmpty() ? "env" : "set-env.txt+env");
    }

    public String mgmtUrl() {
        return protocol + "://" + host + ":" + port + "/management";
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getUser() { return user; }
    public String getPassword() { return password; }
    public String getJbossHome() { return jbossHome; }

    /**
     * Returns all JBOSS_HOME directories to scan for domain server logs.
     * Includes the primary JBOSS_HOME plus any additional host homes from JBOSS_DOMAIN_HOMES.
     */
    public List<String> getAllDomainHomes() {
        List<String> homes = new ArrayList<>();
        if (jbossHome != null && !jbossHome.isBlank()) homes.add(jbossHome);
        for (String path : domainHomes.values()) {
            if (!homes.contains(path)) homes.add(path);
        }
        return homes;
    }

    /**
     * Returns the JBOSS_HOME for a specific host name, or the primary JBOSS_HOME if not mapped.
     */
    public String getHomeForHost(String hostName) {
        String mapped = domainHomes.get(hostName);
        return mapped != null ? mapped : jbossHome;
    }

    /**
     * Returns the host→home mapping for all domain homes.
     */
    public Map<String, String> getDomainHomeMap() {
        Map<String, String> map = new java.util.LinkedHashMap<>(domainHomes);
        if (jbossHome != null && !jbossHome.isBlank() && !map.containsValue(jbossHome)) {
            map.put("default", jbossHome);
        }
        return map;
    }

    private static Map<String, String> parseDomainHomes(String value) {
        Map<String, String> map = new HashMap<>();
        if (value == null || value.isBlank()) return map;
        for (String entry : value.split(",")) {
            String[] parts = entry.strip().split("=", 2);
            if (parts.length == 2 && !parts[0].isBlank() && !parts[1].isBlank()) {
                map.put(parts[0].strip(), parts[1].strip());
            }
        }
        return map;
    }

    private static String resolve(Map<String, String> fileProps, String key, String def) {
        String env = System.getenv(key);
        if (env != null && !env.isBlank()) return env;
        String fileProp = fileProps.get(key);
        if (fileProp != null && !fileProp.isBlank()) return fileProp;
        return def;
    }

    private static Map<String, String> loadSetEnvFile() {
        Map<String, String> props = new HashMap<>();
        Path setEnvPath = findSetEnvFile();
        if (setEnvPath == null) return props;
        try {
            for (String line : Files.readAllLines(setEnvPath)) {
                String trimmed = line.strip();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;
                String content = trimmed.startsWith("export ") ? trimmed.substring(7).strip() : trimmed;
                int eq = content.indexOf('=');
                if (eq > 0) {
                    String key = content.substring(0, eq).strip();
                    String value = content.substring(eq + 1).strip();
                    if ((value.startsWith("\"") && value.endsWith("\""))
                            || (value.startsWith("'") && value.endsWith("'"))) {
                        value = value.substring(1, value.length() - 1);
                    }
                    props.put(key, value);
                }
            }
            LOG.infof("Loaded %d properties from %s", props.size(), setEnvPath);
        } catch (IOException e) {
            LOG.debugf("Failed to read set-env.txt: %s", e.getMessage());
        }
        return props;
    }

    private static Path findSetEnvFile() {
        Path cwd = Path.of(System.getProperty("user.dir"), "set-env.txt");
        if (Files.isRegularFile(cwd)) return cwd;
        Path parent = cwd.getParent() != null ? cwd.getParent().getParent() : null;
        if (parent != null) {
            Path p = parent.resolve("set-env.txt");
            if (Files.isRegularFile(p)) return p;
        }
        Path home = Path.of(System.getProperty("user.home"), "set-env.txt");
        if (Files.isRegularFile(home)) return home;
        return null;
    }
}
