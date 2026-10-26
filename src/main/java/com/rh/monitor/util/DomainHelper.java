package com.rh.monitor.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class DomainHelper {

    private DomainHelper() {}

    public static boolean isDomainLayout(String jbossHome) {
        if (jbossHome == null || jbossHome.isBlank()) return false;
        File serversDir = new File(jbossHome, "domain/servers");
        if (!serversDir.isDirectory()) return false;
        File[] dirs = serversDir.listFiles(File::isDirectory);
        return dirs != null && dirs.length > 0;
    }

    public static List<String> listDomainServerNames(String jbossHome) {
        List<String> names = new ArrayList<>();
        if (jbossHome == null) return names;
        File serversDir = new File(jbossHome, "domain/servers");
        if (!serversDir.isDirectory()) return names;
        File[] dirs = serversDir.listFiles(File::isDirectory);
        if (dirs == null) return names;
        for (File dir : dirs) {
            names.add(dir.getName());
        }
        return names;
    }

    public static List<ServerLog> findAllDomainLogs(String jbossHome, String logFileName) {
        return findAllDomainLogs(java.util.List.of(jbossHome), logFileName);
    }

    public static List<ServerLog> findAllDomainLogs(java.util.List<String> jbossHomes, String logFileName) {
        List<ServerLog> result = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (String home : jbossHomes) {
            if (home == null || home.isBlank()) continue;
            File serversDir = new File(home, "domain/servers");
            if (!serversDir.isDirectory()) continue;
            File[] dirs = serversDir.listFiles(File::isDirectory);
            if (dirs == null) continue;
            for (File dir : dirs) {
                File logFile = new File(dir, "log/" + logFileName);
                if (logFile.isFile()) {
                    String key = logFile.getAbsolutePath();
                    if (seen.add(key)) {
                        result.add(new ServerLog(dir.getName(), key));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Find domain logs using host→home mapping, producing "host/server" qualified names.
     */
    public static List<ServerLog> findAllDomainLogs(Map<String, String> hostToHome, String logFileName) {
        List<ServerLog> result = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (Map.Entry<String, String> entry : hostToHome.entrySet()) {
            String hostName = entry.getKey();
            String home = entry.getValue();
            if (home == null || home.isBlank()) continue;
            File serversDir = new File(home, "domain/servers");
            if (!serversDir.isDirectory()) continue;
            File[] dirs = serversDir.listFiles(File::isDirectory);
            if (dirs == null) continue;
            for (File dir : dirs) {
                File logFile = new File(dir, "log/" + logFileName);
                if (logFile.isFile()) {
                    String key = logFile.getAbsolutePath();
                    if (seen.add(key)) {
                        result.add(new ServerLog(hostName + "/" + dir.getName(), key));
                    }
                }
            }
        }
        return result;
    }

    public static class ServerLog {
        public final String serverName;
        public final String path;

        public ServerLog(String serverName, String path) {
            this.serverName = serverName;
            this.path = path;
        }
    }
}
