package com.rh.monitor.resource;

import com.rh.monitor.config.JBossConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class HeapDumpResource {

    private static final Logger LOG = Logger.getLogger(HeapDumpResource.class);

    @Inject
    JBossConfig config;

    @POST
    @Path("/generate-heap-dump")
    public Map<String, Object> generateHeapDump() {
        LOG.info("Heap dump generation requested");
        String jbossHome = findJbossHome();
        if (jbossHome == null) {
            return Map.of("error", "JBOSS_HOME not set or not found. Set the JBOSS_HOME environment variable.");
        }

        String jbossPid = findJbossPid();
        if (jbossPid == null) {
            LOG.warn("JBoss process not found for heap dump");
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("error", "JBoss process not found. Is JBoss EAP running?");
            r.put("suggestion", "Start JBoss and try again, or check with: jps -l or ps -ef | grep jboss");
            return r;
        }

        String javaHome = findJavaHome();
        String dumpDir = jbossHome + "/heap_dump";
        new File(dumpDir).mkdirs();

        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String filename = "heap_dump_" + ts + ".hprof";
        String filepath = dumpDir + "/" + filename;

        String methodUsed = null;
        List<String> errorMessages = new ArrayList<>();

        // Method 1: jcmd GC.heap_dump
        try {
            String jcmdCmd = javaHome != null ? javaHome + "/bin/jcmd" : "jcmd";
            ProcessBuilder pb = new ProcessBuilder(jcmdCmd, jbossPid, "GC.heap_dump", filepath);
            pb.redirectErrorStream(true);
            Process p = pb.start();
            boolean finished = p.waitFor(300, TimeUnit.SECONDS);
            String output = new String(p.getInputStream().readAllBytes());
            if (finished && p.exitValue() == 0 && new File(filepath).isFile()) {
                methodUsed = "jcmd";
            } else {
                errorMessages.add("jcmd: " + output.strip());
            }
        } catch (Exception e) {
            errorMessages.add("jcmd: " + e.getMessage());
        }

        // Method 2: jmap
        if (methodUsed == null) {
            try {
                String jmapCmd = javaHome != null ? javaHome + "/bin/jmap" : "jmap";
                ProcessBuilder pb = new ProcessBuilder(jmapCmd, "-dump:live,format=b,file=" + filepath, jbossPid);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                boolean finished = p.waitFor(300, TimeUnit.SECONDS);
                String output = new String(p.getInputStream().readAllBytes());
                if (finished && p.exitValue() == 0 && new File(filepath).isFile()) {
                    methodUsed = "jmap";
                } else {
                    errorMessages.add("jmap: " + output.strip());
                }
            } catch (Exception e) {
                errorMessages.add("jmap: " + e.getMessage());
            }
        }

        // Method 3: JBoss CLI
        if (methodUsed == null) {
            try {
                String cliPath = jbossHome + "/bin/jboss-cli.sh";
                if (!new File(cliPath).isFile()) {
                    cliPath = jbossHome + "/bin/jboss-cli.bat";
                }
                if (new File(cliPath).isFile()) {
                    String cliCmd = "/core-service=platform-mbean/type=memory:invoke(name=dumpHeap," +
                            "parameters=[\"" + filepath + "\", \"true\"])";
                    ProcessBuilder pb = new ProcessBuilder(cliPath, "--connect",
                            "--controller=" + config.getHost() + ":" + config.getPort(),
                            "--user=" + config.getUser(),
                            "--password=" + config.getPassword(),
                            "--command=" + cliCmd);
                    pb.redirectErrorStream(true);
                    Process p = pb.start();
                    boolean finished = p.waitFor(300, TimeUnit.SECONDS);
                    String output = new String(p.getInputStream().readAllBytes());
                    if (finished && p.exitValue() == 0 && new File(filepath).isFile()) {
                        methodUsed = "jboss-cli";
                    } else {
                        errorMessages.add("jboss-cli: " + output.strip());
                    }
                } else {
                    errorMessages.add("jboss-cli: jboss-cli.sh not found");
                }
            } catch (Exception e) {
                errorMessages.add("jboss-cli: " + e.getMessage());
            }
        }

        if (methodUsed == null) {
            LOG.errorf("Failed to generate heap dump: %s", errorMessages);
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("error", "Failed to generate heap dump. Tried jcmd, jmap, and jboss-cli.");
            r.put("details", errorMessages);
            r.put("suggestion", "Ensure JAVA_HOME/bin is on PATH and the JBoss process is running.");
            return r;
        }

        long fileSize = new File(filepath).length();
        double sizeMb = Math.round(fileSize * 100.0 / (1024 * 1024)) / 100.0;
        LOG.infof("Heap dump generated successfully: %s (%.2f MB) using %s", filepath, sizeMb, methodUsed);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("file", filepath);
        result.put("filename", filename);
        result.put("dump_dir", dumpDir);
        result.put("size_bytes", fileSize);
        result.put("size_mb", sizeMb);
        result.put("method", methodUsed);
        result.put("jboss_pid", jbossPid);
        result.put("timestamp", LocalDateTime.now().toString());
        return result;
    }

    private String findJbossHome() {
        String jbossHome = config != null ? config.getJbossHome() : System.getenv("JBOSS_HOME");
        if (jbossHome != null && new File(jbossHome).isDirectory()) return jbossHome;
        String home = System.getProperty("user.home");
        String[] candidates = {
                home + "/Documents/Softwares/Claude/jboss/jboss-eap-8.1",
                home + "/Downloads/jboss/jboss-eap-8.1"
        };
        for (String c : candidates) {
            if (new File(c).isDirectory()) return c;
        }
        return null;
    }

    private String findJavaHome() {
        String javaHome = System.getenv("JAVA_HOME");
        if (javaHome != null && new File(javaHome).isDirectory()) return javaHome;
        try {
            ProcessBuilder pb = new ProcessBuilder("which", "jstack");
            Process p = pb.start();
            p.waitFor(5, TimeUnit.SECONDS);
            String path = new String(p.getInputStream().readAllBytes()).strip();
            if (!path.isEmpty()) {
                File jstackFile = new File(path).toPath().toRealPath().toFile();
                return jstackFile.getParentFile().getParent();
            }
        } catch (Exception ignored) {}
        return null;
    }

    private String findJbossPid() {
        try {
            ProcessBuilder pb = new ProcessBuilder("jps", "-l");
            Process p = pb.start();
            p.waitFor(10, TimeUnit.SECONDS);
            String output = new String(p.getInputStream().readAllBytes());
            for (String line : output.strip().split("\n")) {
                String lower = line.toLowerCase();
                if (lower.contains("jboss") || lower.contains("standalone") || lower.contains("wildfly") || lower.contains("org.jboss")) {
                    return line.split("\\s+")[0];
                }
            }
        } catch (Exception ignored) {}
        try {
            ProcessBuilder pb = new ProcessBuilder("ps", "-ef");
            Process p = pb.start();
            p.waitFor(10, TimeUnit.SECONDS);
            String output = new String(p.getInputStream().readAllBytes());
            for (String line : output.strip().split("\n")) {
                String lower = line.toLowerCase();
                if ((lower.contains("jboss") || lower.contains("standalone") || lower.contains("wildfly"))
                        && lower.contains("java") && !lower.contains("grep")) {
                    String[] parts = line.strip().split("\\s+");
                    if (parts.length >= 2) return parts[1];
                }
            }
        } catch (Exception ignored) {}
        return null;
    }
}
