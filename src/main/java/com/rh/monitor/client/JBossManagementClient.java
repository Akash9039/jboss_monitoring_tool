package com.rh.monitor.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rh.monitor.config.JBossConfig;
import com.rh.monitor.config.ServerContext;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

@ApplicationScoped
public class JBossManagementClient {

    private static final Logger LOG = Logger.getLogger(JBossManagementClient.class);

    @Inject
    JBossConfig config;

    @Inject
    ServerContext serverContext;

    private final ObjectMapper mapper = new ObjectMapper();
    private volatile Boolean cachedDomainMode = null;
    private volatile long domainModeCacheTime = 0;
    private static final long DOMAIN_MODE_CACHE_TTL_MS = 60_000;

    // Domain topology (running/all servers) changes rarely; cache it briefly so the many
    // endpoints and scheduled pollers that start with a server-discovery call don't each
    // pay for it on every request.
    private volatile List<String[]> cachedRunningServers = null;
    private volatile long runningServersCacheTime = 0;
    private volatile List<String[]> cachedAllServers = null;
    private volatile long allServersCacheTime = 0;
    private static final long SERVERS_CACHE_TTL_MS = 10_000;

    private static final String CLI_EOF = "\0EOF\0";
    private static final String CLI_PROMPT = "\0PROMPT\0";

    // Pool of persistent jboss-cli connections. A single connection serializes every CLI
    // call behind one lock, so a dashboard that fires many endpoints at once (plus the
    // scheduled pollers) queues them all — the UI sees the last one finish after the SUM of
    // every call, and worker threads block waiting for the lock (503 "worker pool
    // exhaustion"). A small pool lets that many requests run concurrently instead.
    private static final int POOL_SIZE =
            Integer.getInteger("jboss.cli.pool-size",
                    Integer.parseInt(System.getenv().getOrDefault("JBOSS_CLI_POOL_SIZE", "4")));
    private final BlockingQueue<CliConnection> cliPool = new LinkedBlockingQueue<>();
    private volatile boolean poolInitialized = false;
    private final Object poolInitLock = new Object();

    @PreDestroy
    void cleanup() {
        for (CliConnection c : cliPool) {
            c.destroy();
        }
    }

    private String findCliScript() {
        String jbossHome = config.getJbossHome();
        if (jbossHome != null) {
            File sh = new File(jbossHome, "bin/jboss-cli.sh");
            if (sh.canExecute()) return sh.getAbsolutePath();
            File bat = new File(jbossHome, "bin/jboss-cli.bat");
            if (bat.exists()) return bat.getAbsolutePath();
        }
        return "jboss-cli.sh";
    }

    private String buildCliCommand(Map<String, Object> operation) {
        List<String> address = List.of();
        Object addrObj = operation.get("address");
        if (addrObj instanceof List<?> addrList) {
            List<String> parts = new ArrayList<>();
            for (Object item : addrList) {
                parts.add(String.valueOf(item));
            }
            address = parts;
        }

        StringBuilder path = new StringBuilder();
        for (int i = 0; i + 1 < address.size(); i += 2) {
            path.append("/").append(address.get(i)).append("=").append(address.get(i + 1));
        }

        String op = (String) operation.getOrDefault("operation", "read-resource");
        StringBuilder cmd = new StringBuilder();
        if (path.length() > 0) {
            cmd.append(path);
        }
        cmd.append(":").append(op).append("(");

        List<String> params = new ArrayList<>();
        for (Map.Entry<String, Object> entry : operation.entrySet()) {
            String key = entry.getKey();
            if ("operation".equals(key) || "address".equals(key) || "json.pretty".equals(key)) continue;
            Object val = entry.getValue();
            if (val instanceof Boolean) {
                params.add(key + "=" + val);
            } else if (val instanceof Number) {
                params.add(key + "=" + val);
            } else if (val != null) {
                params.add(key + "=" + val);
            }
        }
        cmd.append(String.join(",", params));
        cmd.append(")");
        return cmd.toString();
    }

    public JsonNode mgmtRequest(Map<String, Object> operation) throws Exception {
        LOG.debugf("CLI request: %s", operation.get("operation"));
        String cliCmd = buildCliCommand(operation);
        return executeCli(cliCmd);
    }

    private List<String> buildProcessArgs(boolean useBatchFlag) {
        String cliScript = findCliScript();
        String controller = config.getHost() + ":" + config.getPort();
        List<String> cmdList = new ArrayList<>();
        cmdList.add(cliScript);
        cmdList.add("--connect");
        cmdList.add("--controller=" + controller);
        if (config.getUser() != null && !config.getUser().isBlank()) {
            cmdList.add("--user=" + config.getUser());
        }
        if (config.getPassword() != null && !config.getPassword().isBlank()) {
            cmdList.add("--password=" + config.getPassword());
        }
        cmdList.add("--output-json");
        return cmdList;
    }

    // Connect timeout for the pre-spawn reachability probe (ms).
    private static final int CONTROLLER_PROBE_TIMEOUT_MS =
            Integer.getInteger("jboss.cli.probe-timeout-ms",
                    Integer.parseInt(System.getenv().getOrDefault("JBOSS_CLI_PROBE_TIMEOUT_MS", "800")));

    /**
     * Quick TCP probe of the management controller. Throws immediately if the port can't be
     * opened, so callers avoid paying the ~0.8s {@code jboss-cli.sh} startup only to have it
     * die with "Connection refused" when JBoss is down.
     */
    private void assertControllerReachable() throws IOException {
        String host = config.getHost();
        int port = config.getPort();
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), CONTROLLER_PROBE_TIMEOUT_MS);
        } catch (IOException e) {
            throw new IOException("JBoss management controller not reachable at " + host + ":" + port
                    + " (" + e.getMessage() + ") — is JBoss running?");
        }
    }

    // --- Persistent CLI Connection Pool ---

    private void initPool() {
        if (poolInitialized) return;
        synchronized (poolInitLock) {
            if (poolInitialized) return;
            for (int i = 0; i < POOL_SIZE; i++) {
                cliPool.offer(new CliConnection(i));
            }
            poolInitialized = true;
        }
    }

    private CliConnection borrowConnection() throws InterruptedException {
        initPool();
        return cliPool.take();
    }

    private void releaseConnection(CliConnection conn) {
        cliPool.offer(conn);
    }

    /**
     * One persistent {@code jboss-cli.sh} process. Each connection owns its own process,
     * writer, reader thread and output queue, so several can run commands in parallel — the
     * pool hands out a free one per request. The stdout framing (char-based prompt handshake +
     * {@code CLI_PROMPT} sentinel + JSON brace counting) is identical to the previous
     * single-connection implementation; only the ownership of the streams changed.
     */
    private final class CliConnection {
        private final int id;
        private Process process;
        private BufferedWriter writer;
        private Thread readerThread;
        private final LinkedBlockingQueue<String> output = new LinkedBlockingQueue<>();

        CliConnection(int id) {
            this.id = id;
        }

        void ensureStarted() throws Exception {
            if (process != null && process.isAlive()) return;
            // Booting jboss-cli.sh costs ~0.8s (JVM start + connect handshake). If the
            // controller port isn't even open, that spawn is guaranteed to fail with
            // "died during startup" — so probe the TCP port first and fail fast (<1s) with
            // a clear message instead of paying the startup cost (and flooding the log) on
            // every request while JBoss is down. Costs ~1ms when the port is open.
            assertControllerReachable();
            start();
        }

        private void start() throws Exception {
            destroy();

            List<String> args = buildProcessArgs(false);
            args.add("--no-color-output");
            ProcessBuilder pb = new ProcessBuilder(args);
            pb.redirectErrorStream(true);
            process = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    process.getInputStream(), StandardCharsets.UTF_8));
            writer = new BufferedWriter(new OutputStreamWriter(
                    process.getOutputStream(), StandardCharsets.UTF_8));

            output.clear();

            // Handshake: wait for the interactive prompt BEFORE starting the line reader.
            // jboss-cli prints the prompt (e.g. "[standalone@localhost:9990 /] " or
            // "[domain@localhost:9990 /] ") WITHOUT a trailing newline, then waits for
            // input. A readLine() here would block forever waiting for a newline that only
            // arrives once we send the first command — so we must read char-by-char.
            StringBuilder handshake = new StringBuilder();
            long deadline = System.currentTimeMillis() + 15_000;
            boolean established = false;
            while (System.currentTimeMillis() < deadline) {
                if (reader.ready()) {
                    int c = reader.read();
                    if (c == -1) throw new RuntimeException("CLI process died during startup");
                    handshake.append((char) c);
                    if (handshake.indexOf("@") >= 0 && handshake.indexOf("] ") >= 0) {
                        established = true;
                        break;
                    }
                } else {
                    if (!process.isAlive()) {
                        throw new RuntimeException("CLI process died during startup");
                    }
                    Thread.sleep(50);
                }
            }
            if (!established) {
                throw new RuntimeException("CLI did not produce prompt within 15s");
            }
            LOG.infof("Persistent CLI connection %d established", id);

            readerThread = new Thread(() -> {
                try {
                    StringBuilder lineBuf = new StringBuilder();
                    int ch;
                    while ((ch = reader.read()) != -1) {
                        char c = (char) ch;
                        if (c == '\r') continue;
                        if (c == '\n') {
                            output.offer(lineBuf.toString());
                            lineBuf.setLength(0);
                            continue;
                        }
                        lineBuf.append(c);
                        // The interactive prompt ("[domain@host:port /] ") is printed WITHOUT a
                        // trailing newline, so a line reader never surfaces it. Detect it here and
                        // emit a sentinel so response framing works even when a command produces a
                        // plain-text CLI error instead of JSON. This is the command boundary.
                        if (c == ' ' && lineBuf.length() >= 2
                                && lineBuf.charAt(lineBuf.length() - 2) == ']'
                                && lineBuf.charAt(0) == '['
                                && lineBuf.indexOf("@") > 0) {
                            output.offer(CLI_PROMPT);
                            lineBuf.setLength(0);
                        }
                    }
                } catch (Exception e) {
                    LOG.debugf("CLI reader thread %d ended: %s", id, e.getMessage());
                }
                output.offer(CLI_EOF);
            }, "jboss-cli-reader-" + id);
            readerThread.setDaemon(true);
            readerThread.start();
        }

        void destroy() {
            Process p = process;
            Thread t = readerThread;
            process = null;
            writer = null;
            readerThread = null;
            if (p != null) {
                try { p.destroyForcibly(); } catch (Exception ignored) {}
            }
            if (t != null) {
                t.interrupt();
                try { t.join(1000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            }
            output.clear();
        }

        JsonNode execute(String command, boolean allowRetry) throws Exception {
            ensureStarted();

            // Drain any leftover/stale response lines from a previous command before
            // sending a new one, so a single leftover response can't permanently desync
            // this connection. Safe because only the borrowing thread uses it.
            output.clear();

            writer.write(command);
            writer.newLine();
            writer.flush();

            try {
                JsonNode resp = readOneJsonResponse(System.currentTimeMillis() + 30_000);
                // A pooled CLI session whose controller was restarted (e.g. standalone→domain
                // switch, or any JBoss restart) does NOT necessarily die — it stays alive and
                // returns a plain-text connection error as outcome=failed. Detect that and
                // reconnect with a fresh --connect subprocess, otherwise every request keeps
                // failing until this tool itself is restarted.
                if (allowRetry && isConnectionError(resp)) {
                    LOG.warnf("CLI connection %d lost its controller, reconnecting: %s",
                            id, resp.path("failure-description").asText(""));
                    destroy();
                    return execute(command, false);
                }
                return resp;
            } catch (RuntimeException e) {
                if (allowRetry && (e.getMessage().contains("died") || e.getMessage().contains("Timeout"))) {
                    LOG.warnf("CLI connection %d failed, reconnecting: %s", id, e.getMessage());
                    destroy();
                    return execute(command, false);
                }
                throw e;
            }
        }

        List<JsonNode> executeBatch(List<String> commands) throws Exception {
            ensureStarted();

            // Drain stale responses before sending, so batch results line up with commands.
            output.clear();

            for (String cmd : commands) {
                writer.write(cmd);
                writer.newLine();
            }
            writer.flush();

            long deadline = System.currentTimeMillis() + 60_000;
            List<JsonNode> results = new ArrayList<>(commands.size());
            for (int i = 0; i < commands.size(); i++) {
                try {
                    results.add(readOneJsonResponse(deadline));
                } catch (RuntimeException e) {
                    LOG.warnf("Batch command %d/%d failed on connection %d: %s",
                            i + 1, commands.size(), id, e.getMessage());
                    // A partial batch leaves unread responses queued, which would desync
                    // every following command. Reset this connection so it starts clean.
                    destroy();
                    break;
                }
            }
            return results;
        }

        private JsonNode readOneJsonResponse(long deadline) throws Exception {
            StringBuilder json = new StringBuilder();
            StringBuilder nonJson = new StringBuilder();
            int braceDepth = 0;
            boolean inJson = false;
            boolean inString = false;
            boolean escaped = false;
            boolean bodyStarted = false;
            boolean skipEchoNext = false;

            while (System.currentTimeMillis() < deadline) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                String line = output.poll(remaining, TimeUnit.MILLISECONDS);
                if (line == null) break;
                if (CLI_EOF.equals(line)) {
                    process = null;
                    throw new RuntimeException("CLI process died");
                }

                if (CLI_PROMPT.equals(line)) {
                    if (!bodyStarted) {
                        // Leading prompt of this command's turn; next line is the echoed command.
                        skipEchoNext = true;
                        continue;
                    }
                    if (inJson) break; // incomplete JSON terminated by a prompt (shouldn't happen)
                    // A plain-text CLI error (no JSON) terminated by the next prompt.
                    return cliErrorNode(nonJson.toString());
                }

                if (skipEchoNext) {
                    skipEchoNext = false;
                    bodyStarted = true;
                    continue; // echoed command line
                }
                bodyStarted = true;

                if (!inJson) {
                    int idx = line.indexOf('{');
                    if (idx >= 0) {
                        inJson = true;
                        line = line.substring(idx);
                    } else {
                        if (nonJson.length() > 0) nonJson.append('\n');
                        nonJson.append(line);
                        continue;
                    }
                }

                json.append(line).append('\n');

                for (int i = 0; i < line.length(); i++) {
                    char c = line.charAt(i);
                    if (escaped) { escaped = false; continue; }
                    if (c == '\\' && inString) { escaped = true; continue; }
                    if (c == '"') { inString = !inString; continue; }
                    if (inString) continue;
                    if (c == '{') braceDepth++;
                    else if (c == '}') braceDepth--;
                }

                if (inJson && braceDepth == 0) {
                    return mapper.readTree(json.toString());
                }
            }

            destroy();
            throw new RuntimeException("Timeout reading CLI response");
        }
    }

    // True when a failed CLI response indicates the controller connection is gone (as opposed
    // to a genuine operation error). Such a connection must be torn down and re-established.
    private boolean isConnectionError(JsonNode resp) {
        if (resp == null || "success".equals(resp.path("outcome").asText())) return false;
        String d = resp.path("failure-description").asText("").toLowerCase();
        if (d.isEmpty()) return false;
        return d.contains("not available")
                || d.contains("failed to establish connection")
                || d.contains("connection refused")
                || d.contains("connection closed")
                || d.contains("connection reset")
                || d.contains("could not connect")
                || d.contains("no longer available")
                || d.contains("connectexception")
                || d.contains("wflyprt")            // remoting/protocol connect failures
                || d.contains("closedchannelexception");
    }

    private JsonNode cliErrorNode(String text) {
        ObjectNode node = mapper.createObjectNode();
        node.put("outcome", "failed");
        String desc = text == null ? "" : text.trim();
        node.put("failure-description", desc.isEmpty() ? "CLI command failed with no response" : desc);
        return node;
    }

    private JsonNode executeCli(String command) throws Exception {
        CliConnection conn = borrowConnection();
        try {
            return conn.execute(command, true);
        } finally {
            releaseConnection(conn);
        }
    }

    public List<JsonNode> executeBatch(List<String> commands) throws Exception {
        if (commands.isEmpty()) return List.of();
        if (commands.size() == 1) return List.of(executeCli(commands.get(0)));

        CliConnection conn = borrowConnection();
        try {
            return conn.executeBatch(commands);
        } finally {
            releaseConnection(conn);
        }
    }

    public String buildReadResourceCommand(List<String> address, boolean includeRuntime, boolean recursive) {
        // Scope to the selected server (/host=X/server=Y) when a domain server context is set,
        // so batch reads used by the dashboard target that server instead of the domain controller.
        List<String> resolved = resolveAddress(address);
        StringBuilder path = new StringBuilder();
        for (int i = 0; i + 1 < resolved.size(); i += 2) {
            path.append("/").append(resolved.get(i)).append("=").append(resolved.get(i + 1));
        }
        return path + ":read-resource(include-runtime=" + includeRuntime + ",recursive=" + recursive + ")";
    }

    public String buildReadAttributeCommand(List<String> address, String name) {
        List<String> resolved = resolveAddress(address);
        StringBuilder path = new StringBuilder();
        for (int i = 0; i + 1 < resolved.size(); i += 2) {
            path.append("/").append(resolved.get(i)).append("=").append(resolved.get(i + 1));
        }
        return path + ":read-attribute(name=" + name + ")";
    }

    public String buildOperationCommand(List<String> address, String operation) {
        List<String> resolved = resolveAddress(address);
        StringBuilder path = new StringBuilder();
        for (int i = 0; i + 1 < resolved.size(); i += 2) {
            path.append("/").append(resolved.get(i)).append("=").append(resolved.get(i + 1));
        }
        return path + ":" + operation + "()";
    }

    private List<String> resolveAddress(List<String> address) {
        try {
            if (serverContext == null || !serverContext.hasContext()) return address;
        } catch (Exception e) {
            return address;
        }
        if (!address.isEmpty()) {
            String first = address.get(0);
            if ("host".equals(first) || "server-group".equals(first) || "profile".equals(first)) {
                return address;
            }
        }
        List<String> resolved = new ArrayList<>(4 + address.size());
        resolved.add("host");
        resolved.add(serverContext.getHost());
        resolved.add("server");
        resolved.add(serverContext.getServer());
        resolved.addAll(address);
        return resolved;
    }

    public JsonNode readResource(List<String> address, boolean includeRuntime, boolean recursive,
                                  boolean resolveExpressions) throws Exception {
        List<String> resolved = resolveAddress(address);
        StringBuilder path = new StringBuilder();
        for (int i = 0; i + 1 < resolved.size(); i += 2) {
            path.append("/").append(resolved.get(i)).append("=").append(resolved.get(i + 1));
        }
        String cmd = path + ":read-resource(include-runtime=" + includeRuntime
                + ",recursive=" + recursive
                + (resolveExpressions ? ",resolve-expressions=true" : "")
                + ")";
        return executeCli(cmd);
    }

    public JsonNode readResource(List<String> address, boolean includeRuntime, boolean recursive) throws Exception {
        List<String> resolved = resolveAddress(address);
        StringBuilder path = new StringBuilder();
        for (int i = 0; i + 1 < resolved.size(); i += 2) {
            path.append("/").append(resolved.get(i)).append("=").append(resolved.get(i + 1));
        }
        String cmd = path + ":read-resource(include-runtime=" + includeRuntime
                + ",recursive=" + recursive + ")";
        return executeCli(cmd);
    }

    public JsonNode readResource(List<String> address) throws Exception {
        return readResource(address, true, false);
    }

    public JsonNode readAttribute(List<String> address, String name) throws Exception {
        List<String> resolved = resolveAddress(address);
        StringBuilder path = new StringBuilder();
        for (int i = 0; i + 1 < resolved.size(); i += 2) {
            path.append("/").append(resolved.get(i)).append("=").append(resolved.get(i + 1));
        }
        String cmd = path + ":read-attribute(name=" + name + ")";
        return executeCli(cmd);
    }

    public JsonNode readChildrenNames(List<String> address, String childType) throws Exception {
        List<String> resolved = resolveAddress(address);
        StringBuilder path = new StringBuilder();
        for (int i = 0; i + 1 < resolved.size(); i += 2) {
            path.append("/").append(resolved.get(i)).append("=").append(resolved.get(i + 1));
        }
        String cmd = path + ":read-children-names(child-type=" + childType + ")";
        return executeCli(cmd);
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    public boolean isDomainMode() {
        Boolean cached = cachedDomainMode;
        if (cached != null && (System.currentTimeMillis() - domainModeCacheTime) < DOMAIN_MODE_CACHE_TTL_MS) {
            return cached;
        }
        try {
            JsonNode result = readAttribute(List.of(), "launch-type");
            cached = "DOMAIN".equalsIgnoreCase(result.path("result").asText(""));
        } catch (Exception e) {
            LOG.debugf("isDomainMode check failed: %s", e.getMessage());
            cached = false;
        }
        cachedDomainMode = cached;
        domainModeCacheTime = System.currentTimeMillis();
        return cached;
    }

    public void clearDomainModeCache() {
        cachedDomainMode = null;
        domainModeCacheTime = 0;
        cachedRunningServers = null;
        runningServersCacheTime = 0;
        cachedAllServers = null;
        allServersCacheTime = 0;
    }

    // Extract [host, server] pairs from a wildcard (/host=*/server=*) response, optionally
    // keeping only servers whose "result" (server-state) is "running".
    private List<String[]> parseServerListResponse(JsonNode res, boolean runningOnly) {
        List<String[]> servers = new ArrayList<>();
        JsonNode arr = res.path("result");
        if (!arr.isArray()) return servers;
        for (JsonNode item : arr) {
            if (!"success".equals(item.path("outcome").asText())) continue;
            if (runningOnly && !"running".equalsIgnoreCase(item.path("result").asText(""))) continue;
            String host = null, server = null;
            for (JsonNode a : item.path("address")) {
                if (a.has("host")) host = a.path("host").asText();
                if (a.has("server")) server = a.path("server").asText();
            }
            if (host != null && server != null) servers.add(new String[]{host, server});
        }
        return servers;
    }

    public List<String[]> getDomainServers() {
        List<String[]> cached = cachedAllServers;
        if (cached != null && (System.currentTimeMillis() - allServersCacheTime) < SERVERS_CACHE_TTL_MS) {
            return cached;
        }
        List<String[]> servers = new ArrayList<>();
        if (isDomainMode()) {
            try {
                // One wildcard call returns every configured server (with its host/server
                // address), replacing the previous N+1 read-children-names round-trips.
                JsonNode res = executeCli("/host=*/server-config=*:read-resource(include-runtime=false)");
                JsonNode arr = res.path("result");
                if (arr.isArray()) {
                    for (JsonNode item : arr) {
                        if (!"success".equals(item.path("outcome").asText())) continue;
                        String host = null, server = null;
                        for (JsonNode a : item.path("address")) {
                            if (a.has("host")) host = a.path("host").asText();
                            if (a.has("server-config")) server = a.path("server-config").asText();
                        }
                        if (host != null && server != null) servers.add(new String[]{host, server});
                    }
                }
            } catch (Exception e) {
                LOG.debugf("getDomainServers failed: %s", e.getMessage());
            }
        }
        cachedAllServers = servers;
        allServersCacheTime = System.currentTimeMillis();
        return servers;
    }

    public List<String[]> getRunningDomainServers() {
        List<String[]> cached = cachedRunningServers;
        if (cached != null && (System.currentTimeMillis() - runningServersCacheTime) < SERVERS_CACHE_TTL_MS) {
            return cached;
        }
        List<String[]> running = new ArrayList<>();
        if (isDomainMode()) {
            try {
                // One wildcard call returns every server's state, replacing the previous
                // ~N+hosts+1 serial CLI round-trips.
                JsonNode res = executeCli("/host=*/server=*:read-attribute(name=server-state)");
                running = parseServerListResponse(res, true);
            } catch (Exception e) {
                LOG.debugf("getRunningDomainServers failed: %s", e.getMessage());
            }
        }
        cachedRunningServers = running;
        runningServersCacheTime = System.currentTimeMillis();
        return running;
    }

    /**
     * Domain-mode fan-out replacement: perform ONE wildcard read
     * ({@code /host=*&#47;server=*&#47;<subAddress>:read-resource}) instead of looping a
     * per-server {@link #readResourceDirect} call for each running server.
     *
     * <p>Each item in a wildcard response has the same shape as a single
     * {@code readResourceDirect} response ({@code address}/{@code outcome}/{@code result}),
     * so callers can feed the returned node straight into their existing per-server parsing.
     *
     * @param subAddress address relative to {@code /host=X/server=Y} as key/value pairs,
     *                   e.g. {@code List.of("core-service","platform-mbean","type","memory")}
     * @return map of {@code "host/server"} label to that server's response node
     *         (only successful items are included); empty map on failure
     */
    public Map<String, JsonNode> readWildcardAcrossServers(List<String> subAddress,
                                                           boolean includeRuntime,
                                                           boolean recursive) throws Exception {
        StringBuilder path = new StringBuilder("/host=*/server=*");
        for (int i = 0; i + 1 < subAddress.size(); i += 2) {
            path.append("/").append(subAddress.get(i)).append("=").append(subAddress.get(i + 1));
        }
        String cmd = path + ":read-resource(include-runtime=" + includeRuntime
                + ",recursive=" + recursive + ")";
        JsonNode res = executeCli(cmd);
        Map<String, JsonNode> byServer = new LinkedHashMap<>();
        JsonNode arr = res.path("result");
        if (arr.isArray()) {
            for (JsonNode item : arr) {
                if (!"success".equals(item.path("outcome").asText())) continue;
                String host = null, server = null;
                for (JsonNode a : item.path("address")) {
                    if (a.has("host")) host = a.path("host").asText();
                    if (a.has("server")) server = a.path("server").asText();
                }
                if (host != null && server != null) byServer.put(host + "/" + server, item);
            }
        }
        return byServer;
    }

    public JsonNode readResourceDirect(List<String> address, boolean includeRuntime, boolean recursive) throws Exception {
        StringBuilder path = new StringBuilder();
        for (int i = 0; i + 1 < address.size(); i += 2) {
            path.append("/").append(address.get(i)).append("=").append(address.get(i + 1));
        }
        String cmd = path + ":read-resource(include-runtime=" + includeRuntime
                + ",recursive=" + recursive + ")";
        return executeCli(cmd);
    }
}
