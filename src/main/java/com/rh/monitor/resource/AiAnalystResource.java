package com.rh.monitor.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rh.monitor.client.AnthropicClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.jboss.logging.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Path("/jboss")
@ApplicationScoped
public class AiAnalystResource {

    private static final Logger LOG = Logger.getLogger(AiAnalystResource.class);

    @Inject AnthropicClient ai;
    @Inject ServerResource serverResource;
    @Inject MemoryResource memoryResource;
    @Inject ThreadResource threadResource;
    @Inject DatasourceResource datasourceResource;
    @Inject DeploymentResource deploymentResource;
    @Inject TransactionResource transactionResource;
    @Inject WebResource webResource;
    @Inject ServerLogResource serverLogResource;

    private final ObjectMapper mapper = new ObjectMapper();

    @GET
    @Path("/health")
    @Produces(MediaType.TEXT_PLAIN)
    public Response healthCheck() {
        LOG.info("AI health check requested");
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("server_status", serverResource.getServerStatus());
            data.put("memory", memoryResource.getMemoryStats());
            data.put("gc", memoryResource.getGcStats());
            data.put("threads", threadResource.getThreadStats());
            data.put("thread_pools", threadResource.getThreadPools());
            data.put("datasources", datasourceResource.getDatasources());
            data.put("transactions", transactionResource.getTransactionStats());
            data.put("web", webResource.getWebStats());
            data.put("system", serverResource.getSystemInfo());

            String result = analyze(
                    "You are a JBoss EAP Server Health Analyst. Analyze the provided server monitoring data and provide:\n"
                    + "1. Overall health grade (HEALTHY / WARNING / CRITICAL)\n"
                    + "2. Server status and uptime\n"
                    + "3. Memory usage with heap percentage and GC pressure\n"
                    + "4. Thread health - count, deadlocks, stuck threads\n"
                    + "5. Datasource connection pool utilization\n"
                    + "6. Transaction abort rate\n"
                    + "7. Web request throughput and errors\n"
                    + "8. Actionable recommendations ranked by severity\n"
                    + "Be concise. Use bullet points. Flag CRITICAL items first.",
                    mapper.writeValueAsString(data));
            return Response.ok(result).build();
        } catch (AnthropicClient.RateLimitException e) {
            return Response.status(429).entity(e.getMessage()).build();
        } catch (Exception e) {
            return handleError("health check", e);
        }
    }

    @GET
    @Path("/memory")
    @Produces(MediaType.TEXT_PLAIN)
    public Response analyzeMemory() {
        LOG.info("AI memory analysis requested");
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("memory", memoryResource.getMemoryStats());
            data.put("memory_pools", memoryResource.getMemoryPools());
            data.put("gc", memoryResource.getGcStats());

            String result = analyze(
                    "You are a JBoss EAP Memory Analyst. Analyze the provided JVM memory data and report:\n"
                    + "- Heap usage percentage and trend assessment\n"
                    + "- Memory pool utilization (flag any pool > 80%)\n"
                    + "- GC frequency and pause time analysis\n"
                    + "- Risk of OutOfMemoryError\n"
                    + "- Recommendations (heap size tuning, GC algorithm change, leak investigation)\n"
                    + "Under 10 lines. Be specific with numbers.",
                    mapper.writeValueAsString(data));
            return Response.ok(result).build();
        } catch (AnthropicClient.RateLimitException e) {
            return Response.status(429).entity(e.getMessage()).build();
        } catch (Exception e) {
            return handleError("memory analysis", e);
        }
    }

    @GET
    @Path("/threads")
    @Produces(MediaType.TEXT_PLAIN)
    public Response analyzeThreads() {
        LOG.info("AI thread analysis requested");
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("threads", threadResource.getThreadStats());
            data.put("thread_dump", threadResource.getThreadDump(50));
            data.put("thread_pools", threadResource.getThreadPools());

            String result = analyze(
                    "You are a JBoss EAP Thread Analyst. Analyze the provided thread data and report:\n"
                    + "- Current thread count vs peak\n"
                    + "- Any deadlocks (CRITICAL if found)\n"
                    + "- Thread state distribution (RUNNABLE, BLOCKED, WAITING, TIMED_WAITING)\n"
                    + "- Stuck or long-blocked threads\n"
                    + "- Thread pool saturation (queue size, rejections)\n"
                    + "- Root cause and fix recommendation\n"
                    + "Under 10 lines.",
                    mapper.writeValueAsString(data));
            return Response.ok(result).build();
        } catch (AnthropicClient.RateLimitException e) {
            return Response.status(429).entity(e.getMessage()).build();
        } catch (Exception e) {
            return handleError("thread analysis", e);
        }
    }

    @GET
    @Path("/datasources")
    @Produces(MediaType.TEXT_PLAIN)
    public Response analyzeDatasources() {
        LOG.info("AI datasource analysis requested");
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("datasources", datasourceResource.getDatasources());

            String result = analyze(
                    "You are a JBoss EAP Datasource Analyst. Analyze the provided datasource data and report:\n"
                    + "- Pool utilization (active / max) for each datasource\n"
                    + "- Wait count and average wait time\n"
                    + "- Timed-out connections\n"
                    + "- Creation/destruction rate\n"
                    + "- Long-running query indicators\n"
                    + "- Health grade and recommended pool size adjustments\n"
                    + "Use a table for multi-datasource comparison. Flag any high usage times or slow queries.",
                    mapper.writeValueAsString(data));
            return Response.ok(result).build();
        } catch (AnthropicClient.RateLimitException e) {
            return Response.status(429).entity(e.getMessage()).build();
        } catch (Exception e) {
            return handleError("datasource analysis", e);
        }
    }

    @GET
    @Path("/deployments")
    @Produces(MediaType.TEXT_PLAIN)
    public Response analyzeDeployments() {
        LOG.info("AI deployment analysis requested");
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("deployments", deploymentResource.getDeploymentStatus());
            data.put("web", webResource.getWebStats());
            data.put("sessions", webResource.getHttpSessions());

            String result = analyze(
                    "You are a JBoss EAP Deployment Analyst. Analyze the provided deployment data and report:\n"
                    + "- Enabled/disabled state for each deployment\n"
                    + "- Runtime status\n"
                    + "- Active sessions\n"
                    + "- Request count and error rate per servlet\n"
                    + "- Any failed or degraded deployments (CRITICAL)",
                    mapper.writeValueAsString(data));
            return Response.ok(result).build();
        } catch (AnthropicClient.RateLimitException e) {
            return Response.status(429).entity(e.getMessage()).build();
        } catch (Exception e) {
            return handleError("deployment analysis", e);
        }
    }

    @GET
    @Path("/transactions")
    @Produces(MediaType.TEXT_PLAIN)
    public Response analyzeTransactions() {
        LOG.info("AI transaction analysis requested");
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("transactions", transactionResource.getTransactionStats());

            String result = analyze(
                    "You are a JBoss EAP Transaction Analyst. Analyze the provided transaction data and report:\n"
                    + "- Total transactions (committed + aborted)\n"
                    + "- Abort rate percentage\n"
                    + "- Timed-out transaction count\n"
                    + "- In-flight transactions\n"
                    + "- Application vs resource vs system rollback breakdown\n"
                    + "- Health grade and recommendations\n"
                    + "Under 8 lines.",
                    mapper.writeValueAsString(data));
            return Response.ok(result).build();
        } catch (AnthropicClient.RateLimitException e) {
            return Response.status(429).entity(e.getMessage()).build();
        } catch (Exception e) {
            return handleError("transaction analysis", e);
        }
    }

    @GET
    @Path("/diagnose")
    @Produces(MediaType.TEXT_PLAIN)
    public Response diagnoseSlowness() {
        LOG.info("AI slowness diagnosis requested");
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("health", serverResource.getHealthReport());
            data.put("memory", memoryResource.getMemoryStats());
            data.put("threads", threadResource.getThreadStats());
            data.put("datasources", datasourceResource.getDatasources());
            data.put("transactions", transactionResource.getTransactionStats());
            data.put("web", webResource.getWebStats());
            data.put("system", serverResource.getSystemInfo());

            String result = analyze(
                    "You are a JBoss EAP Performance Analyst. Based on the provided comprehensive monitoring data, "
                    + "identify the single biggest performance bottleneck and recommend a fix.\n"
                    + "Be specific - name the subsystem, the metric, and the threshold breached.\n"
                    + "Output should be under 5 sentences.",
                    mapper.writeValueAsString(data));
            return Response.ok(result).build();
        } catch (AnthropicClient.RateLimitException e) {
            return Response.status(429).entity(e.getMessage()).build();
        } catch (Exception e) {
            return handleError("slowness diagnosis", e);
        }
    }

    @GET
    @Path("/ask")
    @Produces(MediaType.TEXT_PLAIN)
    public Response ask(@QueryParam("q") String question) {
        LOG.infof("AI ask endpoint called with question: %s", question);
        if (question == null || question.isBlank()) {
            return Response.status(400).entity("Please provide a question using the 'q' query parameter.").build();
        }
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("health", serverResource.getHealthReport());
            data.put("memory", memoryResource.getMemoryStats());
            data.put("threads", threadResource.getThreadStats());
            data.put("datasources", datasourceResource.getDatasources());
            data.put("transactions", transactionResource.getTransactionStats());
            data.put("web", webResource.getWebStats());
            data.put("system", serverResource.getSystemInfo());

            String userMessage = "Question: " + question + "\n\nServer monitoring data:\n" + mapper.writeValueAsString(data);

            String result = analyze(
                    "You are a JBoss EAP analyst. Answer the user's specific question about the JBoss server "
                    + "using the provided monitoring data. Answer directly and concisely.",
                    userMessage);
            return Response.ok(result).build();
        } catch (AnthropicClient.RateLimitException e) {
            return Response.status(429).entity(e.getMessage()).build();
        } catch (Exception e) {
            return handleError("AI query", e);
        }
    }

    /**
     * Analyzes the actual EAP server-log entries with the AI. Used by the Server Logs page
     * ("Analyze Errors" / "Deep Analysis"). Unlike /ask, this reads the real server.log ERROR
     * and WARN entries from disk and feeds their content to the model directly — there is no
     * tool-use round-trip, so the model never has to (and must not) invent a tool call.
     *
     * @param level filter (e.g. ERROR, WARN); blank/null means WARN+ERROR+FATAL
     * @param depth "deep" for a thorough analysis, otherwise a concise errors-focused summary
     */
    @GET
    @Path("/analyze-logs")
    @Produces(MediaType.TEXT_PLAIN)
    @SuppressWarnings("unchecked")
    public Response analyzeLogs(@QueryParam("level") String level,
                                @QueryParam("depth") String depth) {
        boolean deep = depth != null && depth.equalsIgnoreCase("deep");
        LOG.infof("AI log analysis requested (level=%s, deep=%s)", level, deep);
        try {
            int maxEntries = deep ? 400 : 150;
            Map<String, Object> logs = serverLogResource.getServerLogs(level, maxEntries);

            String status = String.valueOf(logs.get("status"));
            List<Map<String, Object>> entries =
                    (List<Map<String, Object>>) logs.getOrDefault("entries", java.util.List.of());

            if (!"ok".equals(status) || entries.isEmpty()) {
                String msg = logs.get("message") != null ? String.valueOf(logs.get("message"))
                        : "No ERROR or WARN entries were found in the server log"
                          + (level != null && !level.isBlank() ? " for level " + level : "")
                          + ". Nothing to analyze.";
                return Response.ok("**" + msg + "**").build();
            }

            Map<String, Object> levelSummary =
                    (Map<String, Object>) logs.getOrDefault("level_summary", java.util.Map.of());
            List<Map<String, Object>> kcs =
                    (List<Map<String, Object>>) logs.getOrDefault("kcs_suggestions", java.util.List.of());

            StringBuilder sb = new StringBuilder();
            sb.append("Server log file: ").append(logs.getOrDefault("log_file", "server.log")).append('\n');
            sb.append("Level summary: ").append(levelSummary).append('\n');
            sb.append("Total matching entries: ").append(logs.getOrDefault("total", entries.size()))
              .append(" (showing ").append(entries.size()).append(")\n");

            if (!kcs.isEmpty()) {
                sb.append("\nDistinct JBoss/WFLY error codes seen (code x count):\n");
                for (Map<String, Object> k : kcs) {
                    sb.append("  ").append(k.get("code")).append(" x ").append(k.get("count"))
                      .append(" [").append(k.get("level")).append("] ")
                      .append(k.get("message")).append('\n');
                }
            }

            sb.append("\nLog entries (most recent first):\n");
            for (Map<String, Object> e : entries) {
                sb.append('[').append(e.getOrDefault("timestamp", "")).append("] ")
                  .append(e.getOrDefault("level", "")).append(' ');
                Object srv = e.get("server_name");
                if (srv != null) sb.append('(').append(srv).append(") ");
                Object logger = e.get("logger");
                if (logger != null) sb.append('[').append(logger).append("] ");
                String message = String.valueOf(e.getOrDefault("message", ""));
                if (message.length() > 1000) message = message.substring(0, 1000) + " …[truncated]";
                sb.append(message).append('\n');
            }

            String systemPrompt = deep
                    ? "You are a JBoss EAP 8 log analyst. You are given the ERROR and WARN entries "
                      + "from a live server.log. Produce a thorough root-cause analysis:\n"
                      + "1. Group the entries by underlying problem (not one bullet per line).\n"
                      + "2. For each group: the error/WFLY code, what it means, how many times it "
                      + "occurred, the likely root cause, and the impact on the application.\n"
                      + "3. Correlate related entries (e.g. a connection failure followed by "
                      + "transaction rollbacks) into a single narrative where they belong together.\n"
                      + "4. Call out anything that looks like a recurring or escalating problem.\n"
                      + "Do NOT invent log lines or tools; analyze only what is provided."
                    : "You are a JBoss EAP 8 log analyst. You are given the ERROR and WARN entries "
                      + "from a live server.log. Give a concise, prioritized analysis:\n"
                      + "1. For each unique error/WFLY code, explain what it means and its likely cause.\n"
                      + "2. Note how many times each occurred and which are most severe.\n"
                      + "3. Focus on ERROR/FATAL first, then notable WARN patterns.\n"
                      + "Do NOT invent log lines or tools; analyze only what is provided.";

            String result = analyze(systemPrompt, sb.toString());
            return Response.ok(result).build();
        } catch (AnthropicClient.RateLimitException e) {
            return Response.status(429).entity(e.getMessage()).build();
        } catch (Exception e) {
            return handleError("log analysis", e);
        }
    }

    /**
     * Appended to every AI prompt so each page's AI analysis always ends with a concrete,
     * actionable Resolution for the issues it found — not just an assessment.
     */
    private static final String RESOLUTION_INSTRUCTION =
            "\n\nIMPORTANT: End your response with a clearly labelled section titled "
            + "\"## Resolution\". Under it, give concrete step-by-step actions to resolve the "
            + "issue(s) you identified, ordered by priority. Where relevant, include the exact "
            + "JBoss CLI command, configuration change, or JVM flag to apply. If nothing is "
            + "wrong, state under \"## Resolution\" that no action is needed and list the key "
            + "metrics to keep watching. Keep the Resolution practical and specific.";

    /**
     * Runs the analysis through the AI client with the shared Resolution instruction appended
     * to the system prompt, so every analysis tab returns a resolution alongside its findings.
     */
    private String analyze(String systemPrompt, String data) throws Exception {
        return ai.chat(systemPrompt + RESOLUTION_INSTRUCTION, data);
    }

    private Response handleError(String operation, Exception e) {
        LOG.errorf(e, "AI %s failed", operation);
        return Response.status(500)
                .entity("AI " + operation + " failed: " + e.getMessage())
                .build();
    }
}
