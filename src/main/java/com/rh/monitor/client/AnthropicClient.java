package com.rh.monitor.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class AnthropicClient {

    private static final Logger LOG = Logger.getLogger(AnthropicClient.class);

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 10_000;

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient httpClient;

    {
        HttpClient client;
        try {
            TrustManager[] trustAll = { new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                public void checkClientTrusted(X509Certificate[] c, String t) {}
                public void checkServerTrusted(X509Certificate[] c, String t) {}
            }};
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAll, new java.security.SecureRandom());
            client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .sslContext(sc)
                    .build();
        } catch (Exception e) {
            client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .build();
        }
        httpClient = client;
    }

    private Map<String, String> fileProps;

    private Map<String, String> loadFileProps() {
        if (fileProps != null) return fileProps;
        fileProps = new HashMap<>();
        for (Path p : new Path[]{
                Path.of(System.getProperty("user.dir"), "set-env.txt"),
                Path.of(System.getProperty("user.home"), "set-env.txt")}) {
            if (Files.isRegularFile(p)) {
                try {
                    for (String line : Files.readAllLines(p)) {
                        String t = line.strip();
                        if (t.isEmpty() || t.startsWith("#")) continue;
                        String c = t.startsWith("export ") ? t.substring(7).strip() : t;
                        int eq = c.indexOf('=');
                        if (eq > 0) {
                            String k = c.substring(0, eq).strip();
                            String v = c.substring(eq + 1).strip();
                            if ((v.startsWith("\"") && v.endsWith("\""))
                                    || (v.startsWith("'") && v.endsWith("'")))
                                v = v.substring(1, v.length() - 1);
                            fileProps.put(k, v);
                        }
                    }
                } catch (IOException ignored) {}
                break;
            }
        }
        return fileProps;
    }

    private String env(String key, String def) {
        String v = System.getenv(key);
        if (v != null && !v.isBlank()) return v;
        String fp = loadFileProps().get(key);
        if (fp != null && !fp.isBlank()) return fp;
        return def;
    }

    public String chat(String systemPrompt, String userMessage) throws Exception {
        LOG.debug("Sending chat request to Anthropic API");
        String apiKey = env("ANTHROPIC_API_KEY", null);
        if (apiKey == null) {
            throw new IllegalStateException(
                    "ANTHROPIC_API_KEY environment variable is not set. "
                    + "Please set it in your environment before using AI analysis.");
        }

        String model = env("ANTHROPIC_MODEL", "claude-sonnet-4-6");
        String baseUrl = env("ANTHROPIC_BASE_URL", null);

        boolean isProxy = baseUrl != null && (baseUrl.contains("/models/") || baseUrl.contains(":streamRawPredict") || baseUrl.contains(":rawPredict"));
        String endpoint = isProxy ? baseUrl : (baseUrl != null ? baseUrl : "https://api.anthropic.com") + "/v1/messages";

        Map<String, Object> body = new LinkedHashMap<>();
        if (isProxy) {
            body.put("anthropic_version", "vertex-2023-10-16");
        } else {
            body.put("model", model);
        }
        body.put("max_tokens", 4096);
        body.put("temperature", 0.3);
        body.put("system", systemPrompt);
        body.put("messages", List.of(Map.of("role", "user", "content", userMessage)));

        String json = mapper.writeValueAsString(body);
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(120))
                .POST(HttpRequest.BodyPublishers.ofString(json));

        if (isProxy) {
            reqBuilder.header("Authorization", "Bearer " + apiKey);
        } else {
            reqBuilder.header("x-api-key", apiKey);
            reqBuilder.header("anthropic-version", "2023-06-01");
        }

        HttpRequest request = reqBuilder.build();

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 429) {
                if (attempt < MAX_RETRIES) {
                    LOG.warnf("Anthropic API rate limited, retrying in %dms (attempt %d/%d)", RETRY_DELAY_MS * (attempt + 1), attempt + 1, MAX_RETRIES);
                    Thread.sleep(RETRY_DELAY_MS * (attempt + 1));
                    continue;
                }
                throw new RateLimitException("AI rate limit reached after " + (MAX_RETRIES + 1) + " attempts. Please try again in a few seconds.");
            }

            if (response.statusCode() != 200) {
                LOG.errorf("Anthropic API error: HTTP %d", response.statusCode());
                throw new RuntimeException("Anthropic API error (HTTP " + response.statusCode() + "): " + response.body());
            }

            JsonNode root = mapper.readTree(response.body());
            JsonNode content = root.path("content");
            if (content.isArray() && !content.isEmpty()) {
                return content.get(0).path("text").asText("");
            }
            return "";
        }

        throw new RateLimitException("AI rate limit reached. Please try again shortly.");
    }

    public static class RateLimitException extends Exception {
        public RateLimitException(String message) {
            super(message);
        }
    }
}
