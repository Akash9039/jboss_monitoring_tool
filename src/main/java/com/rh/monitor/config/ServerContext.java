package com.rh.monitor.config;

import jakarta.enterprise.context.RequestScoped;

@RequestScoped
public class ServerContext {

    private String host;
    private String server;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public String getServer() { return server; }
    public void setServer(String server) { this.server = server; }

    public boolean hasContext() {
        return host != null && !host.isBlank() && server != null && !server.isBlank();
    }
}
