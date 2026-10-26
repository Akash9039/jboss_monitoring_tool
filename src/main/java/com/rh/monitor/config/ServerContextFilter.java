package com.rh.monitor.config;

import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;

@Provider
public class ServerContextFilter implements ContainerRequestFilter {

    @Inject
    ServerContext serverContext;

    @Override
    public void filter(ContainerRequestContext ctx) {
        String host = ctx.getUriInfo().getQueryParameters().getFirst("host");
        String server = ctx.getUriInfo().getQueryParameters().getFirst("server");
        if (host != null && !host.isBlank()) serverContext.setHost(host);
        if (server != null && !server.isBlank()) serverContext.setServer(server);
    }
}
