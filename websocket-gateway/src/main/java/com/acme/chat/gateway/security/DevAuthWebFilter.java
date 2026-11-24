package com.acme.chat.gateway.security;

import java.security.Principal;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * DEV-ONLY handshake authentication: ?token=dev-{userId}.
 * Replace with validation of a short-lived, device-bound JWT "WS ticket" issued by auth-service
 * (verify signature locally against a cached JWKS, no remote call per connect).
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class DevAuthWebFilter implements WebFilter {

    public record UserPrincipal(String name) implements Principal {
        @Override public String getName() { return name; }
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!exchange.getRequest().getURI().getPath().startsWith("/ws/")) {
            return chain.filter(exchange);
        }
        String token = exchange.getRequest().getQueryParams().getFirst("token");
        if (token == null || !token.startsWith("dev-")) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }
        String userId = token.substring(4);
        try {
            Long.parseLong(userId);
        } catch (NumberFormatException e) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }
        Principal principal = new UserPrincipal(userId);
        return chain.filter(exchange.mutate().principal(Mono.just(principal)).build());
    }
}
