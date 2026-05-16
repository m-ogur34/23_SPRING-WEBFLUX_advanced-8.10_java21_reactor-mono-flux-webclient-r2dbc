package com.webflux.handler;

import com.webflux.model.Product;
import com.webflux.service.ProductService;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

/**
 * Functional Endpoints — Handler + Router
 * @RestController yerine: daha fonksiyonel, test edilmesi kolay
 *
 * ServerRequest  → gelen istek
 * ServerResponse → giden yanıt
 * Mono<ServerResponse> → reactive response
 */
@Component
public class ProductHandler {

    private final ProductService service;

    public ProductHandler(ProductService service) {
        this.service = service;
    }

    public Mono<ServerResponse> getAll(ServerRequest request) {
        return ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(service.findAll(), Product.class);
    }

    public Mono<ServerResponse> getById(ServerRequest request) {
        Long id = Long.parseLong(request.pathVariable("id"));
        return service.findById(id)
                .flatMap(product ->
                        ServerResponse.ok().bodyValue(product))
                .onErrorResume(RuntimeException.class, ex ->
                        ServerResponse.notFound().build());
    }

    public Mono<ServerResponse> create(ServerRequest request) {
        return request.bodyToMono(Product.class)
                .flatMap(service::create)
                .flatMap(saved ->
                        ServerResponse.ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .bodyValue(saved));
    }

    public Mono<ServerResponse> update(ServerRequest request) {
        Long id = Long.parseLong(request.pathVariable("id"));
        return request.bodyToMono(Product.class)
                .flatMap(body -> service.update(id, body))
                .flatMap(updated ->
                        ServerResponse.ok().bodyValue(updated))
                .onErrorResume(RuntimeException.class, ex ->
                        ServerResponse.notFound().build());
    }

    public Mono<ServerResponse> delete(ServerRequest request) {
        Long id = Long.parseLong(request.pathVariable("id"));
        return service.delete(id)
                .then(ServerResponse.noContent().build());
    }

    public Mono<ServerResponse> getByCategory(ServerRequest request) {
        String category = request.pathVariable("category");
        return ServerResponse.ok()
                .body(service.findByCategory(category), Product.class);
    }

    // Server-Sent Events (SSE) streaming
    public Mono<ServerResponse> stream(ServerRequest request) {
        return ServerResponse.ok()
                .contentType(MediaType.TEXT_EVENT_STREAM)
                .body(service.streamNewProducts(), Product.class);
    }

    public Mono<ServerResponse> totalValue(ServerRequest request) {
        return service.totalInventoryValue()
                .flatMap(total ->
                        ServerResponse.ok().bodyValue(java.util.Map.of("total", total)));
    }

    public Mono<ServerResponse> affordable(ServerRequest request) {
        String category = request.queryParam("category").orElse("electronics");
        BigDecimal maxPrice = request.queryParam("maxPrice")
                .map(BigDecimal::new).orElse(new BigDecimal("50000"));
        return ServerResponse.ok()
                .body(service.findAffordable(category, maxPrice), Product.class);
    }
}
