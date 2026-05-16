package com.webflux.router;

import com.webflux.handler.ProductHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.*;

/**
 * Functional Router — @RestController yerine
 * Route tanımları açık, test edilmesi kolay
 */
@Configuration
public class ProductRouter {

    @Bean
    public RouterFunction<ServerResponse> routes(ProductHandler handler) {
        return RouterFunctions.route()
                .GET("/api/reactive/products", handler::getAll)
                .GET("/api/reactive/products/stream", handler::stream)         // SSE
                .GET("/api/reactive/products/value", handler::totalValue)
                .GET("/api/reactive/products/affordable", handler::affordable)
                .GET("/api/reactive/products/category/{category}", handler::getByCategory)
                .GET("/api/reactive/products/{id}", handler::getById)
                .POST("/api/reactive/products", handler::create)
                .PUT("/api/reactive/products/{id}", handler::update)
                .DELETE("/api/reactive/products/{id}", handler::delete)
                .build();
    }
}
