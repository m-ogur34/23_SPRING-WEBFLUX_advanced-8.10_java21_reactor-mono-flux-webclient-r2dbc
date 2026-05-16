package com.webflux.repository;

import com.webflux.model.Product;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

/**
 * R2dbcRepository — Spring Data R2DBC (non-blocking DB access)
 * Tüm metodlar Mono/Flux döner — hiç thread block olmaz
 */
public interface ProductRepository extends R2dbcRepository<Product, Long> {

    // Method naming → Flux (birden fazla)
    Flux<Product> findByCategory(String category);
    Flux<Product> findByActiveTrue();
    Flux<Product> findByPriceBetween(BigDecimal min, BigDecimal max);
    Flux<Product> findByCategoryAndActiveTrue(String category);

    // Mono (tek sonuç veya boş)
    Mono<Product> findByName(String name);

    // Custom query — Flux
    @Query("SELECT * FROM products WHERE category = :category AND price <= :maxPrice AND active = true")
    Flux<Product> findAffordableByCategory(String category, BigDecimal maxPrice);

    Mono<Long> countByCategory(String category);

    Mono<Boolean> existsByName(String name);
}
