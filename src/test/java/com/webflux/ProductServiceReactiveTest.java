package com.webflux;

import com.webflux.model.Product;
import com.webflux.repository.ProductRepository;
import com.webflux.service.ProductService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

/**
 * StepVerifier — reaktif stream'leri test etmek için Reactor Test araç seti
 *
 * Kullanım:
 *   StepVerifier.create(publisher)
 *     .expectNext(value)
 *     .expectNextMatches(predicate)
 *     .expectError(ExceptionClass.class)
 *     .verifyComplete()
 *     .verify(Duration)   → timeout
 */
@SpringBootTest
class ProductServiceReactiveTest {

    @Autowired
    ProductService service;

    @Autowired
    ProductRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll().block();
    }

    @Test
    void create_emitsProduct() {
        Product product = new Product("Laptop", "electronics", new BigDecimal("25000"), 10);

        StepVerifier.create(service.create(product))
                .expectNextMatches(p -> p.getId() != null && "Laptop".equals(p.getName()))
                .verifyComplete();
    }

    @Test
    void findById_emitsProduct() {
        Product saved = service.create(
                new Product("Phone", "electronics", new BigDecimal("15000"), 5)).block();

        StepVerifier.create(service.findById(saved.getId()))
                .expectNextMatches(p -> "Phone".equals(p.getName()))
                .verifyComplete();
    }

    @Test
    void findById_notFound_emitsError() {
        StepVerifier.create(service.findById(999L))
                .expectErrorMatches(ex ->
                        ex instanceof RuntimeException &&
                        ex.getMessage().contains("999"))
                .verify();
    }

    @Test
    void findAll_emitsAllProducts() {
        service.create(new Product("A", "cat1", new BigDecimal("100"), 1)).block();
        service.create(new Product("B", "cat2", new BigDecimal("200"), 2)).block();

        StepVerifier.create(service.findAll())
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    void findByCategory_filtersCorrectly() {
        service.create(new Product("Laptop", "electronics", new BigDecimal("25000"), 5)).block();
        service.create(new Product("Shirt", "clothing", new BigDecimal("500"), 20)).block();

        StepVerifier.create(service.findByCategory("electronics"))
                .expectNextMatches(p -> "electronics".equals(p.getCategory()))
                .verifyComplete();
    }

    @Test
    void totalInventoryValue_sumsCorrectly() {
        service.create(new Product("X", "test", new BigDecimal("100"), 3)).block();  // 300
        service.create(new Product("Y", "test", new BigDecimal("200"), 2)).block();  // 400

        StepVerifier.create(service.totalInventoryValue())
                .expectNextMatches(total ->
                        total.compareTo(new BigDecimal("700")) == 0)
                .verifyComplete();
    }

    @Test
    void update_returnsUpdatedProduct() {
        Product saved = service.create(
                new Product("Old", "cat", new BigDecimal("100"), 1)).block();

        Product updated = new Product("New", "cat", new BigDecimal("200"), 2);

        StepVerifier.create(service.update(saved.getId(), updated))
                .expectNextMatches(p -> "New".equals(p.getName()) &&
                                        p.getPrice().compareTo(new BigDecimal("200")) == 0)
                .verifyComplete();
    }

    @Test
    void delete_completesEmpty() {
        Product saved = service.create(
                new Product("ToDelete", "cat", new BigDecimal("100"), 1)).block();

        StepVerifier.create(service.delete(saved.getId()))
                .verifyComplete();

        StepVerifier.create(repository.findById(saved.getId()))
                .verifyComplete(); // Mono.empty()
    }

    @Test
    void findByCategories_flatMap_parallel() {
        service.create(new Product("L1", "electronics", new BigDecimal("1000"), 1)).block();
        service.create(new Product("B1", "books", new BigDecimal("50"), 1)).block();

        Flux<Product> result = service.findByCategories(java.util.List.of("electronics", "books"));

        StepVerifier.create(result)
                .expectNextCount(2)
                .verifyComplete();
    }
}
