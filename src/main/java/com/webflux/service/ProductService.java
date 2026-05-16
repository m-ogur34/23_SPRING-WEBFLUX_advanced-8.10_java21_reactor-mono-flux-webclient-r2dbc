package com.webflux.service;

import com.webflux.model.Product;
import com.webflux.repository.ProductRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/**
 * Reactive Service — Reactor Core desenleri:
 *   map         → dönüşüm (sync)
 *   flatMap     → async dönüşüm, paralel
 *   concatMap   → async dönüşüm, sıralı
 *   filter      → filtreleme
 *   switchIfEmpty → boş Mono/Flux'u değiştir
 *   onErrorResume → hata durumunda fallback
 *   zipWith     → iki Publisher'ı birleştir
 *   timeout     → süre aşımı
 *   retry       → hata sonrası tekrar dene
 *   cache       → sonucu önbelleğe al
 */
@Service
public class ProductService {

    private final ProductRepository repository;

    public ProductService(ProductRepository repository) {
        this.repository = repository;
    }

    // Temel CRUD — Mono/Flux döner, hiç block olmaz
    public Mono<Product> create(Product product) {
        return repository.save(product);
    }

    public Mono<Product> findById(Long id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new RuntimeException("Product not found: " + id)));
    }

    public Flux<Product> findAll() {
        return repository.findAll();
    }

    public Flux<Product> findByCategory(String category) {
        return repository.findByCategoryAndActiveTrue(category);
    }

    public Mono<Product> update(Long id, Product updated) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new RuntimeException("Not found: " + id)))
                .map(existing -> {
                    existing.setName(updated.getName());
                    existing.setPrice(updated.getPrice());
                    existing.setStock(updated.getStock());
                    return existing;
                })
                .flatMap(repository::save);
    }

    public Mono<Void> delete(Long id) {
        return repository.deleteById(id);
    }

    // ── Reaktif Desenler ──────────────────────────────────────────────────────

    // flatMap — async dönüşüm (her eleman için DB call)
    public Flux<Product> findByCategories(List<String> categories) {
        return Flux.fromIterable(categories)
                .flatMap(repository::findByCategoryAndActiveTrue);  // paralel, hızlı
        // NOT: concatMap kullan → sıra önemliyse
    }

    // zip — iki Mono'yu birleştir
    public Mono<String> getProductSummary(Long id) {
        Mono<Product> productMono = repository.findById(id);
        Mono<Long> countMono = repository.countByCategory("electronics");

        return Mono.zip(productMono, countMono)
                .map(tuple -> "Product: %s, Category total: %d"
                        .formatted(tuple.getT1().getName(), tuple.getT2()));
    }

    // onErrorResume — fallback
    public Flux<Product> findAffordable(String category, BigDecimal maxPrice) {
        return repository.findAffordableByCategory(category, maxPrice)
                .onErrorResume(ex -> {
                    System.err.println("Query failed, fallback to findAll: " + ex.getMessage());
                    return repository.findByActiveTrue();
                });
    }

    // timeout + retry
    public Mono<Product> findByIdWithRetry(Long id) {
        return repository.findById(id)
                .timeout(Duration.ofSeconds(5))
                .retry(3)
                .switchIfEmpty(Mono.error(new RuntimeException("Not found after retries: " + id)));
    }

    // publishOn — blocking işlemi başka thread'e taşı
    public Mono<String> processWithBlocking(Long id) {
        return repository.findById(id)
                .publishOn(Schedulers.boundedElastic())  // blocking-safe thread pool
                .map(product -> {
                    // Simulated blocking operation (file I/O, legacy lib)
                    try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    return "Processed: " + product.getName();
                });
    }

    // cache — bir Mono'yu birden fazla subscriber paylaşır
    private final Mono<List<Product>> cachedProducts = repository.findByActiveTrue()
            .collectList()
            .cache(Duration.ofMinutes(5));

    public Mono<List<Product>> getCachedActiveProducts() {
        return cachedProducts;
    }

    // SSE stream — Server-Sent Events için Flux
    public Flux<Product> streamNewProducts() {
        return repository.findAll()
                .delayElements(Duration.ofMillis(500));  // 500ms aralıklarla gönder
    }

    // collectList, buffer, window
    public Mono<Long> countByCategory(String category) {
        return repository.countByCategory(category);
    }

    // reduce — Flux → tek değer
    public Mono<BigDecimal> totalInventoryValue() {
        return repository.findByActiveTrue()
                .map(p -> p.getPrice().multiply(BigDecimal.valueOf(p.getStock())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
