# 23 — Spring WebFlux

**Difficulty:** Advanced (8/10) · **Java 21** · **Spring Boot 3.2.5**

Reactive programming — Project Reactor (Mono/Flux), R2DBC non-blocking DB, WebClient, functional endpoints ve StepVerifier ile test.

---

## Reactive vs Blocking

```
Blocking (Spring MVC)           Reactive (WebFlux)
──────────────────────          ──────────────────────
Thread pool (200 thread)        Event loop (CPU core sayısı)
1 request → 1 thread            1 thread → N request
Thread DB beklerken BLOCK       Callback/signal ile devam
Yüksek concurrency → OOM        Yüksek concurrency → OK
Öğrenmesi kolay                 Öğrenmesi zor (Mono/Flux)
```

**Kullanım kararı:** I/O-bound + yüksek concurrency → WebFlux. CPU-bound + az concurrency → MVC yeterli.

---

## Mono ve Flux

```java
// Mono — 0 veya 1 eleman
Mono<Product> product = repository.findById(1L);
Mono<Void> deleted = repository.deleteById(1L);
Mono.just("value");           // var olan değer
Mono.empty();                 // boş
Mono.error(new RuntimeException()); // hata

// Flux — 0..N eleman
Flux<Product> all = repository.findAll();
Flux.fromIterable(list);      // koleksiyondan
Flux.range(1, 10);            // 1-10 arası int
Flux.interval(Duration.ofSeconds(1)); // her sn bir Long
```

---

## Temel Operatörler

### Dönüşüm
```java
// map — sync dönüşüm (değer hazır)
mono.map(product -> product.getName());

// flatMap — async dönüşüm (başka Mono/Flux döner), PARALEL
flux.flatMap(category -> repository.findByCategory(category));

// concatMap — async dönüşüm, SIRALI (sıra önemliyse)
flux.concatMap(id -> repository.findById(id));
```

### Filtreleme
```java
flux.filter(p -> p.isActive())
    .take(10)           // max 10 al
    .skip(5)            // ilk 5'i atla
    .distinct(Product::getCategory)
```

### Hata Yönetimi
```java
mono.onErrorReturn(defaultProduct)              // hata → varsayılan değer
    .onErrorResume(ex -> fallback())            // hata → başka Publisher
    .onErrorMap(ex -> new AppException(ex))     // hata dönüşümü
    .doOnError(ex -> log.error("...", ex))      // hata side effect
```

### Birleştirme
```java
// zip — iki Publisher'ı eş zamanlı çalıştır, ikisi de bitmesini bekle
Mono.zip(productMono, userMono)
    .map(tuple -> tuple.getT1().getName() + " by " + tuple.getT2().getName());

// merge — iki Flux'u birleştir (sırası önemli değil)
Flux.merge(flux1, flux2);

// concat — sıralı birleştir (flux1 biter, flux2 başlar)
Flux.concat(flux1, flux2);
```

### Boşluk Kontrolü
```java
mono.switchIfEmpty(Mono.error(new NotFoundException("...")))
mono.defaultIfEmpty(defaultValue)
```

### Timeout ve Retry
```java
mono.timeout(Duration.ofSeconds(5))
    .retry(3)
    .onErrorResume(TimeoutException.class, ex -> fallback());
```

---

## R2DBC — Non-blocking DB

```java
// JPA yerine R2DBC — hiç thread block olmaz
public interface ProductRepository extends R2dbcRepository<Product, Long> {
    Flux<Product> findByCategory(String category);   // → Flux<Product>
    Mono<Product> findByName(String name);           // → Mono<Product>

    @Query("SELECT * FROM products WHERE category = :category AND price <= :maxPrice")
    Flux<Product> findAffordableByCategory(String category, BigDecimal maxPrice);
}
```

**R2DBC vs JPA:**
- R2DBC: non-blocking, reactive, H2/PostgreSQL/MySQL/MongoDB
- JPA: blocking (Hibernate), Spring MVC ile
- WebFlux + JPA = blocking thread pool — karıştırma (publishOn ile çözülür)

---

## Functional Endpoints (Handler + Router)

```java
// Handler — business logic
@Component
public class ProductHandler {
    public Mono<ServerResponse> getById(ServerRequest request) {
        Long id = Long.parseLong(request.pathVariable("id"));
        return service.findById(id)
            .flatMap(product -> ServerResponse.ok().bodyValue(product))
            .onErrorResume(RuntimeException.class, ex -> ServerResponse.notFound().build());
    }
}

// Router — route mapping
@Bean
public RouterFunction<ServerResponse> routes(ProductHandler handler) {
    return RouterFunctions.route()
        .GET("/api/products/{id}", handler::getById)
        .POST("/api/products", handler::create)
        .build();
}
```

**@RestController vs Functional Endpoints:**
- @RestController: tanıdık, Spring MVC'den gelenlere kolay
- Functional: daha kompact, test edilmesi kolay, daha functional

---

## WebClient — Non-blocking HTTP

```java
// Tek istek
Mono<Product> result = webClient.get()
    .uri("/products/{id}", id)
    .retrieve()
    .bodyToMono(Product.class)
    .timeout(Duration.ofSeconds(5))
    .onErrorReturn(defaultProduct);

// Liste
Flux<Product> list = webClient.get()
    .uri("/products")
    .retrieve()
    .bodyToFlux(Product.class);

// Paralel istekler
Mono.zip(
    webClient.get().uri("/products/1").retrieve().bodyToMono(Product.class),
    webClient.get().uri("/users/1").retrieve().bodyToMono(User.class)
).map(tuple -> combine(tuple.getT1(), tuple.getT2()));
```

---

## Server-Sent Events (SSE)

```java
// Router
.GET("/api/products/stream", handler::stream)

// Handler
public Mono<ServerResponse> stream(ServerRequest request) {
    return ServerResponse.ok()
        .contentType(MediaType.TEXT_EVENT_STREAM)
        .body(service.streamProducts(), Product.class);
}

// Service
public Flux<Product> streamProducts() {
    return repository.findAll()
        .delayElements(Duration.ofMillis(500));  // 500ms aralık
}
```

---

## StepVerifier — Reactive Test

```java
// Normal akış
StepVerifier.create(service.findById(1L))
    .expectNextMatches(p -> "Laptop".equals(p.getName()))
    .verifyComplete();

// Hata testi
StepVerifier.create(service.findById(999L))
    .expectErrorMatches(ex -> ex instanceof RuntimeException)
    .verify();

// N eleman
StepVerifier.create(service.findAll())
    .expectNextCount(5)
    .verifyComplete();

// Timeout
StepVerifier.create(service.findById(1L))
    .expectNextCount(1)
    .verifyComplete(Duration.ofSeconds(5));
```

---

## REST Endpoints (Functional)

| Method | URL | Açıklama |
|--------|-----|----------|
| GET | `/api/reactive/products` | Tüm ürünler (Flux) |
| GET | `/api/reactive/products/{id}` | Ürün getir (Mono) |
| GET | `/api/reactive/products/stream` | SSE stream |
| GET | `/api/reactive/products/value` | Envanter değeri |
| GET | `/api/reactive/products/affordable?category=&maxPrice=` | Filtreli |
| POST | `/api/reactive/products` | Oluştur |
| PUT | `/api/reactive/products/{id}` | Güncelle |
| DELETE | `/api/reactive/products/{id}` | Sil |

---

## Mülakat Soruları

**Q: `flatMap` vs `concatMap` vs `switchMap` farkı nedir?**
A: `flatMap`: Her eleman için yeni bir Publisher başlatır, hepsi paralel çalışır — sıra garantisi yok, yüksek throughput. `concatMap`: Sıralı — önceki Publisher tamamlanmadan sonraki başlamaz, sıra garantili ama yavaş. `switchMap`: Yeni eleman gelince önceki işlemi iptal eder — arama kutusunda her tuş vuruşu önceki HTTP isteğini iptal etmek için ideal. Seçim kuralı: DB birden fazla kaydı paralel yükle → `flatMap`; dosyaları sırayla işle → `concatMap`; kullanıcı input → `switchMap`.

**Q: WebFlux neden her durumda Spring MVC'den iyi değil?**
A: WebFlux I/O-bound yüksek concurrency için tasarlanmış: event loop + non-blocking I/O ile az thread ile çok istek. Spring MVC: thread-per-request, daha basit, debug kolay. CPU-bound işlerde (hesaplama, görüntü işleme) WebFlux avantaj sağlamaz — event loop thread'i bloklamak zararlı. Dezavantajlar: öğrenmesi zor (Mono/Flux operatör zinciri, stack trace okunması güç), hibernate/JDBC gibi blocking kütüphanelerle uyumsuz (R2DBC gerekir), ekip bilgisi gerekir. Kural: "Binlerce eş zamanlı uzun süreli bağlantı (SSE, WebSocket, chat) veya çok sayıda upstream API çağrısı" → WebFlux. Klasik REST + DB → MVC yeterli.

**Q: `publishOn` vs `subscribeOn` farkı nedir?**
A: `subscribeOn`: Subscription başladığında (upstream) hangi Scheduler'da çalışacağını belirler — genellikle pipeline'ın en başına konulur, tüm upstream'i etkiler. `publishOn`: Bu operatörden sonraki downstream operatörleri başka Scheduler'a taşır — pipeline içinde context switch. Kullanım: Blocking I/O yapan kodu event loop'tan ayırmak için `publishOn(Schedulers.boundedElastic())`. `Schedulers.boundedElastic()` → blocking uyumlu, thread havuzu büyüyebilir. `Schedulers.parallel()` → CPU-bound, CPU core sayısı kadar thread. `Schedulers.single()` → tek thread, sıralı.

**Q: R2DBC neden Hibernate kadar yaygın değil? Ne zaman tercih edilir?**
A: R2DBC sınırlılıkları: Lazy loading yok (N+1 için `@EntityGraph` benzeri yok), entity relationship mapping sınırlı (`@OneToMany` otomatik JOIN yok), ekosistem daha küçük, complex query desteği zayıf. Hibernate: JPQL, Criteria API, entity graph, 2nd level cache, mature ecosystem. Tercih: WebFlux + tamamen non-blocking uygulama → R2DBC. Eğer JPA gerekiyorsa `publishOn(Schedulers.boundedElastic())` ile blocking JPA thread'ını event loop'tan ayır (tavsiye edilmez ama mümkün). Karar: Microservice, basit CRUD, yüksek concurrency → R2DBC. Complex domain model, raporlama, ORM özellikleri → JPA + Spring MVC.

**Q: Reactor'da backpressure nedir? Ne zaman sorun olur?**
A: Backpressure: Producer tüketenden hızlı üretince downstream'in "yavaşla" sinyali gönderebilmesi. Project Reactor'da Reactive Streams standardı — consumer `request(N)` ile kaç eleman istediğini bildirir. Sorun: SSE stream'de client bağlantısı yavaş ama server her 500ms event üretiyorsa buffer dolabilir. Çözüm: `onBackpressureBuffer(1000)` — 1000 aşılınca DROP. `onBackpressureDrop()` — yeni geleni at. `onBackpressureLatest()` — sadece son değeri tut. WebFlux HTTP: HTTP protokolü flow control mekanizması sayesinde genellikle otomatik yönetilir.

**Q: Functional endpoint neden `@RestController`'a tercih edilir?**
A: Functional endpoint (Router + Handler): Request matching kodu Java'da, compile-time doğrulama, daha kolay unit test (Handler'ı Spring context olmadan test et). `@RestController`: Tanıdık, az kod, reflection tabanlı mapping. Test farkı: Functional → `handler.getById(serverRequest)` direkt çağrılır, `@RestController` → `@WebFluxTest` + `WebTestClient` gerekir. WebFlux'ta ikisi de çalışır; büyük takımlarda `@RestController` daha anlaşılır, performans farkı yok.

**Q: Reaktif pipeline'da hata yönetimi nasıl yapılır?**
A: `onErrorReturn(default)`: Hata gelince sabit bir değer döndür — fallback için. `onErrorResume(ex -> fallback())`: Hata gelince başka bir Publisher'a geç — fallback service call. `onErrorMap(ex -> new AppEx(ex))`: Hata tipini dönüştür — domain exception wrapping. `retry(3)`: Hata durumunda 3 kez tekrar dene — geçici ağ hatası için. `retryWhen(Retry.backoff(3, Duration.ofMillis(500)))`: Exponential backoff ile retry. Dikkat: `onErrorReturn` hataları yutar — loglama için `doOnError()` ile birlikte kullan. `StepVerifier.create(...).expectError(RuntimeException.class).verify()` ile test et.

---

## Çalıştırma

```bash
# H2 ile (in-memory)
mvn spring-boot:run

# Test (StepVerifier)
mvn test

# Akış testi
curl -N http://localhost:8080/api/reactive/products/stream
```
