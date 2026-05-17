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
 * REAKTİF ÜRÜN SERVİSİ — Project Reactor Desenleri
 * ===================================================
 *
 * Neden Reactive (WebFlux), neden klasik Spring MVC değil?
 *
 *   Klasik MVC (blocking):
 *     Her HTTP isteği → bir thread bloke olur (DB/HTTP bekliyorken).
 *     1000 eş zamanlı istek → 1000 thread (bellek yükü: her thread ~1MB stack).
 *     Thread pool tükening: 1001. istek → kuyrukta bekler.
 *
 *   WebFlux (non-blocking):
 *     Her HTTP isteği → event loop thread'i (bloke olmaz).
 *     DB yanıtı bekliyorken thread başka isteği işler.
 *     1000 eş zamanlı istek → 8-16 thread (CPU çekirdeği sayısı kadar).
 *     Yüksek throughput, düşük bellek kullanımı.
 *
 * Ne zaman WebFlux seçilmez?
 *   Blocking kütüphane varsa (JDBC, dosya I/O): Thread bloke olur — fayda yok.
 *   R2DBC (reaktif DB driver) gereklidir (JDBC değil).
 *   Basit uygulamalar: Karmaşıklık maliyeti faydasını geçer.
 *
 * Temel tipler:
 *   Mono<T>: 0 veya 1 eleman — HTTP GET by ID, save() gibi tek sonuçlar.
 *   Flux<T>: 0-N eleman — findAll(), stream gibi çoklu sonuçlar.
 *   Her ikisi de Publisher — subscribe() çağrılana kadar hiçbir şey çalışmaz (lazy).
 *
 * Bu servisteki desenler:
 *   switchIfEmpty  → Boş Mono'ya fallback (orElseThrow reaktif karşılığı)
 *   map / flatMap  → Sync dönüşüm vs async dönüşüm
 *   Mono.zip       → İki bağımsız sorguyu paralel çalıştır, sonuçları birleştir
 *   onErrorResume  → Hata durumunda alternatif veri kaynağına geç
 *   timeout+retry  → Ağ hataları için dayanıklılık
 *   publishOn      → Blocking işlemi farklı thread pool'a kaydır
 *   cache          → Birden fazla subscriber aynı sonucu paylaşır
 *   delayElements  → SSE için hız kontrolü
 *   reduce         → Flux'u tek değere aggregate et
 */
@Service
public class ProductService {

    private final ProductRepository repository;

    public ProductService(ProductRepository repository) {
        this.repository = repository;
    }

    // ── Temel CRUD ─────────────────────────────────────────────────────────────

    /**
     * Ürün kaydet. Mono<Product> döner — bloke olmaz.
     * Dönen Mono'yu subscribe eden katman (WebFlux handler): I/O tamamlanınca sonucu alır.
     */
    public Mono<Product> create(Product product) {
        return repository.save(product);
    }

    /**
     * ID ile ürün bul.
     *
     * switchIfEmpty neden orElseThrow'un reaktif karşılığıdır?
     *   Klasik: optional.orElseThrow(() -> new RuntimeException(...))
     *   Reaktif: .switchIfEmpty(Mono.error(...))
     *   repository.findById() boş Mono döndürürse → error Mono'ya geç → 404 yanıtı.
     *   Boş kontrolü subscriber'a bırakmak yerine chain içinde erken handle edilir.
     */
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

    /**
     * Ürün güncelle.
     *
     * map vs flatMap farkı burada görülür:
     *   .map(existing -> { ... return existing; }): Sync dönüşüm — nesne üzerinde alan set et.
     *     Bir Publisher döndürmez, sadece değeri dönüştürür.
     *   .flatMap(repository::save): Async dönüşüm — save() bir Mono<Product> döner.
     *     flatMap: İç Mono'yu "flatten" eder — Mono<Mono<Product>> değil, Mono<Product> döner.
     *
     * Kural: Dönüşüm fonksiyonu Publisher döndürüyorsa flatMap, döndürmüyorsa map.
     */
    public Mono<Product> update(Long id, Product updated) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new RuntimeException("Not found: " + id)))
                .map(existing -> {
                    // map: Sync — existing nesnesini değiştir, reactive chain kesintisiz devam eder
                    existing.setName(updated.getName());
                    existing.setPrice(updated.getPrice());
                    existing.setStock(updated.getStock());
                    return existing;
                })
                .flatMap(repository::save); // flatMap: save() bir Mono döner — flatten gerekli

    }

    public Mono<Void> delete(Long id) {
        return repository.deleteById(id);
    }

    // ── Reaktif Desenler ──────────────────────────────────────────────────────

    /**
     * flatMap ile paralel async çağrı.
     *
     * flatMap neden paralel işler?
     *   Flux.fromIterable(categories) → sıralı bir Flux oluşturur.
     *   .flatMap(fn): Her eleman için fn çağrılır — ama sonuçlar geldikçe downstream'e geçer.
     *   DB çağrıları aynı anda başlar → en erken gelen önce downstream'e geçer.
     *   Sıra korunmaz — hız önceliklidir.
     *
     * concatMap farkı ne zaman önemli?
     *   concatMap: Bir kategori tamamlanmadan diğerine başlamaz — sıra garantili.
     *   flatMap: Tüm kategoriler paralel işlenir — sıra garanti değil.
     *   Kullanım: "Electronics, Clothing, Books" sonuçlarının karışık gelmesi kabul edilebilirse flatMap.
     *   Sıra kritikse (önce pahalı ürünler, sonra ucuz): concatMap.
     */
    public Flux<Product> findByCategories(List<String> categories) {
        return Flux.fromIterable(categories)
                .flatMap(repository::findByCategoryAndActiveTrue); // paralel DB çağrıları
        // concatMap ile değiştir → sıra önemliyse (ama daha yavaş)
    }

    /**
     * Mono.zip — İki Bağımsız Sorguyu Paralel Çalıştır.
     *
     * Neden zip?
     *   productMono ve countMono bağımsız — birinin sonucu diğerine girdi değil.
     *   Sıralı çağrı: product gelir → sonra count → toplam süre = t1 + t2.
     *   Zip (paralel): Her ikisi aynı anda başlar → toplam süre = max(t1, t2).
     *   Büyük sistemlerde: 2 bağımsız DB sorgusu → 2x hız kazancı.
     *
     * tuple neden?
     *   Zip iki farklı tipte değer üretir — tek bir tip dönemez.
     *   Tuple2<Product, Long>: tuple.getT1() = Product, tuple.getT2() = count.
     *   map(tuple → ...): Her iki sonuç birleşince map çalışır.
     */
    public Mono<String> getProductSummary(Long id) {
        Mono<Product> productMono = repository.findById(id);
        Mono<Long> countMono = repository.countByCategory("electronics");

        return Mono.zip(productMono, countMono)  // İkisi paralel başlar
                .map(tuple -> "Product: %s, Category total: %d"
                        .formatted(tuple.getT1().getName(), tuple.getT2()));
    }

    /**
     * onErrorResume — Graceful Degradation (Zarif Bozulma).
     *
     * onErrorResume nedir?
     *   Upstream (repository) hata fırlatırsa → fallback Publisher'a geç.
     *   Exception yutulmaz — fallback ile devam edilir.
     *   Kullanıcıya hata yerine alternatif veri gösterilir.
     *
     * Ne zaman kullanılır?
     *   Kompleks sorgu başarısız → basit sorgu ile devam et.
     *   Harici servis düştü → önbellekten veya varsayılan veri döndür.
     *   Circuit breaker pattern: Hata oranı yüksekse fallback'e geç.
     *
     * Dikkat: Tüm hatalar için aynı fallback uygun olmayabilir.
     *   onErrorResume(SpecificException.class, ex -> ...): Sadece belirli hata tipinde.
     *   Tüm hataları yutmak: Gerçek sorunlar gizlenebilir.
     */
    public Flux<Product> findAffordable(String category, BigDecimal maxPrice) {
        return repository.findAffordableByCategory(category, maxPrice)
                .onErrorResume(ex -> {
                    System.err.println("Query failed, fallback to findAll: " + ex.getMessage());
                    return repository.findByActiveTrue(); // Fallback: tüm aktif ürünler
                });
    }

    /**
     * timeout + retry — Ağ Dayanıklılığı.
     *
     * timeout: Belirtilen sürede yanıt gelmezse hata fırlatır.
     *   Neden gerekli? Reaktif sistemde yanıt gelmeyebilir — thread bloke olmaz ama sonsuz bekler.
     *   timeout: En fazla 5 saniye bekle, gelmezse TimeoutException.
     *
     * retry(3): Hata oluşunca N kez tekrar abone ol (subscribe).
     *   Geçici ağ hatalarında: Birkaç denemede başarılı olabilir.
     *   Dikkat: Timeout her retry'da da uygulanır → toplam süre 3 × 5sn = 15sn olabilir.
     *   Daha iyi: .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))) — üstel bekleme ile.
     *
     * switchIfEmpty (tekrar): Tüm retry'lardan sonra da boş gelirse hata.
     *   Retry: Hata exception için; switchIfEmpty: Boş sonuç için — ikisi farklı durum.
     */
    public Mono<Product> findByIdWithRetry(Long id) {
        return repository.findById(id)
                .timeout(Duration.ofSeconds(5))    // 5sn içinde yanıt gelmezse hata
                .retry(3)                          // Geçici hatalarda 3 kez tekrar dene
                .switchIfEmpty(Mono.error(new RuntimeException("Not found after retries: " + id)));
    }

    /**
     * publishOn — Blocking İşlemi Thread Pool'dan Koru.
     *
     * Problem: Reaktif event loop thread bloke olursa tüm sistem durur.
     *   Reactive thread (event loop): CPU-bound, hızlı görevler.
     *   Blocking işlem (Thread.sleep, dosya I/O, legacy JDBC): Thread'i bloke eder.
     *   Eğer event loop thread bloke olursa: Diğer istekler işlenemez → throughput sıfır.
     *
     * publishOn(Schedulers.boundedElastic()):
     *   Bu noktadan sonraki işlemler başka thread pool'da çalışır.
     *   boundedElastic: Blocking-safe, elastik thread pool (gerektiğinde büyür/küçülür).
     *   Alternatif: Schedulers.parallel() — CPU-bound işlemler için, blocking değil.
     *   Kural: Blocking kod → boundedElastic. CPU-yoğun kod → parallel. Event loop → default.
     *
     * subscribeOn farkı:
     *   publishOn: Bu operator'dan sonraki downstream'i etkiler.
     *   subscribeOn: Tüm chain'i etkiler (kaynak subscribe dahil) — sadece bir kez kullanılır.
     */
    public Mono<String> processWithBlocking(Long id) {
        return repository.findById(id)
                .publishOn(Schedulers.boundedElastic())  // Sonraki map başka thread pool'da
                .map(product -> {
                    // Blocking işlem: Legacy kütüphane, dosya I/O, thread.sleep
                    // boundedElastic thread'de çalışır — event loop bloke olmaz
                    try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    return "Processed: " + product.getName();
                });
    }

    /**
     * cache() — Birden Fazla Subscriber Aynı Sonucu Paylaşır.
     *
     * Neden cache?
     *   Mono/Flux: Her subscribe() → kaynak yeniden çalışır (cold publisher).
     *   findByActiveTrue() → Her çağrıda DB sorgusu.
     *   5 dakikada 1000 istek: 1000 DB sorgusu → gereksiz yük.
     *
     * .cache(Duration.ofMinutes(5)):
     *   İlk subscribe → DB sorgusu → sonuç cache'e alınır.
     *   Sonraki subscribe → cache'den döner — DB sorgusu yok.
     *   5 dakika sonra cache süresi dolar → bir sonraki subscribe'da DB sorgusu yapılır.
     *
     * Field olarak tanımlanması:
     *   Her getCachedActiveProducts() çağrısında yeni Mono oluşturulmasın.
     *   Field: Tek bir Mono instance — cache bu instance üzerinde çalışır.
     *   Her çağrıda yeni Mono: Cache'in anlamı kalmaz — her seferinde sıfırlanır.
     */
    private final Mono<List<Product>> cachedProducts = repository.findByActiveTrue()
            .collectList()
            .cache(Duration.ofMinutes(5)); // 5 dakika boyunca DB'ye gitmez

    public Mono<List<Product>> getCachedActiveProducts() {
        return cachedProducts;
    }

    /**
     * delayElements — Server-Sent Events (SSE) için Hız Kontrolü.
     *
     * SSE nedir?
     *   HTTP bağlantısı açık kalır — sunucu anlık veri iter (push).
     *   Client tarafı EventSource API ile dinler.
     *   Kullanım: Canlı ürün güncellemeleri, bildirimler, dashboard.
     *
     * delayElements(Duration.ofMillis(500)):
     *   Her ürün arasında 500ms bekle.
     *   Neden? SSE client'ı çok hızlı gelen veriyi işleyemeyebilir (backpressure yönetimi).
     *   Stream simülasyonu: Gerçek uygulamada DB değişikliklerini dinleyen R2DBC listener kullanılır.
     */
    public Flux<Product> streamNewProducts() {
        return repository.findAll()
                .delayElements(Duration.ofMillis(500)); // Her ürün 500ms aralıkla gönderilir
    }

    public Mono<Long> countByCategory(String category) {
        return repository.countByCategory(category);
    }

    /**
     * reduce — Flux'u Tek Değere Aggregate Et.
     *
     * reduce ne yapar?
     *   Flux<Product> → her elemanı accumulator ile birleştirir → Mono<BigDecimal>.
     *   SQL: SELECT SUM(price * stock) FROM products WHERE active = true
     *   Reaktif: Tüm ürünler stream'de gelirken anlık hesaplama.
     *
     * reduce(initialValue, accumulator):
     *   BigDecimal.ZERO: Başlangıç değeri (ilk akümülatör).
     *   BigDecimal::add: Her adımda mevcut toplama yeni değeri ekle.
     *   Son eleman → Mono<BigDecimal> döner.
     *
     * .map(p -> p.getPrice().multiply(...)):
     *   Her ürün için değer hesapla → reduce bu değerleri toplar.
     *   Reactive pipeline: Stream edilir, bellekte tüm liste tutulmaz (büyük katalog için önemli).
     */
    public Mono<BigDecimal> totalInventoryValue() {
        return repository.findByActiveTrue()
                .map(p -> p.getPrice().multiply(BigDecimal.valueOf(p.getStock()))) // fiyat × stok
                .reduce(BigDecimal.ZERO, BigDecimal::add); // Tüm değerleri topla
    }
}
