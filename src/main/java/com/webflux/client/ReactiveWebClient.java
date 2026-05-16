package com.webflux.client;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

/**
 * WebClient — RestTemplate'in reaktif karşılığı
 * Non-blocking HTTP client — thread block etmez
 */
@Component
public class ReactiveWebClient {

    private final WebClient webClient;

    public ReactiveWebClient(WebClient.Builder builder) {
        this.webClient = builder
                .baseUrl("https://jsonplaceholder.typicode.com")
                .defaultHeader("Content-Type", "application/json")
                .build();
    }

    // GET → Mono (tek nesne)
    public Mono<Map> getPost(int id) {
        return webClient.get()
                .uri("/posts/{id}", id)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(5))
                .onErrorReturn(Map.of("error", "timeout"));
    }

    // GET → Flux (liste)
    public Flux<Map> getAllPosts() {
        return webClient.get()
                .uri("/posts")
                .retrieve()
                .bodyToFlux(Map.class)
                .take(10);  // ilk 10
    }

    // POST
    public Mono<Map> createPost(Map<String, Object> body) {
        return webClient.post()
                .uri("/posts")
                .bodyValue(body)
                .retrieve()
                .bodyToMono(Map.class);
    }

    // Parallel calls — zip ile birleştir
    public Mono<String> getPostWithUser(int postId, int userId) {
        Mono<Map> postMono = getPost(postId);
        Mono<Map> userMono = webClient.get()
                .uri("/users/{id}", userId)
                .retrieve()
                .bodyToMono(Map.class);

        return Mono.zip(postMono, userMono)
                .map(tuple -> "Post: %s, User: %s"
                        .formatted(tuple.getT1().get("title"), tuple.getT2().get("name")));
    }

    // Exchange spec — ham response (status, headers)
    public Mono<String> getWithStatusHandling(int id) {
        return webClient.get()
                .uri("/posts/{id}", id)
                .exchangeToMono(response -> {
                    if (response.statusCode().is2xxSuccessful()) {
                        return response.bodyToMono(Map.class)
                                .map(body -> "OK: " + body.get("title"));
                    } else {
                        return Mono.just("Error: " + response.statusCode().value());
                    }
                });
    }
}
