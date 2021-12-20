package ru.romanow.data.producer;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@Service
@RequiredArgsConstructor
public class RestClient {
    private final WebClient webClient;

    public Flux<BookPart> makeRequest(@NotNull String name, @NotNull Integer id) {
        return webClient
                .get()
                .uri("/files/{id}/{id}-0.txt", id, id)
                .retrieve()
                .bodyToFlux(String.class)
                .map(text -> new BookPart(name, text));
    }

    @Data
    public static class BookPart {
        private final String name;
        private final String text;
    }
}
