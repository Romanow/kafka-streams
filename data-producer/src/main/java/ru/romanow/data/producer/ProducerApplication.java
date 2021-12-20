package ru.romanow.data.producer;

import com.google.gson.Gson;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toMap;

@SpringBootApplication
public class ProducerApplication {
    private static final Pattern CSV_PATTERN = Pattern.compile("^(.*);(\\d+)$");
    private static final String BOOKS = "data/books.csv";
    private static final String TOPIC = "books-topic";

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Bean
    public WebClient webClient() {
        return WebClient.builder().baseUrl("https://www.gutenberg.org/").build();
    }

    @Bean
    public CommandLineRunner runner(@NotNull KafkaTemplate<String, String> kafkaTemplate,
                                    @NotNull RestClient restClient) {
        return args -> {
            final Gson gson = new Gson();

            final URI uri = ClassLoader.getSystemResource(BOOKS).toURI();
            final Map<String, Integer> books = Files.lines(Path.of(uri))
                    .map(CSV_PATTERN::matcher)
                    .filter(Matcher::find)
                    .collect(toMap(m -> m.group(1), m -> Integer.valueOf(m.group(2))));

            Flux.fromIterable(books.entrySet())
                    .flatMap(book -> restClient.makeRequest(book.getKey(), book.getValue()))
                    .doOnNext(line -> kafkaTemplate.send(TOPIC, gson.toJson(line)))
                    .blockLast();
        };
    }
}
