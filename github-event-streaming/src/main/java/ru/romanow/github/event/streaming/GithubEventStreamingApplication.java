package ru.romanow.github.event.streaming;

import com.google.gson.Gson;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.slf4j.LoggerFactory.getLogger;

@SpringBootApplication
public class GithubEventStreamingApplication {
    private static final Logger logger = getLogger(GithubEventStreamingApplication.class);

    private static final String GITHUB_CHANGE_EVENTS_TOPIC = "github-change-events";
    private static final String GITHUB_TOKEN = "GITHUB_TOKEN";
    private static final String PUSH_EVENT = "PushEvent";

    public static void main(String[] args) {
        SpringApplication.run(GithubEventStreamingApplication.class, args);
    }

    @Bean
    public WebClient webClient(WebClient.Builder builder) {
        return builder
                .defaultHeaders(header -> header.setBasicAuth("Romanow", System.getenv(GITHUB_TOKEN)))
                .build();
    }

    @Bean
    public CommandLineRunner runner(WebClient webClient, KafkaTemplate<String, String> kafkaTemplate) {
        return args -> {
            final Gson gson = new Gson();
            final AtomicReference<String> etag = new AtomicReference<>();
            while (true) {
                webClient.get()
                        .uri("https://api.github.com/events?per_page=30")
                        .ifNoneMatch(etag.get())
                        .accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .toEntity(new ParameterizedTypeReference<List<GithubEvent>>() {})
                        .doOnError(throwable -> logger.error("", throwable))
                        .doOnSuccess(resp -> etag.set(resp.getHeaders().getETag()))
                        .flatMapMany(resp -> Flux.fromIterable(resp.getBody()))
                        .filter(event -> event.getType().equalsIgnoreCase(PUSH_EVENT))
                        .flatMap(event -> webClient.get()
                                .uri(event.repo.url)
                                .retrieve()
                                .bodyToMono(Repository.class)
                                .filter(repository -> repository.language != null)
                                .map(event::setRepo))
                        .doOnNext(event -> logger.info("Github event: {}", event))
                        .doOnNext(event -> kafkaTemplate.send(GITHUB_CHANGE_EVENTS_TOPIC, gson.toJson(event)))
                        .subscribe();

                Thread.sleep(5000);
            }
        };
    }

    @Bean
    public NewTopic githubEventsTopic() {
        return TopicBuilder
                .name(GITHUB_CHANGE_EVENTS_TOPIC)
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Data
    @Accessors(chain = true)
    static class GithubEvent {
        private String id;
        private String type;
        private Repository repo;
    }

    @Data
    @Accessors(chain = true)
    static class Repository {
        private Long id;
        private String name;
        private String url;
        private String language;
    }
}
