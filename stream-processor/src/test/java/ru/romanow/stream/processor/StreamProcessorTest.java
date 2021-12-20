package ru.romanow.stream.processor;

import com.google.gson.Gson;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import ru.romanow.stream.processor.models.BookAndWord;
import ru.romanow.stream.processor.models.BookPart;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.slf4j.LoggerFactory.getLogger;
import static org.testcontainers.utility.DockerImageName.parse;
import static ru.romanow.stream.processor.StreamProcessor.INPUT_TOPIC;

@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class StreamProcessorTest {
    private static final Logger logger = getLogger(StreamProcessorTest.class);
    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka";
    private static final Pattern CSV_PATTERN = Pattern.compile("^(.*);(\\d+)$");
    private static final String BOOKS = "data/books.csv";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(parse(KAFKA_IMAGE))
            .withEmbeddedZookeeper();

    @TempDir
    private static File tempDir;

    @Autowired
    private WebClient.Builder builder;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private StreamsBuilderFactoryBean factoryBean;

    @BeforeEach
    void init()
            throws Exception {
        final Gson gson = new Gson();

        final URI uri = ClassLoader.getSystemResource(BOOKS).toURI();
        final Map<String, Integer> books = Files.lines(Path.of(uri))
                .map(CSV_PATTERN::matcher)
                .filter(Matcher::find)
                .collect(toMap(m -> m.group(1), m -> Integer.valueOf(m.group(2))));

        Flux.fromIterable(books.entrySet())
                .flatMap(book -> makeRequest(book.getKey(), book.getValue()))
                .doOnNext(line -> kafkaTemplate.send(INPUT_TOPIC, gson.toJson(line)))
                .blockLast();
    }

    @Test
    void test()
            throws Exception {
        Thread.sleep(10_000);

        final KafkaStreams streams = factoryBean.getKafkaStreams();
        final ReadOnlyKeyValueStore<BookAndWord, Long> store =
                streams.store(StoreQueryParameters.fromNameAndType("count", keyValueStore()));

        logger.info("{}", store.get(new BookAndWord().setWord("from").setBook("Dracula by Bram Stoker")));

//        assertThat(store.get(new BookAndWord().setBook("War and Peace").setWord("world"))).isEqualTo(1);
    }

    public Flux<BookPart> makeRequest(@NotNull String name, @NotNull Integer id) {
        return builder
                .baseUrl("https://www.gutenberg.org/")
                .build()
                .get()
                .uri("/files/{id}/{id}-0.txt", id, id)
                .retrieve()
                .bodyToFlux(String.class)
                .map(text -> new BookPart().setName(name).setText(text));
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.streams.state.dir", tempDir::getAbsolutePath);
    }
}