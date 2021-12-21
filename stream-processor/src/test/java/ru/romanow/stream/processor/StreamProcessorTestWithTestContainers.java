package ru.romanow.stream.processor;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
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
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;
import static org.apache.kafka.streams.state.QueryableStoreTypes.timestampedKeyValueStore;
import static org.slf4j.LoggerFactory.getLogger;
import static org.testcontainers.utility.DockerImageName.parse;
import static ru.romanow.stream.processor.StreamProcessor.INPUT_TOPIC;
import static ru.romanow.stream.processor.StreamProcessor.RESULT;

@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class StreamProcessorTestWithTestContainers {
    private static final Logger logger = getLogger(StreamProcessorTestWithTestContainers.class);
    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka";
    private static final String BOOK = "data/Sorting Hat song.txt";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(parse(KAFKA_IMAGE))
            .withEmbeddedZookeeper();

    @TempDir
    private static File tempDir;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private StreamsBuilderFactoryBean factoryBean;

    @BeforeEach
    void init()
            throws Exception {
        final URI uri = ClassLoader.getSystemResource(BOOK).toURI();
        Files.lines(Path.of(uri)).forEach(line -> kafkaTemplate.send(INPUT_TOPIC, line));
    }

    @Test
    void test()
            throws InterruptedException {
        Thread.sleep(30000);

        final KafkaStreams streams = factoryBean.getKafkaStreams();
        final ReadOnlyKeyValueStore<String, ValueAndTimestamp<Long>> store =
                streams.store(StoreQueryParameters.fromNameAndType(RESULT, timestampedKeyValueStore()));

        store.all().forEachRemaining(v -> logger.info("{} -> {}", v.key, v.value.value()));
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.streams.state.dir", tempDir::getAbsolutePath);
    }
}