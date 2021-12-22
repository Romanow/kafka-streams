package ru.romanow.stream.processor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.test.IntegrationTest;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.utility.DockerImageName.parse;
import static ru.romanow.stream.processor.StreamProcessor.INPUT_TOPIC;
import static ru.romanow.stream.processor.StreamProcessor.OUTPUT_TOPIC;

@Disabled
@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class StreamProcessorTestWithTestContainers
        extends StreamProcessorCommonTest {
    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(parse(KAFKA_IMAGE))
            .withEmbeddedZookeeper();

    @TempDir
    private static File tempDir;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @ParameterizedTest
    @MethodSource("factory")
    void test(String name, Set<String> result)
            throws Exception {
        final URI uri = ClassLoader.getSystemResource(name).toURI();
        Files.lines(Path.of(uri)).forEach(line -> kafkaTemplate.send(INPUT_TOPIC, line));

        final Consumer<String, String> consumer = configureConsumer(KAFKA.getBootstrapServers(), OUTPUT_TOPIC);

        final Set<String> actual = Collections.synchronizedSet(new HashSet<>());
        final ExecutorService service = Executors.newSingleThreadExecutor();
        final Future<?> consumingTask = service.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<String, String> record : records) {
                    actual.add(record.value());
                }
            }
        });

        try {
            Awaitility.await()
                    .atMost(600, SECONDS)
                    .until(() -> result.equals(actual));
        } finally {
            consumingTask.cancel(true);
            service.awaitTermination(200, MILLISECONDS);
        }
    }

    @NotNull
    private Consumer<String, String> configureConsumer(@NotNull String bootstrapServers, @NotNull String outputTopicName) {
        Map<String, Object> properties = KafkaTestUtils
                .consumerProps(String.join(",", bootstrapServers), "testGroup", "true");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, String> consumer =
                new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), new StringDeserializer())
                        .createConsumer();
        consumer.subscribe(Collections.singleton(outputTopicName));
        return consumer;
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.streams.state.dir", tempDir::getAbsolutePath);
    }
}