package ru.romanow.stream.processor;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.slf4j.LoggerFactory.getLogger;
import static ru.romanow.stream.processor.StreamProcessor.*;

class StreamProcessorTest {
    private static final Logger logger = getLogger(StreamProcessorTest.class);
    private static final String BOOK = "data/Sorting Hat song.txt";

    @Test
    void test()
            throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final Topology topology = new StreamProcessor().createTopology(streamsBuilder);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            final var inputTopic = driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
            final var outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());

            final URI uri = ClassLoader.getSystemResource(BOOK).toURI();
            Files.lines(Path.of(uri)).forEach(inputTopic::pipeInput);

            driver.getKeyValueStore(RESULT)
                    .all()
                    .forEachRemaining(v -> logger.info("{} -> {}", v.key, v.value));
        }
    }
}