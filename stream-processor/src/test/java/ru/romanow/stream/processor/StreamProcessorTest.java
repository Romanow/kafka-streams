package ru.romanow.stream.processor;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.LoggerFactory.getLogger;
import static ru.romanow.stream.processor.StreamProcessor.INPUT_TOPIC;
import static ru.romanow.stream.processor.StreamProcessor.OUTPUT_TOPIC;

class StreamProcessorTest
        extends StreamProcessorCommonTest {

    @ParameterizedTest
    @MethodSource("factory")
    void test(String name, Set<String> result)
            throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final Topology topology = new StreamProcessor().createTopology(streamsBuilder);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            final var inputTopic = driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
            final var outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

            final URI uri = ClassLoader.getSystemResource(name).toURI();
            Files.lines(Path.of(uri)).forEach(inputTopic::pipeInput);

            assertThat(outputTopic.readValuesToList())
                    .containsExactlyInAnyOrder(result.toArray(String[]::new));
        }
    }
}