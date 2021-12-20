package ru.romanow.stream.processor;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.romanow.stream.processor.models.BookAndWord;
import ru.romanow.stream.processor.models.BookPart;

import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

@Configuration
public class StreamProcessor {
    private static final Logger logger = getLogger(ProcessorApplication.class);

    private static final Pattern WORDS_PATTERN = Pattern.compile("((\\b[^\\s]+\\b)((?<=\\.\\w).)?)");
    public static final String INPUT_TOPIC = "books-topic";

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        final KStream<String, BookPart> messageStream = streamsBuilder
                .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(BookPart.class)));

        messageStream
                .selectKey((key, value) -> value.getName())
                .flatMapValues((key, value) -> words(value.getText()))
                .map((key, value) -> KeyValue.pair(new BookAndWord().setBook(key).setWord(value), value))
                .groupByKey(Grouped.with(new JsonSerde<>(BookAndWord.class), Serdes.String()))
                .count(Materialized.as("count"));

        return streamsBuilder.build();
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(INPUT_TOPIC)
                .build();
    }

    private List<String> words(String text) {
        return WORDS_PATTERN.matcher(text.toLowerCase())
                .results()
                .map(MatchResult::group)
                .collect(toList());
    }
}
