package ru.romanow.stream.processor;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.romanow.stream.processor.models.ItemHolder;
import ru.romanow.stream.processor.models.WordCounter;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import static java.util.Arrays.stream;
import static java.util.Comparator.comparing;
import static java.util.Comparator.nullsLast;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.slf4j.LoggerFactory.getLogger;

@Configuration
public class StreamProcessor {
    private static final Logger logger = getLogger(ProcessorApplication.class);

    private static final Pattern WORDS_PATTERN = Pattern.compile("((\\b[^\\s]+\\b)((?<=\\.\\w).)?)");
    public static final String INPUT_TOPIC = "books-topic";
    public static final String OUTPUT_TOPIC = "word-count";
    public static final String RESULT = "words-count";

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        final KStream<String, String> messageStream = streamsBuilder
                .stream(INPUT_TOPIC, Consumed.with(String(), String()));

        messageStream
                .flatMapValues((key, line) -> words(line))
                .groupBy((key, word) -> word, Grouped.with(String(), String()))
                .count()
                .toStream()
                .map((key, value) -> pair("key", new WordCounter(key, value)))
                .groupByKey(Grouped.with(String(), new JsonSerde<>(WordCounter.class)))
                .aggregate(ItemHolder::new,
                        (key, value, holder) -> {
                            holder.getWords()[3] = value;
                            Arrays.sort(holder.getWords(), nullsLast(comparing(WordCounter::getCount).reversed()));
                            holder.getWords()[3] = null;
                            return holder;
                        },
                        Materialized.<String, ItemHolder, KeyValueStore<Bytes, byte[]>>as(RESULT)
                                .withKeySerde(String())
                                .withValueSerde(new JsonSerde<>(ItemHolder.class)))
                .toStream()
                .flatMap((key, value) -> stream(value.getWords())
                        .filter(Objects::nonNull)
                        .map(w -> pair(w.getWord(), w.getCount()))
                        .collect(toList()))
                .to(OUTPUT_TOPIC, Produced.with(String(), Long()));
        ;

        return streamsBuilder.build();
    }

    @Bean
    public NewTopic inputTopic() {
        return TopicBuilder.name(INPUT_TOPIC).build();
    }

    @Bean
    public NewTopic outputTopic() {
        return TopicBuilder.name(OUTPUT_TOPIC).build();
    }

    private List<String> words(String text) {
        return WORDS_PATTERN.matcher(text.toLowerCase())
                .results()
                .map(MatchResult::group)
                .collect(toList());
    }
}
