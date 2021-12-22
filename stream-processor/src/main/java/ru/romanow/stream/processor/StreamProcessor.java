package ru.romanow.stream.processor;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.common.serialization.Serdes.String;

@Configuration
public class StreamProcessor {
    private static final Pattern WORDS_PATTERN = Pattern.compile("((\\b[^\\s]+\\b)((?<=\\.\\w).)?)");
    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "distinct-words";
    public static final String STORE_NAME = "store";

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        final StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores
                .keyValueStoreBuilder(Stores.inMemoryKeyValueStore(STORE_NAME), String(), String());

        final KStream<String, String> messageStream = streamsBuilder
                .addStateStore(storeBuilder)
                .stream(INPUT_TOPIC, Consumed.with(String(), String()));

        messageStream
                .flatMapValues((key, line) -> words(line))
                .transformValues(DistinctTransformer::new, STORE_NAME)
                .filter(((key, value) -> value != null))
                .to(OUTPUT_TOPIC, Produced.with(String(), String()));

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

    static class DistinctTransformer
            implements ValueTransformerWithKey<String, String, String> {
        private KeyValueStore<String, String> store;

        @Override
        public void init(ProcessorContext context) {
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public String transform(String readOnlyKey, String value) {
            final String v = store.get(value);
            if (v == null) {
                store.put(value, value);
                return value;
            } else {
                return null;
            }
        }

        @Override
        public void close() {
        }
    }
}