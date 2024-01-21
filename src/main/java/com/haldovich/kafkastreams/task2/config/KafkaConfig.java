package com.haldovich.kafkastreams.task2.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafkaStreams
public class KafkaConfig {

    @Value("${kafka.task2.input-topic}")
    private String inputTopic;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        return new KafkaStreamsConfiguration(Map.of(
            APPLICATION_ID_CONFIG, "my-uid",
            BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092",
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName()
        ));
    }

    @Bean(name = "branchedMap")
    public Map<String, KStream<Integer, String>> splitAndFilter(StreamsBuilder builder) {
        KStream<String, String> inputStream = builder.stream(inputTopic);

        KStream<Integer, String> flattedMap = inputStream
            .filter((key, value) -> value != null)
            .flatMap((key, value) -> {
                List<KeyValue<Integer, String>> result = new ArrayList<>();
                String[] words = value.split("\\s+");
                for (String word : words) {
                    result.add(KeyValue.pair(word.length(), word));
                }
                return result;
            });

        flattedMap
            .foreach((key, value) -> System.out.println("Key: " + key + ", Value: " + value));

        return flattedMap.split(Named.as("words"))
            .branch((key, value) -> key < 10, Branched.as("-short"))
            .branch((key, value) -> key >= 10, Branched.as("-long"))
            .noDefaultBranch();
    }

    @Bean(name = "shortWithA")
    public KStream<Integer, String> shortFilterWithA(@Qualifier("branchedMap") Map<String, KStream<Integer, String>> branch) {
        return branch.get("words-short").filter(((key, value) -> value.contains("a")));
    }

    @Bean(name = "longWithA")
    public KStream<Integer, String> longFilterWithA(@Qualifier("branchedMap") Map<String, KStream<Integer, String>> branch) {
        return branch.get("words-long").filter(((key, value) -> value.contains("a")));
    }

    @Bean
    public KStream<Integer, String> filterAndMerge(@Qualifier("shortWithA") KStream<Integer, String> s1,
                                                   @Qualifier("longWithA") KStream<Integer, String> s2) {
        s1.merge(s2).foreach((key, value) -> System.out.println("Key: " + key + ", Values with A: " + value));
        return s1;
    }

    @Bean
    public KafkaStreams kafkaStreams(StreamsBuilder builder) {
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, kStreamsConfigs().asProperties());
        streams.start();
        return streams;
    }
}
