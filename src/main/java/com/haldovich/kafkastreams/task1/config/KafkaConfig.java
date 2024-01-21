package com.haldovich.kafkastreams.task1.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafkaStreams
public class KafkaConfig {

    @Value("${kafka.task1.input-topic}")
    private String inputTopic;

    @Value("${kafka.task1.output-topic}")
    private String outputTopic;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        return new KafkaStreamsConfiguration(Map.of(
            APPLICATION_ID_CONFIG, "my-uid",
            BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092",
            DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName()
        ));
    }

    @Bean
    public KStream<String, String> transferData(StreamsBuilder builder) {
        KStream<String, String> inputStream = builder.stream(inputTopic);
        inputStream.peek(((key, value) -> System.out.println("Key: " + key + " Value: " + value)))
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        return inputStream;
    }

    @Bean
    public KafkaStreams kafkaStreams(StreamsBuilder builder) {
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, kStreamsConfigs().asProperties());
        streams.start();
        return streams;
    }
}
