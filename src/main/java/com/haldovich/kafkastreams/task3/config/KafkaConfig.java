package com.haldovich.kafkastreams.task3.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.time.Duration;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    @Value("${kafka.task3.first-input-topic}")
    private String firstTopic;

    @Value("${kafka.task3.second-input-topic}")
    private String secondTopic;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        return new KafkaStreamsConfiguration(Map.of(
            APPLICATION_ID_CONFIG, "my-uid",
            BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092",
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName(),
            DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName()
        ));
    }

    @Bean
    public KStream<String, String> splitAndFilter(StreamsBuilder builder) {
        KStream<Integer, String> firstStream = builder.stream(firstTopic);
        KStream<Integer, String> secondStream = builder.stream(secondTopic);

        KStream<Integer, String> firstFiltered = firstStream
            .filter((key, value) -> value != null && value.contains(":"));

        KStream<Integer, String> secondFiltered = secondStream
            .filter((key, value) -> value != null && value.contains(":"));

        KStream<String, String> stream1 = firstFiltered.map((key, value) -> {
            String newKey = value.split(":")[0];
            return new KeyValue<>(newKey, value);
        });
        KStream<String, String> stream2 = secondFiltered.map((key, value) -> {
            String newKey = value.split(":")[0];
            return new KeyValue<>(newKey, value);
        });

        stream1.print(Printed.toSysOut());
        stream2.print(Printed.toSysOut());

        stream1
            .join(stream2,
                (leftValue, rightValue) -> String.join("-", leftValue, rightValue),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30)))
            .foreach((key, value) -> System.out.println("Test"));

        stream1.print(Printed.toSysOut());

        return stream1;
    }

    @Bean
    public KafkaStreams kafkaStreams(StreamsBuilder builder) {
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, kStreamsConfigs().asProperties());
        streams.start();
        return streams;
    }
}
