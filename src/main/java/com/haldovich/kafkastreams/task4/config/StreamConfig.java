package com.haldovich.kafkastreams.task4.config;

import com.haldovich.kafkastreams.task4.DtoSerde;
import com.haldovich.kafkastreams.task4.dto.Dto;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class StreamConfig {

    @Value("${kafka.task4.input-topic}")
    private String inputTopic;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        return new KafkaStreamsConfiguration(Map.of(
            APPLICATION_ID_CONFIG, "my-uid",
            BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092",
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()
        ));
    }

    @Bean
    public KStream<String, Dto> kStream(StreamsBuilder builder) {
        return builder
            .stream(inputTopic, Consumed.with(Serdes.String(), new DtoSerde()))
            .filter((k, v) -> v != null)
            .peek((k, v) -> System.out.println("Key: " + k + " Value: " + v));
    }

    @Bean
    public KafkaStreams kafkaStreams(StreamsBuilder builder) {
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, kStreamsConfigs().asProperties());
        streams.start();
        return streams;
    }
}
