package com.haldovich.kafkastreams.task1;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
@RequiredArgsConstructor
public class Task1_1 {

    private static final Properties properties;
    private static final String INPUT_TOPIC = "task1-1";
    private static final String OUTPUT_TOPIC = "task1-2";

    static {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-uid");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    public static void main(String[] args) {
        SpringApplication.run(Task1_1.class, args);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> firstStream = builder.stream(INPUT_TOPIC);

        firstStream.peek((key, value) -> System.out.println("key: " + key + " value: " + value))
            .mapValues((value) -> value + " Modified")
            .to(OUTPUT_TOPIC);

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, properties);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }

}
