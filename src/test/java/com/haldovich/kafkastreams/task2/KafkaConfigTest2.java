package com.haldovich.kafkastreams.task2;

import com.haldovich.kafkastreams.task2.config.KafkaConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaConfigTest2 {

    @Spy
    @InjectMocks
    private KafkaConfig kafkaConfig;

    private Properties props;
    private StreamsBuilder streamsBuilder = new StreamsBuilder();
    private TopologyTestDriver testDriver;

    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<Integer, String> longWordsTopic;
    private TestOutputTopic<Integer, String> shortWordsTopic;
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<Integer> integerSerde = new Serdes.IntegerSerde();

    Map<String, KStream<Integer, String>> stringKStreamMap;

    private static final String INPUT_V_K = "test oawdq as qpfq faf awioa wafa wfawdawdqadf";

    @Before
    public void init() {
        kafkaConfig = new KafkaConfig();
        ReflectionTestUtils.setField(kafkaConfig, "inputTopic", "task2");

        props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "my-test-app");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    }

    @Test
    public void splitAndFilter() {
        Map<String, KStream<Integer, String>> stringKStreamMap1 = kafkaConfig.splitAndFilter(streamsBuilder);
        stringKStreamMap1.get("words-short").to("task2-1", Produced.with(Serdes.Integer(), Serdes.String()));
        stringKStreamMap1.get("words-long").to("task2-2", Produced.with(Serdes.Integer(), Serdes.String()));
        stringKStreamMap = stringKStreamMap1;
        Topology topology = streamsBuilder.build();

        testDriver = new TopologyTestDriver(topology, props);
        inputTopic = testDriver.createInputTopic("task2", stringSerde.serializer(), stringSerde.serializer());
        shortWordsTopic = testDriver.createOutputTopic("task2-1", integerSerde.deserializer(), stringSerde.deserializer());
        longWordsTopic = testDriver.createOutputTopic("task2-2", integerSerde.deserializer(), stringSerde.deserializer());

        // Read and Verify Output
        inputTopic.pipeInput("test", INPUT_V_K);

        List<KeyValue<Integer, String>> shortList = shortWordsTopic.readKeyValuesToList();
        List<KeyValue<Integer, String>> longList = longWordsTopic.readKeyValuesToList();

        assertThat(shortList.size()).isEqualTo(7);
        assertThat(longList.size()).isEqualTo(1);
        assertThat(shortList).containsAll(List.of(
            new KeyValue<>(4, "test"),
            new KeyValue<>(5, "oawdq"),
            new KeyValue<>(2, "as"),
            new KeyValue<>(4, "qpfq"),
            new KeyValue<>(3, "faf"),
            new KeyValue<>(5, "awioa"),
            new KeyValue<>(4, "wafa")
        ));
        assertThat(longList).containsAll(List.of(
            new KeyValue<>(12, "wfawdawdqadf")
        ));

        assertEquals(0, shortWordsTopic.getQueueSize());
        assertEquals(0, longWordsTopic.getQueueSize());
    }

    @Test
    public void shortFilterWithA() {
        Map<String, KStream<Integer, String>> map = kafkaConfig.splitAndFilter(streamsBuilder);
        KStream<Integer, String> kStream = kafkaConfig.shortFilterWithA(map);

        kStream.to("task2-shortA", Produced.with(Serdes.Integer(), Serdes.String()));

        Topology topology = streamsBuilder.build();
        testDriver = new TopologyTestDriver(topology, props);
        inputTopic = testDriver.createInputTopic("task2", stringSerde.serializer(), stringSerde.serializer());
        longWordsTopic = testDriver.createOutputTopic("task2-shortA", integerSerde.deserializer(), stringSerde.deserializer());

        // Read and Verify Output
        inputTopic.pipeInput("test", INPUT_V_K);

        List<KeyValue<Integer, String>> shortList = longWordsTopic.readKeyValuesToList();

        assertThat(shortList.size()).isEqualTo(5);
        assertThat(shortList).containsAll(List.of(
            new KeyValue<>(5, "oawdq"),
            new KeyValue<>(2, "as"),
            new KeyValue<>(3, "faf"),
            new KeyValue<>(5, "awioa"),
            new KeyValue<>(4, "wafa")
        ));

        assertEquals(0, longWordsTopic.getQueueSize());
    }

    @Test
    public void longFilterWithA() {
        Map<String, KStream<Integer, String>> map = kafkaConfig.splitAndFilter(streamsBuilder);
        KStream<Integer, String> kStream = kafkaConfig.longFilterWithA(map);

        kStream.to("task2-longA", Produced.with(Serdes.Integer(), Serdes.String()));

        Topology topology = streamsBuilder.build();
        testDriver = new TopologyTestDriver(topology, props);
        inputTopic = testDriver.createInputTopic("task2", stringSerde.serializer(), stringSerde.serializer());
        longWordsTopic = testDriver.createOutputTopic("task2-longA", integerSerde.deserializer(), stringSerde.deserializer());

        // Read and Verify Output
        inputTopic.pipeInput("test", INPUT_V_K);

        List<KeyValue<Integer, String>> longList = longWordsTopic.readKeyValuesToList();

        assertThat(longList.size()).isEqualTo(1);
        assertThat(longList).containsAll(List.of(
            new KeyValue<>(12, "wfawdawdqadf")
        ));

        assertEquals(0, longWordsTopic.getQueueSize());
    }
}
