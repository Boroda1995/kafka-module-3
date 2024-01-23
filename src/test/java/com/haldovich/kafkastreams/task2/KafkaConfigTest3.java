package com.haldovich.kafkastreams.task2;

import com.haldovich.kafkastreams.task3.config.KafkaConfig;
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

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaConfigTest3 {

    @Spy
    @InjectMocks
    private KafkaConfig kafkaConfig;
    private Properties props;
    private StreamsBuilder streamsBuilder = new StreamsBuilder();
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> firstInputTopic;
    private TestInputTopic<String, String> secondInputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private Serde<String> stringSerde = new Serdes.StringSerde();

    private static final String INPUT_V_K = "test oawdq as qpfq faf awioa wafa wfawdawdqadf";

    @Before
    public void init() {
        kafkaConfig = new KafkaConfig();
        ReflectionTestUtils.setField(kafkaConfig, "firstTopic", "task3-1");
        ReflectionTestUtils.setField(kafkaConfig, "secondTopic", "task3-2");
        ReflectionTestUtils.setField(kafkaConfig, "mergeDuration", 1);

        props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "my-test-app");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    }

    @Test
    public void splitAndFilter() throws InterruptedException {
        KStream<String, String> stream = kafkaConfig.splitAndFilter(streamsBuilder);
        stream.to("task3-3", Produced.with(Serdes.String(), Serdes.String()));
        Topology topology = streamsBuilder.build();

        testDriver = new TopologyTestDriver(topology, props);
        firstInputTopic = testDriver.createInputTopic("task3-1", stringSerde.serializer(), stringSerde.serializer());
        secondInputTopic = testDriver.createInputTopic("task3-2", stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic("task3-3", stringSerde.deserializer(), stringSerde.deserializer());

        // Read and Verify Output
        firstInputTopic.pipeInput("2", "ko:Sasha", 1L);
        secondInputTopic.pipeInput("2", "ko:Hello", 3L);

        assertThat(outputTopic.readValue()).isEqualTo("ko:Sasha-ko:Hello");
    }
}
