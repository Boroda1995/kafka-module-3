package com.haldovich.kafkastreams.task2;

import com.haldovich.kafkastreams.task1.config.KafkaConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
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
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaConfigTest1 {

    @Spy
    @InjectMocks
    private KafkaConfig kafkaConfig;

    private Properties props;
    private StreamsBuilder streamsBuilder = new StreamsBuilder();
    private TopologyTestDriver testDriver;

    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    private Serde<String> stringSerde = new Serdes.StringSerde();

    @Before
    public void init() {
        kafkaConfig = new KafkaConfig();
        ReflectionTestUtils.setField(kafkaConfig, "inputTopic", "task1-1");
        ReflectionTestUtils.setField(kafkaConfig, "outputTopic", "task1-2");

        props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "my-test-app");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        kafkaConfig.transferData(streamsBuilder);
        Topology topology = streamsBuilder.build();

        testDriver = new TopologyTestDriver(topology, props);
        inputTopic = testDriver.createInputTopic("task1-1", stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic("task1-2", stringSerde.deserializer(), stringSerde.deserializer());
    }

    @Test
    public void getTest() {
        final String testValue = "test";
        inputTopic.pipeInput(testValue, testValue);
        KeyValue<String, String> stringStringKeyValue = outputTopic.readKeyValue();
        assertThat(stringStringKeyValue.key).isEqualTo(testValue);
        assertThat(stringStringKeyValue.value).isEqualTo(testValue);

        assertEquals(0, outputTopic.getQueueSize());
    }
}
