package com.haldovich.kafkastreams.task4;

import com.haldovich.kafkastreams.task4.dto.Dto;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class DtoSerde extends Serdes.WrapperSerde<Dto> {
    public DtoSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(Dto.class));
    }
}
