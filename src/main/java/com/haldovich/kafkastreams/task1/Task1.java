package com.haldovich.kafkastreams.task1;

import com.haldovich.kafkastreams.task1.config.KafkaConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackageClasses = KafkaConfig.class)
public class Task1 {

    public static void main(String[] args) {
        SpringApplication.run(Task1.class, args);
    }
}
