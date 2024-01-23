package com.haldovich.kafkastreams.task4;

import com.haldovich.kafkastreams.task4.config.StreamConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackageClasses = StreamConfig.class)
public class Task4 {

    public static void main(String[] args) {
        SpringApplication.run(Task4.class, args);
    }
}
