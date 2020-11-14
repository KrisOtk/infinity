package com.zwq.infinity.plugin.metrics.kafka;

import io.micrometer.core.instrument.step.StepRegistryConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Data
@Slf4j
public class KafkaConfig implements StepRegistryConfig {

    private String topic;
    private Duration step = Duration.of(30, ChronoUnit.SECONDS);
    private String host;
    private Boolean enabled = false;
    private String timesFieldName = "@timestamp";

    public KafkaConfig(String topic, Integer step, String host, Boolean enabled) {
        this.topic = topic;
        this.step = Duration.of(step, ChronoUnit.SECONDS);
        this.host = host;
        this.enabled = enabled;
    }

    public KafkaConfig(String topic, Integer step, String host) {
        new KafkaConfig(topic, step, host, false);
    }

    @Override
    public String prefix() {
        return "zlog-kafka";
    }

    @Override
    public String get(String s) {
        return null;
    }

    public boolean enabled() {
        log.info("metrics信号量采集启动状态:{}", enabled);
        return getEnabled();
    }

    public Duration step() {
        return this.getStep();
    }

}
