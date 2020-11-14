package com.zwq.infinity.plugin.metrics;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.plugin.metrics.kafka.KafkaConfig;
import com.zwq.infinity.plugin.metrics.kafka.KafkaMeterRegistry;
import com.zwq.infinity.util.NetworkUtil;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MicroKafkaMetric extends BaseMetric {
    private final KafkaMeterRegistry kafkaMeterRegistry;
    private final Cache<Object, Counter> counterCache;
    private final List<String> tags;
    private final String host;

    /**
     * <p>基础构造方法</p>
     * <p>builder通过反射调用构造方法进行实例化</p>
     *
     * @param config
     */
    public MicroKafkaMetric(Map<String, Object> config) {
        super(config);
        KafkaConfig kafkaConfig = getKafkaConfig(config);
        kafkaMeterRegistry = new KafkaMeterRegistry(kafkaConfig, Clock.SYSTEM);
        Integer cacheSize = (Integer) config.getOrDefault("cacheSize", 1000);
        Integer cacheTimeMin = (Integer) config.getOrDefault("cacheTimeMin", 10);
        this.tags = new ArrayList<>((List<String>) config.getOrDefault("tags", new ArrayList<String>()));
        this.host = NetworkUtil.getHostIp();
        counterCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(cacheTimeMin, TimeUnit.MINUTES).removalListener((k, v, event) -> {
                }).build();

        //jvm相关监控
        JvmGcMetrics jvmGcMetrics = new JvmGcMetrics();
        ClassLoaderMetrics classLoaderMetrics = new ClassLoaderMetrics();
        JvmMemoryMetrics jvmMemoryMetrics = new JvmMemoryMetrics();
        JvmThreadMetrics jvmThreadMetrics = new JvmThreadMetrics();
        jvmGcMetrics.bindTo(kafkaMeterRegistry);
        classLoaderMetrics.bindTo(kafkaMeterRegistry);
        jvmMemoryMetrics.bindTo(kafkaMeterRegistry);
        jvmThreadMetrics.bindTo(kafkaMeterRegistry);
    }

    private static KafkaConfig getKafkaConfig(Map config) {
        String topic = (String) config.get("topic");
        Integer step = (Integer) config.getOrDefault("step", 300);
        String host = (String) config.getOrDefault("host", "127.0.0.1");
        Boolean enabled = (Boolean) config.getOrDefault("enabled", false);
        return new KafkaConfig(topic, step, host, enabled);
    }


    public Counter getCounter(String key, String name, String extraTag) {
        return counterCache.get(key, i ->
                Counter.builder(name)
                        .tag("extraTag", extraTag)
                        .tag("host", host)
                        .tags(this.tags.toArray(new String[0]))
                        .register(kafkaMeterRegistry));
    }
}
