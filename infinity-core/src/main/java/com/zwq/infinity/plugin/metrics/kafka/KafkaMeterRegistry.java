package com.zwq.infinity.plugin.metrics.kafka;

import com.zwq.infinity.util.NetworkUtil;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.MeterPartition;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.micrometer.core.instrument.util.TimeUtils;
import io.micrometer.core.lang.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.micrometer.core.instrument.util.StringEscapeUtils.escapeJson;

@Slf4j
public class KafkaMeterRegistry extends StepMeterRegistry {
    private static final ThreadFactory DEFAULT_THREAD_FACTORY = new NamedThreadFactory("kafka-metrics-publisher");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    private final KafkaConfig config;
    //    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;


    public KafkaMeterRegistry(KafkaConfig config, Clock clock) {
        this(config, clock, DEFAULT_THREAD_FACTORY);
    }

    private KafkaMeterRegistry(KafkaConfig config, Clock clock, ThreadFactory threadFactory) {
        super(config, clock);
        config().namingConvention((name, type, baseUnit) -> name);
        this.config = config;

        Map<String, Object> properties = producerFactory(config);
        kafkaProducer = new KafkaProducer<>(properties);
        this.topic = config.getTopic();
//        this.kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(properties));
//        this.kafkaTemplate.setDefaultTopic(config.getTopic());
        start(threadFactory);
    }


    //    private DefaultKafkaProducerFactory<String, String> producerFactory(KafkaConfig config) {
    private Map<String, Object> producerFactory(KafkaConfig config) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getHost());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        String clientId = NetworkUtil.getHostIp() + "--" + System.getProperty("PID");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        return properties;
    }

    @Override
    public void start(ThreadFactory threadFactory) {
        if (config.enabled()) {
            log.info("publish metrics to kafka every " + TimeUtils.format(config.step()));
        }
        super.start(threadFactory);
    }


    @Override
    protected void publish() {
        List<List<Meter>> partition = MeterPartition.partition(this, config.batchSize());
        for (List<Meter> batch : partition) {
            try {
                batch.forEach(meter -> meter.match(
                        this::writeGauge, this::writeCounter,
                        this::writeTimer, this::writeSummary,
                        this::writeLongTaskTimer, this::writeTimeGauge,
                        this::writeFunctionCounter, this::writeFunctionTimer, this::writeMeter));
            } catch (Throwable e) {
                log.error("failed to send metrics to kafka", e);
            }
        }
    }


    // VisibleForTesting
    Optional<String> writeCounter(Counter counter) {
        return writeCounter(counter, counter.count());
    }

    // VisibleForTesting
    Optional<String> writeFunctionCounter(FunctionCounter counter) {
        return writeCounter(counter, counter.count());
    }

    private Optional<String> writeCounter(Meter meter, Double value) {
        if (meter.getId().getName().startsWith("api") && value == 0) {
            return Optional.empty();
        }
        if (Double.isFinite(value)) {
            return Optional.of(writeDocument(meter, builder -> {
                builder.append(",\"count\":").append(value);
            }));
        }
        return Optional.empty();
    }

    // VisibleForTesting
    private Optional<String> writeGauge(Gauge gauge) {
        double value = gauge.value();
        if ("api.percentile".equals(gauge.getId().getName())) {
            if (value == 0) {
                return Optional.empty();
            }
        }
        if (Double.isFinite(value)) {
            return Optional.of(writeDocument(gauge, builder -> builder.append(",\"value\":").append(value)));
        }
        return Optional.empty();
    }

    // VisibleForTesting
    private Optional<String> writeTimeGauge(TimeGauge gauge) {
        double value = gauge.value(getBaseTimeUnit());
        if (Double.isFinite(value)) {
            return Optional.of(writeDocument(gauge, builder -> {
                builder.append(",\"value\":").append(value);
            }));
        }
        return Optional.empty();
    }

    // VisibleForTesting
    Optional<String> writeFunctionTimer(FunctionTimer timer) {
        return Optional.of(writeDocument(timer, builder -> {
            builder.append(",\"count\":").append(timer.count());
            builder.append(",\"sum\" :").append(timer.totalTime(getBaseTimeUnit()));
            builder.append(",\"mean\":").append(timer.mean(getBaseTimeUnit()));
        }));
    }

    // VisibleForTesting
    Optional<String> writeLongTaskTimer(LongTaskTimer timer) {
        return Optional.of(writeDocument(timer, builder -> {
            builder.append(",\"activeTasks\":").append(timer.activeTasks());
            builder.append(",\"duration\":").append(timer.duration(getBaseTimeUnit()));
        }));
    }

    // VisibleForTesting
    private Optional<String> writeTimer(Timer timer) {
        if ("api".equals(timer.getId().getName()) && timer.count() == 0) {
            return Optional.empty();
        }
        return Optional.of(writeDocument(timer, builder -> {
            builder.append(",\"count\":").append(timer.count());
            builder.append(",\"sum\":").append(timer.totalTime(getBaseTimeUnit()));
            builder.append(",\"mean\":").append(timer.mean(getBaseTimeUnit()));
            builder.append(",\"max\":").append(timer.max(getBaseTimeUnit()));
        }));

    }

    // VisibleForTesting
    Optional<String> writeSummary(DistributionSummary summary) {
        summary.takeSnapshot();
        return Optional.of(writeDocument(summary, builder -> {
            builder.append(",\"count\":").append(summary.count());
            builder.append(",\"sum\":").append(summary.totalAmount());
            builder.append(",\"mean\":").append(summary.mean());
            builder.append(",\"max\":").append(summary.max());
        }));
    }

    // VisibleForTesting
    private Optional<String> writeMeter(Meter meter) {
        Iterable<Measurement> measurements = meter.measure();
        List<String> names = new ArrayList<>();
        // Snapshot values should be used throughout this method as there are chances for values to be changed in-between.
        List<Double> values = new ArrayList<>();
        for (Measurement measurement : measurements) {
            double value = measurement.getValue();
            if (!Double.isFinite(value)) {
                continue;
            }
            names.add(measurement.getStatistic().getTagValueRepresentation());
            values.add(value);
        }
        if (names.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(writeDocument(meter, builder -> {
            for (int i = 0; i < names.size(); i++) {
                builder.append(",\"").append(names.get(i)).append("\":\"").append(values.get(i)).append("\"");
            }
        }));
    }

    // VisibleForTesting
    private String writeDocument(Meter meter, Consumer<StringBuilder> consumer) {
        StringBuilder sb = new StringBuilder();
        String timestamp = TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(config().clock().wallTime()));
        String name = getConventionName(meter.getId());
        String type = meter.getId().getType().toString().toLowerCase();
        sb.append("{\"").append(config.getTimesFieldName()).append("\":\"").append(timestamp).append('"')
                .append(",\"name\":\"").append(escapeJson(name)).append('"')
                .append(",\"type\":\"").append(type).append('"');

        List<Tag> tags = getConventionTags(meter.getId());
        for (Tag tag : tags) {
            sb.append(",\"").append(escapeJson(tag.getKey())).append("\":\"")
                    .append(escapeJson(tag.getValue())).append('"');
        }

        consumer.accept(sb);
        sb.append("}");
        String result = sb.toString();
//        kafkaTemplate.sendDefault(result);
        log.debug("result:{}", result);
        kafkaProducer.send(new ProducerRecord<>(topic, result));
        return result;
    }

    @Override
    @NonNull
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }
}
