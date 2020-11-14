package com.zwq.infinity.plugin.outputs;

import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.baseplugin.BaseOutput;
import com.zwq.infinity.render.TemplateRender;
import com.zwq.infinity.util.InfinityJsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class Kafka extends BaseOutput {
    private Producer<String, Object> producer;
    private TemplateRender format;
    private Properties props;
    private TemplateRender topicRender;

    public Kafka(Map<String, Object> config) {
        super(config);
    }

    public Kafka(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    @Override
    protected void prepare() {
        if (!this.config.containsKey("topic")) {
            log.info("topic must be included in config");
            System.exit(1);
        }
        String topic = (String) this.config.get("topic");
        this.topicRender = TemplateRender.getRender(topic);
        if (this.config.containsKey("format")) {
            String format = (String) this.config.get("format");
            this.format = TemplateRender.getRender(format);
        }

        props = new Properties();
        Map<String, String> producerSettings = (HashMap<String, String>) this.config.get("producer_settings");
        if (MapUtils.isNotEmpty(producerSettings)) {
            producerSettings.forEach((key, value) -> props.put(key, value));
        } else {
            log.info("producer_settings must be included in config");
            System.exit(1);
        }

        if (props.get("bootstrap.servers") == null) {
            log.info("bootstrap.servers must be included in producer_settings");
            System.exit(1);
        }

        props.putIfAbsent("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    @Override
    protected void emit(Map event) {
        String topic = topicRender.render(event).toString();
        if (this.format == null) {
            producer.send(new ProducerRecord<>(topic, InfinityJsonUtil.toJSON(event)));
        } else {
            Object message = this.format.render(event);
            if (message != null) {
                producer.send(new ProducerRecord<>(topic, message.toString()));
            }
        }
    }

    @Override
    public void shutdown() {
        log.info("close producer and then shutdown");
        producer.close();
    }
}
