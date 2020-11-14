package com.zwq.infinity.plugin.inputs;


import com.zwq.infinity.baseplugin.BaseInput;
import com.zwq.infinity.baseplugin.BaseMetric;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;


@Slf4j
public class Kafka2 extends BaseInput {
    private ExecutorService consumerExecutor;
    private Map<String, Integer> topics;
    private Properties props;
    private final ArrayList<ConsumerThread> consumerThreadsList = new ArrayList<>();

    public Kafka2(Map<String, Object> config) {
        super(config);
    }
    public Kafka2(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    private AtomicLong messageCount = new AtomicLong(0);
    private AtomicLong last = new AtomicLong(0);


    @Override
    protected void prepare() {
        topics = (HashMap<String, Integer>) this.config.get("topic");
        props = new Properties();
        Map<String, Object> consumerSettings = (HashMap<String, Object>) this.config.get("consumer_settings");
        consumerSettings.putIfAbsent("enable.auto.commit", true);//是否自动提交
        consumerSettings.putIfAbsent("auto.commit.interval.ms", 5000);//自动提交间隔,默认5000
        consumerSettings.putIfAbsent("max.poll.records", 2000);//默认500,单次最多拉去多少条数据
        consumerSettings.putIfAbsent("max.poll.interval.ms", 30000);//默认20s
        consumerSettings.putIfAbsent("session.timeout.ms", 30000);//consumer会话超时时间,超过阈值触发重新分配
        consumerSettings.putIfAbsent("heartbeat.interval.ms", 3000);//默认3s,心跳间隔
        consumerSettings.putIfAbsent("max.partition.fetch.bytes", 2097152);//一次fetch从一个partition中取得的records最大大小,默认1048576=1M,设置2M
        consumerSettings.putIfAbsent("fetch.min.bytes ", 2097152);//默认1,最小返回数据量,设置2M
        consumerSettings.putIfAbsent("fetch.max.bytes ", 52428800);//默认52428800=50M,最大返回数据量
        consumerSettings.putIfAbsent("fetch.max.wait.ms", 5000);//默认500,fetch最长等待时间
        consumerSettings.forEach((key, value) -> props.put(key, value));
    }

    @Override
    public void emit() {
        //Create Consumer Streams Map
        topics.forEach((topic, size) -> {
            KafkaConsumer consumer = new KafkaConsumer<>(props);
            int partitionSize = consumer.partitionsFor(topic).size();
            int threadSize = (partitionSize < size) ? partitionSize : size;
            consumerExecutor = Executors.newFixedThreadPool(threadSize);
            IntStream.range(0, threadSize).forEach(a -> {
                ConsumerThread task = new ConsumerThread(topic, props, this);
                consumerThreadsList.add(task);
                consumerExecutor.submit(task);
            });

        });
        last.set(System.currentTimeMillis());
    }

    @Override
    public void shutdown() {
        consumerThreadsList.forEach(consumerThread -> {
            KafkaConsumer<String, String> consumer = consumerThread.consumer;
            consumer.wakeup();
            consumer.close();
        });
        consumerExecutor.shutdown();
        try {
            consumerExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.info("kafka executor shutdown error：{}", ExceptionUtils.getStackTrace(e));
        }
    }

    private class ConsumerThread implements Runnable {
        private final KafkaConsumer<String, String> consumer;

        ConsumerThread(String topicName, Properties props, Kafka2 kafka) {
            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(Collections.singletonList(topicName));
        }


        @Override
        public void run() {
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10_000));
                    records.forEach(consumerRecord -> {
                        try {
                            String value = consumerRecord.value();
                            messageCount.incrementAndGet();
                            if (messageCount.get() % 100_0000 == 0) {
                                if (messageCount.get() > 1000_0000) {
                                    messageCount.set(0);
                                }
                                log.info("100w消息处理耗时,{}", (System.currentTimeMillis() - last.get()));
                                last.set(System.currentTimeMillis());
                            }
                            process(value);
                        } catch (Exception e) {
                            log.info("put to blockQueue error:{}", ExceptionUtils.getStackTrace(e));
                        }
                    });
                } catch (Throwable e) {
                    log.info("poll from kafka error：{}", ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

}
