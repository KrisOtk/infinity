package com.zwq.infinity.plugin.inputs;


import com.zwq.infinity.baseplugin.BaseInput;
import com.zwq.infinity.baseplugin.BaseMetric;
import com.zwq.infinity.plugin.metrics.MicroKafkaMetric;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;


@Slf4j
public class Kafka extends BaseInput {
    private static final Integer POOL_SIZE = 5;
    private static final Integer MAX_QUEUE_SIZE = 1_0000;
    private ExecutorService consumerExecutor;
    private Map<String, Integer> topics;
    private Properties props;
    private final ArrayList<ConsumerThread> consumerThreadsList = new ArrayList<>();
    private Integer processPoolSize;


    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicLong last = new AtomicLong(0);
    private LinkedBlockingQueue<String> blockingQueue;
    private Integer maxQueueSize;
    private ExecutorService processExecutors;

    public Kafka(Map<String, Object> config) {
        super(config);
    }

    public Kafka(Map<String, Object> config, List<BaseMetric> baseMetrics) {
        super(config, baseMetrics);
    }

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

        if (this.config.get("processPoolSize") != null) {
            Integer processPoolSize = (Integer) this.config.get("processPoolSize");
            log.info("kafka^^^processPoolSize:{}", processPoolSize);
            this.processPoolSize = processPoolSize;
        } else {
            this.processPoolSize = POOL_SIZE;
        }


        LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(1000);
        this.processExecutors = new ThreadPoolExecutor(processPoolSize, processPoolSize, 1, TimeUnit.MINUTES, workQueue);

        if (this.config.get("maxQueueSize") != null) {
            Integer maxQueueSize = (Integer) this.config.get("maxQueueSize");
            log.info("queue^^^maxQueueSize:{}", maxQueueSize);
            this.blockingQueue = new LinkedBlockingQueue<>(maxQueueSize);
            this.maxQueueSize = maxQueueSize;
        } else {
            this.blockingQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
            this.maxQueueSize = MAX_QUEUE_SIZE;
        }
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
        for (int i = 0; i < this.processPoolSize; i++) {
            processExecutors.submit(() -> {
                try {
                    while (true) {
                        String data = blockingQueue.poll(1, TimeUnit.SECONDS);
                        if (null != data) {
                            messageCount.incrementAndGet();
                            if (messageCount.get() % 100_0000 == 0) {
                                if (messageCount.get() > 1000_0000) {
                                    messageCount.set(0);
                                }
                                log.info("100w消息处理耗时,{}, {}", (System.currentTimeMillis() - last.get()), blockingQueue.size());
                                last.set(System.currentTimeMillis());
                            }
                            process(data);
                        } else {
                            log.debug("过去2s未从队列中获取数据，sleep 500ms");
                            Thread.sleep(500);
                        }
                    }
                } catch (InterruptedException e) {
                    log.info("从队列获取数据失败:{}", ExceptionUtils.getStackTrace(e));
                } finally {
                    log.info("退出消费者线程");
                }
            });
        }
    }

    @Override
    public void shutdown() {
        consumerThreadsList.forEach(consumerThread -> {
            KafkaConsumer<String, String> consumer = consumerThread.consumer;
            consumer.wakeup();
            consumer.close();
        });
        consumerExecutor.shutdown();
        processExecutors.shutdown();
        try {
            consumerExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            processExecutors.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.info("kafka executor shutdown error：{}", ExceptionUtils.getStackTrace(e));
        }
    }

    private class ConsumerThread implements Runnable {
        private final KafkaConsumer<String, String> consumer;
        private final List<BaseMetric> metrics;

        ConsumerThread(String topicName, Properties props, Kafka kafka) {
            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(Collections.singletonList(topicName));
            this.metrics = kafka.getMetrics();
        }


        @Override
        public void run() {
            while (true) {
                try {
                    Set<TopicPartition> assignment = new HashSet<>();
                    while (blockingQueue.remainingCapacity() < maxQueueSize * 0.15) {
                        assignment = consumer.assignment();
                        consumer.pause(assignment);
                    }
                    if (CollectionUtils.isNotEmpty(assignment)) {
                        consumer.resume(consumer.assignment());
                    }
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5_000));
                    saveMetric(this.metrics, records.count());
                    records.forEach(consumerRecord -> {
                        try {
                            String value = consumerRecord.value();
                            blockingQueue.put(value);
                        } catch (InterruptedException e) {
                            log.info("put to blockQueue error:{}", ExceptionUtils.getStackTrace(e));
                        }
                    });

                } catch (Throwable e) {
                    log.info("poll from kafka error：{}", ExceptionUtils.getStackTrace(e));
                }
            }
        }


    }

    private void saveMetric(List<BaseMetric> metrics, int count) {
        metrics.forEach(baseMetric -> {
            if (baseMetric instanceof MicroKafkaMetric) {
                MicroKafkaMetric microKafkaMetric = (MicroKafkaMetric) baseMetric;
                //请求总数
                Counter counter = microKafkaMetric.getCounter("kafka.poll.total", "kafka.poll.total", "kafka.poll.total");
                counter.increment(count);
            }
        });
    }
}
