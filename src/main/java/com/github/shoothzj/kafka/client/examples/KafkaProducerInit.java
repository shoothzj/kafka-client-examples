package com.github.shoothzj.kafka.client.examples;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class KafkaProducerInit {

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("kafka-producer-init").build();

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, threadFactory);

    private final int port;

    private volatile Producer<String, String> producer;

    public KafkaProducerInit() {
        this(KafkaConstant.DEFAULT_PORT);
    }

    public KafkaProducerInit(int port) {
        this.port = port;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format(KafkaConstant.BOOTSTRAP_TEMPLATE, port));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            // 2MB
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, "2097152");
            props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "5000");
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "500");
            producer = new KafkaProducer<>(props);
            executorService.shutdown();
        } catch (Exception e) {
            log.error("init kafka producer error, exception is ", e);
        }
    }

    public Producer<String, String> getProducer() {
        return producer;
    }

}
