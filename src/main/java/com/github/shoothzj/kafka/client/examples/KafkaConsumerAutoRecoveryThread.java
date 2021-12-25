package com.github.shoothzj.kafka.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public abstract class KafkaConsumerAutoRecoveryThread extends Thread {

    private final String topic;

    private final String groupId;

    private boolean needBuild;

    protected KafkaConsumer<String, String> consumer;

    /**
     * 上一次低CPU的时间
     */
    private long time;

    public KafkaConsumerAutoRecoveryThread(String topic, String groupId) {
        this.topic = topic;
        this.groupId = groupId;
        this.needBuild = true;
        this.setName("kafka-consumer-" + topic);
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            try {
                safeRun();
            } catch (Exception e) {
                log.error("topic {} error, loop exited ", topic, e);
            }
        }
    }

    private void safeRun() {
        if (needBuild) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.BOOTSTRAP_SERVERS);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
            KafkaConsumer<String, String> consumer = null;
            try {
                consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList(topic));
                this.consumer = consumer;
                this.needBuild = false;
            } catch (Exception ex) {
                log.error("init kafka consumer error, exception is ", ex);
                if (consumer != null) {
                    try {
                        consumer.close();
                    } catch (Exception exception) {
                        log.error("ignore exception ", exception);
                    }
                }
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        if (!needBuild) {
            doConsume(consumer.poll(Duration.ofMillis(500)));
        }
        if (currentThreadCpu() <= 90) {
            time = System.currentTimeMillis();
        }
        if (System.currentTimeMillis() - time > 2 * 60 * 1000) {
            needBuild = true;
        }
        try {
            consumer.close();
        } catch (Exception exception) {
            log.error("ignore exception ", exception);
        }
    }

    protected abstract int currentThreadCpu();

    protected abstract void doConsume(ConsumerRecords<String, String> records);

}

