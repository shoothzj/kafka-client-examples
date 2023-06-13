package com.github.shoothzj.kafka.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author hezhangjian
 */
@Slf4j
public abstract class KafkaConsumerThread extends Thread {

    private final String topic;

    private final String groupId;

    private final int port;

    private boolean initSuccess;

    protected KafkaConsumer<String, String> consumer;

    public KafkaConsumerThread(String topic, String groupId) {
        this(topic, groupId, KafkaConstant.DEFAULT_PORT);
    }

    public KafkaConsumerThread(String topic, String groupId, int port) {
        this.topic = topic;
        this.groupId = groupId;
        this.port = port;
        this.initSuccess = false;
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

    private void buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format(KafkaConstant.BOOTSTRAP_TEMPLATE, KafkaConstant.DEFAULT_PORT));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString().toLowerCase(Locale.ENGLISH));
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    KafkaConsumerThread.this.onPartitionsAssigned(partitions);
                }
            });
            this.consumer = consumer;
            this.initSuccess = true;
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

    private void rebuildConsumer() {
        this.initSuccess = false;
        this.consumer.close();
        buildConsumer();
    }

    private void safeRun() {
        if (!initSuccess) {
            buildConsumer();
        }
        if (initSuccess) {
            try {
                doConsume(consumer.poll(Duration.ofMillis(500)));
            } catch (KafkaException e) {
                if (KafkaUtil.isRecordCorrupt(e)) {
                    log.info("record is corrupt, rebuilding consumer", e);
                    rebuildConsumer();
                } else {
                    throw e;
                }
            }
        }
    }

    protected void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    protected abstract void doConsume(ConsumerRecords<String, String> records);

}
