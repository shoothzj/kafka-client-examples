package com.github.shoothzj.kafka.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public abstract class KafkaConsumerThread extends Thread {

    private final String topic;

    private final String groupId;

    private boolean initSuccess;

    protected KafkaConsumer<String, String> consumer;

    public KafkaConsumerThread(String topic, String groupId) {
        this.topic = topic;
        this.groupId = groupId;
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

    private void safeRun() {
        if (!initSuccess) {
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
        if (initSuccess) {
            doConsume(consumer.poll(Duration.ofMillis(500)));
        }
    }

    protected void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    protected abstract void doConsume(ConsumerRecords<String, String> records);

}
