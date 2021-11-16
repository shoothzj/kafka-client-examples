package com.github.shoothzj.kafka.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.concurrent.Semaphore;

/**
 * @author hezhangjian
 */
@Slf4j
public class KafkaConsumerThreadSemaphore extends KafkaConsumerThread {

    /**
     * Semaphore保证最多同时处理500条消息
     */
    private final Semaphore semaphore = new Semaphore(500);


    public KafkaConsumerThreadSemaphore(String topic, String groupId) {
        super(topic, groupId);
    }

    @Override
    protected void doConsume(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            doConsumeRecord(record);
        }
    }

    private void doConsumeRecord(ConsumerRecord<String, String> record) {
        try {
            semaphore.acquire();
            consumeRecordAsync(record, e -> {
                semaphore.release();
                if (e != null) {
                    log.error("topic {} offset {} exception is ", record.topic(), record.offset(), e);
                }
            });
        } catch (Exception e) {
            semaphore.release();
            // 业务方法可能会抛出异常
            log.error("topic {} offset {} exception is ", record.topic(), record.offset(), e);
        }
    }

    private void consumeRecordAsync(ConsumerRecord<String, String> record, BusinessCallback callback) {
        if (System.currentTimeMillis() % 2 == 0) {
            callback.callback(null);
        } else {
            callback.callback(new Exception("exception"));
        }
    }

}
