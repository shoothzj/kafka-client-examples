package com.github.shoothzj.kafka.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author hezhangjian
 */
@Slf4j
public class KafkaConsumerThreadAtLeastOnce extends KafkaConsumerThread {

    public KafkaConsumerThreadAtLeastOnce(String topic, String groupId) {
        super(topic, groupId);
    }

    protected void doConsume(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            while (!consumeRecordSync(record)) {
            }
        }
        consumer.commitAsync();
    }

    private boolean consumeRecordSync(ConsumerRecord<String, String> record) {
        return System.currentTimeMillis() % 2 == 0;
    }

}