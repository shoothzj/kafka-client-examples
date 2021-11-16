package com.github.shoothzj.kafka.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author hezhangjian
 */
@Slf4j
public class KafkaProducerSyncStrictlyOrdered {

    Producer<String, String> producer;

    public void sendMsg(String topic, String key, String value) {
        while (true) {
            try {
                final RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic, key, value)).get();
                log.info("topic {} send success, msg offset is {}", recordMetadata.topic(), recordMetadata.offset());
                break;
            } catch (Exception e) {
                log.error("exception is ", e);
            }
        }
    }

}
