package com.github.shoothzj.kafka.client.examples;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class KafkaProducerService {

    private final KafkaProducerInit kafkaProducerInit;

    public KafkaProducerService() {
        this.kafkaProducerInit = new KafkaProducerInit();
    }

    public KafkaProducerService(int port) {
        this.kafkaProducerInit = new KafkaProducerInit(port);
    }

    public void sendMsg(String topic, String key, String value) {
        final Producer<String, String> producer = kafkaProducerInit.getProducer();
        if (producer == null) {
            // haven't init success
            log.error("producer haven't init success, send msg failed");
            return;
        }
        try {
            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("kafka msg key {} send failed, exception is ", key, exception);
                    return;
                }
                log.info("kafka msg send success partition {} offset {}", metadata.partition(), metadata.offset());
            });
        } catch (Exception e) {
            log.error("kafka msg key {} send failed, exception is ", key, e);
        }
    }

    private final Timer timer = new HashedWheelTimer();

    private void sendMsgWithRetry(String topic, String key, String value, int retryTimes, int maxRetryTimes) {
        final Producer<String, String> producer = kafkaProducerInit.getProducer();
        if (producer == null) {
            // haven't init success
            log.error("producer haven't init success, send msg failed");
            return;
        }
        try {
            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("kafka msg send success partition {} offset {}", metadata.partition(), metadata.offset());
                    return;
                }
                if (retryTimes < maxRetryTimes) {
                    log.warn("kafka msg key {} send failed, begin to retry {} times exception is ", key, retryTimes, exception);
                    timer.newTimeout(timeout -> KafkaProducerService.this.sendMsgWithRetry(topic, key, value, retryTimes + 1, maxRetryTimes), 1L << retryTimes, TimeUnit.SECONDS);
                    return;
                }
                log.error("kafka msg key {} send failed, exception is ", key, exception);
            });
        } catch (Exception e) {
            if (retryTimes < maxRetryTimes) {
                log.warn("kafka msg key {} send failed, begin to retry {} times exception is ", key, retryTimes, e);
                timer.newTimeout(timeout -> this.sendMsgWithRetry(topic, key, value, retryTimes + 1, maxRetryTimes), 1L << retryTimes, TimeUnit.SECONDS);
            } else {
                log.error("kafka msg key {} send failed, exception is ", key, e);
            }
        }
    }

}
