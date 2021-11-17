package com.github.shoothzj.kafka.client.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author hezhangjian
 */
@Slf4j
public class KafkaConsumerThreadAsyncAtLeastOnce extends KafkaConsumerThread {

    private final ConcurrentHashMap<TopicPartition, OffsetHelper> skipListMap = new ConcurrentHashMap<>();

    public KafkaConsumerThreadAsyncAtLeastOnce(String topic, String groupId) {
        super(topic, groupId);
    }

    @Override
    protected void doConsume(ConsumerRecords<String, String> records) {
        Set<TopicPartition> set = new HashSet<>();
        for (ConsumerRecord<String, String> record : records) {
            final TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            set.add(topicPartition);
            skipListMap.putIfAbsent(topicPartition, new OffsetHelper(record.offset() - 1));
            consumeRecordAsync(record, new AsyncConsumeCallback(record));
        }
        skipListMap.forEach((topicPartition, offsetHelper) -> skipListMap.computeIfPresent(topicPartition, (topicPartition1, offsetHelper1) -> {
            while (true) {
                final Long first = offsetHelper1.skipListSet.pollFirst();
                if (first == null) {
                    break;
                }
                if (first == offsetHelper1.lastCommitOffset + 1) {
                    offsetHelper1.lastCommitOffset = first;
                } else {
                    offsetHelper1.skipListSet.add(first);
                    break;
                }
            }
            return offsetHelper1;
        }));
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
        for (TopicPartition topicPartition : set) {
            final OffsetHelper offsetHelper = skipListMap.get(topicPartition);
            if (offsetHelper == null) {
                continue;
            }
            commitOffsets.put(topicPartition, new OffsetAndMetadata(offsetHelper.lastCommitOffset));
        }
        consumer.commitAsync(commitOffsets, (offsets, exception) -> {
            if (exception != null) {
                log.error("commit offset failed ", exception);
            }
        });
    }

    @Override
    protected void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            skipListMap.remove(partition);
        }
    }

    static class OffsetHelper {

        long lastCommitOffset;

        final ConcurrentSkipListSet<Long> skipListSet;

        public OffsetHelper(long offset) {
            this.lastCommitOffset = offset;
            this.skipListSet = new ConcurrentSkipListSet<>();
        }
    }

    class AsyncConsumeCallback implements BusinessCallback {

        private final ConsumerRecord<String, String> record;

        public AsyncConsumeCallback(ConsumerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public void callback(Exception e) {
            if (e == null) {
                final TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                final OffsetHelper offsetHelper = skipListMap.get(topicPartition);
                if (offsetHelper != null) {
                    offsetHelper.skipListSet.add(record.offset());
                }
            } else {
                consumeRecordAsync(record, new AsyncConsumeCallback(record));
            }
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
