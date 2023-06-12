# kafka-client-examples
描述了一些Kafka客户端编码相关的最佳实践，并提供了可商用的样例代码，供大家研发的时候参考，提升大家接入Kafka的效率。在生产环境上，Kafka的地址信息往往都通过配置中心或者是k8s域名发现的方式获得，这块不是这篇文章描述的重点，以`KafkaConstant.BOOTSTRAP_SERVERS`代替。本文中的例子均已上传到[github](https://github.com/Shoothzj/kafka-client-examples)

## 生产者

### 初始化生产者重要参数

#### acks

消息持久化的核心参数。

`acks=0`代表生产者不会等待任何服务器的响应，把`record`加入到buffer中就认为已经送达。没有任何的持久化保证。

`acks=1`代表生产者会等待leader节点的响应，leader节点会在消息持久化后给生产者返回响应消息。

`acks=2`代表生产者会等待leader节点的响应，leader节点会等待一个`replica`节点持久化完消息后返回响应。

`acks=all`等同于`acks=-1`代表生产者会等待leader节点的响应，leader节点会等待所有的`replica`节点持久化完才会返回。

在**Kafka**集群商用的时候，现在都会选择多AZ或多主机的方式，使得Kafka**集群达到高可用。（对应的故障域为AZ或主机）。一般有3AZ和5AZ。一般要求单AZ宕机系统可正常运行不丢数据。这里建议配置为`acks=1`，保证单AZ宕机后，系统消息不丢失。

#### batch相关参数

因为批量发送模式底层由定时任务实现，如果该topic上消息数较小，则不建议开启`batch`。（测试时，如果打开批量，工作负载的cpu无明显降低，就不要开启`batch`了）

- **batch.size** 批量发送最大消息大小，单位为bytes
- **linger.ms** 批量发送时间，设置为0时，代表立刻发出

#### request.timeout.ms

客户端请求的最大超时时间。如果请求没有在超时之前完成，客户端会按照策略进行一些重试。

应该配置地比`replica.lag.time.max.ms`大，降低不必要的生产者重试。

默认为30s，该默认值在数据中心内部显得有些保守，建议调低。

#### buffer.memory

生产者可用于缓冲等待发送到服务器的记录的总内存字节数。该配置和消息量与**Kafka**的最大不可用时间密切相关。假设**PaaS**厂商或自建的**Kafka**可保证`Kafka`在升级或AZ宕机时不可用时间最大为10s，并且您的最大业务消息吞吐量为10MB/s，那么就可以将`buffer.memory`配置为150MB（比100MB略大）。

#### max.block.ms
send会触发一些`fetchMetadata`或分配内存的操作。这个配置决定了`send`的最大阻塞时间。


### 初始化producer样例

一般我们会要求`kafka`生产者创建失败不影响服务的启动，服务可以启动，而kafka生产者在后台一直尝试，直到创建成功。

`kafka`的生产者是不区分topic的，在数据量较小的情况下，所有topic都可以共用一个生产者。一些场景，单生产者可能会因为`io`线程达到极限或其他限制，不能满足我们的性能要求。这时候，我们就需要多个`kafka`生产者，可以随机分配，也可以按topic来分配。如果仅有一个topic量特别大，那么只能随机分配。如果多个topic数据比较平均，建议按topic分配，这样可以减少producer请求元数据的次数，节约集群的性能。

#### 一个生产者一个初始化线程，适用于Producer较少的场景

```java
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

    private volatile Producer<String, String> producer;

    public KafkaProducerInit() {
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.BOOTSTRAP_SERVERS);
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
```



#### 多个生产者一个初始化线程，适用Producer较多的场景，以随机分配举例

```java
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class KafkaProducersInit {

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("kafka-producers-init").build();

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, threadFactory);

    private CopyOnWriteArrayList<Producer<String, String>> producers;

    private int initIndex;

    private final List<String> topics;

    public KafkaProducersInit(List<String> topics) {
        this.topics = topics;
    }

    public void init() {
        executorService.scheduleWithFixedDelay(this::initWithRetry, 0, 10, TimeUnit.SECONDS);
    }

    private void initWithRetry() {
        if (initIndex == topics.size()) {
            executorService.shutdown();
            return;
        }
        for (; initIndex < topics.size(); initIndex++) {
            try {
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.BOOTSTRAP_SERVERS);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.ACKS_CONFIG, "1");
                // 2MB
                props.put(ProducerConfig.BATCH_SIZE_CONFIG, "2097152");
                props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
                props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
                props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "5000");
                props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "500");
                KafkaProducer<String, String> producer = new KafkaProducer<>(props);
                producers.add(producer);
            } catch (Exception e) {
                log.error("init kafka producer error, exception is ", e);
                break;
            }
        }
    }

    public Producer<String, String> getProducer(int idx) {
        return producers.get(idx);
    }

}
```



### 可以接受消息丢失的发送

```java
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
```

以上为正确处理`Producer`创建失败和发送失败的回调函数。但是由于在生产环境下，`kafka`并不是一直保持可用的，会因为虚拟机故障、`kafka`服务升级等导致发送失败。这个时候如果要保证消息发送成功，就需要对消息发送进行重试。

### 可以容忍极端场景下的发送丢失

```java
    private final Timer timer = new HashedWheelTimer();

    private void sendMsgWithRetry(String topic, String key, String value, int retryTimes, int maxRetryTimes) {
        final Producer<String, String> producer = exampleKafkaProducerInit.getProducer();
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
                    timer.newTimeout(timeout -> ExampleKafkaProducerService.this.sendMsgWithRetry(topic, key, value, retryTimes + 1, maxRetryTimes), 1L << retryTimes, TimeUnit.SECONDS);
                    return;
                }
                log.error("kafka msg key {} send failed, exception is ", key, exception);
            });
        } catch (Exception e) {
            log.error("kafka msg key {} send failed, exception is ", key, e);
            this.sendMsgWithRetry(topic, key, value, retryTimes + 1, maxRetryTimes);
        }
    }
```

这里在发送失败后，做了退避重试，可以容忍`kafka`服务端故障一段时间。比如退避7次、初次间隔为1s，那么就可以容忍`1+2+4+8+16+32+64=127s`的故障。这已经足够满足大部分生产环境的要求了。<br/>
因为理论上存在超过127s的故障，所以还是要在极端场景下，向上游返回失败。

### 生产者Partition级别严格保序

生产者严格保序的要点：一次只发送一条消息，确认发送成功后再发送下一条消息。实现上可以使用同步异步两种模式：

- 同步模式的要点就是循环发送，直到上一条消息发送成功后，再启动下一条消息发送
- 异步模式的要点是观测上一条消息发送的future，如果失败也一直重试，成功则启动下一条消息发送

值得一提的是，这个模式下，partition间是可以并行的，可以使用`OrderedExecutor`或`per partition per thread`

同步模式举例：

```java
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author hezhangjian
 */
@Slf4j
public class ExampleKafkaProducerSyncStrictlyOrdered {

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
```



## 消费者

### 初始化消费者重要参数

#### 拉取相关
- **max.poll.records** 一次`poll()`方法最多能拉取到多少条消息
- **max.poll.interval.ms** 最多在`poll()`方法中等待的时间。在kafka-client版本`0.10.1`之前，消费者的心跳是在`poll`的时候顺带发出的。需要注意`max.poll.interval.ms`需要大于两次`poll`方法的间隔，否则会触发无意义的rebalance

这两个参数往往被大家误解，`kafka`并不会保证拉取到`max.poll.records`条消息，也不保证会等待`max.poll.interval.ms`时间。以`max.poll.records`为500，`max.poll.interval.ms`为500ms举例，`Kafka Consumer`有可能会在`50ms`后返回，携带100多条消息。

#### enable.auto.commit

是否开启自动提交。如果打开，消费者会自动提交offset。商用系统里，如果要求消息至少处理一次，一般会选择手动提交offset。

#### auto.commit.interval.ms

自动提交间隔

#### request.timeout.ms

客户端请求的最大超时时间。如果请求没有在超时之前完成，客户端会按照策略进行一些重试。

默认为30s，该默认值在数据中心内部显得有些保守，建议调低。

#### auto.offset.reset

订阅开始的位置，根据业务需求决定放到最前或者最后。


### 创建消费者原则

消费者只有创建成功才能工作，不像生产者可以向上游返回失败，所以消费者要一直重试创建。示例代码如下：
注意：消费者和topic可以是一对多的关系，消费者可以订阅多个topic。

创建消费者样例
```java
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
import java.util.Locale;
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
        if (initSuccess) {
            doConsume(consumer.poll(Duration.ofMillis(500)));
        }
    }

    protected void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    protected abstract void doConsume(ConsumerRecords<String, String> records);

}
```

### 消费者达到至少一次语义

`Kafka`达到至少一次语义，核心就是等待消息处理成功后再进行commit提交`offset`。

#### 同步模式举例

这里需要注意，如果处理消息时长差距比较大，同步处理的方式可能会让本来可以很快处理的消息得不到处理的机会。

```java
    private void doConsumeAtLeastOnceSync(KafkaConsumer<String, String> consumer) {
        final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> record : consumerRecords) {
            while (!consumeRecordSync(record)) {
            }
        }
        consumer.commitSync(Duration.ofMillis(200));
    }

    private boolean consumeRecordSync(ConsumerRecord<String, String> record) {
        return random.nextBoolean();
    }
```

#### 异步模式举例

异步的话需要考虑内存的限制，因为异步的方式可以很快地从`kafka`消费，不会被业务操作阻塞，这样 **inflight** 的消息可能会非常多。可以通过下面的`消费者繁忙时阻塞拉取消息，不再进行业务处理`通过判断**inflight**消息数来阻塞处理。这里使用跳表来提交`offset`，是本文中最复杂的代码

```java
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

    private final long kafkaMaxDiscontinuousOffsetWaitTimeMills;

    public KafkaConsumerThreadAsyncAtLeastOnce(String topic, String groupId, long maxOffsetForwardStuckMills) {
        super(topic, groupId);
        this.kafkaMaxDiscontinuousOffsetWaitTimeMills = maxOffsetForwardStuckMills;
    }

    public KafkaConsumerThreadAsyncAtLeastOnce(String topic, String groupId, int port, long maxOffsetForwardStuckMills) {
        super(topic, groupId, port);
        this.kafkaMaxDiscontinuousOffsetWaitTimeMills = maxOffsetForwardStuckMills;
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
                offsetHelper.setPreviousAckQueueHead(first);
                if (first == offsetHelper1.lastCommitOffset + 1) {
                    offsetHelper1.lastCommitOffset = first;
                } else if (kafkaMaxDiscontinuousOffsetWaitTimeMills > 0
                        && offsetHelper1.previousAckQueueHead > offsetHelper1.lastCommitOffset
                        && System.currentTimeMillis() - offsetHelper1.ackQueueHeadUpdateTime > this.kafkaMaxDiscontinuousOffsetWaitTimeMills) {

                    // if records are not ordered, like '1 2 5', the local offset will be stuck and never forward,
                    // so we want set the offset being stuck tolerance wait time

                    log.warn("last commit offset updateTime {} exceed kafka max discontinuous offset wait time {}",
                            offsetHelper1.ackQueueHeadUpdateTime, kafkaMaxDiscontinuousOffsetWaitTimeMills);
                    offsetHelper1.lastCommitOffset = offsetHelper1.previousAckQueueHead;
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
            commitOffsets.put(topicPartition, new OffsetAndMetadata(offsetHelper.lastCommitOffset + 1));
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

        long previousAckQueueHead = -1;

        long ackQueueHeadUpdateTime;

        final ConcurrentSkipListSet<Long> skipListSet;

        public OffsetHelper(long offset) {
            this.lastCommitOffset = offset;
            this.skipListSet = new ConcurrentSkipListSet<>();
        }

        public void setPreviousAckQueueHead(long previousAckQueueHead) {
            if (this.previousAckQueueHead == previousAckQueueHead){
                return;
            }
            this.previousAckQueueHead = previousAckQueueHead;
            this.ackQueueHeadUpdateTime = System.currentTimeMillis();
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
```



### 消费者繁忙时阻塞拉取消息，不再进行业务处理

当消费者处理不过来时，通过阻塞`listener`方法，不再进行业务处理。避免在微服务积累太多消息导致OOM，可以通过RateLimiter或者Semaphore控制处理。

```java
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
```

### Kafka消费者Bug导致CPU 100%

kafka的客户端存在一个已知问题，偶现CPU 100%。如果您也碰到了这个问题，可以采用如下策略：在Kafka CPU 超过90%，并且持续2min的时候。对KafkaConsumer进行重建。其中获取线程cpu的方法，可参见[java 根据线程统计CPU](https://www.jianshu.com/p/a0abe36654d7)

```java
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
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
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString().toLowerCase(Locale.ENGLISH));
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
```




## 致谢

感谢 [天翔](https://github.com/Jack-Jiang)、 [志伟](https://github.com/xuzhiweiand)和 [李雪](https://github.com/tracy-java)的审稿。