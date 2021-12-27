package com.github.shoothzj.kafka.client.examples;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

class KafkaProducersInitTest {

    @BeforeAll
    static void setLogLevel() {
        Configurator.setRootLevel(Level.INFO);
    }

    @Test
    @Timeout(value = 30)
    void testKafkaProducerInit() throws Exception {
        TestKfkServer kfkServer = new TestKfkServer();
        kfkServer.start();
        List<String> topics = List.of("topic1", "topic2");
        KafkaProducersInit kafkaProducersInit = new KafkaProducersInit(topics);
        kafkaProducersInit.init();
        TimeUnit.SECONDS.sleep(10);
        Assertions.assertNotNull(kafkaProducersInit.getProducer(0));
        Assertions.assertNotNull(kafkaProducersInit.getProducer(1));
        kfkServer.close();
    }

}