package com.github.shoothzj.kafka.client.examples;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

class KafkaProducerInitTest {

    @BeforeAll
    static void setLogLevel() {
        Configurator.setRootLevel(Level.INFO);
    }

    @Test
    @Timeout(value = 30)
    void testKafkaProducerInit() throws Exception {
        TestKfkServer kfkServer = new TestKfkServer();
        kfkServer.start();
        KafkaProducerInit kafkaProducerInit = new KafkaProducerInit();
        kafkaProducerInit.init();
        TimeUnit.SECONDS.sleep(10);
        Assertions.assertNotNull(kafkaProducerInit.getProducer());
        kfkServer.close();
    }

}