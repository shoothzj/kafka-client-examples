package com.github.shoothzj.kafka.client.examples;

import org.apache.kafka.common.KafkaException;

public class KafkaUtil {

    public static boolean isRecordCorrupt(KafkaException e) {
        if (e.getMessage().contains(KafkaConstant.ERROR_MSG_CORRUPT)) {
            return true;
        } else {
            if (e.getCause() instanceof KafkaException) {
                return isRecordCorrupt((KafkaException) e.getCause());
            } else {
                return false;
            }
        }
    }

}
