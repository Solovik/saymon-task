package com.solovik.task.saymon.msg;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializer for Kafka.
 */
public class SomeSourceMessageDeserializer implements Deserializer<SomeSourceMessage> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public SomeSourceMessage deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, SomeSourceMessage.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
