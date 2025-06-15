package com.solovik.task.saymon;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solovik.task.saymon.msg.SomeSourceMessage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;

/**
 * MADE FOR INTEGRATION TESTS PURPOSE.
 * Generates SourceMessages to SOURCE topic.
 */
@Slf4j
public class SendMsgs {
    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        System.out.println(config.getConfig("kafka.producer").root().unwrapped().toString());

        ObjectMapper mapper = new ObjectMapper();

        KafkaProducer<String, SomeSourceMessage> producer = new KafkaProducer<>(config.getConfig("kafka.producer").root().unwrapped(),
                new StringSerializer(),
                (topic, data) -> {
                    try {
                        return mapper.writeValueAsBytes(data);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });

        List<SomeSourceMessage> msgs = List.of(
                new SomeSourceMessage(1000L, Map.of("A", "value1", "B", "80"), 80.0),
                new SomeSourceMessage(1005L, Map.of("A", "value1", "B", "80"), 90.0),
                new SomeSourceMessage(1010L, Map.of("A", "value1", "B", "40"), 40.0),
                new SomeSourceMessage(1300L, Map.of("A", "value1", "B", "80"), 110.0),
                new SomeSourceMessage(2000L, Map.of("A", "value1", "B", "60"), 60.0)

        );
        for (SomeSourceMessage msg : msgs) {
            try {
                RecordMetadata meta = producer.send(
                        new ProducerRecord<>("SOURCE", "1", msg)
                ).get();

                log.info(meta.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
