package com.solovik.task.saymon;

import com.solovik.task.saymon.dedup.WindowDedup;
import com.solovik.task.saymon.filter.FilterBuilder;
import com.solovik.task.saymon.grouping.GroupingBuilder;
import com.solovik.task.saymon.msg.*;
import com.solovik.task.saymon.window.WindowAggregator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Entry point of application.
 * Apply <i>application.conf</i> configuration on application.
 */
@Slf4j
public class SaymonApp {
    private static final ExecutorService executor = Executors.newFixedThreadPool(4);
    private static String outputTopic = "OUTPUT_TOPIC";

    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        Config pipelineConfig = config.getObject("application.pipeline").toConfig();

        SaymonService service = SaymonService.builder()
                .deduplicator(new WindowDedup(
                        pipelineConfig.getLong("deduplication.windowMs"),
                        Set.copyOf(pipelineConfig.getStringList("deduplication.labels"))))
                .filtration(new FilterBuilder(pipelineConfig.getObjectList("filtering")).createPredicate())
                .groupingBuilder(new GroupingBuilder(Set.copyOf(pipelineConfig.getStringList("groupBy"))))
                .windowAggregator(new WindowAggregator(pipelineConfig.getInt("aggregateIntervalInSeconds")))
                .build();


        KafkaConsumer<String, SomeSourceMessage> consumer = new KafkaConsumer<>(config.getConfig("kafka.consumer").root().unwrapped());
        consumer.subscribe(List.of(pipelineConfig.getString("inputTopic")));

        KafkaProducer<String, SomeSinkMessage> producer = new KafkaProducer<>(config.getConfig("kafka.producer").root().unwrapped());
        outputTopic = pipelineConfig.getString("outputTopic");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            consumer.close();
            producer.close();
        }));

        while (true) {
            ConsumerRecords<String, SomeSourceMessage> records = consumer.poll(Duration.ofMillis(pipelineConfig.getLong("retrieveIntervalMs")));
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            Iterable<SourceMessage> sms = StreamSupport.stream(records.spliterator(), false)
                    .map(ConsumerRecord::value)
                    .collect(Collectors.toList());

            CompletableFuture<Void> future = CompletableFuture
                    .supplyAsync(() -> service.process(() -> sms), executor)
                    .thenAccept(sinkMessages -> {
                        sinkMessages.forEach(sinkMessage -> {
                            producer.send(
                                    new ProducerRecord<>(outputTopic, sinkMessage),
                                    (metadata, exception) -> {
                                        if (exception != null) {
                                            exception.printStackTrace();
                                        } else {
                                            log.debug("Sent: " + sinkMessage);
                                        }
                                    }
                            );
                        });
                    });

            futures.add(future);
        }
    }
}
