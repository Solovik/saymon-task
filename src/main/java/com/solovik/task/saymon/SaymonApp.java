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
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.StreamSupport;

/**
 * Entry point of application.
 * Apply <i>application.conf</i> configuration on application.
 */
@Slf4j
public class SaymonApp {
    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    private static String outputTopic = "OUTPUT_TOPIC";

    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        Config pipelineConfig = config.getObject("application.pipeline").toConfig();

        SourceMessageCache cache = new SourceMessageCache(pipelineConfig.getLong("cacheMessageDelaySeconds"));

        SaymonService service = SaymonService.builder()
                .cache(cache)
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

        executor.scheduleAtFixedRate(() -> {
                    ConsumerRecords<String, SomeSourceMessage> records = consumer.poll(Duration.ofMillis(pipelineConfig.getLong("consumerTimeoutMs")));

                    log.debug("Got messages. Count: " + records.count());

                    StreamSupport.stream(records.spliterator(), false)
                            .map(ConsumerRecord::value)
                            .forEach(service::put);
                },
                1000,
                pipelineConfig.getLong("runPeriodMs"),
                TimeUnit.MILLISECONDS);

        executor.scheduleAtFixedRate(() -> {
                    List<SomeSinkMessage> msgs = service.process();
                    msgs.forEach(msg -> producer.send(new ProducerRecord<>(outputTopic, msg)));
                    log.info("Processed messages. Count: " + msgs.size());
                },
                2000,
                pipelineConfig.getLong("runPeriodMs"),
                TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            consumer.close();
            producer.close();
        }));
    }
}
