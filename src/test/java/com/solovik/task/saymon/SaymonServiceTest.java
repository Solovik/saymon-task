package com.solovik.task.saymon;

import com.solovik.task.saymon.dedup.WindowDedup;
import com.solovik.task.saymon.filter.FilterBuilder;
import com.solovik.task.saymon.grouping.GroupingBuilder;
import com.solovik.task.saymon.msg.SinkMessage;
import com.solovik.task.saymon.msg.SomeSinkMessage;
import com.solovik.task.saymon.msg.SomeSourceMessage;
import com.solovik.task.saymon.msg.SourceMessage;
import com.solovik.task.saymon.window.WindowAggregator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SaymonServiceTest {
    @Test
    public void basicTest() {
        Config config = ConfigFactory.load();
        Config pipelineConfig = config.getObject("application.pipeline").toConfig();

        SaymonService service = SaymonService.builder()
                .deduplicator(new WindowDedup(
                        pipelineConfig.getLong("deduplication.windowMs"),
                        Set.copyOf(pipelineConfig.getStringList("deduplication.labels"))))
                .filtration((msg) -> true)
                .filtration(new FilterBuilder(pipelineConfig.getObjectList("filtering")).createPredicate())
                .groupingBuilder(new GroupingBuilder(Set.copyOf(pipelineConfig.getStringList("groupBy"))))
                .windowAggregator(new WindowAggregator(pipelineConfig.getInt("aggregateIntervalInSeconds")))
                .build();

        Iterable<SourceMessage> msgs = List.of(
                new SomeSourceMessage(1000L, Map.of("A", "value1", "B", "80"), 80.0),
                new SomeSourceMessage(1005L, Map.of("A", "value1", "B", "80"), 90.0),
                new SomeSourceMessage(1010L, Map.of("A", "value1", "B", "40"), 40.0),
                new SomeSourceMessage(1300L, Map.of("A", "value1", "B", "80"), 110.0),
                new SomeSourceMessage(2000L, Map.of("A", "value1", "B", "60"), 60.0)
        );

        List<SomeSinkMessage> res = service.process(() -> msgs);

        System.out.println("*** count: " + res.size());
        System.out.println("*** : " + res);



    }
}
