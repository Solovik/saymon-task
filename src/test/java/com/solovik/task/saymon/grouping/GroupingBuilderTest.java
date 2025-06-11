package com.solovik.task.saymon.grouping;

import com.solovik.task.saymon.msg.SomeSourceMessage;
import com.solovik.task.saymon.msg.SourceMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupingBuilderTest {
    @Test
    public void createCollectorTest() {
        GroupingBuilder groupingBuilder = new GroupingBuilder(Set.of("A", "B"));

        List<SourceMessage> msgs = List.of(
                new SomeSourceMessage(1000L, Map.of("A", "value1", "B", "60"), 1.0),
                new SomeSourceMessage(1100L, Map.of("A", "value2", "B", "60"), 2.0),
                new SomeSourceMessage(1200L, Map.of("A", "value1", "B", "60"), 3.0),
                new SomeSourceMessage(1300L, Map.of("A", "value2", "B", "60"), 4.0),
                new SomeSourceMessage(1400L, Map.of("A", "value1", "B", "40"), 5.0)
        );

        Collector<SourceMessage, ?, Map<Map<String, String>, List<SourceMessage>>> collector = groupingBuilder.createGroupByCollector();

        Map<Map<String, String>, List<SourceMessage>> res = msgs.stream().collect(collector);

        Map<String, String> key1 = Map.of("A", "value1", "B", "40");
        Map<String, String> key2 = Map.of("A", "value1", "B", "60");
        Map<String, String> key3 = Map.of("A", "value2", "B", "60");
        List<SourceMessage> expected1 = List.of(new SomeSourceMessage(1400L, Map.of("A", "value1", "B", "40"), 5.0));
        List<SourceMessage> expected2 = List.of(
                new SomeSourceMessage(1000L, Map.of("A", "value1", "B", "60"), 1.0),
                new SomeSourceMessage(1200L, Map.of("A", "value1", "B", "60"), 3.0)
        );
        List<SourceMessage> expected3 = List.of(
                new SomeSourceMessage(1100L, Map.of("A", "value2", "B", "60"), 2.0),
                new SomeSourceMessage(1300L, Map.of("A", "value2", "B", "60"), 4.0)
        );

        assertTrue(res.get(key1).containsAll(expected1) &&
                expected1.containsAll(res.get(key1)));

        assertTrue(res.get(key2).containsAll(expected2) &&
                expected2.containsAll(res.get(key2)));

        assertTrue(res.get(key3).containsAll(expected3) &&
                expected3.containsAll(res.get(key3)));
    }
}
