package com.solovik.task.saymon.window;

import com.solovik.task.saymon.msg.SomeSinkMessage;
import com.solovik.task.saymon.msg.SomeSourceMessage;
import com.solovik.task.saymon.msg.SourceMessage;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WindowAggregatorTest {
    @Test
    public void oneWindowAggregateTest() {
        Map<String, String> key1 = Map.of("A", "value1", "B", "40");
        List<SourceMessage> list1 = List.of(
                new SomeSourceMessage(1400L, Map.of("A", "value1", "B", "40"), 15.0),
                new SomeSourceMessage(1500L, Map.of("A", "value1", "B", "40"), 5.0)
        );

        Map<Map<String, String>, List<SourceMessage>> groupedMessages = Map.of(
                key1, list1
        );

        WindowAggregator windowAggregator = new WindowAggregator(1);

        List<SomeSinkMessage> res = groupedMessages.entrySet().stream()
                .flatMap(entry -> windowAggregator.aggregate(entry.getKey(), entry.getValue()))
                .toList();

        System.out.println(Arrays.toString(res.toArray()));

        assertEquals(1, res.size());

        assertEquals(new SomeSinkMessage(
                Map.of("A", "value1", "B", "40"),
                1400L,
                1500L,
                20.0,
                5.0,
                15.0,
                2
        ), res.getFirst());
    }

    @Test
    public void multipleWindowsAggregateTest() {
        Map<String, String> key1 = Map.of("A", "value1", "B", "40");
        List<SourceMessage> list1 = List.of(
                new SomeSourceMessage(1400L, Map.of("A", "value1", "B", "40"), 4.0),
                new SomeSourceMessage(2500L, Map.of("A", "value1", "B", "40"), 6.0)
        );

        Map<Map<String, String>, List<SourceMessage>> groupedMessages = Map.of(
                key1, list1
        );

        WindowAggregator windowAggregator = new WindowAggregator(1);

        List<SomeSinkMessage> res = groupedMessages.entrySet().stream()
                .flatMap(entry -> windowAggregator.aggregate(entry.getKey(), entry.getValue()))
                .toList();

        assertEquals(2, res.size());
    }

    @Test
    public void biggerWindowAggregateTest() {
        Map<String, String> key1 = Map.of("A", "value1", "B", "40");
        List<SourceMessage> list1 = List.of(
                new SomeSourceMessage(400L, Map.of("A", "value1", "B", "40"), 4.0),
                new SomeSourceMessage(1500L, Map.of("A", "value1", "B", "40"), 6.0)
        );

        Map<Map<String, String>, List<SourceMessage>> groupedMessages = Map.of(
                key1, list1
        );

        WindowAggregator windowAggregator = new WindowAggregator(2);

        List<SomeSinkMessage> res = groupedMessages.entrySet().stream()
                .flatMap(entry -> windowAggregator.aggregate(entry.getKey(), entry.getValue()))
                .toList();

        assertEquals(1, res.size());

        assertEquals(new SomeSinkMessage(
                Map.of("A", "value1", "B", "40"),
                400L,
                1500L,
                10.0,
                4.0,
                6.0,
                2
        ), res.getFirst());
    }
}
