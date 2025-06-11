package com.solovik.task.saymon.filter;

import com.solovik.task.saymon.msg.SomeSourceMessage;
import com.solovik.task.saymon.msg.SourceMessage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FilterBuilderTest {
    private static String CONFIG = """
            application {
                pipeline {
                    filtering: [
                        {label: A, condition: "=", value: "value1"},
                        {label: B, condition: ">", value: 50}
                    ]
                }
            }
            """;

    @Test
    public void test() {
        Config config = ConfigFactory.parseString(CONFIG);

        Stream<SourceMessage> messageStream = Stream.of(
                new SomeSourceMessage(1000L, Map.of("A", "value1", "B", "60"), 1.0),
                new SomeSourceMessage(1000L, Map.of("A", "value2", "B", "60"), 1.0),
                new SomeSourceMessage(1000L, Map.of("A", "value1", "B", "40"), 1.0)
        );

        Predicate<SourceMessage> predicate = new FilterBuilder(config.getObjectList("application.pipeline.filtering")).createPredicate();

        List<SourceMessage> res = messageStream
                .filter(predicate)
                .collect(Collectors.toList());

        assertEquals(1, res.size());
        assertEquals(new SomeSourceMessage(1000L, Map.of("A", "value1", "B", "60"), 1.0), res.getFirst());
    }
}
