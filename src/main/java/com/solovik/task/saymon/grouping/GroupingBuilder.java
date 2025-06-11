package com.solovik.task.saymon.grouping;

import com.solovik.task.saymon.msg.SourceMessage;
import lombok.AllArgsConstructor;

import java.util.*;
import java.util.stream.Collector;

import java.util.stream.Collectors;

/**
 * Class creates <code>Collector</code> to comply <i>GROUP BY</i> logic.
 * Grouping keys are came from the path <i>application.pipeline.groupBy<i/> in <i>application.conf</i>.
 *
 * Initially there is just one <code>Collector</code> which aggregates in <code>List&ltSourceMessage&gt</code>.
 */
@AllArgsConstructor
public class GroupingBuilder {
    private Set<String> keys;

    public Collector<SourceMessage, ?, Map<Map<String, String>, List<SourceMessage>>> createGroupByCollector() {
        return Collectors.groupingBy(msg -> keys.stream().collect(Collectors.toMap(key -> key, key -> msg.labels().get(key))));
//        return Collectors.groupingBy(msg -> keys.stream().map(msg.labels()::get).collect(Collectors.toSet()));
    }
}
