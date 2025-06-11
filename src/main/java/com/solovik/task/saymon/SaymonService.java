package com.solovik.task.saymon;

import com.solovik.task.saymon.dedup.WindowDedup;
import com.solovik.task.saymon.msg.SomeSinkMessage;
import com.solovik.task.saymon.msg.Source;
import com.solovik.task.saymon.msg.SourceMessage;
import com.solovik.task.saymon.grouping.GroupingBuilder;
import com.solovik.task.saymon.window.WindowAggregator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Predicate;

/**
 * The main pipeline's logic.
 * <code>Builder</code> used for convenient.
 */
@AllArgsConstructor
@Builder
@Slf4j
public class SaymonService {
    private WindowDedup deduplicator;
    private Predicate<SourceMessage> filtration;
    private GroupingBuilder groupingBuilder;
    private WindowAggregator windowAggregator;

    public List<SomeSinkMessage> process(Source source) {
        return deduplicator.deduplicate(source.source())
                .filter(filtration)
                .collect(groupingBuilder.createGroupByCollector())
                .entrySet()
                .stream()
                .flatMap(entry -> windowAggregator.aggregate(entry.getKey(), entry.getValue()))
                .toList();
    }
}
