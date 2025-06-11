package com.solovik.task.saymon.window;

import com.solovik.task.saymon.msg.SomeSinkMessage;
import com.solovik.task.saymon.msg.SourceMessage;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Aggregate <code>List&ltSourceMessage&gt</code> into <code>Stream&ltSomeSinkMessage&gt</code>
 * <code>intervalSeconds</code> is the <i>aggregateIntervalInSeconds</i> from <i>applicatioin.conf</i>
 */
@AllArgsConstructor
public class WindowAggregator {
    private int intervalSeconds;

    public Stream<SomeSinkMessage> aggregate(Map<String, String> labels, List<SourceMessage> sourceMessages) {
        return sourceMessages.stream()
                .collect(Collectors.groupingBy(msg -> msg.timestamp() / (intervalSeconds * 1000)))
                .values()
                .stream()
                .map(msgs -> processGroupOfSourceMessage(msgs, labels));
    }

    private SomeSinkMessage processGroupOfSourceMessage(List<SourceMessage> sourceMessages, Map<String, String> labels) {
        long beginTs = sourceMessages.getFirst().timestamp();
        long endTs = sourceMessages.getFirst().timestamp();
        double min = sourceMessages.getFirst().value();
        double max = sourceMessages.getFirst().value();

        return sourceMessages.stream()
                .collect(() -> new SourceMessageConsumer(labels, beginTs, endTs, min, max), SourceMessageConsumer::accept, SourceMessageConsumer::combine)
                .getSinkMessage();
    }
}
