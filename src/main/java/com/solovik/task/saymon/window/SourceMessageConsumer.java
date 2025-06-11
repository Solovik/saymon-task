package com.solovik.task.saymon.window;

import com.solovik.task.saymon.msg.SomeSinkMessage;
import com.solovik.task.saymon.msg.SourceMessage;
import lombok.Data;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Collects <code>SourceMessages</code> and returns <code>SinkMessage</code> as an aggregation.
 */
@Data
public class SourceMessageConsumer implements Consumer<SourceMessage> {
    private final Map<String, String> labels;
    private long from;
    private long to;
    private double min;
    private double max;
    private double total = 0;
    private int count;

    public SourceMessageConsumer(Map<String, String> labels, long from, long to, double min, double max) {
        this.labels = labels;
        this.from = from;
        this.to = to;
        this.min = min;
        this.max = max;
    }

    @Override
    public void accept(SourceMessage sourceMessage) {
        total += sourceMessage.value();
        count++;
        if (sourceMessage.timestamp() < from) this.from = sourceMessage.timestamp();
        if (sourceMessage.timestamp() > to) this.to = sourceMessage.timestamp();
        if (sourceMessage.value() < this.min) this.min = sourceMessage.value();
        if (sourceMessage.value() > this.max) this.max = sourceMessage.value();
    }

    public void combine(SourceMessageConsumer other) {
        setTotal(getTotal() + other.getTotal());
        setCount(getCount() + other.getCount());
        if (other.getFrom() < getFrom()) setFrom(other.getFrom());
        if (other.getTo() > getTo()) setTo(other.getTo());
        if (other.getMin() < getMin()) setMin(other.getMin());
        if (other.getMax() > getMax()) setMax(other.getMax());
    }

    public SomeSinkMessage getSinkMessage() {
        return new SomeSinkMessage(
                getLabels(),
                getFrom(),
                getTo(),
                getTotal(),
                getMin(),
                getMax(),
                getCount()
        );
    }
}
