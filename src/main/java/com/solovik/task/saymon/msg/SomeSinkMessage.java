package com.solovik.task.saymon.msg;

import lombok.Data;

import java.util.Map;

/**
 * Implementation of <code>SinkMessage</code>
 */
@Data
public class SomeSinkMessage implements SinkMessage {
    private final Map<String, String> labels;
    private final long from;
    private final long to;
    private final double total;
    private final double min;
    private final double max;
    private final int count;

    @Override
    public long from() {
        return from;
    }

    @Override
    public long to() {
        return to;
    }

    @Override
    public Map<String, String> labels() {
        return labels;
    }

    @Override
    public double min() {
        return min;
    }

    @Override
    public double max() {
        return max;
    }

    @Override
    public double avg() {
        return count() > 0 ? total / count() : 0;
    }

    @Override
    public int count() {
        return count;
    }
}
