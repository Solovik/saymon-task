package com.solovik.task.saymon.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Implementation of <code>SourceMessage</code>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SomeSourceMessage implements SourceMessage {
    private Long timestamp;
    private Map<String, String> labels;
    private double value;

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public Map<String, String> labels() {
        return labels;
    }

    @Override
    public double value() {
        return value;
    }
}
