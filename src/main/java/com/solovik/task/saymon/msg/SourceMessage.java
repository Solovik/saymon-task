package com.solovik.task.saymon.msg;

import java.util.Map;

public interface SourceMessage {
    long timestamp();
    Map<String, String> labels();
    double value();
}
