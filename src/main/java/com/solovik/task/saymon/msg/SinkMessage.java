package com.solovik.task.saymon.msg;

import java.util.Map;

public interface SinkMessage {
    long from();
    long to();
    Map<String, String> labels();
    double min();
    double max();
    double avg();
    int count();
}
