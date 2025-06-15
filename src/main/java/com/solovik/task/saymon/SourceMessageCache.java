package com.solovik.task.saymon;

import com.solovik.task.saymon.msg.CachedSomeSourceMessage;
import com.solovik.task.saymon.msg.Source;
import com.solovik.task.saymon.msg.SourceMessage;

import java.util.LinkedList;
import java.util.concurrent.DelayQueue;

public class SourceMessageCache implements Source {
    private final DelayQueue<CachedSomeSourceMessage> queue;
    private final Long cacheTimeoutSeconds;

    public SourceMessageCache(Long cacheTimeoutSeconds) {
        queue = new DelayQueue<>();
        this.cacheTimeoutSeconds = cacheTimeoutSeconds;
    }

    public boolean put(SourceMessage message) {
        return queue.offer(new CachedSomeSourceMessage(message, cacheTimeoutSeconds * 1000));
    }

    @Override
    public Iterable<SourceMessage> source() {
        LinkedList<CachedSomeSourceMessage> res = new LinkedList<>();
        queue.drainTo(res);
        return () -> res.stream().map(CachedSomeSourceMessage::getMessage).iterator();
    }
}
