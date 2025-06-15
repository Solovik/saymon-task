package com.solovik.task.saymon.msg;

import lombok.Data;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Data
public class CachedSomeSourceMessage implements Delayed {
    private final SourceMessage message;
    private final long expireTimeMillis;

    public CachedSomeSourceMessage(SourceMessage message, long delayMs) {
        this.message = message;
        this.expireTimeMillis = System.currentTimeMillis() + delayMs;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(expireTimeMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(this.expireTimeMillis, ((CachedSomeSourceMessage) o).expireTimeMillis);
    }
}
