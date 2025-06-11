package com.solovik.task.saymon.dedup;

import com.solovik.task.saymon.msg.SourceMessage;
import lombok.AllArgsConstructor;

import java.util.*;
import java.util.stream.Stream;

/**
 * Deduplication by window.
 * Window in milliseconds.
 * First value in the group has a higher priority.
 */
@AllArgsConstructor
public class WindowDedup {
    private Long window;
    private Set<String> keys;

    public Stream<SourceMessage> deduplicate(Iterable<SourceMessage> sourceMessages) {
        return deduplicateIter(sourceMessages).stream();
    }

    public List<SourceMessage> deduplicateIter(Iterable<SourceMessage> sourceMessages) {
        Map<Map<String, String>, Long> lastSeen = new HashMap<>(1024);
        List<SourceMessage> result = new ArrayList<>();

        for (SourceMessage msg : sourceMessages) {
            if (msg.labels().keySet().containsAll(keys)) {
                Map<String, String> key = getKey(msg.labels());
                Long prevTime = lastSeen.getOrDefault(key, 0L);

                if (msg.timestamp() - window > prevTime) {
                    lastSeen.put(key, msg.timestamp());
                    result.add(msg);
                }
            }
        }

        return result;
    }

    private Map<String, String> getKey(Map<String, String> labels) {
        Map<String, String> result = new HashMap<>();

        for (String key : keys) {
            result.put(key, labels.get(key));
        }

        return result;
    }
}
