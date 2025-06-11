package com.solovik.task.saymon.dedup;

import com.solovik.task.saymon.msg.SomeSourceMessage;
import com.solovik.task.saymon.msg.SourceMessage;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class WindowDedupTest {
    @Test
    void testDeduplicationWithinWindow() {
        Map<String, String> payload1 = Map.of("key1", "value1", "key2", "value2");
        Map<String, String> payload2 = Map.of("key1", "value1", "key2", "value3");
        Set<String> labelKeys = Set.of("key1");

        WindowDedup wd = new WindowDedup(200L, labelKeys);

        List<SourceMessage> input = List.of(
                new SomeSourceMessage(2000L, payload1, 1.0),
                new SomeSourceMessage(2050L, payload2, 2.0));

        Iterable<SourceMessage> deduped = wd.deduplicateIter(input);

        Iterator<SourceMessage> result = deduped.iterator();
        SourceMessage sm1 = result.next();
        assertEquals(1.0, sm1.value());
        assertEquals("value2", sm1.labels().get("key2"));
        assertFalse(result.hasNext());
    }

    @Test
    void testDeduplicationExtended() {
        Map<String, String> payload1 = Map.of("key1", "value1", "key2", "value2");
        Map<String, String> payload2 = Map.of("key1", "value1", "key2", "value3");
        Map<String, String> payload3 = Map.of("key1", "value2");
        Set<String> labelKeys = Set.of("key1");

        WindowDedup wd = new WindowDedup(200L, labelKeys);

        List<SourceMessage> input = List.of(
                new SomeSourceMessage(2000L, payload1, 1.0),
                new SomeSourceMessage(2050L, payload3, 3.0),
                new SomeSourceMessage(2100L, payload2, 2.0),
                new SomeSourceMessage(3000L, payload3, 4.0)
        );

        Iterable<SourceMessage> deduped = wd.deduplicateIter(input);

        List<SourceMessage> result = new ArrayList<>();
        deduped.iterator().forEachRemaining(result::add);

        assertEquals(3, result.size());
        // First
        assertEquals("value2", result.getFirst().labels().get("key2"));
        assertEquals(1.0, result.getFirst().value());
        assertEquals(2000L, result.getFirst().timestamp());

        // Second
        assertNull(result.get(1).labels().get("key2"));
        assertEquals("value2", result.get(1).labels().get("key1"));
        assertEquals(3.0, result.get(1).value());
        assertEquals(2050L, result.get(1).timestamp());

        //Third
        assertEquals("value2", result.get(2).labels().get("key1"));
        assertEquals(4.0, result.get(2).value());
        assertEquals(3000L, result.get(2).timestamp());
    }

    @Test
    void testEmptyInput() {
        Set<String> labelKeys = Set.of("key1");

        List<SourceMessage> input = List.of();

        WindowDedup wd = new WindowDedup(200L, labelKeys);

        Iterable<SourceMessage> deduped = wd.deduplicateIter(input);
        assertFalse(deduped.iterator().hasNext());
    }

    @Test
    void testSingleMessage() {
        Map<String, String> payload1 = Map.of("key1", "value1");
        Set<String> labelKeys = Set.of("key1");

        List<SourceMessage> input = List.of(new SomeSourceMessage(1000L, payload1, 1.0));

        WindowDedup wd = new WindowDedup(200L, labelKeys);

        Iterable<SourceMessage> deduped = wd.deduplicateIter(input);

        Iterator<SourceMessage> result = deduped.iterator();
        assertEquals("value1", result.next().labels().get("key1"));
        assertFalse(result.hasNext());
    }
}
