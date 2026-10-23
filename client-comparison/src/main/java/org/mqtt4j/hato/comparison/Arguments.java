package org.mqtt4j.hato.comparison;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/// Settings written as `key=value` words: the driver's command line, the command line that the
/// driver gives each subscriber JVM, and the result line that a subscriber prints.
record Arguments(Map<String, String> values) {

    Arguments {
        values = Map.copyOf(values);
    }

    static Arguments parse(List<String> words) {
        var values = new HashMap<String, String>();
        for (String word : words) {
            int separator = word.indexOf('=');
            if (separator < 1) {
                throw new IllegalArgumentException("Expected key=value: " + word);
            }
            values.put(word.substring(0, separator), word.substring(separator + 1));
        }
        return new Arguments(values);
    }

    static Arguments parse(String line) {
        return parse(Arrays.asList(line.trim().split("\\s+")));
    }

    /// These settings with `key` set to `value`.
    Arguments with(String key, String value) {
        var copy = new HashMap<>(values);
        copy.put(key, value);
        return new Arguments(copy);
    }

    String text(String key) {
        String value = values.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing " + key);
        }
        return value;
    }

    String text(String key, String fallback) {
        return values.getOrDefault(key, fallback);
    }

    long number(String key) {
        return Long.parseLong(text(key));
    }

    long number(String key, long fallback) {
        String value = values.get(key);
        return value == null ? fallback : Long.parseLong(value);
    }

    /// A comma-separated value as its elements.
    List<String> list(String key, String fallback) {
        return List.of(text(key, fallback).split(","));
    }
}
