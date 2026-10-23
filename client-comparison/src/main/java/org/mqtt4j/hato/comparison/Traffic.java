package org.mqtt4j.hato.comparison;

import java.util.List;

/// What a sender sends in one run: `count` messages of `payloadSize` bytes at `qos`, to `topics`
/// in turn, each when `schedule` makes it due, with at most `window` of them unacknowledged at
/// QoS 1.
record Traffic(
        List<String> topics,
        int qos,
        int payloadSize,
        int window,
        long count,
        Schedule schedule
) {

    Traffic {
        topics = List.copyOf(topics);
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("Traffic needs a topic");
        }
    }
}
