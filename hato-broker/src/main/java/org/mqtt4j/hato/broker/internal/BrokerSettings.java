package org.mqtt4j.hato.broker.internal;

import java.util.function.Consumer;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;

// The internal limits and test hooks of one broker, which `BrokerServer.Builder` checks before
// the broker starts. The connect timeout is in nanoseconds. The capabilities are what the broker
// announces to MQTT 5.0 clients until a test changes them.
record BrokerSettings(
        long connectTimeout,
        long sendBytes,
        int maxInflight,
        long maxInflightBytes,
        int maxQueued,
        long maxQueuedBytes,
        int maxSessions,
        int maxRetained,
        long maxRetainedBytes,
        int maxHeld,
        Capabilities capabilities,
        Timer scheduler,
        Consumer<Packet> beforeProcess,
        Runnable beforeDeliveryWrite
) {
}
