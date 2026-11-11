package org.mqtt4j.hato.broker.internal;

import java.util.function.Consumer;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;

// The internal limits and test hooks of one broker, which `BrokerServer.Builder` checks before
// the broker starts. The connect timeout is in nanoseconds. `inPlace` is the event budget for
// selector threads that take an idle session's turn. A zero budget sends all actor work to the
// pool. The capabilities are what the broker announces to MQTT 5.0 clients until a test changes
// them. The selector calls `beforeProcess` before handing a packet to its session and
// `beforeDeliveryWrite` before marking the deliveries of a write as sent. The session's actor
// calls `beforeHandle` when it starts handling a packet.
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
        int inPlace,
        Capabilities capabilities,
        Timer scheduler,
        Consumer<Packet> beforeProcess,
        Consumer<Packet> beforeHandle,
        Runnable beforeDeliveryWrite
) {
}
