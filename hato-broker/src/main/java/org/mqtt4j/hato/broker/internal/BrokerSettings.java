package org.mqtt4j.hato.broker.internal;

import java.util.function.Consumer;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;

// The internal limits and test hooks of one broker, which `BrokerServer.Builder` checks before
// the broker starts. The connect timeout is in nanoseconds. `inPlace` is how many events of its
// session a connection's reader or writer runs itself before it hands the rest over (see
// Session). The capabilities are what the broker announces to MQTT 5.0 clients until a test
// changes them. A connection's reader calls `beforeProcess` before it hands a packet to the
// session, and the session's actor calls `beforeHandle` as it starts to handle one. A
// connection's writer calls `beforeDeliveryWrite` before it marks the deliveries of a write as
// sent.
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
