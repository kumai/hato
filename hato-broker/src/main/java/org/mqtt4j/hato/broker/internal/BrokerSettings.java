package org.mqtt4j.hato.broker.internal;

import java.util.function.Consumer;
import java.util.function.Predicate;

import org.mqtt4j.hato.broker.internal.common.concurrent.Timer;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;

// The internal limits and test hooks of one broker, which `BrokerServer.Builder` checks before
// the broker starts. The connect timeout is in nanoseconds. `inPlace` is the event budget for
// selector threads that take an idle session's turn. A zero budget sends all actor work to the
// pool. The capabilities are what the broker announces to MQTT 5.0 clients until a test changes
// them. The selector calls `beforeProcess` before handing a packet to its session. It asks
// `deliveryWriteHeld` before marking the deliveries of a write as sent: while the answer is
// true, the connection keeps that write back and goes on with its input, and asks again each
// time its selector looks at it, which `Connection.wakeNow()` brings about. The session's actor
// calls `beforeHandle` when it starts handling a packet. A selector calls `beforeSelect` each
// time before it waits for its sockets, outside the work of any connection, so what a test
// throws there fails the selector itself.
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
        Predicate<Connection> deliveryWriteHeld,
        Runnable beforeSelect
) {
}
