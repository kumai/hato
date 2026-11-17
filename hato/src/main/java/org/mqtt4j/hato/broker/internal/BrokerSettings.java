package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.broker.BrokerListener;
import org.mqtt4j.hato.internal.concurrent.Timer;
import org.mqtt4j.hato.internal.protocol.Packet;

// The internal limits and test hooks of one broker, which `BrokerServer.Builder` checks before
// the broker starts. The connect timeout is in nanoseconds. `maxAwaitingConnect` bounds the
// connections that the broker has accepted no CONNECT on yet, and `maxClients` those that it
// has accepted one on: its clients. `maxPacketSize` is the largest packet in bytes that the
// broker takes from a client, whatever its protocol version, and `maxConnectSize` the largest
// that it takes as a CONNECT, from a peer that it knows nothing of yet; the smaller of the two
// bounds a CONNECT. `maxQueued` and `maxQueuedBytes` bound the QoS 1 and 2 deliveries that wait
// for one session, and `maxQueuedAtMostOnce` and `maxQueuedAtMostOnceBytes` its QoS 0
// deliveries. `maxSessionExpiry` is the longest that a session outlives its connection,
// in seconds. `maxSubscriptions` and `maxSubscriptionLength` bound the subscriptions of
// one session: their number, and the characters of their filters in all. `maxDeliveryMemory`
// is the memory in bytes that the deliveries that wait for all the sessions may take
// together, and `deliveryMemoryUnit` how much of it a thread of the actor pool fetches for
// itself at a time (see MemoryBudget). The byte limits on deliveries and on retained messages
// count what these hold on the heap, which can be many times the bytes of their packets (see
// PublicationMemory); `sendBytes` bounds both for what waits in a connection's send queue.
// `selectors` and `actors` are the numbers of selector threads and of threads in the actor
// pool. The capabilities are what the broker announces to MQTT 5.0 clients until a test
// changes them, within its own limits. The application's listener, if there is one, hears of
// the QoS 1 and 2 deliveries that the limits keep from a session (see DropReports). The
// application's authenticator, if there is one, decides on every CONNECT; a decision may take
// `authenticationTimeout` nanoseconds, and at most `maxAuthentications` run at once, while
// further CONNECTs wait for one of them to end.
//
// The selector calls `beforeProcess` before handing a packet to its session. It asks
// `deliveryWriteHeld` before marking the deliveries of a write as sent: while the answer is
// true, the connection keeps that write back and goes on with its input, and asks again each
// time its selector looks at it, which `Connection.wakeNow()` brings about. The session's actor
// calls `beforeHandle` when it starts handling a packet. A selector calls `beforeSelect` each
// time before it waits for its sockets, outside the work of any connection, so what a test
// throws there fails the selector itself. The accepting thread calls `beforeAccept` each time
// before it asks the listener for a connection, and takes what a test throws there as the
// failure of that attempt. A session calls `beforeHandOver` with its client identifier, under
// its inbox, once it has counted which of its oldest waiting deliveries or retained messages
// are to make room in the broker's memory for a new one and before they hand their room over,
// so that a test can have other sessions take room or hold publications in between. A
// session's actor calls `beforeAttach` with the client identifier when a CONNECT takes the
// session over, once the connection that had it has stopped and the session is ready, just
// before the new connection attaches, so that what a test throws there stands for a takeover
// that fails on the way. The starting broker runs `beforeAcceptingStarts` just before it starts
// its accepting thread, so that what a test throws there stands for a thread that could not
// start, after others did.
record BrokerSettings(
        long connectTimeout,
        int maxAwaitingConnect,
        int maxClients,
        int maxPacketSize,
        int maxConnectSize,
        long sendBytes,
        int maxInflight,
        long maxInflightBytes,
        int maxQueued,
        long maxQueuedBytes,
        int maxQueuedAtMostOnce,
        long maxQueuedAtMostOnceBytes,
        int maxSessions,
        long maxSessionExpiry,
        int maxSubscriptions,
        long maxSubscriptionLength,
        int maxRetained,
        long maxRetainedBytes,
        int maxHeld,
        long maxDeliveryMemory,
        long deliveryMemoryUnit,
        int selectors,
        int actors,
        Capabilities capabilities,
        @Nullable BrokerListener listener,
        @Nullable Authenticator authenticator,
        long authenticationTimeout,
        int maxAuthentications,
        Timer scheduler,
        Consumer<Packet> beforeProcess,
        Consumer<Packet> beforeHandle,
        Predicate<Connection> deliveryWriteHeld,
        Runnable beforeSelect,
        AcceptHook beforeAccept,
        Consumer<String> beforeHandOver,
        Consumer<String> beforeAttach,
        Runnable beforeAcceptingStarts
) {

    // What a test runs before each attempt to accept a connection.
    @FunctionalInterface
    interface AcceptHook {

        void run() throws IOException;
    }
}
