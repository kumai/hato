package org.mqtt4j.hato.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.mqtt4j.hato.broker.internal.BrokerServer;
import org.mqtt4j.hato.internal.concurrent.ClassPreload;
import org.mqtt4j.hato.internal.log.Log;

/// An experimental, in-memory MQTT 3.1.1 and 5.0 broker for tests.
///
/// [#builder()] sets the address and the limits, and [#start()] starts a broker with the
/// defaults. Each start creates an independent broker. Clean Session true and false and QoS 0/1/2
/// publications are supported. A persistent session retains subscriptions, unacknowledged
/// QoS 1/2 deliveries, received QoS 2 identifiers awaiting PUBREL, and queued QoS 1/2 deliveries
/// while disconnected. Reconnecting with the same identifier resumes it with Session Present
/// true and closes any previous connection with that identifier; Clean Session true discards
/// its previous session.
/// Incomplete QoS 1/2 deliveries resume before queued deliveries. A PUBLISH whose transmission
/// was attempted but remains unacknowledged uses DUP true; one whose transmission has not
/// started uses DUP false. Once PUBREC has arrived, QoS 2 resumes with PUBREL instead of PUBLISH.
/// Incoming QoS 2 publications are forwarded on PUBLISH; repeated PUBLISH packets with the same
/// identifier before PUBREL are acknowledged without forwarding again. A QoS 1 PUBLISH that
/// reuses such an identifier ends that wait, as a client that lost its session state does.
/// Offline QoS 0 deliveries are discarded. An empty identifier with Clean Session false is
/// refused with Identifier rejected. Overlapping subscriptions of a session receive one copy at
/// their highest QoS. Credentials are not checked. Shared subscriptions are not supported: a
/// `$share/` filter is refused, in MQTT 3.1.1 with failure 0x80.
///
/// A retained publication becomes the retained message of its topic in place of the previous
/// one, and a retained publication with an empty payload removes it; both reach current
/// subscribers with the retain flag cleared. A new subscription receives the retained messages
/// that match it, with the retain flag set, at the lower of their QoS and its own.
///
/// A connection's Will is published when the connection ends without a DISCONNECT: when the
/// network connection closes or fails, when the keep-alive expires, when the client breaks the
/// protocol, and when another connection with the same client identifier takes over, after that
/// connection's CONNACK. A DISCONNECT discards the Will, unless it is an MQTT 5.0 DISCONNECT
/// with a reason code other than Normal disconnection. A retained Will is handled as a retained
/// publication.
///
/// MQTT 5.0 support covers what client tests need. A positive Session Expiry Interval makes a
/// session persistent, as Clean Session false does, and Clean Start false resumes it. The session
/// ends once that many seconds have passed since its last connection ended; the client's
/// DISCONNECT can change the interval, though not from 0. The broker bounds how long a session
/// outlives its connection: a client that asks for longer, as with 0xFFFFFFFF, gets that bound and
/// learns it from the Session Expiry Interval in CONNACK, and a Clean Session false session of MQTT
/// 3.1.1 ends when the bound has passed. A session resumes only with the protocol version
/// that created it. An empty client identifier is assigned and returned in CONNACK. Publication
/// properties reach MQTT 5.0 subscribers unchanged and are dropped for MQTT 3.1.1 subscribers,
/// except that a Message Expiry Interval arrives less the whole seconds that the message waited in
/// the broker. A copy whose lifetime passed before the broker could start sending it is dropped,
/// for MQTT 3.1.1 subscribers too, and so is an expired retained message; a delivery that started
/// in time is resent with what is left, down to 0. A No Local subscription receives no publications
/// from its own client, though it does receive the retained messages that its client published. A
/// Retain As Published subscription receives the publisher's retain flag; when only some
/// overlapping subscriptions use it, a retained publication arrives once with each flag. Retain
/// Handling sends the retained messages for every SUBSCRIBE, only for a new subscription, or never.
/// A delivery carries the Subscription Identifiers of the subscriptions that it is for: of all the
/// overlapping ones in a single copy, and in each of the two copies of a retained publication the
/// identifiers of the subscriptions that receive that copy. A delivery keeps the identifiers that
/// it was matched with even if the subscription is made again before it is sent. A Will with a Will
/// Delay Interval waits that many seconds after its connection ends, unless the session ends first:
/// at once for a session without expiry, and otherwise as the Session Expiry Interval passes, or as
/// a connection with the same client identifier starts a new session. A connection that resumes
/// the session within the delay, also by taking over, cancels the Will. The properties of a
/// Will, except the delay, reach MQTT 5.0 subscribers as publication properties, and its Message
/// Expiry Interval counts from its publication. By default, CONNACK
/// announces only that shared subscriptions are not supported, the largest packet that the broker
/// takes, and a Topic Alias Maximum of 16: the aliases that a client sets last for its
/// connection. A test can make the broker announce smaller
/// limits with the test kit's `BrokerControls`, and the broker then holds clients to them; the test
/// kit can also make it refuse publications, subscriptions, and connections, or end a connection,
/// with a reason code of the test's choice. In turn, the broker keeps to the client's Receive
/// Maximum on each connection, resending on a resumed session no more than the new connection
/// allows, and discards a publication larger than the client's Maximum Packet Size as if the client
/// had received and acknowledged it. An Authentication Method is refused with Bad authentication
/// method. AUTH, a PUBLISH with a Subscription Identifier or an invalid Topic Alias, or a SUBSCRIBE
/// with a `$share/` filter closes the connection. A PUBREC with a failure reason code ends the
/// QoS 2 delivery without PUBREL. Before it closes an MQTT 5.0 connection for a malformed or
/// forbidden packet, an expired keep-alive, or another connection with the same client
/// identifier, the broker sends DISCONNECT with the reason code; a peer that stops reading may
/// not receive it.
///
/// A client that sends requests faster than it reads the responses is held back: the broker
/// stops reading from it until there is room for the next response, rather than closing its
/// connection. QoS 1/2 deliveries wait when unacknowledged deliveries reach their count or size
/// limit. Deliveries also wait while a subscriber's connection is slow to accept data. The
/// number and total size of waiting deliveries are bounded, and a subscriber that falls further
/// behind misses deliveries, whatever their QoS, while its connection and session go on. The
/// retained messages for a new subscription wait apart from them, so the subscription receives
/// every retained message that it matches. Those still waiting go when the client unsubscribes,
/// and a client that keeps subscribing faster than it takes them misses the oldest. A delivery
/// too large for the broker's internal size limits does not reach its subscriber either.
/// The memory that waiting deliveries and waiting retained messages take together, for all
/// sessions, is bounded as well. While it is used up, a session that is behind or has no
/// connection misses some of them, whatever their QoS, before it reaches its own limits; a
/// client that keeps up goes on receiving. A [BrokerListener] hears of the QoS 1 and QoS 2
/// deliveries that a session misses.
/// The clients that the broker serves at once are bounded: a CONNECT beyond the limit is refused,
/// in MQTT 5.0 with Quota exceeded and in MQTT 3.1.1 with Server unavailable, unless it comes for
/// the session of a client that is connected, whose connection it takes over. Connections that
/// have had no CONNECT accepted yet are bounded apart from the clients, and one beyond their
/// limit is closed at once. A CONNECT larger than the broker takes from an unknown peer closes its
/// connection without a CONNACK. A later packet larger than the broker takes ends the connection
/// too: an MQTT 5.0 client, which learned the limit from CONNACK, receives DISCONNECT with Packet
/// too large, and an MQTT 3.1.1 client, which cannot learn it, receives nothing.
/// A new persistent session is refused when the persistent-session limit is reached, in MQTT
/// 5.0 with Quota exceeded and in MQTT 3.1.1 with Server unavailable. The number and total size
/// of retained messages are bounded too: a retained publication beyond either limit is
/// delivered but not kept, which leaves its topic without a retained message. So are the
/// subscriptions of a session: one beyond their limit fails in SUBACK, in MQTT 5.0 with Quota
/// exceeded, and subscribes to nothing, while subscribing again to a filter needs no room.
/// [MqttBrokerBuilder] sets the limits that an application may need to change, and documents
/// their defaults; the other limits are internal and may change. All state is lost when the
/// broker stops. This experimental API is outside the compatibility promise.
public final class MqttBroker implements AutoCloseable {

    // How long close() gives the logging backend to take what was logged until then. A backend
    // that needs longer is one that the application must not wait for.
    private static final Duration LOG_WAIT_AT_CLOSE = Duration.ofSeconds(1);

    private final BrokerServer server;

    private MqttBroker(BrokerServer server) {
        this.server = server;
    }

    /// {@return a builder whose settings all start at their defaults}
    public static MqttBrokerBuilder builder() {
        // First, before anything touches the other classes of the broker.
        ClassPreload.broker.awaitLoaded();
        return new MqttBrokerBuilder();
    }

    /// Starts a broker with the default settings, on a port that the system assigns on the
    /// loopback address. The same as `builder().start()`.
    ///
    /// @return
    ///   the started broker
    /// @throws IOException
    ///   if the listening socket cannot be opened
    public static MqttBroker start() throws IOException {
        return builder().start();
    }

    // Starts a broker at `address` with `settings` and the threads that suit this machine.
    // MqttBrokerBuilder calls this, after MqttBroker.builder() has preloaded the broker's
    // classes.
    static MqttBroker start(
            BrokerServer.Builder settings,
            InetSocketAddress address
    ) throws IOException {
        // The testkit's controls reach a broker's server through the internal control port,
        // which the module exports to the testkit alone.
        BrokerServer.serversOf(broker -> broker.server);
        return new MqttBroker(settings.start(address, Runtime.getRuntime().availableProcessors()));
    }

    /// {@return the assigned listening port, including after close}
    public int port() {
        return server.port();
    }

    /// Observes the end of this broker without requesting it. The future completes once the
    /// broker has ended, through [#close()] or because an internal failure stopped it: its port
    /// is free, its connections are closed, it has finished its work, and it calls no
    /// application code any more, the [BrokerListener] included. It completes normally either
    /// way; the listener hears of a failure. Each call returns an independent future.
    ///
    /// A listener call that never returns keeps the future pending. To listen on the same port
    /// again, the return of [#close()] is enough; this future waits for the listener as well.
    ///
    /// @return
    ///   a future that completes at the end of this broker
    public CompletableFuture<Void> terminated() {
        return server.terminated().copy();
    }

    /// Stops accepting connections, closes all peers, and discards their session state and the
    /// retained messages, without publishing Wills, those that wait for their delay included.
    /// When it returns, the listening port is free and every connection is closed. Repeated
    /// calls are harmless. Does not wait for peer I/O or send MQTT packets. Before it returns,
    /// it gives the logging backend a moment to take what the broker has logged.
    @Override
    public void close() {
        server.close();
        // An application may exit as soon as it has closed its broker. The statements made
        // until now have reached the logging backend by then; if they have not within the
        // limit, the application goes on without them.
        boolean _ = Log.awaitWritten(LOG_WAIT_AT_CLOSE);
    }
}
