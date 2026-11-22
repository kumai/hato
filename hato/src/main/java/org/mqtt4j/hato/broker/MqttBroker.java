package org.mqtt4j.hato.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.BrokerServer;
import org.mqtt4j.hato.internal.concurrent.ClassPreload;
import org.mqtt4j.hato.internal.inproc.InprocBrokers;
import org.mqtt4j.hato.internal.log.Log;

import static java.util.Objects.requireNonNull;

/// An MQTT 3.1.1 and 5.0 broker that runs inside the application.
///
/// [#builder()] sets the addresses, TLS, the name, the limits, the [Authenticator], and the
/// [BrokerListener], and [#start()] starts a broker with the defaults. Each start creates an
/// independent broker. Its methods are safe to call concurrently from any thread. The broker
/// keeps all of its state in memory, and all of it is lost when the broker stops.
///
/// ## Sessions
///
/// Clean Session true and false and QoS 0/1/2 publications are supported. A persistent session
/// retains subscriptions, unacknowledged QoS 1/2 deliveries, received QoS 2 identifiers awaiting
/// PUBREL, and queued QoS 1/2 deliveries while disconnected. Reconnecting with the same
/// identifier resumes it with Session Present true and closes any previous connection with that
/// identifier; Clean Session true discards its previous session.
/// Incomplete QoS 1/2 deliveries resume before queued deliveries. A PUBLISH whose transmission
/// was attempted but remains unacknowledged uses DUP true; one whose transmission has not
/// started uses DUP false. Once PUBREC has arrived, QoS 2 resumes with PUBREL instead of PUBLISH.
/// Incoming QoS 2 publications are forwarded on PUBLISH; repeated PUBLISH packets with the same
/// identifier before PUBREL are acknowledged without forwarding again. A QoS 1 PUBLISH that
/// reuses such an identifier ends that wait, as a client that lost its session state does.
/// Offline QoS 0 deliveries are discarded. An empty identifier with Clean Session false is
/// refused with Identifier rejected. Overlapping subscriptions of a session receive one copy at
/// their highest QoS. Shared subscriptions are not supported: a `$share/` filter is refused, in
/// MQTT 3.1.1 with failure 0x80.
///
/// ## Retained messages and Wills
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
/// ## MQTT 5.0
///
/// MQTT 5.0 support leaves out shared subscriptions and enhanced authentication. A positive
/// Session Expiry Interval makes a session persistent, as Clean Session false does, and Clean
/// Start false resumes it. The session ends once that many seconds have passed since its last
/// connection ended; the client's DISCONNECT can change the interval, though not from 0. The
/// broker bounds how long a session outlives its connection: a client that asks for longer, as
/// with 0xFFFFFFFF, gets that bound and learns it from the Session Expiry Interval in CONNACK,
/// and a Clean Session false session of MQTT 3.1.1 ends when the bound has passed. A session
/// resumes only with the protocol version that created it. An empty client identifier is
/// assigned and returned in CONNACK. Publication properties reach MQTT 5.0 subscribers unchanged
/// and are dropped for MQTT 3.1.1 subscribers, except that a Message Expiry Interval arrives less
/// the whole seconds that the message waited in the broker. A copy whose lifetime passed before
/// the broker could start sending it is dropped, for MQTT 3.1.1 subscribers too, and so is an
/// expired retained message; a delivery that started in time is resent with what is left, down
/// to 0.
///
/// A No Local subscription receives no publications from its own client, though it does receive
/// the retained messages that its client published. A Retain As Published subscription receives
/// the publisher's retain flag; when only some overlapping subscriptions use it, a retained
/// publication arrives once with each flag. Retain Handling sends the retained messages for
/// every SUBSCRIBE, only for a new subscription, or never. A delivery carries the Subscription
/// Identifiers of the subscriptions that it is for: of all the overlapping ones in a single
/// copy, and in each of the two copies of a retained publication the identifiers of the
/// subscriptions that receive that copy. A delivery keeps the identifiers that it was matched
/// with even if the subscription is made again before it is sent.
///
/// A Will with a Will Delay Interval waits that many seconds after its connection ends, unless
/// the session ends first: at once for a session without expiry, and otherwise as the Session
/// Expiry Interval passes, or as a connection with the same client identifier starts a new
/// session. A connection that resumes the session within the delay, also by taking over,
/// cancels the Will. The properties of a Will, except the delay, reach MQTT 5.0 subscribers as
/// publication properties, and its Message Expiry Interval counts from its publication.
///
/// By default, CONNACK announces only that shared subscriptions are not supported, the largest
/// packet that the broker takes, and a Topic Alias Maximum: the aliases that a client sets last
/// for its connection. In turn, the broker keeps to the client's Receive Maximum on each
/// connection, resending on a resumed session no more than the new connection allows, and
/// discards a publication larger than the client's Maximum Packet Size as if the client had
/// received and acknowledged it. No other packet exceeds that size either: a client whose
/// Maximum Packet Size takes no CONNACK is refused before its session changes, with
/// Implementation specific error where that fits, and a SUBSCRIBE or UNSUBSCRIBE whose answer
/// does not fit ends the connection with Packet too large before it is carried out. A refusal
/// or DISCONNECT that does not fit is left out. An Authentication Method is refused with Bad
/// authentication method. AUTH, a PUBLISH with a Subscription Identifier or an invalid Topic
/// Alias, or a SUBSCRIBE with a `$share/` filter closes the connection. A PUBREC with a failure
/// reason code ends the QoS 2 delivery without PUBREL. Before it closes an MQTT 5.0 connection
/// for a malformed or forbidden packet, an expired keep-alive, or another connection with the
/// same client identifier, the broker sends DISCONNECT with the reason code; a peer that stops
/// reading may not receive it.
///
/// ## Flow control and limits
///
/// A client that sends requests faster than it reads the responses is held back: the broker
/// stops reading from it until there is room for the next response, rather than closing its
/// connection. QoS 1/2 deliveries wait when unacknowledged deliveries reach their count or size
/// limit. Deliveries also wait while a subscriber's connection is slow to accept data. The
/// number and total size of waiting deliveries are bounded, and a subscriber that falls further
/// behind misses deliveries, whatever their QoS, while its connection and session go on. The
/// retained messages for a new subscription wait apart from them, so the subscription receives
/// every retained message that it matches. Those still waiting go when the client unsubscribes.
/// They take no more than the broker retains: beyond that, a client whose subscriptions overlap
/// still receives each message, though not for every subscription that matches it, and a client
/// that keeps subscribing faster than it takes them misses the oldest. A delivery too large for
/// the broker's internal size limits does not reach its subscriber either.
/// The memory that waiting deliveries and waiting retained messages take together, for all
/// sessions, is bounded as well. While it is used up, a session that is behind or has no
/// connection misses some of them, whatever their QoS, before it reaches its own limits; a
/// client that keeps up goes on receiving. A [BrokerListener] hears of the QoS 1 and QoS 2
/// deliveries that a session misses.
///
/// The clients that the broker serves at once are bounded: a CONNECT beyond the limit is refused,
/// in MQTT 5.0 with Quota exceeded and in MQTT 3.1.1 with Server unavailable, unless it comes for
/// the session of a client that is connected, whose connection it takes over. Connections that
/// have had no CONNECT accepted yet are bounded apart from the clients, and network connections
/// apart from in-process ones: while as many network connections wait as their limit allows, a
/// further one waits to be accepted until one of them is accepted or ends, and while as many
/// in-process connections wait, a further one is refused. A CONNECT that is larger, or carries
/// more properties, than the broker takes from an unknown peer closes its connection without a
/// CONNACK. A later packet larger than the broker takes ends the connection too: an MQTT 5.0
/// client, which learned the limit from CONNACK, receives DISCONNECT with Packet too large, and
/// an MQTT 3.1.1 client, which cannot learn it, receives nothing.
/// A new persistent session is refused when the persistent-session limit is reached, in MQTT
/// 5.0 with Quota exceeded and in MQTT 3.1.1 with Server unavailable. The number and total size
/// of retained messages are bounded too: a retained publication beyond either limit is
/// delivered but not kept, which leaves its topic without a retained message. So are the
/// subscriptions of a session: one beyond their limit fails in SUBACK, in MQTT 5.0 with Quota
/// exceeded, and subscribes to nothing, while subscribing again to a filter needs no room.
/// [MqttBrokerBuilder] sets the limits that an application may need to change, and documents
/// their defaults; the other limits are internal and may change.
///
/// The strings of the packets that the broker passes on, topics and properties among them,
/// contain no control character, from U+0001 to U+001F or from U+007F to U+009F, and no Unicode
/// noncharacter: MQTT lets a receiver treat a packet with one as malformed, and some clients
/// drop their connection over it. So a packet from a client with such a character in any of its
/// strings ends the connection as a malformed packet does: an MQTT 5.0 client receives
/// DISCONNECT with Malformed Packet, an MQTT 3.1.1 client nothing, and a CONNECT with one closes
/// its connection without a CONNACK.
///
/// ## Authentication and authorization
///
/// An [Authenticator] decides on every client that connects, and on what it may publish and
/// subscribe to; without one, credentials are not checked. A broker listens beyond the loopback
/// address only with an authenticator, or when [MqttBrokerBuilder#allowAnonymous()] lets it
/// accept clients without one.
///
/// ## TLS
///
/// [MqttBrokerBuilder#listenTls(InetSocketAddress, javax.net.ssl.SSLContext)] has the broker
/// listen with TLS, alone or next to a plain listener on another port, and [#tlsPort()] tells
/// its port. The broker can ask the clients for certificates, which the [Authenticator] then
/// sees, along with whether a client came through TLS. A client whose handshake fails, or that
/// does not finish it and send its CONNECT in time, loses its connection, and so does one that
/// asks for a new handshake once it has connected.
///
/// ## Clients in the same JVM
///
/// A Hato client in the same JVM can reach the broker by its [URI][#uri()]. It passes its
/// packets in memory, and the broker serves it as it serves a client over TCP.
///
/// ## Operation
///
/// The [BrokerListener] hears of each client that the broker accepts, and of the end of its
/// connection with the reason. [#clients()] lists the clients that the broker serves, with their
/// subscriptions, and with what each has done on its connection if
/// [MqttBrokerBuilder#recordClientActivity()] has the broker record it. [#disconnect(String)]
/// closes the connection of a client, and [#disconnect(BrokerListener.Connection)] a connection
/// that the listener heard of or the list showed. [#stats()] counts the clients, sessions,
/// retained messages, and deliveries.
///
/// ## Ending
///
/// [#close()] stops the broker at once, and [#shutdown(Duration)] once its clients have what
/// waits for them. An internal failure can stop it as well, which the [BrokerListener] hears of,
/// and [#terminated()] observes every end.
///
/// ## Tests
///
/// In tests, the test kit's `BrokerControls` can make the broker announce smaller limits, which
/// it then holds clients to, and make it refuse publications, subscriptions, and connections, or
/// end a connection, with a reason code of the test's choice.
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

    // Starts a broker that listens plain at `address`, or not at all for null, and with TLS as
    // `settings` say, with the threads that suit this machine. MqttBrokerBuilder calls this, after
    // MqttBroker.builder() has preloaded the broker's classes.
    static MqttBroker start(
            BrokerServer.Builder settings,
            @Nullable InetSocketAddress address
    ) throws IOException {
        // The testkit's controls reach a broker's server through the internal control port,
        // which the module exports to the testkit alone.
        BrokerServer.serversOf(broker -> broker.server);
        return new MqttBroker(settings.start(address, Runtime.getRuntime().availableProcessors()));
    }

    /// The port of the plain listener, as the system assigned it for port zero.
    ///
    /// @return
    ///   the port of the plain listener, including after close
    /// @throws IllegalStateException
    ///   if the broker listens with TLS alone
    public int port() {
        return server.plainPort();
    }

    /// The port of the TLS listener, as the system assigned it for port zero.
    ///
    /// @return
    ///   the port of the TLS listener, including after close
    /// @throws IllegalStateException
    ///   if the broker has no TLS listener
    public int tlsPort() {
        return server.tlsPort();
    }

    /// The name of this broker: the one that [MqttBrokerBuilder#name(String)] gave, or the one
    /// that the broker took without it. A broker holds its name from its start until it ends,
    /// through [#close()] or because an internal failure stopped it, or until a
    /// [#shutdown(Duration)] begins, as it holds its ports. While it does, no other broker in the
    /// JVM starts with that name, except one whose Hato classes come from another class loader;
    /// after that, one can.
    ///
    /// @return
    ///   the name of this broker, including after close
    public String name() {
        return server.name();
    }

    /// The URI by which a client in the same JVM reaches this broker: `inproc://` and its
    /// [name][#name()]. A client built with it, by [org.mqtt4j.hato.MqttClientBuilder#server(URI)],
    /// passes its packets to the broker in memory, through neither a socket nor their encoding,
    /// and the broker serves it as it serves a client over TCP: the same [Authenticator], without
    /// a remote address, and the same limits. Only a client whose Hato classes come from the same
    /// class loader as the broker's reaches it.
    ///
    /// @return
    ///   the URI of this broker for clients in the same JVM, including after close
    public URI uri() {
        return InprocBrokers.uri(server.name());
    }

    /// Closes the connection of the client `clientId`, as for an administrative reason: an MQTT
    /// 5.0 client receives DISCONNECT with Administrative action first. The client did not
    /// disconnect, so its Will is published as for a lost connection, after its Will Delay
    /// Interval if it has one. Its session stays as the client asked: a session that outlives
    /// its connection waits for the client to come back, and expires as any other. A client that
    /// connects again meets the [Authenticator], if there is one, as any other, which keeps the
    /// client out by refusing it. The [BrokerListener] hears that the connection ended for
    /// [BrokerListener.DisconnectReason#APPLICATION].
    ///
    /// The call returns at once. From then on, the broker begins to handle no packet that the
    /// client sent on the connection.
    ///
    /// The call closes the connection that the client has as it comes, which can be newer than
    /// the last one that the listener heard of. [#disconnect(BrokerListener.Connection)] closes
    /// a connection that the listener heard of, and never a later one of the same client.
    ///
    /// @param clientId
    ///   the identifier of the client to disconnect
    /// @return
    ///   whether the client had a connection that this call closes; false if it has none, or
    ///   if its connection was closing already
    public boolean disconnect(String clientId) {
        requireNonNull(clientId, "clientId");
        return server.disconnect(clientId);
    }

    /// Closes `connection`, which the [BrokerListener] heard of or [#clients()] showed, if it is
    /// still open, as [#disconnect(String)] closes the connection of a client. A later
    /// connection of the same client stays open, so an application that decides about one
    /// connection, for example by the certificate that
    /// [BrokerListener.Connection#clientCertificates()] gives, closes that one alone, also when
    /// the client has connected again meanwhile.
    ///
    /// @param connection
    ///   the object that [BrokerListener#onConnected(BrokerListener.Connection)] received for
    ///   the connection, or that [ConnectedClient#connection()] gave
    /// @return
    ///   whether this call closes the connection; false if it has ended or was closing already,
    ///   or if this broker did not report it
    public boolean disconnect(BrokerListener.Connection connection) {
        requireNonNull(connection, "connection");
        return server.disconnect(connection);
    }

    /// Reads what the broker counts; see [BrokerStats]. It reads the counts without stopping the
    /// broker's work, and works after the broker has ended too. A call takes time in proportion
    /// to the sessions that the broker holds, so a monitor that reports several of the values
    /// reads them from one call.
    ///
    /// @return
    ///   the counts
    public BrokerStats stats() {
        return new BrokerStats(server.counts());
    }

    /// Reads the clients that the broker serves now, the ones that [#stats()] counts as its
    /// connections; see [ConnectedClient]. A broker that
    /// [records the activity of its clients][MqttBrokerBuilder#recordClientActivity()] shows what
    /// each has done on its connection as well. It reads them without stopping the broker's work,
    /// so each value is as it was a moment ago: the list can still hold a client whose
    /// connection has just ended, and leave out one that is connecting again at that moment. It
    /// holds each client once. A call takes time in proportion to the sessions that the broker
    /// holds and to the subscriptions of the clients that it serves, including those that the
    /// list leaves out, and memory in proportion to those clients and their subscriptions.
    /// Once the broker has ended, it serves no client.
    ///
    /// @return
    ///   the clients that the broker serves, in no particular order
    public List<ConnectedClient> clients() {
        return server.clients().stream().map(ConnectedClient::new).toList();
    }

    /// Observes the end of this broker without requesting it. The future completes once the
    /// broker has ended, through [#close()], through [#shutdown(Duration)], or because an
    /// internal failure stopped it: its ports are free, its connections are closed, it has
    /// finished its work, and it calls no application code any more, the [BrokerListener], the
    /// [Authenticator], and the key and trust managers of the TLS context included. It completes
    /// normally either way; the listener hears of a failure. Before the future completes, the
    /// broker gives the logging backend a moment to take what it has logged, as [#close()] does.
    ///
    /// Each call returns an independent future. The continuations attached while it waits run,
    /// as it completes, on a thread of its own, so one that waits, even for a future of another
    /// call, holds up neither the other futures nor the broker; cancelling one changes nothing
    /// else. Once the broker has ended, a call returns a completed future. A future may already
    /// be complete; continuations attached afterwards follow the usual [CompletableFuture]
    /// execution rules and may run on the attaching application thread. Keep a future rather
    /// than calling again while the broker runs: each future that waits for the end takes a
    /// thread when it comes.
    ///
    /// A listener call that never returns keeps the future pending. To listen on the same port
    /// again, the return of [#close()] is enough; this future waits for the listener as well.
    ///
    /// @return
    ///   a future that completes at the end of this broker
    public CompletableFuture<Void> terminated() {
        return server.terminated();
    }

    /// Shuts the broker down once its clients have what waits for them; [#close()] stops it at
    /// once instead.
    ///
    /// The broker takes no further connection from the start: it frees its ports and its name,
    /// which another broker can take from then on, and refuses a client that it has yet to
    /// accept with Server unavailable, return code 3 in MQTT 3.1.1. It goes on serving the
    /// clients that are connected, also reading what they send, until each of them has what
    /// waits for it, a QoS 1 or QoS 2 delivery once the client has acknowledged it, or until
    /// `timeout` has passed. A publication that a client sent before this call, and that the
    /// broker had yet to read, waits for them too. Then each MQTT 5.0 client receives DISCONNECT
    /// with Server shutting down, the clients have a moment to close their connections, and the
    /// broker closes as [#close()] does: it discards the session state and the retained
    /// messages, publishes no Will of the connections that it closes, and tells the
    /// [BrokerListener] nothing of them. What waits for the clients that are away is lost with
    /// their sessions. To have what the application's own clients publish last delivered, it
    /// disconnects them before this call.
    ///
    /// The call returns once the broker has closed, its ports free and every connection closed,
    /// which can be a moment after `timeout`, and gives the logging backend a moment to take
    /// what the broker has logged, as [#close()] does. [#close()] and an interrupt of the calling
    /// thread cut the shutdown short, as `timeout` does, and the interrupt stays with the thread.
    /// An internal failure meanwhile ends the broker as it otherwise does. A later call waits for
    /// the same shutdown and returns the same result, and a call after [#close()], or after an
    /// internal failure has stopped the broker, returns false.
    ///
    /// @param timeout
    ///   how long the clients may take to have what waits for them; zero to wait for nothing.
    ///   Must be at most 2147483647 milliseconds
    /// @return
    ///   whether the clients that were connected had what waited for them within `timeout`
    /// @throws IllegalArgumentException
    ///   if `timeout` is negative or longer than allowed
    public boolean shutdown(Duration timeout) {
        requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.compareTo(MqttBrokerBuilder.MAX_TIMEOUT) > 0) {
            throw new IllegalArgumentException(
                    "Shutdown timeout must be zero or positive and at most "
                            + MqttBrokerBuilder.MAX_TIMEOUT.toMillis() + " ms"
            );
        }
        boolean delivered = server.shutdown(timeout);
        // As after close(), the application may exit now.
        boolean _ = Log.awaitWritten(LOG_WAIT_AT_CLOSE);
        return delivered;
    }

    /// Stops accepting connections, closes all peers, and discards their session state and the
    /// retained messages, without publishing Wills, those that wait for their delay included.
    /// When it returns, the listening ports are free and every connection is closed. Repeated
    /// calls are harmless, and a call while [#shutdown(Duration)] runs cuts it short. Does not
    /// wait for peer I/O or send MQTT packets. It interrupts the [Authenticator]'s decisions and
    /// the TLS handshakes' calls into the key and trust managers that are running, and does not
    /// wait for them. Before it returns, it gives the logging backend a moment to take what the
    /// broker has logged.
    @Override
    public void close() {
        server.close();
        // An application may exit as soon as it has closed its broker. The statements made
        // until now have reached the logging backend by then; if they have not within the
        // limit, the application goes on without them.
        boolean _ = Log.awaitWritten(LOG_WAIT_AT_CLOSE);
    }
}
