package org.mqtt4j.hato.broker;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.mqtt4j.hato.broker.internal.BrokerServer;

/// An experimental, in-memory MQTT 3.1.1 and 5.0 broker for tests.
///
/// Each start creates an independent broker. Clean Session true and false and QoS 0/1/2
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
/// ends once that many seconds have passed since its last connection ended, unless the interval is
/// 0xFFFFFFFF; the client's DISCONNECT can change the interval, though not from 0. A Clean Session
/// false session of MQTT 3.1.1 does not expire. A session resumes only with the protocol version
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
/// at once for a session without expiry, and otherwise as the Session Expiry Interval passes, as
/// the broker discards the session, or as a connection with the same client identifier starts a new
/// session. A connection that resumes the session within the delay, also by taking over, cancels
/// the Will. The properties of a Will, except the delay, reach MQTT 5.0 subscribers as publication
/// properties, and its Message Expiry Interval counts from its publication. By default, CONNACK
/// announces only that shared subscriptions are not supported, and a Topic Alias Maximum of 16: the
/// aliases that a client sets last for its connection. A test can make the broker announce smaller
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
/// number and total size of waiting deliveries are bounded; the retained messages sent to a new
/// subscription wait among them. Exceeding either limit discards that session and closes its
/// connection, if any; its next connection has Session Present false. A delivery too large for
/// the broker's internal size limits also discards its target session, even an otherwise empty
/// one.
/// A new persistent session is refused with Server unavailable when the
/// persistent-session limit is reached. The number and total size of retained messages are
/// bounded too: a retained publication beyond either limit is delivered but not kept, which
/// leaves its topic without a retained message. These limits are provisional and may change.
/// All state is lost when the broker stops. This experimental API is outside the compatibility
/// promise.
public final class MqttBroker implements AutoCloseable {

    static {
        // The testkit's controls reach a broker's server through the internal control port,
        // which the module exports to the testkit alone.
        BrokerServer.serversOf(broker -> broker.server);
    }

    private final BrokerServer server;

    private MqttBroker(BrokerServer server) {
        this.server = server;
    }

    /// Starts on a system-assigned port on the loopback address.
    ///
    /// @return
    ///     the started broker
    /// @throws IOException
    ///     if the listening socket cannot be opened
    public static MqttBroker start() throws IOException {
        return start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    /// Starts listening at `address`; port zero selects a system-assigned port.
    ///
    /// @param address
    ///     the address to listen at
    /// @return
    ///     the started broker
    /// @throws IOException
    ///     if the listening socket cannot be opened
    public static MqttBroker start(InetSocketAddress address) throws IOException {
        return new MqttBroker(BrokerServer.start(address));
    }

    /// {@return the assigned listening port, including after close}
    public int port() {
        return server.port();
    }

    /// Stops accepting connections, closes all peers, and discards their session state and the
    /// retained messages, without publishing Wills, those that wait for their delay included.
    /// Repeated calls are harmless. Does not wait for peer I/O or send MQTT packets.
    @Override
    public void close() {
        server.close();
    }
}
