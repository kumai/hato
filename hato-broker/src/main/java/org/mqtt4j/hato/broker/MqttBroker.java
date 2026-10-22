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
/// Offline QoS 0 deliveries are discarded. Wills are refused with Server unavailable; an empty
/// identifier with Clean Session false is refused with Identifier rejected. Overlapping
/// subscriptions of a session receive one copy at their highest QoS. Credentials are not
/// checked. `$share/` filters are matched literally, without shared routing.
///
/// A retained publication becomes the retained message of its topic in place of the previous
/// one, and a retained publication with an empty payload removes it; both reach current
/// subscribers with the retain flag cleared. A new subscription receives the retained messages
/// that match it, with the retain flag set, at the lower of their QoS and its own.
///
/// MQTT 5.0 support covers what client tests need. A positive Session Expiry Interval makes a
/// session persistent, as Clean Session false does, but the session never expires; Clean Start
/// false resumes it. A session resumes only with the protocol version that created it. An empty
/// client identifier is assigned and returned in CONNACK. Publication properties reach MQTT 5.0
/// subscribers unchanged and are dropped for MQTT 3.1.1 subscribers; Message Expiry Interval is
/// not counted down, so retained messages do not expire. A No Local subscription receives no
/// publications from its own client, though it does receive the retained messages that its
/// client published. A Retain As Published subscription receives the publisher's retain flag;
/// when only some overlapping subscriptions use it, a retained publication arrives once with
/// each flag. Retain Handling sends the retained messages for every SUBSCRIBE, only for a new
/// subscription, or never. CONNACK announces only that Subscription Identifiers are not
/// supported, and the broker ignores the client's Receive Maximum and Maximum Packet Size. An
/// Authentication Method is refused with Bad authentication method. AUTH, a PUBLISH with a Topic
/// Alias or a Subscription Identifier, or a SUBSCRIBE with a Subscription Identifier closes the
/// connection. A PUBREC with a failure reason code ends the QoS 2 delivery without PUBREL.
/// Before it closes an MQTT 5.0 connection for a malformed or forbidden packet, an expired
/// keep-alive, or another connection with the same client identifier, the broker sends
/// DISCONNECT with the reason code; a peer that stops reading may not receive it.
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

    private final BrokerServer server;

    private MqttBroker(BrokerServer server) {
        this.server = server;
    }

    /// Starts on a system-assigned port on the loopback address.
    ///
    /// @throws IOException if the listening socket cannot be opened
    public static MqttBroker start() throws IOException {
        return start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    /// Starts listening at `address`; port zero selects a system-assigned port.
    ///
    /// @throws IOException if the listening socket cannot be opened
    public static MqttBroker start(InetSocketAddress address) throws IOException {
        return new MqttBroker(BrokerServer.start(address));
    }

    /// Returns the assigned listening port, including after close.
    public int port() {
        return server.port();
    }

    /// Stops accepting connections, closes all peers, and discards their session state and the
    /// retained messages. Repeated calls are harmless. Does not wait for peer I/O or send MQTT
    /// packets.
    @Override
    public void close() {
        server.close();
    }
}
