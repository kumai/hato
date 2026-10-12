package org.mqtt4j.hato.broker;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.mqtt4j.hato.broker.internal.BrokerServer;

/// An experimental, in-memory MQTT 3.1.1 broker for tests.
///
/// Each start creates an independent broker. Clean Session true and false and QoS 0/1
/// publications are supported. A persistent session retains subscriptions, unacknowledged
/// QoS 1 deliveries, and queued QoS 1 deliveries while disconnected. Reconnecting with the
/// same identifier resumes it with Session Present true and closes any previous connection
/// with that identifier; Clean Session true discards its previous session.
/// Incomplete QoS 1 deliveries resume before queued deliveries. A delivery whose transmission
/// was attempted but remains unacknowledged uses DUP true; one whose transmission has not
/// started uses DUP false.
/// Offline QoS 0 deliveries are discarded. Wills are refused with Server unavailable;
/// an empty identifier with Clean Session false is refused with Identifier rejected.
/// Retained publications are forwarded to current subscribers but are not stored.
/// Credentials are not checked. `$share/` filters are matched literally, without shared routing.
///
/// QoS 1 deliveries wait when unacknowledged deliveries reach their count or size limit.
/// Deliveries also wait while a subscriber's connection is slow to accept data. The number and
/// total size of waiting deliveries are bounded. Exceeding either limit discards that session
/// and closes its connection, if any; its next connection has Session Present false. A delivery
/// too large for the broker's internal size limits also discards its target session, even an
/// otherwise empty one.
/// A new persistent session is refused with Server unavailable when the
/// persistent-session limit is reached. These limits are provisional and may change.
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

    /// Stops accepting connections, closes all peers, and discards their session state.
    /// Repeated calls are harmless. Does not wait for peer I/O or send MQTT packets.
    @Override
    public void close() {
        server.close();
    }
}
