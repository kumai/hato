package org.mqtt4j.mqtj.broker;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.mqtt4j.mqtj.broker.internal.BrokerServer;

/// An experimental, in-memory MQTT 3.1.1 broker for tests.
///
/// Each start creates an independent broker. Clean Session true and false and QoS 0/1
/// publications are supported. A persistent session retains subscriptions, unacknowledged
/// QoS 1 deliveries, and queued QoS 1 deliveries while disconnected. Reconnecting with the
/// same identifier resumes it with Session Present true and closes any previous connection
/// with that identifier; Clean Session true discards its previous session.
/// Incomplete QoS 1 deliveries resume before queued deliveries. Those handed to the previous
/// connection's writer use DUP true; those not yet handed to it use DUP false.
/// Offline QoS 0 deliveries are discarded. Wills are refused with Server unavailable;
/// an empty identifier with Clean Session false is refused with Identifier rejected.
/// Retained publications are forwarded to current subscribers but are not stored.
/// Credentials are not checked. `$share/` filters are matched literally, without shared routing.
///
/// QoS 1 deliveries wait for acknowledgements when the unacknowledged-delivery window is full.
/// The session's waiting queue has separate count and byte limits. Exceeding those limits
/// discards that session and closes its connection, if any; its next connection has Session
/// Present false. A single encoded delivery that cannot fit the normal send queue or its
/// applicable QoS 1 window also discards its target session, even an otherwise empty one.
/// A new persistent session is refused with Server unavailable when the
/// persistent-session limit is reached. These provisional internal limits are not configuration
/// contracts. All state is lost when the broker stops. This API is outside the compatibility
/// promise until broker productization.
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
