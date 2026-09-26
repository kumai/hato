package org.mqtt4j.mqtj.broker;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.mqtt4j.mqtj.broker.internal.BrokerServer;

/// An experimental, in-memory MQTT 3.1.1 broker for tests.
///
/// Each start creates an independent broker. Only clean sessions and QoS 0/1 publications
/// are supported. Persistent sessions and Wills are refused with Server unavailable;
/// an empty identifier with Clean Session false is refused with Identifier rejected.
/// Retained publications are forwarded to current subscribers but are not stored.
/// Credentials are not checked. `$share/` filters are matched literally, without shared routing.
///
/// A connection is disconnected when its send queue or its unacknowledged QoS 1 deliveries
/// exceed provisional internal limits. The queue counts every outgoing packet, so a single
/// publication whose packet exceeds its byte limit disconnects each matching subscriber, even
/// an idle one, while the publisher still receives its PUBACK. These limits are not
/// configuration contracts. This API is outside the compatibility promise until broker
/// productization.
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
