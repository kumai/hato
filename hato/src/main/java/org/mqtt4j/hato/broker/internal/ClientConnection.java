package org.mqtt4j.hato.broker.internal;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.broker.BrokerListener;

// A connection of a client as the application's listener hears of it. The listener gets this one
// object with both notices of the connection, so it keeps the equality of its identity.
final class ClientConnection implements BrokerListener.Connection {

    private final String clientId;

    private final @Nullable InetSocketAddress remoteAddress;

    private final MqttVersion version;

    // `clientId` is the identifier that the broker accepted the client with, and `remoteAddress`
    // where the connection comes from, if it comes over a network.
    ClientConnection(String clientId, @Nullable SocketAddress remoteAddress, MqttVersion version) {
        this.clientId = clientId;
        this.remoteAddress = (remoteAddress instanceof InetSocketAddress address) ? address : null;
        this.version = version;
    }

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public Optional<InetSocketAddress> remoteAddress() {
        return Optional.ofNullable(remoteAddress);
    }

    @Override
    public MqttVersion version() {
        return version;
    }

    @Override
    public String toString() {
        return "Connection[clientId=" + clientId + ", remoteAddress=" + remoteAddress
                + ", version=" + version + "]";
    }
}
