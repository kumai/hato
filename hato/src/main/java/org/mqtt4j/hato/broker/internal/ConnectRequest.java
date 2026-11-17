package org.mqtt4j.hato.broker.internal;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.internal.protocol.Packet;

// What the application's authenticator learns of a CONNECT. Its text leaves the credentials out,
// so that no log or exception message that shows it carries them.
final class ConnectRequest implements Authenticator.Request {

    private final String clientId;

    private final @Nullable String username;

    private final byte @Nullable [] password;

    private final @Nullable InetSocketAddress remoteAddress;

    private final MqttVersion version;

    private final boolean overTls;

    private final List<X509Certificate> clientCertificates;

    // `clientId` is the identifier that the client has once accepted, which the broker may have
    // assigned, and `remoteAddress` where the connection comes from, if it comes over a network.
    // `overTls` tells whether it comes to the TLS listener, and `clientCertificates` are those that
    // its handshake presented, which the selector has taken from the engine already, so that the
    // decision does not touch the engine.
    ConnectRequest(
            Packet.Connect request,
            String clientId,
            @Nullable SocketAddress remoteAddress,
            boolean overTls,
            List<X509Certificate> clientCertificates
    ) {
        this.clientId = clientId;
        username = request.username();
        password = request.password();
        this.remoteAddress = (remoteAddress instanceof InetSocketAddress address) ? address : null;
        version = request.version();
        this.overTls = overTls;
        this.clientCertificates = clientCertificates;
    }

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public Optional<String> username() {
        return Optional.ofNullable(username);
    }

    @Override
    public Optional<byte[]> password() {
        // The CONNECT keeps its own array; the application gets a copy each time.
        byte[] own = password;
        return (own == null) ? Optional.empty() : Optional.of(own.clone());
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
    public boolean overTls() {
        return overTls;
    }

    @Override
    public List<X509Certificate> clientCertificates() {
        return clientCertificates;
    }

    @Override
    public String toString() {
        return "ConnectRequest[clientId=" + clientId + ", version=" + version
                + ", overTls=" + overTls + "]";
    }
}
