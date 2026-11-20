package org.mqtt4j.hato.broker.internal;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.broker.BrokerListener;

// A connection of a client as the broker reports it, to the application's listener and in the
// list of clients. The listener gets this one object with both notices of the connection, and the
// list shows the same object, so it keeps the equality of its identity. It refers to nothing of
// the broker's, so an application that keeps it keeps no connection or session. Its text leaves
// the user name out, as the broker's logs and the authenticator's request do.
final class ClientConnection implements BrokerListener.Connection {

    private final String clientId;

    private final @Nullable String username;

    private final @Nullable InetSocketAddress remoteAddress;

    private final MqttVersion version;

    private final @Nullable Topic willTopic;

    private final boolean overTls;

    private final List<X509Certificate> clientCertificates;

    // `clientId` is the identifier that the broker accepted the client with, `username` the user
    // name of its CONNECT, if any, `remoteAddress` where the connection comes from, if it comes
    // over a network, `willTopic` the topic of the CONNECT's Will, if any, and `overTls` and
    // `clientCertificates` whether it came to the TLS listener and the certificates, if any, that
    // the client presented there.
    ClientConnection(
            String clientId,
            @Nullable String username,
            @Nullable SocketAddress remoteAddress,
            MqttVersion version,
            @Nullable Topic willTopic,
            boolean overTls,
            List<X509Certificate> clientCertificates
    ) {
        this.clientId = clientId;
        this.username = username;
        this.remoteAddress = (remoteAddress instanceof InetSocketAddress address) ? address : null;
        this.version = version;
        this.willTopic = willTopic;
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
    public Optional<InetSocketAddress> remoteAddress() {
        return Optional.ofNullable(remoteAddress);
    }

    @Override
    public MqttVersion version() {
        return version;
    }

    @Override
    public Optional<Topic> willTopic() {
        return Optional.ofNullable(willTopic);
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
        return "Connection[clientId=" + clientId + ", remoteAddress=" + remoteAddress
                + ", version=" + version + ", overTls=" + overTls + "]";
    }
}
