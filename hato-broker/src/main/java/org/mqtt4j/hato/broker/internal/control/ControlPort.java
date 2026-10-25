package org.mqtt4j.hato.broker.internal.control;

import java.util.List;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.BrokerServer;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;

/// The test controls of one broker, for the testkit, which turns them into its public
/// operations. The signatures use only JDK types, the broker's public types, and the types
/// declared here, because the shared implementation, such as packets, is a different type in
/// each jar.
public final class ControlPort {

    /// The kinds of response that a test can hold back.
    public enum Response {
        SUBACK,
        UNSUBACK,
        PUBACK,
        PUBREC,
        PUBCOMP
    }

    /// A response that a connection of `clientId` holds back.
    public record Held(Response kind, String clientId, int packetId) {
    }

    private final BrokerServer server;

    private ControlPort(BrokerServer server) {
        this.server = server;
    }

    /// The controls of `broker`.
    public static ControlPort of(MqttBroker broker) {
        return new ControlPort(BrokerServer.of(requireNonNull(broker, "broker must not be null")));
    }

    /// See [BrokerServer#refuseSubscriptions(String)].
    public void refuseSubscriptions(String filter) {
        server.refuseSubscriptions(filter);
    }

    /// See [BrokerServer#refuseSubscriptions(String, int, String)].
    public void refuseSubscriptions(String filter, int reasonCode, @Nullable String reasonString) {
        server.refuseSubscriptions(filter, reasonCode, reasonString);
    }

    /// See [BrokerServer#refusePublications(String, int, String)].
    public void refusePublications(String filter, int reasonCode, @Nullable String reasonString) {
        server.refusePublications(filter, reasonCode, reasonString);
    }

    /// See [BrokerServer#clearPublicationRule(String)].
    public void clearPublicationRule(String filter) {
        server.clearPublicationRule(filter);
    }

    /// See [BrokerServer#refuseConnections(String, int, String, String)].
    public void refuseConnections(
            String clientId,
            int reasonCode,
            @Nullable String reasonString,
            @Nullable String serverReference
    ) {
        server.refuseConnections(clientId, reasonCode, reasonString, serverReference);
    }

    /// See [BrokerServer#clearConnectionRule(String)].
    public void clearConnectionRule(String clientId) {
        server.clearConnectionRule(clientId);
    }

    /// See [BrokerServer#grantAtMost(String, int)].
    public void grantAtMost(String filter, int maximumLevel) {
        server.grantAtMost(filter, maximumLevel);
    }

    /// See [BrokerServer#clearSubscriptionRule(String)].
    public void clearSubscriptionRule(String filter) {
        server.clearSubscriptionRule(filter);
    }

    /// See [BrokerServer#holdResponses(Class, String)].
    public void holdResponses(Response kind, String clientId) {
        server.holdResponses(typeOf(kind), clientId);
    }

    /// See [BrokerServer#releaseResponses(Class, String)].
    public void releaseResponses(Response kind, String clientId) {
        server.releaseResponses(typeOf(kind), clientId);
    }

    /// See [BrokerServer#heldResponses()].
    public List<Held> heldResponses() {
        return server.heldResponses().stream()
                .map(ControlPort::held)
                .toList();
    }

    /// See [BrokerServer#connectedClients()].
    public Set<String> connectedClients() {
        return server.connectedClients();
    }

    /// See [BrokerServer#disconnect(String)].
    public void disconnect(String clientId) {
        server.disconnect(clientId);
    }

    /// See [BrokerServer#disconnect(String, int, String, String)].
    public void disconnect(
            String clientId,
            int reasonCode,
            @Nullable String reasonString,
            @Nullable String serverReference
    ) {
        server.disconnect(clientId, reasonCode, reasonString, serverReference);
    }

    /// See [BrokerServer#announce(int, boolean, boolean, boolean, int, int, int)].
    public void announce(
            int maximumQosLevel,
            boolean retainAvailable,
            boolean wildcardSubscriptionAvailable,
            boolean subscriptionIdentifiersAvailable,
            int receiveMaximum,
            int maximumPacketSize,
            int topicAliasMaximum
    ) {
        server.announce(
                maximumQosLevel,
                retainAvailable,
                wildcardSubscriptionAvailable,
                subscriptionIdentifiersAvailable,
                receiveMaximum,
                maximumPacketSize,
                topicAliasMaximum
        );
    }

    private static Class<? extends Packet> typeOf(Response kind) {
        return switch (kind) {
            case SUBACK -> Packet.SubAck.class;
            case UNSUBACK -> Packet.UnsubAck.class;
            case PUBACK -> Packet.PubAck.class;
            case PUBREC -> Packet.PubRec.class;
            case PUBCOMP -> Packet.PubComp.class;
        };
    }

    private static Held held(BrokerServer.Held held) {
        String clientId = held.clientId();
        return switch (held.response()) {
            case Packet.SubAck ack -> new Held(Response.SUBACK, clientId, ack.packetId());
            case Packet.UnsubAck ack -> new Held(Response.UNSUBACK, clientId, ack.packetId());
            case Packet.PubAck ack -> new Held(Response.PUBACK, clientId, ack.packetId());
            case Packet.PubRec ack -> new Held(Response.PUBREC, clientId, ack.packetId());
            case Packet.PubComp ack -> new Held(Response.PUBCOMP, clientId, ack.packetId());
            // Only the kinds that typeOf names are ever held.
            default -> throw new IllegalStateException(
                    "Held " + held.response().getClass().getSimpleName()
            );
        };
    }
}
