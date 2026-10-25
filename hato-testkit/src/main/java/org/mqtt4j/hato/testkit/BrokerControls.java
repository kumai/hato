package org.mqtt4j.hato.testkit;

import java.util.List;
import java.util.Set;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.control.ControlPort;

import static java.util.Objects.requireNonNull;

/// Experimental test controls for an embedded broker: rules that refuse publications,
/// subscriptions, or connections, or grant subscriptions a lower QoS, responses held back until
/// the test releases them, the capabilities that the broker announces to MQTT 5.0 clients, and
/// the clients connected now, which the test can disconnect. Like the broker API, the controls
/// are outside the compatibility promise until productization.
///
/// The test starts and closes the broker through the broker's own API, and the controls act on
/// it; the controls that [#of(MqttBroker)] returns for one broker share its state. Each method
/// takes effect for what the broker handles after the method returns, and may be called from
/// any thread, also while clients are connected. Until a test uses them, the controls change
/// nothing.
///
/// A subscription rule applies to SUBSCRIBE requests for exactly its filter, as written, without
/// matching other filters. The broker answers a refused filter with the rule's reason code, or
/// Unspecified error when the rule gives none, which MQTT 3.1.1 reports as failure 0x80, and
/// makes no subscription; a subscription that the client made to the filter before stays as it
/// was. A filter granted at most a QoS is granted the lower of that QoS and the requested one,
/// and its deliveries, retained messages included, follow the granted QoS. A rule lasts until
/// it is cleared or replaced, or until the broker closes.
///
/// A publication rule applies to the PUBLISH packets whose topic its filter matches; a topic that
/// several rules match takes the rule set last. The broker delivers a refused publication to no
/// subscriber and does not retain it, in both MQTT versions and at every QoS. An MQTT 5.0 client
/// learns of the refusal from the PUBACK or PUBREC that carries the rule's reason, and a refused
/// QoS 2 publication ends there; an MQTT 3.1.1 client receives an ordinary acknowledgement, and
/// a QoS 0 publication no answer. The rules do not apply to Wills.
///
/// A connection rule applies to CONNECT requests with exactly its client identifier, as written,
/// and refuses them before they affect a session or a Will; connections accepted before stay. An
/// MQTT 5.0 client receives the rule's reason in CONNACK. An MQTT 3.1.1 client receives the
/// return code that MQTT 3.1.1 has for the reason code, if it has one, and otherwise sees its
/// connection closed without CONNACK.
///
/// A reason's strings reach MQTT 5.0 clients only. The broker leaves the Reason String out of
/// PUBACK, PUBREC, and SUBACK for a client that asked for no problem information, and leaves out
/// the Reason String, and then the Server Reference, as far as a packet would otherwise exceed
/// the client's Maximum Packet Size.
///
/// Holding back a kind of response to a client holds every later response of that kind to the
/// connections of the client, without taking room in the broker's send queue. Releasing sends
/// the held responses in the order held, ahead of later responses, and later responses of that
/// kind go out at once. The responses held for a connection are dropped when it ends; a client
/// that resumes its session sends its requests again. A connection holds at most 65,535
/// responses, one for each packet identifier, and the broker closes a connection that would
/// hold more. [#heldResponses()] shows when the broker holds a response, so a test can wait
/// for one, for example with Awaitility.
///
/// An announcement applies to the MQTT 5.0 connections that the broker accepts after
/// [#announce(BrokerCapabilities)] returns, and lasts until the next one or until the broker
/// closes. A connection keeps the capabilities that it was accepted with until it ends, and what
/// the broker holds already stays as it was: sessions, the QoS granted to their subscriptions,
/// retained messages, and the deliveries that wait. MQTT 3.1.1 connections learn of no
/// capabilities and are held to none.
public final class BrokerControls {

    private final ControlPort port;

    private BrokerControls(ControlPort port) {
        this.port = port;
    }

    /// The controls of `broker`.
    ///
    /// @throws NullPointerException if `broker` is null
    public static BrokerControls of(MqttBroker broker) {
        requireNonNull(broker, "broker must not be null");
        return new BrokerControls(ControlPort.of(broker));
    }

    /// Refuses later subscriptions to exactly `filter`.
    ///
    /// @throws NullPointerException if `filter` is null
    public void refuseSubscriptions(TopicFilter filter) {
        port.refuseSubscriptions(requireNonNull(filter, "filter must not be null").pattern());
    }

    /// Refuses later subscriptions to exactly `filter` with `reason`, whose reason code must be
    /// a failure that MQTT 5.0 allows in SUBACK (MQTT 5.0 section 3.9.3). A SUBACK carries the
    /// Reason String of the first filter refused with one.
    ///
    /// @throws NullPointerException if `filter` or `reason` is null
    /// @throws IllegalArgumentException if the reason code is not such a failure, `reason` has
    ///         a Server Reference, or its Reason String is not an MQTT UTF-8 string
    public void refuseSubscriptions(TopicFilter filter, BrokerReason reason) {
        requireNonNull(filter, "filter must not be null");
        requireNonNull(reason, "reason must not be null");
        requireNoServerReference(reason);
        port.refuseSubscriptions(
                filter.pattern(),
                reason.reasonCode().value(),
                reason.reasonString().orElse(null)
        );
    }

    /// Grants later subscriptions to exactly `filter` at most `maximum`.
    ///
    /// @throws NullPointerException if `filter` or `maximum` is null
    public void grantAtMost(TopicFilter filter, Qos maximum) {
        requireNonNull(filter, "filter must not be null");
        requireNonNull(maximum, "maximum must not be null");
        port.grantAtMost(filter.pattern(), maximum.level());
    }

    /// Removes the rule for `filter`, if there is one, so that later subscriptions to it are
    /// granted as requested.
    ///
    /// @throws NullPointerException if `filter` is null
    public void clearSubscriptionRule(TopicFilter filter) {
        port.clearSubscriptionRule(requireNonNull(filter, "filter must not be null").pattern());
    }

    /// Refuses later publications to the topics that `filter` matches with `reason`, whose
    /// reason code must be a failure that MQTT 5.0 allows in PUBACK and PUBREC (MQTT 5.0
    /// sections 3.4.2.1 and 3.5.2.1).
    ///
    /// @throws NullPointerException if `filter` or `reason` is null
    /// @throws IllegalArgumentException if the reason code is not such a failure, `reason` has
    ///         a Server Reference, or its Reason String is not an MQTT UTF-8 string
    public void refusePublications(TopicFilter filter, BrokerReason reason) {
        requireNonNull(filter, "filter must not be null");
        requireNonNull(reason, "reason must not be null");
        requireNoServerReference(reason);
        port.refusePublications(
                filter.pattern(),
                reason.reasonCode().value(),
                reason.reasonString().orElse(null)
        );
    }

    /// Removes the publication rule for `filter`, if there is one, so that later publications to
    /// its topics go through unless another rule refuses them.
    ///
    /// @throws NullPointerException if `filter` is null
    public void clearPublicationRule(TopicFilter filter) {
        port.clearPublicationRule(requireNonNull(filter, "filter must not be null").pattern());
    }

    /// Refuses later connections of the client with `clientId` with `reason`, whose reason code
    /// must be a failure that MQTT 5.0 allows in CONNACK (MQTT 5.0 section 3.2.2.2).
    ///
    /// @throws NullPointerException if `clientId` or `reason` is null
    /// @throws IllegalArgumentException if the reason code is not such a failure, a Server
    ///         Reference goes with another reason code, or a string is not an MQTT UTF-8 string
    public void refuseConnections(String clientId, BrokerReason reason) {
        requireNonNull(clientId, "clientId must not be null");
        requireNonNull(reason, "reason must not be null");
        port.refuseConnections(
                clientId,
                reason.reasonCode().value(),
                reason.reasonString().orElse(null),
                reason.serverReference().orElse(null)
        );
    }

    /// Removes the connection rule for `clientId`, if there is one, so that later connections of
    /// the client are accepted again.
    ///
    /// @throws NullPointerException if `clientId` is null
    public void clearConnectionRule(String clientId) {
        port.clearConnectionRule(requireNonNull(clientId, "clientId must not be null"));
    }

    /// Holds back the responses of `kind` to the client with `clientId`, from now until
    /// [#releaseResponses(ResponseKind, String)].
    ///
    /// @throws NullPointerException if `kind` or `clientId` is null
    public void holdResponses(ResponseKind kind, String clientId) {
        requireNonNull(kind, "kind must not be null");
        requireNonNull(clientId, "clientId must not be null");
        port.holdResponses(responseOf(kind), clientId);
    }

    /// The responses that the broker holds back now, for each connection in the order held, as an
    /// unmodifiable list that later changes do not affect.
    public List<HeldResponse> heldResponses() {
        return port.heldResponses().stream()
                .map(held -> new HeldResponse(
                        kindOf(held.kind()),
                        held.clientId(),
                        held.packetId()
                ))
                .toList();
    }

    /// Stops holding back the responses of `kind` to the client with `clientId`, and sends
    /// those held in the order held.
    ///
    /// @throws NullPointerException if `kind` or `clientId` is null
    public void releaseResponses(ResponseKind kind, String clientId) {
        requireNonNull(kind, "kind must not be null");
        requireNonNull(clientId, "clientId must not be null");
        port.releaseResponses(responseOf(kind), clientId);
    }

    /// The identifiers of the clients that have an open connection now, as an unmodifiable set
    /// that later changes do not affect.
    public Set<String> connectedClients() {
        return port.connectedClients();
    }

    /// Closes the open connection of the client with `clientId`, as a broker does for an
    /// administrative reason: an MQTT 5.0 client first receives DISCONNECT with Administrative
    /// action. The client did not disconnect, so its Will is published as for a lost connection,
    /// after its Will Delay Interval if it has one. Does nothing if the client has no open
    /// connection.
    ///
    /// @throws NullPointerException if `clientId` is null
    public void disconnect(String clientId) {
        port.disconnect(requireNonNull(clientId, "clientId must not be null"));
    }

    /// Closes the open connection of the client with `clientId` as [#disconnect(String)] does,
    /// but an MQTT 5.0 client first receives DISCONNECT with `reason`, whose reason code must be
    /// one that MQTT 5.0 lets a server send in DISCONNECT (MQTT 5.0 section 3.14.2.1). Even
    /// with Normal disconnection, the Will is published, because the client did not
    /// disconnect.
    ///
    /// @throws NullPointerException if `clientId` or `reason` is null
    /// @throws IllegalArgumentException if a server cannot send the reason code, a Server
    ///         Reference goes with another reason code, or a string is not an MQTT UTF-8 string
    public void disconnect(String clientId, BrokerReason reason) {
        requireNonNull(clientId, "clientId must not be null");
        requireNonNull(reason, "reason must not be null");
        port.disconnect(
                clientId,
                reason.reasonCode().value(),
                reason.reasonString().orElse(null),
                reason.serverReference().orElse(null)
        );
    }

    /// Makes the broker announce `capabilities` to the MQTT 5.0 clients that connect from now
    /// on, and hold their connections to them. Announce [BrokerCapabilities#defaults()] to go
    /// back to what the broker announces by itself.
    ///
    /// @throws NullPointerException if `capabilities` is null
    public void announce(BrokerCapabilities capabilities) {
        requireNonNull(capabilities, "capabilities must not be null");
        port.announce(
                capabilities.maximumQos().level(),
                capabilities.retainAvailable(),
                capabilities.wildcardSubscriptionAvailable(),
                capabilities.subscriptionIdentifiersAvailable(),
                capabilities.receiveMaximum(),
                capabilities.maximumPacketSize(),
                capabilities.topicAliasMaximum()
        );
    }

    // MQTT 5.0 carries a Server Reference in CONNACK and DISCONNECT only, so the broker's rules
    // for acknowledgements take none, and a reason with one is refused rather than sent without.
    private static void requireNoServerReference(BrokerReason reason) {
        if (reason.serverReference().isPresent()) {
            throw new IllegalArgumentException(
                    "A Server Reference goes only with a refused or ended connection"
            );
        }
    }

    private static ControlPort.Response responseOf(ResponseKind kind) {
        return switch (kind) {
            case SUBACK -> ControlPort.Response.SUBACK;
            case UNSUBACK -> ControlPort.Response.UNSUBACK;
            case PUBACK -> ControlPort.Response.PUBACK;
            case PUBREC -> ControlPort.Response.PUBREC;
            case PUBCOMP -> ControlPort.Response.PUBCOMP;
        };
    }

    private static ResponseKind kindOf(ControlPort.Response response) {
        return switch (response) {
            case SUBACK -> ResponseKind.SUBACK;
            case UNSUBACK -> ResponseKind.UNSUBACK;
            case PUBACK -> ResponseKind.PUBACK;
            case PUBREC -> ResponseKind.PUBREC;
            case PUBCOMP -> ResponseKind.PUBCOMP;
        };
    }
}
