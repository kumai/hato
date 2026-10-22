package org.mqtt4j.hato.testkit;

import java.util.List;
import java.util.Set;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.control.ControlPort;

import static java.util.Objects.requireNonNull;

/// Test controls for an embedded broker: rules that refuse subscriptions or grant them a lower
/// QoS, responses held back until the test releases them, and the clients connected now.
///
/// The test starts and closes the broker through the broker's own API, and the controls act on
/// it; the controls that [#of(MqttBroker)] returns for one broker share its state. Each method
/// takes effect for what the broker handles after the method returns, and may be called from
/// any thread, also while clients are connected. Until a test uses them, the controls change
/// nothing.
///
/// A subscription rule applies to SUBSCRIBE requests for exactly its filter, as written, without
/// matching other filters. The broker answers a refused filter with failure, which MQTT 5.0
/// reports as Unspecified error, and makes no subscription; a subscription that the client made
/// to the filter before stays as it was. A filter granted at most a QoS is granted the lower of
/// that QoS and the requested one, and its deliveries, retained messages included, follow the
/// granted QoS. A rule lasts until it is cleared or replaced, or until the broker closes.
///
/// Holding back a kind of response to a client holds every later response of that kind to the
/// connections of the client, without taking room in the broker's send queue. Releasing sends
/// the held responses in the order held, ahead of later responses, and later responses of that
/// kind go out at once. The responses held for a connection are dropped when it ends; a client
/// that resumes its session sends its requests again. A connection holds at most 65,535
/// responses, one for each packet identifier, and the broker closes a connection that would
/// hold more. [#heldResponses()] shows when the broker holds a response, so a test can wait
/// for one, for example with Awaitility.
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

    /// Holds back the responses of `kind` to the client with `clientId`, from now until
    /// [#releaseResponses(ResponseKind, String)].
    ///
    /// @throws NullPointerException if `kind` or `clientId` is null
    public void holdResponses(ResponseKind kind, String clientId) {
        requireNonNull(kind, "kind must not be null");
        requireNonNull(clientId, "clientId must not be null");
        port.holdResponses(responseOf(kind), clientId);
    }

    /// The responses that the broker holds back now, for each connection in the order held.
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

    /// The identifiers of the clients that have an open connection now.
    public Set<String> connectedClients() {
        return port.connectedClients();
    }

    /// Closes the open connection of the client with `clientId`, as a broker does for an
    /// administrative reason: an MQTT 5.0 client first receives DISCONNECT with Administrative
    /// action. The client did not disconnect, so its Will is published. Does nothing if the
    /// client has no open connection.
    ///
    /// @throws NullPointerException if `clientId` is null
    public void disconnect(String clientId) {
        port.disconnect(requireNonNull(clientId, "clientId must not be null"));
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
