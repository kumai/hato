package org.mqtt4j.hato.broker.internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.log.Log;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.util.Objects.requireNonNull;

// The client on one connection, as the broker sees it: it accepts the CONNECT that opens the
// connection, then handles each packet that the client sends. Its state belongs to routing: the
// session that the CONNECT attached, the Will, and the responses that a test holds back.
final class Client {

    private static final Log log = Log.of(Client.class);

    private final Connection connection;

    private final Routing routing;

    private final BrokerSettings settings;

    private @Nullable Session session;

    // The Will of the accepted connection, until the connection's end publishes it or a
    // normal DISCONNECT discards it.
    private Packet.@Nullable Will will;

    // The responses that a test holds back from this connection, in the order held. They end
    // with the connection.
    final ArrayDeque<Packet> held = new ArrayDeque<>();

    Client(Connection connection, Routing routing) {
        this.connection = connection;
        this.routing = routing;
        settings = routing.settings;
    }

    // Accepts the CONNECT that opens the connection, or refuses it, and returns whether it was
    // accepted. Called by the reader, which holds neither lock.
    boolean connect(Packet.Connect request) {
        // MQTT 3.1.1 refuses an empty identifier for a persistent session [MQTT-3.1.3-8];
        // MQTT 5.0 assigns one below whatever the session.
        if (request.version() == MqttVersion.V3_1_1
                && !request.cleanStart()
                && request.clientId().isEmpty()) {
            connection.refuse(
                    ReasonCode.CLIENT_IDENTIFIER_NOT_VALID,
                    "an empty persistent identifier"
            );
            return false;
        }
        if (request.properties().find(PropertyId.AUTHENTICATION_METHOD).isPresent()) {
            connection.refuse(
                    ReasonCode.BAD_AUTHENTICATION_METHOD,
                    "authentication for client '" + request.clientId() + "'"
            );
            return false;
        }
        // The broker publishes a Will as its connection ends, so it cannot keep one for a
        // delay that a new connection of the session would cancel [MQTT-3.1.3-9]. Only
        // whether the delay is positive matters.
        Packet.Will requestedWill = request.will();
        boolean delayed = requestedWill != null && requestedWill.properties()
                .find(PropertyId.WILL_DELAY_INTERVAL)
                .filter(delay -> delay instanceof Property.FourByteValue value
                        && value.value() > 0)
                .isPresent();
        if (delayed) {
            connection.refuse(
                    ReasonCode.SERVER_UNAVAILABLE,
                    "a delayed Will for client '" + request.clientId() + "'"
            );
            return false;
        }
        boolean keep = switch (request.version()) {
            case V3_1_1 -> !request.cleanStart();
            // Only whether the interval is positive matters; sessions do not expire.
            case V5 -> request.properties().find(PropertyId.SESSION_EXPIRY_INTERVAL)
                    .filter(interval -> interval instanceof Property.FourByteValue value
                            && value.value() > 0)
                    .isPresent();
        };
        String id;
        synchronized (routing) {
            Connection previous;
            Session accepted;
            synchronized (connection) {
                if (connection.ended) {
                    return false;
                }
                // Inline expiry or a scheduling failure must happen before taking over a peer.
                connection.keepAlive(request.keepAliveSeconds());
                if (connection.ended) {
                    return false;
                }
                id = request.clientId();
                if (id.isEmpty()) {
                    do {
                        id = "hato" + UUID.randomUUID().toString().replace("-", "");
                    } while (routing.sessions.containsKey(id));
                }
                Session existing = routing.sessions.get(id);
                boolean present = !request.cleanStart()
                        && existing != null
                        && existing.kept
                        && existing.version == request.version();
                // Resuming or replacing a kept session reuses its place in the limit.
                if (keep && (existing == null || !existing.kept)
                        && routing.sessions.values().stream().filter(value -> value.kept).count()
                        >= settings.maxSessions()) {
                    connection.refuse(
                            ReasonCode.SERVER_UNAVAILABLE,
                            "persistent session capacity for client '" + id + "'"
                    );
                    return false;
                }
                previous = existing == null ? null : existing.connection;
                if (present) {
                    accepted = requireNonNull(existing);
                    if (previous != null) {
                        accepted.detach(previous);
                    }
                    accepted.kept = keep;
                } else {
                    if (existing != null) {
                        existing.clear();
                    }
                    accepted = new Session(routing, id, request.version(), keep);
                    routing.sessions.put(id, accepted);
                }
                connection.clientId = id;
                session = accepted;
                will = requestedWill;
                accepted.connection = connection;
                Properties properties = switch (request.version()) {
                    case V3_1_1 -> Properties.EMPTY;
                    case V5 -> acceptanceV5(id, request.clientId().isEmpty());
                };
                connection.enqueue(new Packet.ConnAck(present, ReasonCode.SUCCESS, properties));
            }
            // Do not hold the new peer's monitor while ending the old peer.
            if (previous != null) {
                previous.disconnect(
                        ReasonCode.SESSION_TAKEN_OVER,
                        "taken over by " + connection.remoteAddress()
                );
            }
            // CONNACK, retransmissions, and queued deliveries stay ahead of later publications,
            // including the Will of the connection that this one took over.
            accepted.resume();
            if (previous != null) {
                previous.client.publishWill();
            }
        }
        log.info("Broker accepted client {} from {} on port {}",
                id, connection.remoteAddress(), routing.port);
        return true;
    }

    // The properties of an MQTT 5.0 CONNACK that accepts the connection. The broker sets no
    // Subscription Identifiers on its deliveries, so it tells clients to send none; and only
    // MQTT 5.0 can tell a client the identifier assigned to it.
    private static Properties acceptanceV5(String id, boolean assigned) {
        var entries = new ArrayList<Property>();
        entries.add(new Property.ByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE, 0));
        if (assigned) {
            entries.add(new Property.StringValue(PropertyId.ASSIGNED_CLIENT_IDENTIFIER, id));
        }
        return new Properties(entries);
    }

    // The session of this connection while it is attached and the connection has not ended.
    // Called under routing.
    @Nullable Session currentSession() {
        Session owner = session;
        return (owner != null && owner.connection == connection && !connection.isEnded())
                ? owner
                : null;
    }

    // Publishes the Will of this connection, which ended without a DISCONNECT that discards
    // it, as its client would have published the message. Called under routing; taking the
    // Will means that no later end publishes it again. A closing broker publishes none,
    // because every session goes with it.
    void publishWill() {
        Packet.Will last = will;
        Session owner = session;
        will = null;
        if (last == null || owner == null || routing.closed) {
            return;
        }
        // The Will properties travel as the publication's, without the delay, which only
        // the Will has; User Properties keep their order [MQTT-3.1.3-10]. The packet
        // identifier is a placeholder, because every delivery takes its own.
        var properties = new Properties(last.properties().entries().stream()
                .filter(property -> property.id() != PropertyId.WILL_DELAY_INTERVAL)
                .toList());
        var publication = new Packet.Publish(
                last.topic(),
                last.payload(),
                last.qos(),
                last.retain(),
                false,
                (last.qos() == Qos.AT_MOST_ONCE) ? 0 : 1,
                properties
        );
        routing.route(publication, owner);
    }

    // Handles one packet that the client sent on its accepted connection. Called under routing.
    void process(Session owner, Packet packet) {
        switch (packet) {
            case Packet.Subscribe request -> {
                // CONNACK announced that Subscription Identifiers are not supported, so one
                // is a Protocol Error that subscribes to nothing (MQTT 5.0 section
                // 3.2.2.3.12). MQTT 3.1.1 has no properties.
                if (request.properties().find(PropertyId.SUBSCRIPTION_IDENTIFIER).isPresent()) {
                    connection.disconnect(
                            ReasonCode.SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED,
                            "Subscription Identifier although none are supported"
                    );
                    return;
                }
                var grants = new ArrayList<ReasonCode>();
                // The subscriptions that receive retained messages after SUBACK.
                var retaining = new ArrayList<Packet.Subscription>();
                for (Packet.Subscription requested : request.subscriptions()) {
                    // A test rule can refuse the filter, which then subscribes to nothing
                    // and leaves an earlier subscription to it as it was, or grant it less
                    // than the requested QoS.
                    Qos maximum = routing.allowedQos(requested);
                    if (maximum == null) {
                        grants.add(ReasonCode.UNSPECIFIED_ERROR);
                        continue;
                    }
                    boolean lowered = maximum.level() < requested.qos().level();
                    Packet.Subscription subscription = lowered
                            ? new Packet.Subscription(
                                    requested.filter(),
                                    maximum,
                                    requested.noLocal(),
                                    requested.retainAsPublished(),
                                    requested.retainHandling()
                            )
                            : requested;
                    Packet.Subscription replaced = owner.subscriptions.put(
                            subscription.filter(),
                            subscription
                    );
                    ReasonCode granted = switch (subscription.qos()) {
                        case AT_MOST_ONCE -> ReasonCode.SUCCESS;
                        case AT_LEAST_ONCE -> ReasonCode.GRANTED_QOS_1;
                        case EXACTLY_ONCE -> ReasonCode.GRANTED_QOS_2;
                    };
                    grants.add(granted);
                    // Retain Handling 0 sends retained messages for every SUBSCRIBE, 1 only
                    // for a new subscription, and 2, the only other value, never
                    // [MQTT-3.3.1-9] [MQTT-3.3.1-10] [MQTT-3.3.1-11]. MQTT 3.1.1 uses 0.
                    boolean sendsRetained = switch (subscription.retainHandling()) {
                        case 0 -> true;
                        case 1 -> replaced == null;
                        default -> false;
                    };
                    if (sendsRetained) {
                        retaining.add(subscription);
                    }
                }
                send(new Packet.SubAck(request.packetId(), grants, Properties.EMPTY));
                for (Packet.Subscription subscription : retaining) {
                    owner.deliverRetained(subscription);
                }
            }
            case Packet.Unsubscribe request -> {
                var results = new ArrayList<ReasonCode>();
                for (TopicFilter filter : request.filters()) {
                    boolean existed = owner.subscriptions.remove(filter) != null;
                    results.add(
                            existed ? ReasonCode.SUCCESS : ReasonCode.NO_SUBSCRIPTION_EXISTED
                    );
                }
                // MQTT 3.1.1 UNSUBACK carries no results.
                List<ReasonCode> reasonCodes = switch (owner.version) {
                    case V3_1_1 -> List.of();
                    case V5 -> results;
                };
                send(new Packet.UnsubAck(request.packetId(), reasonCodes, Properties.EMPTY));
            }
            case Packet.Publish publish -> {
                // The broker announces no Topic Alias Maximum, so no alias is valid, and only
                // a server sets Subscription Identifiers. MQTT 3.1.1 has no properties.
                if (publish.properties().find(PropertyId.TOPIC_ALIAS).isPresent()) {
                    throw new PacketDecodeException(
                            ReasonCode.TOPIC_ALIAS_INVALID,
                            "Topic Alias without an announced maximum"
                    );
                }
                if (publish.properties().find(PropertyId.SUBSCRIPTION_IDENTIFIER).isPresent()) {
                    throw new PacketDecodeException(
                            ReasonCode.PROTOCOL_ERROR,
                            "Subscription Identifier from a client"
                    );
                }
                if (publish.qos() == Qos.EXACTLY_ONCE) {
                    if (owner.received.get(publish.packetId())) {
                        send(new Packet.PubRec(
                                publish.packetId(),
                                ReasonCode.SUCCESS,
                                Properties.EMPTY
                        ));
                        return;
                    }
                    owner.received.set(publish.packetId());
                } else if (publish.qos() == Qos.AT_LEAST_ONCE) {
                    // Reusing the identifier abandons a QoS 2 exchange still waiting for
                    // PUBREL, as a client that lost its state does. Its message was already
                    // forwarded on PUBLISH, so nothing is lost.
                    owner.received.clear(publish.packetId());
                }
                routing.route(publish, owner);
                // Acknowledge only after every delivery was enqueued or its target closed.
                if (publish.qos() == Qos.AT_LEAST_ONCE) {
                    send(new Packet.PubAck(
                            publish.packetId(),
                            ReasonCode.SUCCESS,
                            Properties.EMPTY
                    ));
                } else if (publish.qos() == Qos.EXACTLY_ONCE) {
                    send(new Packet.PubRec(
                            publish.packetId(),
                            ReasonCode.SUCCESS,
                            Properties.EMPTY
                    ));
                }
            }
            case Packet.PubAck ack -> owner.acknowledge(ack.packetId(), false);
            case Packet.PubRec ack -> owner.release(ack.packetId(), ack.reasonCode());
            case Packet.PubRel ack -> {
                owner.received.clear(ack.packetId());
                send(new Packet.PubComp(ack.packetId(), ReasonCode.SUCCESS, Properties.EMPTY));
            }
            case Packet.PubComp ack -> owner.acknowledge(ack.packetId(), true);
            case Packet.PingReq _ -> send(new Packet.PingResp());
            case Packet.Disconnect disconnect -> {
                // Only a normal disconnection discards the Will [MQTT-3.14.4-3]; MQTT 5.0
                // publishes it for any other reason code, Disconnect with Will Message
                // among them (MQTT 5.0 section 3.1.2.5).
                boolean normal = disconnect.reasonCode() == ReasonCode.SUCCESS;
                if (normal) {
                    will = null;
                }
                connection.close(
                        normal
                                ? "client sent DISCONNECT"
                                : "client sent DISCONNECT with " + disconnect.reasonCode()
                );
            }
            case Packet.AliasPublish _ -> throw new PacketDecodeException(
                    ReasonCode.TOPIC_ALIAS_INVALID,
                    "Topic Alias without an announced maximum"
            );
            // Name only the type; the INFO end log must not carry fields such as AUTH data.
            case Packet.Connect _,
                 Packet.ConnAck _,
                 Packet.SubAck _,
                 Packet.UnsubAck _,
                 Packet.PingResp _,
                 Packet.Auth _ -> throw new PacketDecodeException(
                         ReasonCode.PROTOCOL_ERROR,
                         "Unexpected " + packet.getClass().getSimpleName() + " packet"
                 );
        }
    }

    // Called under routing, which guards the test controls.
    private void send(Packet packet) {
        // A response that a test holds back waits outside the send queue, and so do the later
        // ones of its kind, until the test releases them.
        if (routing.holdsBack(packet.getClass(), connection.clientId)) {
            if (held.size() >= settings.maxHeld()) {
                connection.close("held responses over their limit");
            } else {
                held.addLast(packet);
            }
            return;
        }
        connection.respond(packet);
    }

    // Moves the held responses of `type` behind any released before, in the order held, and
    // sends what fits. Called under routing.
    void release(Class<? extends Packet> type) {
        var released = new ArrayList<Packet>();
        for (Iterator<Packet> responses = held.iterator(); responses.hasNext(); ) {
            Packet response = responses.next();
            if (type.isInstance(response)) {
                responses.remove();
                released.add(response);
            }
        }
        connection.release(released);
    }

    // Detaches the session from the connection, which has ended, and publishes the Will. Called
    // under routing.
    void connectionEnded() {
        Session owner = session;
        if (owner != null) {
            owner.detach(connection);
        }
        // The Will follows the end of the network connection ([MQTT-3.1.2-8] in MQTT
        // 3.1.1), so a persistent session that subscribes to it receives it as an
        // absent client.
        publishWill();
    }
}
