package org.mqtt4j.hato.broker.internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.log.Log;
import org.mqtt4j.hato.broker.internal.common.protocol.IncomingAliases;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.protocol.SharedTopicFilter;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.util.Objects.requireNonNull;

// The client on one connection, as the broker sees it: it accepts the CONNECT that opens the
// connection, then handles each packet that the client sends. Its state belongs to routing: the
// session that the CONNECT attached, what CONNACK announced, the Will, the Topic Aliases, and
// the responses that a test holds back.
final class Client {

    private static final Log log = Log.of(Client.class);

    private final Connection connection;

    private final Routing routing;

    private final BrokerSettings settings;

    private @Nullable Session session;

    // The Will of the accepted connection, until the connection's end publishes it or a
    // normal DISCONNECT discards it.
    private Packet.@Nullable Will will;

    // What CONNACK announced to the client, which the broker holds it to for the connection's
    // life; nothing for MQTT 3.1.1 or before acceptance.
    private Capabilities limits = Capabilities.UNANNOUNCED;

    // The Topic Aliases that the client has set on this connection, from its acceptance; none
    // without an announced Topic Alias Maximum, as in MQTT 3.1.1.
    private @Nullable IncomingAliases aliases;

    // The responses that a test holds back from this connection, in the order held. They end
    // with the connection.
    final ArrayDeque<Packet> held = new ArrayDeque<>();

    // The publications that the client sent on this connection and that have no response yet,
    // for an announced Receive Maximum: QoS 2 publications until PUBREL, and the PUBACKs and
    // PUBCOMPs that a test holds back. A response in the send queue counts as sent, so the count
    // never exceeds the client's own, and a client that keeps to the maximum is never closed.
    private final BitSet unreleased = new BitSet();

    private int heldAcknowledgements;

    // What the client announced in its CONNECT, which the session keeps to on this connection.
    // The send quota is its Receive Maximum: how many QoS 1 and QoS 2 publications the broker
    // may send it unanswered at once (MQTT 5.0 section 4.9). The send limit is its Maximum
    // Packet Size. MQTT 3.1.1 announces neither, which leaves the broker's own window and limits.
    int sendQuota = 65_535;

    int sendLimit = Connection.MAX_PACKET_SIZE;

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
        // MQTT 3.1.1 leaves the retention of a Clean Session 0 session to the broker, which
        // keeps it without end.
        long expiry = switch (request.version()) {
            case V3_1_1 -> request.cleanStart() ? 0 : Session.NEVER;
            case V5 -> expiryOf(request.properties()).orElse(0);
        };
        boolean keep = expiry > 0;
        String id;
        synchronized (routing) {
            Connection previous;
            Session accepted;
            synchronized (connection) {
                if (connection.ended) {
                    return false;
                }
                // The connection keeps what its CONNACK announces. MQTT 3.1.1 announces nothing.
                Capabilities offered = switch (request.version()) {
                    case V3_1_1 -> Capabilities.UNANNOUNCED;
                    case V5 -> routing.capabilities;
                };
                // A Will that the announced capabilities cannot carry is refused [MQTT-3.2.2-12]
                // [MQTT-3.2.2-13].
                if (requestedWill != null
                        && requestedWill.qos().level() > offered.maximumQos().level()) {
                    connection.refuse(
                            ReasonCode.QOS_NOT_SUPPORTED,
                            "a Will above the maximum QoS for client '" + request.clientId() + "'"
                    );
                    return false;
                }
                if (requestedWill != null && requestedWill.retain() && !offered.retainAvailable()) {
                    connection.refuse(
                            ReasonCode.RETAIN_NOT_SUPPORTED,
                            "a retained Will for client '" + request.clientId() + "'"
                    );
                    return false;
                }
                // Inline expiry or a scheduling failure must happen before taking over a peer.
                // A Server Keep Alive replaces the client's own [MQTT-3.2.2-21] [MQTT-3.2.2-22].
                Integer serverKeepAlive = offered.serverKeepAlive();
                connection.keepAlive(
                        (serverKeepAlive == null) ? request.keepAliveSeconds() : serverKeepAlive
                );
                if (connection.ended) {
                    return false;
                }
                id = request.clientId();
                if (id.isEmpty()) {
                    do {
                        id = "hato" + UUID.randomUUID().toString().replace("-", "");
                    } while (routing.sessions.containsKey(id));
                }
                // An expired session is gone, for resuming and for the limit alike.
                routing.discardExpiredSessions();
                Session existing = routing.sessions.get(id);
                boolean present = !request.cleanStart()
                        && existing != null
                        && existing.kept()
                        && existing.version == request.version();
                // Resuming or replacing a kept session reuses its place in the limit.
                if (keep && (existing == null || !existing.kept())
                        && routing.sessions.values().stream().filter(Session::kept).count()
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
                    accepted.expiry = expiry;
                } else {
                    if (existing != null) {
                        existing.clear();
                    }
                    accepted = new Session(routing, id, request.version(), expiry);
                    routing.sessions.put(id, accepted);
                }
                connection.clientId = id;
                session = accepted;
                will = requestedWill;
                limits = offered;
                sendQuota = sendQuotaOf(request);
                sendLimit = sendLimitOf(request);
                if (offered.topicAliasMaximum() > 0) {
                    aliases = new IncomingAliases(offered.topicAliasMaximum());
                }
                Integer maximumPacketSize = offered.maximumPacketSize();
                if (maximumPacketSize != null) {
                    connection.limitPacketSize(maximumPacketSize);
                }
                accepted.connection = connection;
                Properties properties = switch (request.version()) {
                    case V3_1_1 -> Properties.EMPTY;
                    case V5 -> acceptanceV5(id, request.clientId().isEmpty(), offered);
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

    // The properties of an MQTT 5.0 CONNACK that accepts the connection. The broker has no shared
    // subscriptions, so it tells clients to send none. It announces the capabilities that differ
    // from what a client assumes without them, and only MQTT 5.0 can tell a client the identifier
    // assigned to it.
    private static Properties acceptanceV5(String id, boolean assigned, Capabilities offered) {
        var entries = new ArrayList<Property>();
        if (!offered.subscriptionIdentifiersAvailable()) {
            entries.add(new Property.ByteValue(PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE, 0));
        }
        entries.add(new Property.ByteValue(PropertyId.SHARED_SUBSCRIPTION_AVAILABLE, 0));
        int topicAliasMaximum = offered.topicAliasMaximum();
        if (topicAliasMaximum > 0) {
            entries.add(new Property.TwoByteValue(
                    PropertyId.TOPIC_ALIAS_MAXIMUM,
                    topicAliasMaximum
            ));
        }
        Qos maximumQos = offered.maximumQos();
        if (maximumQos != Qos.EXACTLY_ONCE) {
            entries.add(new Property.ByteValue(PropertyId.MAXIMUM_QOS, maximumQos.level()));
        }
        if (!offered.retainAvailable()) {
            entries.add(new Property.ByteValue(PropertyId.RETAIN_AVAILABLE, 0));
        }
        if (!offered.wildcardSubscriptionAvailable()) {
            entries.add(new Property.ByteValue(PropertyId.WILDCARD_SUBSCRIPTION_AVAILABLE, 0));
        }
        Integer receiveMaximum = offered.receiveMaximum();
        if (receiveMaximum != null) {
            entries.add(new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, receiveMaximum));
        }
        Integer maximumPacketSize = offered.maximumPacketSize();
        if (maximumPacketSize != null) {
            entries.add(new Property.FourByteValue(
                    PropertyId.MAXIMUM_PACKET_SIZE,
                    maximumPacketSize
            ));
        }
        Integer serverKeepAlive = offered.serverKeepAlive();
        if (serverKeepAlive != null) {
            entries.add(new Property.TwoByteValue(PropertyId.SERVER_KEEP_ALIVE, serverKeepAlive));
        }
        if (assigned) {
            entries.add(new Property.StringValue(PropertyId.ASSIGNED_CLIENT_IDENTIFIER, id));
        }
        return new Properties(entries);
    }

    // The Subscription Identifier that a SUBSCRIBE gives every subscription that it makes or
    // replaces, and without one 0, which no identifier takes (MQTT 5.0 section 3.8.2.1.2). MQTT
    // 3.1.1 has no properties.
    private static int identifierOf(Packet.Subscribe request) {
        Property identifier = request.properties()
                .find(PropertyId.SUBSCRIPTION_IDENTIFIER)
                .orElse(null);
        return switch (identifier) {
            case Property.VariableByteValue(var _, var value) -> value;
            case null, default -> 0;
        };
    }

    // The Session Expiry Interval in `properties` of a CONNECT or DISCONNECT, in seconds.
    private static OptionalLong expiryOf(Properties properties) {
        return switch (properties.find(PropertyId.SESSION_EXPIRY_INTERVAL).orElse(null)) {
            case Property.FourByteValue(var _, var value) -> OptionalLong.of(value);
            case null, default -> OptionalLong.empty();
        };
    }

    // The QoS 1 and QoS 2 publications that the client takes unanswered at once: the Receive
    // Maximum of its CONNECT, 65,535 without one [MQTT-3.3.4-9].
    private static int sendQuotaOf(Packet.Connect request) {
        return switch (request.properties().find(PropertyId.RECEIVE_MAXIMUM).orElse(null)) {
            case Property.TwoByteValue(var _, var value) -> value;
            case null, default -> 65_535;
        };
    }

    // The largest packet that the client takes: the Maximum Packet Size of its CONNECT, and
    // without one, or above it, the largest that MQTT allows [MQTT-3.1.2-24].
    private static int sendLimitOf(Packet.Connect request) {
        return switch (request.properties().find(PropertyId.MAXIMUM_PACKET_SIZE).orElse(null)) {
            case Property.FourByteValue(var _, var value) ->
                    (int) Math.min(value, Connection.MAX_PACKET_SIZE);
            case null, default -> Connection.MAX_PACKET_SIZE;
        };
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
                int identifier = identifierOf(request);
                // Where CONNACK announced that Subscription Identifiers are not supported, one
                // is a Protocol Error that subscribes to nothing (MQTT 5.0 section 3.2.2.3.12).
                if (identifier != 0 && !limits.subscriptionIdentifiersAvailable()) {
                    connection.disconnect(
                            ReasonCode.SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED,
                            "Subscription Identifier although none are supported"
                    );
                    return;
                }
                // The broker has no shared subscriptions, which CONNACK announced in MQTT 5.0,
                // so a shared filter there is a Protocol Error that subscribes to nothing
                // (MQTT 5.0 section 3.2.2.3.13) and ends the connection [MQTT-4.13.1-1].
                if (owner.version == MqttVersion.V5 && request.subscriptions().stream()
                        .anyMatch(requested -> SharedTopicFilter.isShared(requested.filter()))) {
                    connection.disconnect(
                            ReasonCode.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED,
                            "shared subscription although none are supported"
                    );
                    return;
                }
                // So is a wildcard when CONNACK announced that none are available (MQTT 5.0
                // section 3.2.2.3.11).
                if (!limits.wildcardSubscriptionAvailable() && request.subscriptions().stream()
                        .anyMatch(requested -> hasWildcard(requested.filter()))) {
                    connection.disconnect(
                            ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED,
                            "wildcard subscription although none are supported"
                    );
                    return;
                }
                var grants = new ArrayList<ReasonCode>();
                // The subscriptions that receive retained messages after SUBACK.
                var retaining = new ArrayList<Session.Subscribed>();
                for (Packet.Subscription requested : request.subscriptions()) {
                    // An MQTT 3.1.1 client cannot learn that shared subscriptions are missing,
                    // so the broker refuses the filter rather than match it as written: a
                    // client that means a shared subscription would wait for nothing.
                    if (SharedTopicFilter.isShared(requested.filter())) {
                        grants.add(ReasonCode.UNSPECIFIED_ERROR);
                        continue;
                    }
                    // A test rule can refuse the filter, which then subscribes to nothing
                    // and leaves an earlier subscription to it as it was, or grant it less
                    // than the requested QoS. The announced Maximum QoS lowers the grant too,
                    // as a broker without QoS 1 or 2 still accepts every SUBSCRIBE
                    // [MQTT-3.2.2-10].
                    Qos allowed = routing.allowedQos(requested);
                    if (allowed == null) {
                        grants.add(ReasonCode.UNSPECIFIED_ERROR);
                        continue;
                    }
                    Qos maximum = Qos.of(
                            Math.min(allowed.level(), limits.maximumQos().level())
                    );
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
                    var subscribed = new Session.Subscribed(subscription, identifier);
                    Session.Subscribed replaced = owner.subscriptions.put(
                            subscription.filter(),
                            subscribed
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
                        retaining.add(subscribed);
                    }
                }
                send(new Packet.SubAck(request.packetId(), grants, Properties.EMPTY));
                for (Session.Subscribed subscribed : retaining) {
                    owner.deliverRetained(subscribed);
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
            case Packet.Publish publish -> receive(owner, resolved(publish));
            case Packet.AliasPublish publish -> receive(owner, resolved(publish));
            case Packet.PubAck ack -> owner.acknowledge(ack.packetId(), false);
            case Packet.PubRec ack -> owner.release(ack.packetId(), ack.reasonCode());
            case Packet.PubRel ack -> {
                owner.received.clear(ack.packetId());
                unreleased.clear(ack.packetId());
                send(new Packet.PubComp(ack.packetId(), ReasonCode.SUCCESS, Properties.EMPTY));
            }
            case Packet.PubComp ack -> owner.acknowledge(ack.packetId(), true);
            case Packet.PingReq _ -> send(new Packet.PingResp());
            case Packet.Disconnect disconnect -> {
                // The client may change the Session Expiry Interval as it leaves, but not from
                // 0, which makes the DISCONNECT invalid (MQTT 5.0 section 3.14.2.2.2).
                OptionalLong expiry = expiryOf(disconnect.properties());
                if (expiry.isPresent()) {
                    if (owner.expiry == 0 && expiry.getAsLong() != 0) {
                        throw new PacketDecodeException(
                                ReasonCode.PROTOCOL_ERROR,
                                "Session Expiry Interval on DISCONNECT after none on CONNECT"
                        );
                    }
                    owner.expiry = expiry.getAsLong();
                }
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

    // `publish` without its Topic Alias, after the alias, if any, takes its topic. The aliases
    // last for this connection [MQTT-3.3.2-7]; without them, as in MQTT 3.1.1, none is valid.
    private Packet.Publish resolved(Packet.Publish publish) {
        IncomingAliases table = aliases;
        if (table != null) {
            return table.set(publish);
        }
        if (publish.properties().find(PropertyId.TOPIC_ALIAS).isPresent()) {
            throw noAliases();
        }
        return publish;
    }

    // The PUBLISH that a Topic Alias without a topic stands for.
    private Packet.Publish resolved(Packet.AliasPublish publish) {
        IncomingAliases table = aliases;
        if (table == null) {
            throw noAliases();
        }
        return table.resolve(publish);
    }

    private static PacketDecodeException noAliases() {
        return new PacketDecodeException(
                ReasonCode.TOPIC_ALIAS_INVALID,
                "Topic Alias without an announced maximum"
        );
    }

    // Forwards a publication from the client and acknowledges it. Only a server sets
    // Subscription Identifiers. MQTT 3.1.1 has no properties.
    private void receive(Session owner, Packet.Publish publish) {
        if (publish.properties().find(PropertyId.SUBSCRIPTION_IDENTIFIER).isPresent()) {
            throw new PacketDecodeException(
                    ReasonCode.PROTOCOL_ERROR,
                    "Subscription Identifier from a client"
            );
        }
        // A PUBLISH beyond what CONNACK announced is a Protocol Error (MQTT 5.0 sections
        // 3.2.2.3.4 and 3.2.2.3.5, and 3.3.4 for the Receive Maximum). The first two end the
        // connection here, as the SUBSCRIBE checks do, because a PacketDecodeException carries
        // only the reasons for a packet that cannot be accepted as read.
        if (publish.qos().level() > limits.maximumQos().level()) {
            connection.disconnect(
                    ReasonCode.QOS_NOT_SUPPORTED,
                    "PUBLISH above the announced Maximum QoS"
            );
            return;
        }
        if (publish.retain() && !limits.retainAvailable()) {
            connection.disconnect(
                    ReasonCode.RETAIN_NOT_SUPPORTED,
                    "retained PUBLISH although retained messages are not supported"
            );
            return;
        }
        Integer receiveMaximum = limits.receiveMaximum();
        boolean unanswered = publish.qos() == Qos.AT_LEAST_ONCE
                || (publish.qos() == Qos.EXACTLY_ONCE && !unreleased.get(publish.packetId()));
        if (receiveMaximum != null && unanswered
                && unreleased.cardinality() + heldAcknowledgements >= receiveMaximum) {
            throw new PacketDecodeException(
                    ReasonCode.RECEIVE_MAXIMUM_EXCEEDED,
                    "more unanswered QoS 1 and 2 publications than the Receive Maximum"
            );
        }
        if (publish.qos() == Qos.EXACTLY_ONCE) {
            unreleased.set(publish.packetId());
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
            unreleased.clear(publish.packetId());
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

    // Called under routing, which guards the test controls.
    private void send(Packet packet) {
        // A response that a test holds back waits outside the send queue, and so do the later
        // ones of its kind, until the test releases them.
        if (routing.holdsBack(packet.getClass(), connection.clientId)) {
            if (held.size() >= settings.maxHeld()) {
                connection.close("held responses over their limit");
            } else {
                held.addLast(packet);
                if (answersPublication(packet)) {
                    heldAcknowledgements++;
                }
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
                if (answersPublication(response)) {
                    heldAcknowledgements--;
                }
            }
        }
        connection.release(released);
    }

    // Whether `response` ends the client's QoS 1 or 2 publication, as far as the client's
    // Receive Maximum counts it.
    private static boolean answersPublication(Packet response) {
        return response instanceof Packet.PubAck || response instanceof Packet.PubComp;
    }

    private static boolean hasWildcard(TopicFilter filter) {
        return filter.pattern().contains("+") || filter.pattern().contains("#");
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
