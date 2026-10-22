package org.mqtt4j.hato.client.internal;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.SequencedMap;
import java.util.SequencedSet;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.CapacityLimit;
import org.mqtt4j.hato.ClientStats.Usage;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttNotConnectedException;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttPublicationRefusedException;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.SessionCore.State;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.client.internal.SessionCore.isFailure;
import static org.mqtt4j.hato.client.internal.SessionCore.protocolError;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// The session core's part for outgoing publications. It keeps the session's QoS 1 and QoS 2
/// exchanges, replays them on a continued session, answers PUBREC with PUBREL, and decides each
/// publication's future when its exchange or its connection ends. Publications beyond the
/// broker's Receive Maximum wait, in session order, until earlier exchanges finish. The core
/// decides when queued work may use send capacity; this part reaches the rest of the core only
/// through [SessionCore.Context].
final class Publications {

    private static final Log log = Log.of(Publications.class);

    private static final long INFLIGHT_BYTE_LIMIT = 32L * 1024 * 1024;

    // A Topic Alias property: its identifier and its two-byte value.
    private static final int TOPIC_ALIAS_BYTES = 3;

    // A broker can repeat PUBREC at will, so PUBREC responses bound their completion events in
    // the same way. Replayed PUBRELs are already bounded by the session's exchanges.
    private static final int RELEASE_WRITE_LIMIT = 16;

    private final SessionCore.Port port;

    private final SessionCore.Context core;

    private final String clientId;

    private final boolean cleanStart;

    // The connection's codec, which sizes the retained PUBLISH of a persistent session.
    private final PacketCodec codec;

    // The session's QoS 1 and QoS 2 exchanges, including publications that wait to be sent.
    private final int inflightLimit;

    // The session's exchanges by packet identifier. PUBLISH entries retain send order; replacing
    // one with PUBREL moves it to the end in PUBREC order. Both stages share the identifier and
    // count window across connections. A new session replaces them.
    private SequencedMap<Integer, Inflight> inflight = new LinkedHashMap<>();

    // Retained PUBLISH wire sizes, counted only for a persistent session until PUBACK/REC.
    private long bytes;

    // The current connection's writes and responses; a connection end replaces them.
    private Window window = new Window();

    private boolean unknownPubAckLogged;

    private boolean downgradedPubAckLogged;

    private boolean unknownPubRecLogged;

    private boolean unknownPubCompLogged;

    Publications(
            SessionCore.Port port,
            SessionCore.Context core,
            String clientId,
            boolean cleanStart,
            PacketCodec codec,
            int inflightLimit
    ) {
        this.port = port;
        this.core = core;
        this.clientId = clientId;
        this.cleanStart = cleanStart;
        this.codec = codec;
        this.inflightLimit = inflightLimit;
    }

    void publish(long operation, MqttMessage message) {
        if (!ready(operation)) {
            return;
        }
        Optional<ReasonCode> refusal = refusal(message);
        if (refusal.isPresent()) {
            port.complete(
                    operation,
                    null,
                    new MqttPublicationRefusedException(refusal.orElseThrow())
            );
            return;
        }
        // A new publication neither overtakes nor joins exchanges that wait for send capacity.
        // SUBACK waiting does not block publication.
        if (window.stalled) {
            capacityFailure(operation, CapacityLimit.SEND_QUEUE, "Send queue capacity");
            return;
        }
        int packetId = 0;
        SendReceipt receipt = null;
        if (message.qos() != Qos.AT_MOST_ONCE) {
            if (inflight.size() >= inflightLimit) {
                capacityFailure(
                        operation,
                        CapacityLimit.INFLIGHT_PUBLISHES,
                        "Publication capacity"
                );
                return;
            }
            // Beyond the broker's Receive Maximum, a publication waits for an earlier exchange to
            // finish, and later ones wait behind it. QoS 0 never waits.
            boolean waits = !window.unsent.isEmpty() || outstanding() >= receiveMaximum();
            // A persistent session keeps the PUBLISH until PUBACK or PUBREC, and a clean one only
            // while it waits; the byte budget counts what is kept.
            boolean kept = !cleanStart || waits;
            int byteCount = 0;
            if (kept) {
                long remaining = INFLIGHT_BYTE_LIMIT - bytes;
                if (MessageProperties.bodySize(message) > remaining) {
                    capacityFailure(
                            operation,
                            CapacityLimit.PUBLICATION_BYTES,
                            "Publication byte capacity"
                    );
                    return;
                }
                byteCount = wireSize(message);
                if (byteCount > remaining) {
                    capacityFailure(
                            operation,
                            CapacityLimit.PUBLICATION_BYTES,
                            "Publication byte capacity"
                    );
                    return;
                }
            }
            OptionalInt allocated = core.allocatePacketId();
            if (allocated.isEmpty()) {
                capacityFailure(
                        operation,
                        CapacityLimit.PACKET_IDENTIFIERS,
                        "Packet identifier capacity"
                );
                return;
            }
            packetId = allocated.getAsInt();
            var publication = new Publication(
                    packetId,
                    operation,
                    message.qos(),
                    kept ? message : null,
                    byteCount
            );
            inflight.put(packetId, publication);
            bytes += byteCount;
            if (waits) {
                window.unsent.add(publication);
                return;
            }
            receipt = publication.receipt;
        }
        long ticket = core.nextTicket();
        if (packetId == 0) {
            window.publications.put(ticket, operation);
        }
        TopicAlias alias = alias(message);
        SendQueue.OfferResult result = port.publish(
                core.generation(),
                ticket,
                message,
                packetId,
                receipt,
                alias
        );
        if (result == SendQueue.OfferResult.ACCEPTED) {
            window.aliases.accepted(message.topic(), alias);
            awaitPublication(ticket, message.qos(), packetId);
        } else if (result == SendQueue.OfferResult.FULL) {
            if (packetId == 0) {
                window.publications.remove(ticket);
            } else {
                removeInflight(packetId);
            }
            capacityFailure(operation, CapacityLimit.SEND_QUEUE, "Send queue capacity");
        }
        // CLOSED waits for the connection's terminal notification, as do SUBSCRIBE/UNSUBSCRIBE.
    }

    void acknowledged(Packet.PubAck ack) {
        Inflight exchange = inflight.get(ack.packetId());
        if (exchange == null) {
            if (!unknownPubAckLogged) {
                unknownPubAckLogged = true;
                log.warn("Ignoring PUBACK for unknown packet identifier {} on {}",
                        ack.packetId(), clientId);
            }
            return;
        }
        switch (exchange) {
            case Release _ -> stageMismatch("PUBACK does not match the outgoing QoS stage");
            case Publication publication when publication.qos == Qos.EXACTLY_ONCE ->
                    downgraded(publication);
            case Publication publication when isFailure(ack.reasonCode()) -> refused(
                    publication,
                    ack.reasonCode(),
                    ConnectionEstablishment.text(ack.properties(), PropertyId.REASON_STRING)
                            .orElse(null)
            );
            case Publication publication -> {
                logReason("PUBACK", ack.packetId(), ack.properties());
                completePublication(publication, ack.reasonCode());
            }
        }
    }

    void release(Packet.PubRec ack) {
        int packetId = ack.packetId();
        Inflight exchange = inflight.get(packetId);
        if (isFailure(ack.reasonCode())) {
            refusedRelease(
                    exchange,
                    ack.reasonCode(),
                    ConnectionEstablishment.text(ack.properties(), PropertyId.REASON_STRING)
                            .orElse(null)
            );
            return;
        }
        switch (exchange) {
            case Publication publication -> {
                if (publication.qos != Qos.EXACTLY_ONCE) {
                    stageMismatch("PUBREC does not match the outgoing QoS stage");
                    return;
                }
                // The receiver owns the body after PUBREC, but the id and count stay reserved
                // until PUBCOMP. Replacing the entry drops the body and retains PUBREC order.
                bytes -= publication.bytes;
                window.unsent.remove(publication);
                core.responses().answered(ResponseDeadlines.Kind.PUBREC, packetId);
                logReason("PUBREC", packetId, ack.properties());
                // The result reports PUBREC's reason code once PUBCOMP completes the exchange.
                inflight.putLast(
                        packetId,
                        new Release(packetId, publication.operation, ack.reasonCode())
                );
            }
            case Release pending -> {
                // A PUBREL replay still waiting for capacity also answers this duplicate. The
                // scan is bounded by the 64 exchanges; index it only if that limit grows.
                if (window.releaseReplays.contains(pending)) {
                    return;
                }
            }
            case null -> {
                if (!unknownPubRecLogged) {
                    unknownPubRecLogged = true;
                    log.warn("Received PUBREC for unknown packet identifier {} on {}",
                            packetId, clientId);
                }
                window.unknownReleases.putIfAbsent(packetId, 0);
            }
        }
        // DISCONNECT remains last. A persistent session still records the new stage so the
        // next connection continues with PUBREL, even though this one cannot send it.
        if (core.state() == State.DISCONNECTING) {
            return;
        }
        window.releases.add(packetId);
    }

    void completed(int packetId) {
        Inflight exchange = inflight.get(packetId);
        if (exchange == null) {
            Integer admitted = window.unknownReleases.get(packetId);
            if (admitted != null && admitted > 1) {
                // The answer to one of several PUBRELs for an unknown PUBREC.
                window.unknownReleases.put(packetId, admitted - 1);
            } else if (admitted != null) {
                // The last answer to a PUBREL for an unknown PUBREC makes its id available.
                window.unknownReleases.remove(packetId);
                window.releases.remove(packetId);
                core.responses().answered(ResponseDeadlines.Kind.PUBCOMP, packetId);
            } else if (!unknownPubCompLogged) {
                unknownPubCompLogged = true;
                log.warn("Ignoring PUBCOMP for unknown packet identifier {} on {}",
                        packetId, clientId);
            }
            return;
        }
        if (!(exchange instanceof Release release)) {
            stageMismatch("PUBCOMP does not match the outgoing QoS stage");
            return;
        }
        completePublication(release, release.reasonCode);
    }

    /// Queues the session's exchanges for a connection that continues the session.
    void resume() {
        for (Inflight exchange : inflight.values()) {
            switch (exchange) {
                case Publication publication -> window.unsent.add(publication);
                case Release release -> window.releaseReplays.add(release);
            }
        }
    }

    /// Sends what this connection has yet to send and returns whether nothing waits for send
    /// capacity; while something does, the core sends no other queued work. The PUBRELs that a
    /// continued session resends go first, because they need no room under the broker's Receive
    /// Maximum and their PUBCOMPs make some. Publications follow in session order while room
    /// lasts. One that waits only for room holds back later publications, but no other packet:
    /// those keep flowing even when the broker's quota is used up (MQTT 5.0 section 4.9). A
    /// duplicate PUBREC shares a PUBREL replay still waiting here instead of adding another
    /// response.
    boolean sendQueued() {
        Release release;
        while ((release = window.releaseReplays.peekFirst()) != null) {
            long ticket = core.nextTicket();
            if (sendRelease(ticket, release.packetId) != SendQueue.OfferResult.ACCEPTED) {
                window.stalled = true;
                return false;
            }
            window.releaseReplays.removeFirst();
        }
        Publication next;
        while ((next = window.unsent.peekFirst()) != null && outstanding() < receiveMaximum()) {
            MqttMessage message = requireNonNull(next.message);
            // A continued session can reach a broker with stricter limits. Sending would end the
            // connection, and every reconnection would replay the same publication.
            Optional<ReasonCode> refusal = refusal(message);
            if (refusal.isPresent()) {
                refused(next, refusal.orElseThrow(), null);
                continue;
            }
            long ticket = core.nextTicket();
            TopicAlias alias = alias(message);
            SendQueue.OfferResult result = port.publish(
                    core.generation(),
                    ticket,
                    message,
                    next.packetId,
                    next.receipt,
                    alias
            );
            if (result != SendQueue.OfferResult.ACCEPTED) {
                window.stalled = true;
                return false;
            }
            window.aliases.accepted(message.topic(), alias);
            awaitPublication(ticket, next.qos, next.packetId);
            window.unsent.removeFirst();
            if (cleanStart) {
                // A clean session never resends, so it keeps the PUBLISH only until it is sent.
                bytes -= next.bytes;
                next.bytes = 0;
                next.message = null;
            }
        }
        window.stalled = false;
        return true;
    }

    /// Answers fresh PUBRECs within the bound on PUBREL writes awaiting completion.
    void sendReleases() {
        while (!window.releases.isEmpty() && window.releaseWrites.size() < RELEASE_WRITE_LIMIT) {
            int id = window.releases.getFirst();
            long ticket = core.nextTicket();
            if (sendRelease(ticket, id) != SendQueue.OfferResult.ACCEPTED) {
                // Re-creation uses the regular quota; it must not wait for acknowledgement room.
                break;
            }
            window.releases.removeFirst();
            window.releaseWrites.add(ticket);
            window.unknownReleases.computeIfPresent(id, (_, admitted) -> admitted + 1);
        }
    }

    /// Completes the QoS 0 publication that `ticket` wrote, and returns whether the write was a
    /// PUBREC response, whose completion makes room for the next one.
    boolean written(long ticket) {
        Long publication = window.publications.remove(ticket);
        if (publication != null) {
            port.complete(publication, PublishResult.of(), null);
        }
        return window.releaseWrites.remove(ticket);
    }

    /// Whether an unknown PUBREC still holds `packetId`. A PUBREL answering it has no session
    /// entry, and its PUBCOMP must not land on a new exchange using the same id, so the id stays
    /// unavailable until PUBCOMP, including the time after I/O admitted the response.
    boolean holds(int packetId) {
        return window.unknownReleases.containsKey(packetId);
    }

    /// Settles the publications of an ended connection. QoS 0 publications belong to it and are
    /// never replayed. When `hold`, a scheduled reconnection keeps a persistent session's
    /// publications pending, and the next connection decides them: continuing completes their
    /// exchanges, and losing the session fails them. Otherwise their outcome is unknown now.
    void connectionEnded(boolean hold, Throwable failure) {
        for (long operation : window.publications.values()) {
            port.complete(operation, null, failure);
        }
        if (!hold) {
            failPublications(failure);
        }
        window = new Window();
    }

    /// Fails the futures of the session's QoS 1 and QoS 2 publications with an unknown outcome.
    /// Their exchanges stay for a later connection unless the session goes too.
    void failPublications(Throwable failure) {
        for (Inflight exchange : inflight.values()) {
            if (exchange.operation != 0) {
                port.complete(exchange.operation, null, failure);
                exchange.operation = 0;
            }
        }
    }

    /// The session's exchanges go with it, so publications still waiting for them have an
    /// unknown outcome.
    void sessionDiscarded() {
        failPublications(new MqttConnectionLostException());
        inflight = new LinkedHashMap<>();
        bytes = 0;
    }

    private boolean ready(long operation) {
        if (core.state() == State.CLOSED) {
            port.complete(operation, null, new IllegalStateException("Client is closed"));
        } else if (core.state() != State.CONNECTED) {
            port.complete(operation, null, new MqttNotConnectedException());
        } else {
            return true;
        }
        return false;
    }

    private void capacityFailure(long operation, CapacityLimit limit, String message) {
        core.refused(limit);
        port.complete(operation, null, new MqttCapacityException(message));
    }

    /// The publications' values in [ClientSnapshot.Sending] that the core knows. Publications
    /// that wait to be sent count in their first stage.
    Values values() {
        long pubAck = 0;
        long pubRec = 0;
        long pubComp = 0;
        for (Inflight exchange : inflight.values()) {
            switch (exchange) {
                case Publication publication when publication.qos == Qos.AT_LEAST_ONCE -> pubAck++;
                case Publication _ -> pubRec++;
                case Release _ -> pubComp++;
            }
        }
        return new Values(
                pubAck,
                pubRec,
                pubComp,
                new Usage(inflight.size(), inflightLimit),
                new Usage(bytes, INFLIGHT_BYTE_LIMIT)
        );
    }

    /// How many more exchanges the established connection lets the client have in progress,
    /// within the broker's Receive Maximum.
    int quota() {
        return Math.max(0, receiveMaximum() - outstanding());
    }

    /// The exchanges that take room under the broker's Receive Maximum: the publications that
    /// this connection has sent, and every exchange awaiting PUBCOMP. MQTT 5.0 section 4.9 lets a
    /// new connection leave out an exchange whose PUBREL it resends, but a broker can count it
    /// from the reconnection on, as Mosquitto does; counting it here keeps within both.
    private int outstanding() {
        return inflight.size() - window.unsent.size();
    }

    /// How many exchanges may be outstanding on this connection: the broker's Receive Maximum,
    /// within the session's own limit.
    private int receiveMaximum() {
        return Math.min(inflightLimit, core.receiveMaximum());
    }

    private void awaitPublication(long ticket, Qos qos, int packetId) {
        ResponseDeadlines responses = core.responses();
        switch (qos) {
            case AT_MOST_ONCE -> {
            }
            case AT_LEAST_ONCE -> responses.add(ticket, ResponseDeadlines.Kind.PUBACK, packetId);
            case EXACTLY_ONCE -> responses.add(ticket, ResponseDeadlines.Kind.PUBREC, packetId);
        }
    }

    private SendQueue.OfferResult sendRelease(long ticket, int packetId) {
        SendQueue.OfferResult result = port.send(
                core.generation(),
                ticket,
                new Packet.PubRel(packetId, ReasonCode.SUCCESS, EMPTY),
                null
        );
        if (result == SendQueue.OfferResult.ACCEPTED) {
            core.releaseAdmitted();
            core.responses().add(ticket, ResponseDeadlines.Kind.PUBCOMP, packetId);
        }
        return result;
    }

    /// Brokers without QoS 2, such as RabbitMQ with MQTT 3.1.1, accept a QoS 2 PUBLISH as QoS 1
    /// and answer PUBACK. That ends the broker's exchange, possibly after delivering the message.
    /// Only this publication fails: the connection stays, even while disconnecting, and a
    /// persistent session does not resume the exchange or deliver the message again.
    private void downgraded(Publication publication) {
        if (!downgradedPubAckLogged) {
            downgradedPubAckLogged = true;
            log.warn("Broker accepted QoS 2 packet identifier {} at QoS 1 with PUBACK on {}",
                    publication.packetId, clientId);
        }
        removeInflight(publication.packetId);
        if (publication.operation != 0) {
            port.complete(
                    publication.operation,
                    null,
                    new MqttProtocolException(
                            "PUBACK accepted a QoS 2 publication at QoS 1",
                            ReasonCode.PROTOCOL_ERROR
                    )
            );
        }
    }

    /// A PUBREC with a failure reason code ends the exchange, and no PUBREL answers it (MQTT
    /// 5.0 section 4.3.3). Only a QoS 2 publication still waiting for PUBREC has an exchange to
    /// end; an exchange that the broker already accepted, or one that this client does not
    /// know, needs nothing.
    private void refusedRelease(
            @Nullable Inflight exchange,
            ReasonCode reason,
            @Nullable String reasonString
    ) {
        switch (exchange) {
            case Publication publication when publication.qos == Qos.EXACTLY_ONCE ->
                    refused(publication, reason, reasonString);
            case Publication _ -> stageMismatch("PUBREC does not match the outgoing QoS stage");
            case Release _ -> {
            }
            case null -> {
            }
        }
    }

    /// The reason code with which the broker would refuse `message`, by the limits that it
    /// announced for the connection, or empty when it would accept the message. MQTT 5.0 forbids
    /// sending such a publication ([MQTT-3.2.2-11], [MQTT-3.2.2-14], [MQTT-3.2.2-15]), and a
    /// broker that receives one ends the connection with the same reason code.
    private Optional<ReasonCode> refusal(MqttMessage message) {
        ConnAck limits = core.limits();
        if (message.qos().level() > limits.maximumQos().level()) {
            return Optional.of(ReasonCode.QOS_NOT_SUPPORTED);
        }
        if (message.retain() && !limits.retainAvailable()) {
            return Optional.of(ReasonCode.RETAIN_NOT_SUPPORTED);
        }
        if (exceeds(message, limits.maximumPacketSize())) {
            return Optional.of(ReasonCode.PACKET_TOO_LARGE);
        }
        return Optional.empty();
    }

    /// Whether the PUBLISH that carries `message` is larger than `limit`. Without a limit below
    /// what MQTT can carry, the send queue bounds the size, as it does for MQTT 3.1.1.
    private boolean exceeds(MqttMessage message, int limit) {
        if (limit >= ConnectionEstablishment.LARGEST_PACKET_SIZE) {
            return false;
        }
        try {
            return wireSize(message) > limit;
        } catch (IllegalArgumentException _) {
            // No Remaining Length can describe the packet, so no limit admits it.
            return true;
        }
    }

    /// The Topic Alias that a PUBLISH of `message` uses on this connection. It uses none when
    /// the broker takes no aliases, or when the alias property could make it larger than the
    /// broker accepts: whether a publication fits is decided without its alias, because aliases
    /// differ from one connection to the next.
    private TopicAlias alias(MqttMessage message) {
        int maximum = core.topicAliasMaximum();
        if (maximum == 0) {
            return TopicAlias.NONE;
        }
        TopicAlias alias = window.aliases.choose(message.topic(), maximum);
        int limit = core.limits().maximumPacketSize();
        // Setting an alias adds it to the topic, which makes the longest form; the form that
        // carries the alias alone is never longer.
        if (alias != TopicAlias.NONE
                && limit < ConnectionEstablishment.LARGEST_PACKET_SIZE
                && exceeds(message, limit - TOPIC_ALIAS_BYTES)) {
            return TopicAlias.NONE;
        }
        return alias;
    }

    /// The size of the PUBLISH that carries `message`, including its fixed header and its
    /// properties.
    private int wireSize(MqttMessage message) {
        return codec.encodePublishHeader(
                message.topic(),
                message.qos(),
                message.retain(),
                false,
                message.qos() == Qos.AT_MOST_ONCE ? 0 : 1,
                MessageProperties.of(message),
                message.payloadSize()
        )
                .length + message.payloadSize();
    }

    /// The broker did not accept the publication, which ends its exchange: the packet identifier
    /// is free again, and the session does not resume the exchange.
    private void refused(
            Publication publication,
            ReasonCode reason,
            @Nullable String reasonString
    ) {
        removeInflight(publication.packetId);
        if (publication.operation != 0) {
            port.complete(
                    publication.operation,
                    null,
                    new MqttPublicationRefusedException(reason, reasonString)
            );
        }
    }

    private void stageMismatch(String message) {
        // A pending DISCONNECT keeps its write and close sequence. The entry keeps its stage,
        // and the connection end fails its future.
        if (core.state() != State.DISCONNECTING) {
            core.fail(protocolError(message));
        }
    }

    private void completePublication(Inflight exchange, ReasonCode reason) {
        removeInflight(exchange.packetId);
        if (exchange.operation != 0) {
            port.complete(exchange.operation, PublishResult.of(reason), null);
        }
    }

    /// Logs the explanation that the broker sent with a successful acknowledgement, which the
    /// result does not report.
    private void logReason(String packet, int packetId, Properties properties) {
        ConnectionEstablishment.text(properties, PropertyId.REASON_STRING).ifPresent(reason ->
                log.debug("{} {} reason for {}: {}", packet, packetId, clientId, reason));
    }

    private void removeInflight(int packetId) {
        Inflight exchange = inflight.remove(packetId);
        if (exchange != null) {
            core.releasePacketId(packetId);
            if (exchange instanceof Publication publication) {
                bytes -= publication.bytes;
                core.responses().answered(
                        publication.qos == Qos.AT_LEAST_ONCE
                                ? ResponseDeadlines.Kind.PUBACK
                                : ResponseDeadlines.Kind.PUBREC,
                        packetId
                );
            } else {
                core.responses().answered(ResponseDeadlines.Kind.PUBCOMP, packetId);
            }
            window.unsent.remove(exchange);
            window.releaseReplays.remove(exchange);
            window.releases.remove(packetId);
        }
    }

    // The writes and responses of one connection.
    private static final class Window {

        // QoS 0 publications by ticket; they complete when their write completes.
        final Map<Long, Long> publications = new HashMap<>();
        // Exchanges awaiting PUBCOMP whose PUBREL a continued session resends, in PUBREC order.
        final ArrayDeque<Release> releaseReplays = new ArrayDeque<>();
        // At most 64 publications that this connection has yet to send, in session order: those
        // that a continued session resends, then those that wait for room under the broker's
        // Receive Maximum.
        final ArrayDeque<Publication> unsent = new ArrayDeque<>();
        // Set while a queued exchange waits for send capacity.
        boolean stalled;
        // The Topic Aliases that this connection's PUBLISH packets set.
        final OutgoingAliases aliases = new OutgoingAliases();
        // PUBREC responses have their own FIFO: an application's unready incoming
        // acknowledgement must not block its outgoing publication. Coalescing by id bounds even
        // unknown responses to the protocol's 65,535 identifiers.
        final SequencedSet<Integer> releases = new LinkedHashSet<>();
        // Tickets of PUBREC responses admitted to I/O whose write notification is still pending.
        final Set<Long> releaseWrites = new HashSet<>();
        // Unknown exchanges have no future or persistent state, but their PUBCOMP must not land
        // on a new publication using the same id. Each id stays aside until disconnect, or until
        // PUBCOMP has answered every PUBREL admitted for it; the value counts those PUBRELs.
        // A repeated PUBREC can leave two answers in flight, and the first must not free the id.
        final Map<Integer, Integer> unknownReleases = new HashMap<>();
    }

    /// @param awaitingPubAck QoS 1 publications that await PUBACK
    /// @param awaitingPubRec QoS 2 publications that await PUBREC
    /// @param awaitingPubComp QoS 2 exchanges that await PUBCOMP
    /// @param publications all unfinished exchanges, against their limit
    /// @param keptBytes the bytes that unfinished publications keep, against their limit
    record Values(
            long awaitingPubAck,
            long awaitingPubRec,
            long awaitingPubComp,
            Usage publications,
            Usage keptBytes
    ) {}

    private abstract static sealed class Inflight {

        final int packetId;
        long operation;

        Inflight(int packetId, long operation) {
            this.packetId = packetId;
            this.operation = operation;
        }
    }

    private static final class Publication extends Inflight {

        final Qos qos;
        // A persistent session keeps the PUBLISH until PUBACK or PUBREC so that it can resend
        // it; a clean one keeps it only until it is sent. The byte budget counts what is kept.
        @Nullable MqttMessage message;
        int bytes;
        final @Nullable SendReceipt receipt;

        Publication(
                int packetId,
                long operation,
                Qos qos,
                @Nullable MqttMessage message,
                int bytes
        ) {
            super(packetId, operation);
            this.qos = qos;
            this.message = message;
            this.bytes = bytes;
            receipt = message == null ? null : new SendReceipt();
        }
    }

    private static final class Release extends Inflight {

        // The reason code of the PUBREC that started the release, which the result reports.
        final ReasonCode reasonCode;

        Release(int packetId, long operation, ReasonCode reasonCode) {
            super(packetId, operation);
            this.reasonCode = reasonCode;
        }
    }
}
