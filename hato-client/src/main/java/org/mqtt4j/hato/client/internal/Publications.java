package org.mqtt4j.hato.client.internal;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.SequencedMap;
import java.util.SequencedSet;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttNotConnectedException;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttPublishException;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.SessionCore.State;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;

import static java.util.Objects.requireNonNull;
import static org.mqtt4j.hato.client.internal.SessionCore.isFailure;
import static org.mqtt4j.hato.client.internal.SessionCore.protocolError;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// The session core's part for outgoing publications. It keeps the session's QoS 1 and QoS 2
/// exchanges, replays them on a continued session, answers PUBREC with PUBREL, and decides each
/// publication's future when its exchange or its connection ends. The core decides when queued
/// work may use send capacity; this part reaches the rest of the core only through
/// [SessionCore.Context].
final class Publications {

    private static final Log log = Log.of(Publications.class);

    private static final int INFLIGHT_LIMIT = 64;

    private static final long INFLIGHT_BYTE_LIMIT = 32L * 1024 * 1024;

    // A broker can repeat PUBREC at will, so PUBREC responses bound their completion events in
    // the same way. Replayed PUBRELs are already bounded by the session's exchanges.
    private static final int RELEASE_WRITE_LIMIT = 16;

    private final SessionCore.Port port;

    private final SessionCore.Context core;

    private final String clientId;

    private final boolean cleanStart;

    // The connection's codec, which sizes the retained PUBLISH of a persistent session.
    private final PacketCodec codec;

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
            PacketCodec codec
    ) {
        this.port = port;
        this.core = core;
        this.clientId = clientId;
        this.cleanStart = cleanStart;
        this.codec = codec;
    }

    void publish(long operation, MqttMessage message) {
        if (!ready(operation)) {
            return;
        }
        // Replays precede subscription restoration; SUBACK waiting does not block publication.
        if (!window.replays.isEmpty()) {
            capacityFailure(operation, "Publication replay pending");
            return;
        }
        long ticket = core.nextTicket();
        int packetId = 0;
        SendReceipt receipt = null;
        if (message.qos() != Qos.AT_MOST_ONCE) {
            if (inflight.size() >= INFLIGHT_LIMIT) {
                capacityFailure(operation, "Publication capacity");
                return;
            }
            int byteCount = 0;
            if (!cleanStart) {
                long remaining = INFLIGHT_BYTE_LIMIT - bytes;
                if (message.payloadSize() > remaining) {
                    capacityFailure(operation, "Publication byte capacity");
                    return;
                }
                byteCount = codec.encodePublishHeader(
                        message.topic(),
                        message.qos(),
                        message.retain(),
                        false,
                        1,
                        EMPTY,
                        message.payloadSize()
                )
                        .length + message.payloadSize();
                if (byteCount > remaining) {
                    capacityFailure(operation, "Publication byte capacity");
                    return;
                }
            }
            OptionalInt allocated = core.allocatePacketId();
            if (allocated.isEmpty()) {
                capacityFailure(operation, "Packet identifier capacity");
                return;
            }
            packetId = allocated.getAsInt();
            var publication = new Publication(
                    packetId,
                    operation,
                    message.qos(),
                    cleanStart ? null : message,
                    byteCount
            );
            inflight.put(packetId, publication);
            bytes += byteCount;
            receipt = publication.receipt;
        } else {
            window.publications.put(ticket, operation);
        }
        SendQueue.OfferResult result = port.publish(
                core.generation(),
                ticket,
                message,
                packetId,
                receipt
        );
        if (result == SendQueue.OfferResult.ACCEPTED) {
            awaitPublication(ticket, message.qos(), packetId);
        } else if (result == SendQueue.OfferResult.FULL) {
            if (packetId == 0) {
                window.publications.remove(ticket);
            } else {
                removeInflight(packetId);
            }
            capacityFailure(operation, "Send queue capacity");
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
            case Publication publication when isFailure(ack.reasonCode()) ->
                    refused(publication, ack.reasonCode());
            case Publication publication -> completePublication(publication);
        }
    }

    void release(Packet.PubRec ack) {
        int packetId = ack.packetId();
        Inflight exchange = inflight.get(packetId);
        if (isFailure(ack.reasonCode())) {
            refusedRelease(exchange, ack.reasonCode());
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
                window.replays.remove(publication);
                core.responses().answered(ResponseDeadlines.Kind.PUBREC, packetId);
                inflight.putLast(packetId, new Release(packetId, publication.operation));
            }
            case Release pending -> {
                // A PUBREL replay still waiting for capacity also answers this duplicate. The
                // scan is bounded by the 64 exchanges; index it only if that limit grows.
                if (window.replays.contains(pending)) {
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
        if (!(exchange instanceof Release)) {
            stageMismatch("PUBCOMP does not match the outgoing QoS stage");
            return;
        }
        completePublication(exchange);
    }

    /// Queues the session's exchanges for replay on a connection that continues the session.
    void resume() {
        window.replays.addAll(inflight.values());
    }

    /// Replays the session's exchanges in their order and returns whether every replay was
    /// admitted; while one waits, the core sends no other queued work. A duplicate PUBREC shares
    /// a PUBREL replay still waiting here instead of adding another response.
    boolean replay() {
        Inflight next;
        while ((next = window.replays.peekFirst()) != null) {
            if (inflight.get(next.packetId) != next) {
                window.replays.removeFirst();
                continue;
            }
            long ticket = core.nextTicket();
            SendQueue.OfferResult result = switch (next) {
                case Publication publication -> port.publish(
                        core.generation(),
                        ticket,
                        requireNonNull(publication.message),
                        publication.packetId,
                        requireNonNull(publication.receipt)
                );
                case Release release -> sendRelease(ticket, release.packetId);
            };
            if (result != SendQueue.OfferResult.ACCEPTED) {
                return false;
            }
            if (next instanceof Publication publication) {
                awaitPublication(ticket, publication.qos, publication.packetId);
            }
            window.replays.removeFirst();
        }
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

    private void capacityFailure(long operation, String message) {
        port.complete(operation, null, new MqttCapacityException(message));
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
    private void refusedRelease(@Nullable Inflight exchange, ReasonCode reason) {
        switch (exchange) {
            case Publication publication when publication.qos == Qos.EXACTLY_ONCE ->
                    refused(publication, reason);
            case Publication _ -> stageMismatch("PUBREC does not match the outgoing QoS stage");
            case Release _ -> {
            }
            case null -> {
            }
        }
    }

    /// The broker did not accept the publication, which ends its exchange: the packet identifier
    /// is free again, and the session does not resume the exchange.
    private void refused(Publication publication, ReasonCode reason) {
        removeInflight(publication.packetId);
        if (publication.operation != 0) {
            port.complete(publication.operation, null, new MqttPublishException(reason));
        }
    }

    private void stageMismatch(String message) {
        // A pending DISCONNECT keeps its write and close sequence. The entry keeps its stage,
        // and the connection end fails its future.
        if (core.state() != State.DISCONNECTING) {
            core.fail(protocolError(message));
        }
    }

    private void completePublication(Inflight exchange) {
        removeInflight(exchange.packetId);
        if (exchange.operation != 0) {
            port.complete(exchange.operation, PublishResult.of(), null);
        }
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
            window.replays.remove(exchange);
            window.releases.remove(packetId);
        }
    }

    // The writes and responses of one connection.
    private static final class Window {

        // QoS 0 publications by ticket; they complete when their write completes.
        final Map<Long, Long> publications = new HashMap<>();
        // At most 64 retained publications await queue admission, in the session's order.
        final ArrayDeque<Inflight> replays = new ArrayDeque<>();
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
        final @Nullable MqttMessage message;
        final int bytes;
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

        Release(int packetId, long operation) {
            super(packetId, operation);
        }
    }
}
