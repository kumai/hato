package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ClientStats.OverloadCause;
import org.mqtt4j.hato.ClientStats.Usage;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.AckSequencer.Acknowledgement;
import org.mqtt4j.hato.client.internal.AckSequencer.Kind;
import org.mqtt4j.hato.client.internal.ReceiveTable.Stage;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;

import static org.mqtt4j.hato.client.internal.SessionCore.protocolError;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// The session core's part for incoming publications. It hands deliveries to the shell, keeps
/// the session's receive table, and sends PUBACK, PUBREC, and PUBCOMP in receive order. The core
/// decides when the connection accepts input and when acknowledgements may use the reserved send
/// quota; this part reaches the rest of the core only through [SessionCore.Context].
final class Receptions {

    private static final Log log = Log.of(Receptions.class);

    private static final int ACKNOWLEDGEMENT_LIMIT = 4096;

    // Bound queued completion events even when I/O returns capacity during an actor drain.
    private static final int ACK_WRITE_LIMIT = 16;

    private final SessionCore.Port port;

    private final SessionCore.Context core;

    private final String clientId;

    // Whether CONNECT announces a Receive Maximum that the broker must stay within: MQTT 5.0.
    private final boolean receiveMaximumAnnounced;

    // The session's receptions; a new session replaces them.
    private ReceiveTable received = new ReceiveTable(ACKNOWLEDGEMENT_LIMIT);

    // The connection's acknowledgement order; a connection end replaces it.
    private AckSequencer acknowledgements = new AckSequencer();

    // The Receive Maximum that the current connection's CONNECT announced.
    private int receiveMaximum;

    // The packet identifiers of the QoS 1 and QoS 2 publications that the current connection
    // delivered and whose PUBACK or PUBCOMP the send queue has not accepted yet: the broker's
    // send quota in use (MQTT 5.0 section 4.9), each mapped to TRUE. A connection end replaces
    // them.
    private LongMap<Boolean> quota = new LongMap<>();

    private long nextDelivery;

    private boolean reusedIdentifierLogged;

    // Lifetime counts: PUBLISH packets by QoS level, and publications that matched no
    // subscription.
    private final long[] publishes = new long[Qos.values().length];

    private long unmatched;

    Receptions(
            SessionCore.Port port,
            SessionCore.Context core,
            String clientId,
            boolean receiveMaximumAnnounced
    ) {
        this.port = port;
        this.core = core;
        this.clientId = clientId;
        this.receiveMaximumAnnounced = receiveMaximumAnnounced;
    }

    /// The Receive Maximum that the next CONNECT announces, which the receptions of its
    /// connection must stay within: the room left in the session's receive table, less the
    /// acknowledgements that can await their write, and at least 1.
    int announceReceiveMaximum() {
        // Receptions that a continued session carries over can still await PUBREL, and the
        // broker's new quota does not count them (MQTT 5.0 section 4.9). An acknowledgement that
        // the send queue accepted returns the broker's quota as soon as the broker receives it,
        // but holds its reception until its write completion reaches the actor, which can come
        // after the broker's next publication. Announcing only the room left for both keeps a
        // conforming broker within the receive table.
        receiveMaximum = Math.max(1, received.room() - ACK_WRITE_LIMIT);
        return receiveMaximum;
    }

    /// `receivedAt` is when the transport handed `publish` to the client, or
    /// [DeliveryToken#UNTIMED].
    void deliver(Packet.Publish publish, int bytes, long receivedAt) {
        publishes[publish.qos().ordinal()]++;
        long token = 0;
        boolean continued = false;
        if (publish.qos() != Qos.AT_MOST_ONCE) {
            int packetId = publish.packetId();
            // A broker that sends more than the announced number is in violation, even when the
            // receive table could still hold the publication.
            if (receiveMaximumAnnounced
                    && quota.put(packetId, Boolean.TRUE) == null
                    && quota.size() > receiveMaximum) {
                core.fail(new PacketDecodeException(
                        ReasonCode.RECEIVE_MAXIMUM_EXCEEDED,
                        "More unacknowledged QoS 1 and QoS 2 publications than "
                                + receiveMaximum
                ));
                return;
            }
            Kind kind = switch (publish.qos()) {
                case AT_MOST_ONCE -> throw new IllegalStateException("QoS 0 is not acknowledged");
                case AT_LEAST_ONCE -> Kind.PUBACK;
                case EXACTLY_ONCE -> Kind.PUBREC;
            };
            Optional<ReceiveTable.Entry> existing = received.find(packetId);
            if (existing.isPresent()) {
                ReceiveTable.Entry earlier = existing.orElseThrow();
                earlier.observeReceipt(core.now());
                // QoS 2 joins its unfinished reception until PUBREL even without DUP, while
                // QoS 1 joins only a DUP retransmission. Neither may change the id's QoS.
                if (earlier.qos() == Qos.EXACTLY_ONCE || publish.dup()) {
                    if (publish.qos() != earlier.qos()) {
                        core.fail(protocolError("PUBLISH changes an unfinished id's QoS"));
                        return;
                    }
                    if (earlier.stage() == Stage.WAITING_PUBREL) {
                        // A repeated PUBLISH is answered by a new or still pending PUBREC, but
                        // is never delivered again.
                        acknowledgements.add(earlier, Kind.PUBREC);
                        core.sendCapacityAvailable();
                        return;
                    }
                    if (acknowledgements.contains(earlier, kind)) {
                        return;
                    }
                    // A continued reception joins this connection only when its resend arrives.
                    token = earlier.token();
                    continued = true;
                    acknowledgements.add(earlier, kind);
                } else {
                    received.supersede(packetId);
                    if (acknowledgements.remove(earlier, Kind.PUBACK)
                            || !acknowledgements.contains(earlier, Kind.PUBACK)) {
                        received.remove(earlier);
                        withdrawAcknowledgement(packetId, earlier.token());
                    }
                }
            }
            if (token == 0) {
                token = ++nextDelivery;
                Optional<ReceiveTable.Entry> added = received.add(
                        token,
                        packetId,
                        publish.qos(),
                        core.now()
                );
                if (added.isEmpty()) {
                    core.overloaded(
                            OverloadCause.RECEPTIONS,
                            new MqttCapacityException("Acknowledgement capacity")
                    );
                    return;
                }
                acknowledgements.add(added.orElseThrow(), kind);
            }
        }
        List<Long> matches = core.matching(publish);
        if (matches.isEmpty() && !continued) {
            unmatched++;
            if (token != 0) {
                // A reception that no subscription takes is ready for its acknowledgement now.
                received.ready(token);
                core.sendCapacityAvailable();
            } else {
                log.debug("Discarding unmatched QoS 0 delivery for {} on {}",
                        publish.topic(), clientId);
            }
            return;
        }
        if (!port.deliver(
                new DeliveryToken(
                        core.sessionNumber(),
                        core.generation(),
                        core.attempt(),
                        token,
                        receivedAt
                ),
                matches,
                publish,
                bytes
        )) {
            core.overloaded(
                    OverloadCause.DELIVERIES,
                    new MqttCapacityException("Delivery capacity")
            );
            return;
        }
        if (continued) {
            received.ready(token, port.readyDelivery(token));
            core.sendCapacityAvailable();
        }
    }

    void released(int packetId) {
        Optional<ReceiveTable.Entry> existing = received.find(packetId);
        ReceiveTable.Entry entry;
        if (existing.isPresent()) {
            entry = existing.orElseThrow();
            if (entry.qos() != Qos.EXACTLY_ONCE) {
                core.fail(protocolError("PUBREL targets an unfinished QoS 1 id"));
                return;
            }
            SendReceipt receipt = entry.receipt();
            if (receipt != null && receipt.started()) {
                // The peer's PUBREL proves PUBREC arrived, even when Transport.send has not
                // returned ACCEPTED or reported the write. A queue-only receipt is not enough.
                entry.pubrecAccepted(core.now());
            }
            if (entry.stage() != Stage.WAITING_PUBREL) {
                core.fail(protocolError("PUBREL precedes PUBREC for the received id"));
                return;
            }
            // A PUBREL makes queued duplicate PUBRECs obsolete. Writes already admitted keep
            // their immutable kind and capacity until their own completion notification.
            acknowledgements.remove(entry, Kind.PUBREC);
            port.forgetDelivery(entry.token());
        } else if (acknowledgements.completing(packetId)) {
            return;
        } else {
            // An unknown PUBREL owns response capacity without occupying its packet identifier.
            Optional<ReceiveTable.Entry> added = received.add(
                    ++nextDelivery,
                    packetId,
                    Qos.EXACTLY_ONCE,
                    core.now()
            );
            if (added.isEmpty()) {
                core.overloaded(
                        OverloadCause.RECEPTIONS,
                        new MqttCapacityException("Acknowledgement capacity")
                );
                return;
            }
            entry = added.orElseThrow();
        }
        received.complete(entry);
        acknowledgements.add(entry, Kind.PUBCOMP);
        core.sendCapacityAvailable();
    }

    /// Write completion retires PUBACK/PUBCOMP, while PUBREC retains the id for PUBREL.
    /// The immutable acknowledgement kind keeps an old PUBREC write from retiring a later stage.
    void written(long ticket) {
        acknowledgements.written(ticket).ifPresent(acknowledgement -> {
            ReceiveTable.Entry entry = acknowledgement.entry();
            if (acknowledgement.kind() == Kind.PUBREC) {
                entry.pubrecAccepted(core.now());
            } else {
                received.remove(entry);
            }
            port.forgetDelivery(entry.token());
        });
    }

    /// Tokens belong to the client lifetime; only entries still in the receive table can advance.
    void ready(List<Long> tokens) {
        tokens.forEach(received::ready);
    }

    /// Sends ready acknowledgements in receive order while the reserved quota admits them.
    void sendAcknowledgements() {
        Optional<Acknowledgement> ready;
        while (acknowledgements.writingCount() < ACK_WRITE_LIMIT
                && (ready = acknowledgements.nextReady()).isPresent()) {
            Acknowledgement acknowledgement = ready.orElseThrow();
            ReceiveTable.Entry entry = acknowledgement.entry();
            long ticket = core.nextTicket();
            var receipt = new SendReceipt();
            Packet packet = switch (acknowledgement.kind()) {
                case PUBACK -> new Packet.PubAck(entry.packetId(), ReasonCode.SUCCESS, EMPTY);
                case PUBREC -> new Packet.PubRec(entry.packetId(), ReasonCode.SUCCESS, EMPTY);
                case PUBCOMP -> new Packet.PubComp(entry.packetId(), ReasonCode.SUCCESS, EMPTY);
            };
            if (port.send(
                    core.generation(),
                    ticket,
                    packet,
                    receipt
            )
                    != SendQueue.OfferResult.ACCEPTED) {
                // FULL waits for actual budget release; CLOSED waits for the original cause.
                // Work in the regular quota, such as closures and re-creation, still runs.
                return;
            }
            core.acknowledgementAdmitted();
            acknowledgements.submitted(ticket);
            entry.submitted(receipt, core.now());
            if (acknowledgement.kind() != Kind.PUBREC) {
                // PUBACK and PUBCOMP end the exchange, which returns the broker's send quota.
                quota.remove(entry.packetId());
            }
            if (entry.stage() == Stage.RECEIVING) {
                port.releaseDelivery(entry.token());
            }
        }
    }

    /// `handedReadiness` follows [SessionCore.Port#receiveState()]: receptions it omits reached
    /// no recipient.
    void reconcile(Map<Long, Boolean> handedReadiness, boolean ending) {
        for (ReceiveTable.Entry entry : received.entries()) {
            // The sender can accept a PUBREC during this pass. Judge it by one read, so a QoS 2
            // reception that missed WAITING_PUBREL is not dropped by the QoS 1 rule below.
            boolean accepted = entry.observeReceipt(core.now());
            SendReceipt receipt = entry.receipt();
            Boolean ready = handedReadiness.get(entry.token());
            if (entry.stage() == Stage.WAITING_PUBREL) {
                port.forgetDelivery(entry.token());
            } else if (ending && entry.qos() == Qos.EXACTLY_ONCE
                    && entry.stage() == Stage.RECEIVING && receipt != null && ready == null) {
                // Inline closure can precede Transport.send returning ACCEPTED. Retain this
                // receipt until CONNACK can settle whether PUBREC made the exchange persistent:
                // the single I/O sender returns from this send before it writes the next CONNECT.
                port.forgetDelivery(entry.token());
            } else if (entry.stage() == Stage.COMPLETING || accepted || ready == null
                    || received.find(entry.packetId()).orElse(null) != entry) {
                received.remove(entry);
                port.forgetDelivery(entry.token());
            } else {
                received.ready(entry.token(), ready);
            }
        }
    }

    /// The receptions' part of a statistics snapshot at `now` on the client's clock. Each
    /// reception counts in the one stage that the actor has processed it to.
    Values values(long now) {
        long conditions = 0;
        long turn = 0;
        long writing = 0;
        long release = 0;
        long completing = 0;
        // The longest waits so far, or -1 before any.
        long longestConditions = -1;
        long longestRelease = -1;
        List<ReceiveTable.Entry> entries = received.entries();
        for (ReceiveTable.Entry entry : entries) {
            switch (entry.stage()) {
                case RECEIVING -> {
                    if (entry.receipt() != null) {
                        writing++;
                    } else if (entry.ready()) {
                        turn++;
                    } else {
                        conditions++;
                        longestConditions = Math.max(longestConditions, now - entry.receivedAt());
                    }
                }
                case WAITING_PUBREL -> {
                    release++;
                    longestRelease = Math.max(
                            longestRelease,
                            now - entry.awaitingReleaseSince()
                    );
                }
                case COMPLETING -> completing++;
            }
        }
        return new Values(
                ClientSnapshot.counts(publishes, Qos.class),
                unmatched,
                conditions,
                turn,
                writing,
                release,
                completing,
                new Usage(entries.size(), received.capacity()),
                longest(longestConditions),
                longest(longestRelease)
        );
    }

    /// The longest wait in `nanos`, or null when no reception waited.
    private static @Nullable Duration longest(long nanos) {
        return nanos < 0 ? null : Duration.ofNanos(nanos);
    }

    /// A permanent or nonpersistent end keeps no reception.
    void clear() {
        received.clear();
    }

    void connectionEnded() {
        acknowledgements = new AckSequencer();
        quota = new LongMap<>();
    }

    void sessionDiscarded() {
        received = new ReceiveTable(ACKNOWLEDGEMENT_LIMIT);
    }

    /// The earlier QoS 1 reception was a stale retransmission, or the id was reused without DUP
    /// for another publication. Either way the new reception's PUBACK or PUBREC covers the
    /// broker's exchange, so the earlier one only releases its local delivery and stops holding
    /// back later acknowledgements.
    private void withdrawAcknowledgement(int packetId, long token) {
        if (!reusedIdentifierLogged) {
            reusedIdentifierLogged = true;
            log.warn("Packet identifier {} was reused without DUP before its PUBACK on {}; "
                    + "the earlier reception is not acknowledged", packetId, clientId);
        }
        port.releaseDelivery(token);
        port.forgetDelivery(token);
        core.sendCapacityAvailable();
    }

    /// The receptions' values in [ClientSnapshot.Receiving] that the core knows.
    record Values(
            Map<Qos, Long> publishes,
            long unmatched,
            long awaitingConditions,
            long awaitingTurn,
            long awaitingAcknowledgementWrite,
            long awaitingRelease,
            long awaitingCompletionWrite,
            Usage receptions,
            @Nullable Duration oldestAwaitingConditions,
            @Nullable Duration oldestAwaitingRelease
    ) {}
}
