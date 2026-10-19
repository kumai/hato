package org.mqtt4j.hato.client.internal;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.AckSequencer.Acknowledgement;
import org.mqtt4j.hato.client.internal.AckSequencer.Kind;
import org.mqtt4j.hato.client.internal.ReceiveTable.Stage;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.log.Log;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

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

    // The session's receptions; a new session replaces them.
    private ReceiveTable received = new ReceiveTable(ACKNOWLEDGEMENT_LIMIT);

    // The connection's acknowledgement order; a connection end replaces it.
    private AckSequencer acknowledgements = new AckSequencer();

    private long nextDelivery;

    private boolean reusedIdentifierLogged;

    Receptions(SessionCore.Port port, SessionCore.Context core, String clientId) {
        this.port = port;
        this.core = core;
        this.clientId = clientId;
    }

    void deliver(Packet.Publish publish, int bytes) {
        long token = 0;
        boolean continued = false;
        if (publish.qos() != Qos.AT_MOST_ONCE) {
            int packetId = publish.packetId();
            Kind kind = switch (publish.qos()) {
                case AT_MOST_ONCE -> throw new IllegalStateException("QoS 0 is not acknowledged");
                case AT_LEAST_ONCE -> Kind.PUBACK;
                case EXACTLY_ONCE -> Kind.PUBREC;
            };
            Optional<ReceiveTable.Entry> existing = received.find(packetId);
            if (existing.isPresent()) {
                ReceiveTable.Entry earlier = existing.orElseThrow();
                earlier.observeReceipt();
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
                        publish.qos()
                );
                if (added.isEmpty()) {
                    core.fail(new MqttCapacityException("Acknowledgement capacity"));
                    return;
                }
                acknowledgements.add(added.orElseThrow(), kind);
            }
        }
        List<Long> matches = core.matching(publish.topic());
        if (matches.isEmpty() && !continued) {
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
        var message = MqttMessage.of(
                publish.topic(),
                publish.payload(),
                publish.qos(),
                publish.retain()
        );
        if (!port.deliver(
                new DeliveryToken(core.sessionNumber(), core.generation(), core.attempt(), token),
                matches,
                message,
                publish.dup(),
                bytes
        )) {
            core.fail(new MqttCapacityException("Delivery capacity"));
        } else if (continued) {
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
                entry.pubrecAccepted();
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
                    Qos.EXACTLY_ONCE
            );
            if (added.isEmpty()) {
                core.fail(new MqttCapacityException("Acknowledgement capacity"));
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
                entry.pubrecAccepted();
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
            entry.submitted(receipt);
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
            boolean accepted = entry.observeReceipt();
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

    /// A permanent or nonpersistent end keeps no reception.
    void clear() {
        received.clear();
    }

    void connectionEnded() {
        acknowledgements = new AckSequencer();
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
}
