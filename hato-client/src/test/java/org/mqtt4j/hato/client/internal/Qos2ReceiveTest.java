package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.ReceivePersistenceTest.RecordingPort;
import org.mqtt4j.hato.client.internal.ReceivePersistenceTest.Sent;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.DISCONNECTED;
import static org.mqtt4j.hato.client.internal.common.io.SendQueue.OfferResult.ACCEPTED;
import static org.mqtt4j.hato.client.internal.common.io.SendQueue.OfferResult.FULL;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class Qos2ReceiveTest {

    private final RecordingPort port = new RecordingPort();
    private final SessionCore sut = new SessionCore(
            port,
            "client",
            Duration.ofSeconds(30),
            Duration.ZERO,
            MqttVersion.V3_1_1,
            Duration.ofHours(1),
            16 * 1024 * 1024,
            null,
            false,
            ReconnectBackoff.disabled(),
            RetryRules.DEFAULT,
            Duration.ofSeconds(60)
    );
    private long nextOperation;
    private long generation;
    private boolean subscribed;

    @Test
    void a_qos_two_reception_sends_pubrec_then_pubcomp_and_releases_delivery_at_queue_admission() {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken delivery = port.deliveries.getLast();
        assertThat(acknowledgements()).isEmpty();

        // Act
        ready(delivery);
        Sent received = lastAcknowledgement();

        // Assert
        assertThat(received.packet()).isEqualTo(pubRec(7));
        assertThat(port.released).containsExactly(delivery.acknowledgement());
        assertThat(requireNonNull(received.receipt()).accepted()).isFalse();

        // Act
        requireNonNull(received.receipt()).markAccepted();
        receive(pubRel(7));
        Sent completed = lastAcknowledgement();
        sut.written(generation, received.ticket());
        sut.written(generation, completed.ticket());

        // Assert
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7));
        assertThat(port.released).containsExactly(delivery.acknowledgement());
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "duplicate PUBLISH has DUP={0}")
    @ValueSource(booleans = {false, true})
    void repeats_merge_before_pubrec_and_reply_without_redelivery_after_pubrec(boolean dup) {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken delivery = port.deliveries.getLast();

        // Act: even a repetition without DUP belongs to this unfinished QoS 2 reception.
        receive(publish(7, Qos.EXACTLY_ONCE, dup));
        ready(delivery);
        Sent first = lastAcknowledgement();
        requireNonNull(first.receipt()).markAccepted();
        port.ackAdmission = FULL;
        receive(publish(7, Qos.EXACTLY_ONCE, dup));
        receive(publish(7, Qos.EXACTLY_ONCE, dup));
        port.ackAdmission = ACCEPTED;
        sut.sendCapacityAvailable(generation);

        // Assert: pending repeated responses merge while the application still sees one view.
        assertThat(port.deliveries).containsExactly(delivery);
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubRec(7));
        assertThat(port.released).containsExactly(delivery.acknowledgement());
    }

    @Test
    void pubrel_withdraws_a_repeated_pubrec_still_waiting_for_admission() {
        // Arrange: the first PUBREC reached the transport; a repetition's PUBREC waits for room.
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        ready(port.deliveries.getLast());
        requireNonNull(lastAcknowledgement().receipt()).markAccepted();
        port.ackAdmission = FULL;
        receive(publish(7, Qos.EXACTLY_ONCE, true));

        // Act
        receive(pubRel(7));
        port.ackAdmission = ACCEPTED;
        sut.sendCapacityAvailable(generation);

        // Assert
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void pubcomp_passes_an_unready_head_while_puback_and_pubrec_keep_receive_order() {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        ready(port.deliveries.getLast());
        requireNonNull(lastAcknowledgement().receipt()).markAccepted();
        receive(publish(8, Qos.AT_LEAST_ONCE, false));
        DeliveryToken earlier = port.deliveries.getLast();
        receive(publish(9, Qos.EXACTLY_ONCE, false));
        ready(port.deliveries.getLast());

        // Act: PUBREL is ready, but the preceding incoming PUBLISH is not.
        receive(pubRel(7));

        // Assert: completing the exchange frees the broker's inflight window at once.
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7));

        // Act
        ready(earlier);

        // Assert
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7), pubAck(8), pubRec(9));
    }

    @Test
    void repeated_pubrels_share_one_unwritten_pubcomp_without_using_receive_capacity() {
        // Arrange
        connect(false);
        port.ackAdmission = FULL;

        // Act: more repetitions than the receive table can hold arrive before PUBCOMP leaves.
        for (int repetition = 0; repetition <= 4096; repetition++) {
            receive(pubRel(7));
        }
        port.ackAdmission = ACCEPTED;
        sut.sendCapacityAvailable(generation);

        // Assert
        assertThat(port.ends).isEmpty();
        assertThat(acknowledgements()).containsExactly(pubComp(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void pubrel_frees_the_id_before_pubcomp_and_old_write_notifications_do_not_retire_its_reuse() {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        ready(port.deliveries.getLast());
        Sent received = lastAcknowledgement();
        requireNonNull(received.receipt()).markAccepted();
        receive(pubRel(7));
        Sent completed = lastAcknowledgement();

        // Act: the broker may reuse its id before the PUBCOMP write callback reaches the actor.
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken current = port.deliveries.getLast();
        sut.written(generation, received.ticket());
        sut.written(generation, completed.ticket());
        ready(current);

        // Assert
        assertThat(port.deliveries).hasSize(2);
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7), pubRec(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void an_unknown_pubrel_is_answered_and_does_not_occupy_the_id() {
        // Arrange
        connect(false);

        // Act
        receive(pubRel(7));
        receive(pubRel(7));
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        ready(port.deliveries.getLast());

        // Assert
        assertThat(acknowledgements()).containsExactly(pubComp(7), pubComp(7), pubRec(7));
        assertThat(port.deliveries).hasSize(1);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void an_unmatched_qos_two_publication_completes_without_an_application_delivery() {
        // Arrange: the only subscription matches "data".
        connect(false);
        var unmatched = new Packet.Publish(
                new Topic("other"),
                new byte[] {1},
                Qos.EXACTLY_ONCE,
                false,
                false,
                7,
                EMPTY
        );

        // Act
        receive(unmatched);
        requireNonNull(lastAcknowledgement().receipt()).markAccepted();
        receive(pubRel(7));

        // Assert
        assertThat(port.deliveries).isEmpty();
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "occupied {0}, next {1}")
    @MethodSource("qosChanges")
    void changing_an_unfinished_receivers_qos_is_a_protocol_error(Qos first, Packet next) {
        // Arrange
        connect(false);
        receive(publish(7, first, false));

        // Act
        receive(next);

        // Assert
        assertThat(port.deliveries).hasSize(1);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class));
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void a_qos_two_publish_without_dup_replaces_an_unacknowledged_qos_one_reception() {
        // Arrange
        connect(false);
        receive(publish(7, Qos.AT_LEAST_ONCE, false));
        DeliveryToken old = port.deliveries.getLast();

        // Act
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken current = port.deliveries.getLast();
        ready(old);
        ready(current);

        // Assert
        assertThat(acknowledgements()).containsExactly(pubRec(7));
        assertThat(port.deliveries).hasSize(2);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void pubrel_while_pubrec_is_only_queued_is_a_protocol_error() {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        ready(port.deliveries.getLast());

        // Act: PUBREC has entered the queue, but Transport.send has not started.
        receive(pubRel(7));

        // Assert
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class));
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void a_resumed_session_answers_pubrel_without_delivering_the_publication_again() {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken original = port.deliveries.getLast();
        ready(original);
        Sent received = lastAcknowledgement();
        sut.written(generation, received.ticket());
        sut.failed(generation, new IOException("lost before PUBREL"));

        // Act: the continued broker can start with PUBREL, without sending PUBLISH again.
        connect(true);
        receive(pubRel(7));

        // Assert
        assertThat(port.deliveries).containsExactly(original);
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "repeated PUBLISH has DUP={0}")
    @ValueSource(booleans = {false, true})
    void a_written_pubrec_prevents_repeated_publish_delivery_after_reconnect(boolean dup) {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken original = port.deliveries.getLast();
        ready(original);
        Sent received = lastAcknowledgement();
        sut.written(generation, received.ticket());
        assertThat(port.handed).isEmpty();
        sut.failed(generation, new IOException("lost after PUBREC was written"));

        // Act
        connect(true);
        receive(publish(7, Qos.EXACTLY_ONCE, dup));

        // Assert
        assertThat(port.deliveries).containsExactly(original);
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubRec(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void a_pubrec_accepted_after_inline_close_still_prevents_redelivery() {
        // Arrange: a stopped handler left no valid recipient when inline closure arrived.
        Sent received = loseConnectionDuringPubrec();
        requireNonNull(received.receipt()).markAccepted();

        // Act
        connect(true);
        receive(publish(7, Qos.EXACTLY_ONCE, true));

        // Assert
        assertThat(port.deliveries).hasSize(1);
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubRec(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void a_pubrec_accepted_while_the_connection_end_is_reconciled_prevents_redelivery() {
        // Arrange: the handler finished, and the PUBREC send is still returning from I/O.
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken original = port.deliveries.getLast();
        ready(original);
        SendReceipt receipt = requireNonNull(lastAcknowledgement().receipt());
        receipt.markStarted();
        port.duringEndReconcile = receipt::markAccepted;
        sut.failed(generation, new IOException("ended while PUBREC was being accepted"));
        port.duringEndReconcile = () -> {
        };

        // Act
        connect(true);
        receive(publish(7, Qos.EXACTLY_ONCE, true));

        // Assert
        assertThat(port.deliveries).containsExactly(original);
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubRec(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void an_unaccepted_pubrec_after_inline_close_allows_the_reception_to_start_again() {
        // Arrange
        loseConnectionDuringPubrec();

        // Act
        connect(true);
        receive(publish(7, Qos.EXACTLY_ONCE, true));

        // Assert
        assertThat(port.deliveries).hasSize(2);
        assertThat(acknowledgements()).containsExactly(pubRec(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void an_unfinished_reception_waits_for_its_retransmission_before_sending_pubrec() {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken original = port.deliveries.getLast();
        sut.failed(generation, new IOException("lost before PUBREC"));
        ready(original);

        // Act
        connect(true);
        assertThat(acknowledgements()).isEmpty();
        receive(publish(7, Qos.EXACTLY_ONCE, true));

        // Assert: the shell joins the existing token instead of creating another reception.
        assertThat(port.deliveries).extracting(DeliveryToken::acknowledgement)
                .containsOnly(original.acknowledgement());
        assertThat(acknowledgements()).containsExactly(pubRec(7));
    }

    @Test
    void a_missing_session_drops_a_pubrec_wait_and_old_ack_and_write_events_are_inert() {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken old = port.deliveries.getLast();
        ready(old);
        Sent received = lastAcknowledgement();
        requireNonNull(received.receipt()).markAccepted();
        sut.failed(generation, new IOException("session lost"));

        // Act
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken current = port.deliveries.getLast();
        ready(old);
        sut.written(received.generation(), received.ticket());
        ready(current);

        // Assert
        assertThat(current.acknowledgement()).isNotEqualTo(old.acknowledgement());
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubRec(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void pubrel_during_disconnect_is_not_answered_until_the_session_continues() {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        ready(port.deliveries.getLast());
        requireNonNull(lastAcknowledgement().receipt()).markAccepted();
        sut.disconnect(++nextOperation);
        Sent disconnect = port.sent.getLast();

        // Act
        receive(pubRel(7));
        assertThat(acknowledgements()).containsExactly(pubRec(7));
        sut.sent(generation, disconnect.ticket(), 0);
        connect(true);
        receive(pubRel(7));

        // Assert
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void pubrel_waits_keep_receive_capacity_after_pubrec_is_written() {
        // Arrange
        fillPubrelWaits();

        // Act
        receive(publish(4097, Qos.EXACTLY_ONCE, false));

        // Assert
        assertThat(port.deliveries).hasSize(4096);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class));
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void unwritten_pubcomps_keep_receive_capacity_after_the_identifiers_are_released() {
        // Arrange
        fillPubrelWaits();

        // Act: PUBREL frees ids, while the unwritten PUBCOMP responses still own capacity.
        for (int id = 1; id <= 4096; id++) {
            receive(pubRel(id));
        }
        receive(publish(1, Qos.EXACTLY_ONCE, false));

        // Assert
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class));
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void written_pubcomps_return_receive_capacity() {
        // Arrange
        fillPubrelWaits();

        // Act: each PUBCOMP write returns the capacity its reception still held.
        for (int id = 1; id <= 4096; id++) {
            receive(pubRel(id));
            sut.written(generation, lastAcknowledgement().ticket());
        }
        receive(publish(1, Qos.EXACTLY_ONCE, false));

        // Assert
        assertThat(port.ends).isEmpty();
        assertThat(port.deliveries).hasSize(4097);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void unwritten_pubcomps_return_receive_capacity_when_the_connection_ends() {
        // Arrange
        fillPubrelWaits();
        for (int id = 1; id <= 4096; id++) {
            receive(pubRel(id));
        }
        sut.failed(generation, new IOException("lost before PUBCOMP was written"));

        // Act: the continued broker resends PUBREL for releases without PUBCOMP.
        connect(true);
        receive(pubRel(1));

        // Assert
        assertThat(port.ends).hasSize(1);
        assertThat(lastAcknowledgement().packet()).isEqualTo(pubComp(1));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "session present={0}")
    @ValueSource(booleans = {false, true})
    void a_publication_not_received_before_connection_loss_is_delivered_once_after_reconnect(
            boolean present
    ) {
        // Arrange
        connect(false);
        sut.failed(generation, new IOException("lost before receiving PUBLISH"));

        // Act
        connect(present);
        receive(publish(7, Qos.EXACTLY_ONCE, true));
        ready(port.deliveries.getLast());
        requireNonNull(lastAcknowledgement().receipt()).markAccepted();
        receive(pubRel(7));

        // Assert
        assertThat(port.deliveries).hasSize(1);
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void an_unwritten_pubcomp_is_answered_again_after_the_session_continues() {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken original = port.deliveries.getLast();
        ready(original);
        requireNonNull(lastAcknowledgement().receipt()).markAccepted();
        receive(pubRel(7));
        Sent originalCompletion = lastAcknowledgement();
        sut.failed(generation, new IOException("lost before PUBCOMP was written"));

        // Act
        connect(true);
        receive(pubRel(7));
        Sent repeatedCompletion = lastAcknowledgement();
        sut.written(originalCompletion.generation(), originalCompletion.ticket());
        sut.written(generation, repeatedCompletion.ticket());

        // Assert
        assertThat(port.deliveries).containsExactly(original);
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7), pubComp(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "lost at {0}")
    @EnumSource(ReceiveCut.class)
    void a_missing_session_retires_old_readiness_and_writes_before_delivering_the_id_again(
            ReceiveCut cut
    ) {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken original = port.deliveries.getLast();
        switch (cut) {
            case BEFORE_PUBREC -> {
            }
            case PUBREC_PENDING -> ready(original);
            case PUBCOMP_PENDING -> {
                ready(original);
                requireNonNull(lastAcknowledgement().receipt()).markAccepted();
                receive(pubRel(7));
            }
        }
        List<Sent> originalWrites = port.sent.stream()
                .filter(send -> send.packet() instanceof Packet.PubRec
                        || send.packet() instanceof Packet.PubComp)
                .toList();
        sut.failed(generation, new IOException("session lost at " + cut));
        port.sent.clear();

        // Act
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, true));
        DeliveryToken current = port.deliveries.getLast();
        ready(original);
        originalWrites.forEach(send -> sut.written(send.generation(), send.ticket()));

        // Assert
        assertThat(current.acknowledgement()).isNotEqualTo(original.acknowledgement());
        assertThat(current.session()).isNotEqualTo(original.session());
        assertThat(port.deliveries).containsExactly(original, current);
        assertThat(acknowledgements()).isEmpty();

        // Act
        ready(current);
        requireNonNull(lastAcknowledgement().receipt()).markAccepted();
        receive(pubRel(7));

        // Assert
        assertThat(acknowledgements()).containsExactly(pubRec(7), pubComp(7));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("receiveOrders")
    void completion_and_reconnection_keep_the_reception_identity_in_every_event_order(
            List<ReceiveEvent> order
    ) {
        // Arrange
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken original = port.deliveries.getLast();
        long originalGeneration = generation;
        var expected = new ArrayList<Packet>();
        boolean failed = false;
        boolean reconnected = false;

        // Act: preserve each causal chain while interleaving connection loss with completion.
        for (ReceiveEvent event : order) {
            switch (event) {
                case READY -> {
                    ready(original);
                    if (!failed) {
                        expected.add(pubRec(7));
                    }
                }
                case PUBREC_WRITTEN -> {
                    // No PUBREC was scheduled if loss preceded readiness. Its stale ticket
                    // still cannot affect a later connection or turn readiness into a send.
                    long ticket = port.sent.stream()
                            .filter(send -> send.generation() == originalGeneration
                                    && send.packet() instanceof Packet.PubRec)
                            .mapToLong(Sent::ticket)
                            .findFirst().orElse(Long.MAX_VALUE);
                    sut.written(originalGeneration, ticket);
                }
                case PUBREL -> {
                    sut.received(originalGeneration, pubRel(7), 0, 16);
                    if (!failed) {
                        expected.add(pubComp(7));
                    }
                }
                case FAILURE -> {
                    sut.failed(originalGeneration, new IOException("lost in " + order));
                    failed = true;
                }
                case RECONNECT -> {
                    connect(true);
                    reconnected = true;
                }
            }

            // Assert: readiness alone never sends on the resumed connection, and callbacks
            // from the old generation never change the current receipt or deliver it again.
            assertThat(sut.state()).as("state after %s in %s", event, order)
                    .isEqualTo(failed && !reconnected ? DISCONNECTED : CONNECTED);
            assertThat(port.deliveries).containsExactly(original);
            assertThat(acknowledgements()).containsExactlyElementsOf(expected);
        }

        // Act: complete the remaining exchange using the resumed connection's inputs.
        if (order.indexOf(ReceiveEvent.PUBREL) < order.indexOf(ReceiveEvent.FAILURE)) {
            receive(pubRel(7));
            expected.add(pubComp(7));
        } else {
            receive(publish(7, Qos.EXACTLY_ONCE, true));
            expected.add(pubRec(7));
            requireNonNull(lastAcknowledgement().receipt()).markAccepted();
            receive(pubRel(7));
            expected.add(pubComp(7));
        }

        // Assert
        assertThat(port.deliveries).extracting(DeliveryToken::acknowledgement)
                .containsOnly(original.acknowledgement());
        assertThat(acknowledgements()).containsExactlyElementsOf(expected);
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act: old writes and PUBREL inputs cannot advance the next occupant of the same id.
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        DeliveryToken current = port.deliveries.getLast();
        List<Sent> oldWrites = port.sent.stream()
                .filter(send -> send.generation() == originalGeneration)
                .toList();
        oldWrites.forEach(send -> sut.written(originalGeneration, send.ticket()));
        sut.received(originalGeneration, pubRel(7), 0, 16);
        ready(original);
        assertThat(acknowledgements()).containsExactlyElementsOf(expected);
        ready(current);
        expected.add(pubRec(7));

        // Assert
        assertThat(current.acknowledgement()).isNotEqualTo(original.acknowledgement());
        assertThat(acknowledgements()).containsExactlyElementsOf(expected);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    private Sent loseConnectionDuringPubrec() {
        connect(false);
        receive(publish(7, Qos.EXACTLY_ONCE, false));
        ready(port.deliveries.getLast());
        Sent received = lastAcknowledgement();
        port.handed.clear();
        sut.failed(generation, new IOException("inline close"));
        return received;
    }

    private void connect(boolean present) {
        long operation = ++nextOperation;
        sut.connect(new ConnectionAttempt(operation), 0);
        generation = port.generation;
        sut.opened(generation);
        receive(new Packet.ConnAck(present, ReasonCode.SUCCESS, EMPTY));
        assertThat(requireNonNull(port.results.get(operation)).value())
                .isEqualTo(ConnAck.of(present, port.attempt));
        if (!present) {
            if (!subscribed) {
                long subscription = ++nextOperation;
                sut.subscribe(
                        subscription,
                        Subscribe.of(new TopicFilter("data"), Qos.EXACTLY_ONCE)
                );
                subscribed = true;
            }
            assertThat(port.sent.getLast().packet()).isInstanceOf(Packet.Subscribe.class);
            var packet = (Packet.Subscribe) port.sent.getLast().packet();
            receive(new Packet.SubAck(packet.packetId(), List.of(ReasonCode.GRANTED_QOS_2), EMPTY));
        }
    }

    private void fillPubrelWaits() {
        connect(false);
        for (int id = 1; id <= 4096; id++) {
            receive(publish(id, Qos.EXACTLY_ONCE, false));
            ready(port.deliveries.getLast());
            Sent received = lastAcknowledgement();
            requireNonNull(received.receipt()).markAccepted();
            sut.written(generation, received.ticket());
        }
    }

    private void ready(DeliveryToken token) {
        port.handed.computeIfPresent(token.acknowledgement(), (_, _) -> true);
        sut.deliveryReady(token.acknowledgement());
    }

    private void receive(Packet packet) {
        sut.received(generation, packet, 0, 16);
    }

    private List<Packet> acknowledgements() {
        return port.sent.stream().map(Sent::packet)
                .filter(packet -> packet instanceof Packet.PubAck || packet instanceof Packet.PubRec
                        || packet instanceof Packet.PubComp)
                .toList();
    }

    private Sent lastAcknowledgement() {
        return port.sent.getLast();
    }

    private static Packet.Publish publish(int id, Qos qos, boolean dup) {
        return new Packet.Publish(new Topic("data"), new byte[] {1}, qos, false, dup, id, EMPTY);
    }

    private static Stream<Arguments> qosChanges() {
        return Stream.of(
                Arguments.of(
                        Qos.EXACTLY_ONCE,
                        Named.of("QoS 1", publish(7, Qos.AT_LEAST_ONCE, false))
                ),
                Arguments.of(
                        Qos.EXACTLY_ONCE,
                        Named.of("QoS 1 with DUP", publish(7, Qos.AT_LEAST_ONCE, true))
                ),
                Arguments.of(
                        Qos.AT_LEAST_ONCE,
                        Named.of("QoS 2 with DUP", publish(7, Qos.EXACTLY_ONCE, true))
                ),
                Arguments.of(Qos.AT_LEAST_ONCE, Named.of("PUBREL", pubRel(7)))
        );
    }

    private static Packet.PubAck pubAck(int id) {
        return new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY);
    }

    private static Packet.PubRec pubRec(int id) {
        return new Packet.PubRec(id, ReasonCode.SUCCESS, EMPTY);
    }

    private static Packet.PubRel pubRel(int id) {
        return new Packet.PubRel(id, ReasonCode.SUCCESS, EMPTY);
    }

    private static Packet.PubComp pubComp(int id) {
        return new Packet.PubComp(id, ReasonCode.SUCCESS, EMPTY);
    }

    private static Stream<List<ReceiveEvent>> receiveOrders() {
        List<ReceiveEvent> exchange = List.of(
                ReceiveEvent.READY,
                ReceiveEvent.PUBREC_WRITTEN,
                ReceiveEvent.PUBREL
        );
        // Choosing the two positions of FAILURE then RECONNECT enumerates all ten orders.
        return IntStream.range(0, 4).boxed()
                .flatMap(failure -> IntStream.range(failure + 1, 5).mapToObj(reconnect -> {
                    var order = new ArrayList<ReceiveEvent>();
                    int nextExchange = 0;
                    for (int index = 0; index < 5; index++) {
                        if (index == failure) {
                            order.add(ReceiveEvent.FAILURE);
                        } else if (index == reconnect) {
                            order.add(ReceiveEvent.RECONNECT);
                        } else {
                            order.add(exchange.get(nextExchange++));
                        }
                    }
                    return List.copyOf(order);
                }));
    }

    private enum ReceiveCut {
        BEFORE_PUBREC,
        PUBREC_PENDING,
        PUBCOMP_PENDING
    }

    private enum ReceiveEvent {
        READY,
        PUBREC_WRITTEN,
        PUBREL,
        FAILURE,
        RECONNECT
    }
}
