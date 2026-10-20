package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttPublicationRefusedException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.Publication;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.RecordingPort;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.Result;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.SessionCore.State.CONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.DISCONNECTED;
import static org.mqtt4j.hato.client.internal.SessionCore.State.DISCONNECTING;
import static org.mqtt4j.hato.client.internal.SessionPersistenceTest.messageWithWireBytes;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class Qos2SendTest {

    private final RecordingPort port = new RecordingPort();
    private SessionCore sut = core(false);
    private long nextOperation;
    private long generation;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void qos_two_publish_succeeds_once_at_pubcomp_and_never_at_write_or_pubrec(boolean cleanStart) {
        // Arrange
        sut = core(cleanStart);
        connect(false);
        long operation = publish(message());
        Publication publication = port.accepted.getLast();
        if (!cleanStart) {
            assertThat(requireNonNull(publication.receipt()).accepted()).isFalse();
            requireNonNull(publication.receipt()).markAccepted();
        }

        // Act
        sut.sent(generation, publication.ticket(), 0);
        receive(pubRec(publication.packetId()));
        sut.sent(generation, port.releaseTickets.getLast(), 0);

        // Assert
        assertThat(port.results).doesNotContainKey(operation);
        assertThat(port.releases).containsExactly(pubRel(publication.packetId()));
        assertThat(port.accepted).containsExactly(publication);

        // Act
        receive(pubComp(publication.packetId()));
        Result completed = result(operation);
        receive(pubComp(publication.packetId()));
        sut.sent(generation, publication.ticket(), 0);

        // Assert: the recording port also rejects a second completion of any operation.
        assertThat(completed.value()).isEqualTo(PublishResult.of());
        assertThat(completed.failure()).isNull();
        assertThat(result(operation)).isSameAs(completed);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void pubrel_does_not_wait_for_an_incoming_qos_one_delivery_to_be_acknowledged() {
        // Arrange
        connect(false);
        subscribe("budget");
        receive(qosOnePublish(30_000));
        assertThat(port.deliveries).hasSize(1);
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();

        // Act
        receive(pubRec(id));
        receive(pubComp(id));

        // Assert
        assertThat(port.releases).containsExactly(pubRel(id));
        assertThat(port.sent).noneMatch(Packet.PubAck.class::isInstance);
        assertThat(result(operation).value()).isEqualTo(PublishResult.of());
    }

    @Test
    void subscription_restoration_waiting_for_suback_does_not_hold_back_pubrel() {
        // Arrange
        restoreMoreSubscriptionsThanTheSubackWindow();
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();

        // Act
        receive(pubRec(id));
        receive(pubComp(id));

        // Assert
        assertThat(port.releases).containsExactly(pubRel(id));
        assertThat(result(operation).value()).isEqualTo(PublishResult.of());
    }

    @Test
    void a_pubrel_waiting_for_send_capacity_does_not_hold_back_subscription_restoration() {
        // Arrange
        restoreMoreSubscriptionsThanTheSubackWindow();
        publish(message());
        int id = port.accepted.getLast().packetId();
        port.releaseAllowance = 0;
        receive(pubRec(id));
        var restored = (Packet.Subscribe) port.sent.getFirst();

        // Act: SUBACK frees a re-creation slot while PUBREL still waits for capacity.
        receive(new Packet.SubAck(restored.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY));

        // Assert
        assertThat(port.releases).isEmpty();
        assertThat(port.sent).filteredOn(Packet.Subscribe.class::isInstance).hasSize(17);

        // Act & Assert
        port.releaseAllowance = 1;
        sut.sendCapacityAvailable(generation);
        assertThat(port.releases).containsExactly(pubRel(id));
    }

    @Test
    void pubrel_and_puback_take_turns_when_acknowledgement_room_returns() {
        // Arrange: a PUBREL and unmatched QoS 1 receptions all wait for acknowledgement room.
        connect(false);
        publish(message());
        int id = port.accepted.getLast().packetId();
        port.acknowledgementAllowance = 0;
        receive(pubRec(id));
        for (int reception = 1; reception <= 3; reception++) {
            receive(qosOnePublish(reception));
        }

        // Act: room returns one slot at a time while PUBACKs remain ready.
        for (int slot = 0; slot < 2; slot++) {
            port.acknowledgementAllowance = 1;
            sut.sendCapacityAvailable(generation);
        }

        // Assert
        assertThat(port.releases).containsExactly(pubRel(id));
        assertThat(port.acknowledgements).hasSize(1);
    }

    @Test
    void unknown_responses_preserve_known_exchanges_and_pubrec_is_answered_again() {
        // Arrange
        connect(false);
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();

        // Act
        receive(pubRec(30_000));
        receive(pubRec(30_000));
        receive(pubComp(30_000));
        receive(pubComp(30_000));
        receive(pubRec(id));
        receive(pubRec(id));

        // Assert
        assertThat(port.releases).containsExactly(
                pubRel(30_000),
                pubRel(30_000),
                pubRel(id),
                pubRel(id)
        );
        assertThat(port.results).doesNotContainKey(operation);
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act & Assert
        receive(pubComp(id));
        assertThat(result(operation).value()).isEqualTo(PublishResult.of());
    }

    @ParameterizedTest(name = "unknown PUBREL admitted before publication={0}")
    @ValueSource(booleans = {false, true})
    void a_new_publish_cannot_borrow_an_unknown_pubrel_identifier_until_pubcomp(boolean admitted) {
        // Arrange: the allocator would otherwise choose 1 for its first publication.
        connect(false);
        port.releaseAllowance = admitted ? Integer.MAX_VALUE : 0;
        receive(pubRec(1));

        // Act
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();

        // Assert
        assertThat(id).isNotEqualTo(1);
        assertThat(port.releases).hasSize(admitted ? 1 : 0);

        // Act: the old PUBCOMP must not complete or invalidate the new PUBREC wait.
        port.releaseAllowance = Integer.MAX_VALUE;
        sut.sendCapacityAvailable(generation);
        receive(pubComp(1));

        // Assert
        assertThat(port.results).doesNotContainKey(operation);
        assertThat(sut.state()).isEqualTo(CONNECTED);

        // Act: only PUBCOMP returns the unknown identifier for later allocation.
        receive(pubRec(id));
        receive(pubComp(id));
        MqttMessage following = message().withQos(Qos.AT_LEAST_ONCE);
        boolean reused = false;
        for (int i = 0; i < 65_535 && !reused; i++) {
            publish(following);
            int allocated = port.accepted.getLast().packetId();
            reused = allocated == 1;
            receive(pubAck(allocated));
        }

        // Assert
        assertThat(port.releases).containsExactly(pubRel(1), pubRel(id));
        assertThat(reused).isTrue();
        assertThat(result(operation).value()).isEqualTo(PublishResult.of());
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void a_repeated_unknown_pubrec_keeps_its_identifier_until_each_pubrel_is_answered() {
        // Arrange: the allocator would otherwise choose 1 for its first publication.
        connect(false);
        receive(pubRec(1));
        receive(pubRec(1));
        receive(pubComp(1));

        // Act: the second PUBCOMP must not land on a new exchange.
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();
        receive(pubComp(1));

        // Assert
        assertThat(port.releases).containsExactly(pubRel(1), pubRel(1));
        assertThat(id).isNotEqualTo(1);
        assertThat(port.results).doesNotContainKey(operation);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "session present={0}")
    @ValueSource(booleans = {false, true})
    void an_unknown_exchange_does_not_reserve_an_identifier_in_the_next_connection(
            boolean present
    ) {
        // Arrange
        connect(false);
        receive(pubRec(1));
        long oldGeneration = generation;
        sut.failed(generation, new IOException("connection lost"));
        connect(present);

        // Act
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();
        sut.received(oldGeneration, pubComp(1), 0, 0);

        // Assert
        assertThat(id).isEqualTo(1);
        assertThat(port.results).doesNotContainKey(operation);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void a_subscribe_cannot_borrow_an_identifier_of_an_unknown_pubrel_waiting_for_capacity() {
        // Arrange
        connect(false);
        port.releaseAllowance = 0;
        receive(pubRec(1));

        // Act
        long operation = ++nextOperation;
        sut.subscribe(operation, Subscribe.of(new TopicFilter("budget"), Qos.AT_LEAST_ONCE));
        var request = (Packet.Subscribe) port.sent.getLast();

        // Assert
        assertThat(request.packetId()).isNotEqualTo(1);
        assertThat(port.results).doesNotContainKey(operation);
        assertThat(port.releases).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void an_unsubscribe_cannot_borrow_an_identifier_of_an_unknown_pubrel_waiting_for_capacity() {
        // Arrange: SUBACK releases 1, and the allocator next chooses 2.
        connect(false);
        long subscription = subscribe("budget");
        port.releaseAllowance = 0;
        receive(pubRec(2));

        // Act
        long operation = ++nextOperation;
        sut.unsubscribe(operation, subscription);

        // Assert: other deferred packets can be offered after the removal in the same drain.
        assertThat(port.sent).filteredOn(Packet.Unsubscribe.class::isInstance)
                .singleElement().isInstanceOfSatisfying(
                        Packet.Unsubscribe.class,
                        removal -> assertThat(removal.packetId()).isNotEqualTo(2)
                );
        assertThat(port.results).doesNotContainKey(operation);
        assertThat(port.releases).isEmpty();
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "{0}, after PUBREC={1}, receives {2}")
    @MethodSource("stageMismatches")
    void a_response_for_a_different_exchange_stage_closes_without_success(
            Qos qos,
            boolean received,
            String response
    ) {
        // Arrange
        connect(false);
        long operation = publish(message().withQos(qos));
        int id = port.accepted.getLast().packetId();
        if (received) {
            receive(pubRec(id));
        }

        // Act
        receive(response(response, id));

        // Assert
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause().orElseThrow()).isInstanceOf(MqttProtocolException.class));
        assertThat(result(operation).value()).isNull();
        assertThat(result(operation).failure()).isInstanceOf(MqttConnectionLostException.class);
    }

    @ParameterizedTest(name = "{0}, after PUBREC={1}, receives {2}")
    @MethodSource("stageMismatches")
    void a_response_for_a_different_exchange_stage_does_not_disturb_a_pending_disconnect(
            Qos qos,
            boolean received,
            String response
    ) {
        // Arrange
        connect(false);
        long operation = publish(message().withQos(qos));
        int id = port.accepted.getLast().packetId();
        if (received) {
            receive(pubRec(id));
        }
        long disconnect = ++nextOperation;
        sut.disconnect(disconnect);
        long ticket = port.ticket;
        int sent = port.sent.size();

        // Act
        receive(response(response, id));
        sut.sent(generation, ticket, 0);

        // Assert
        assertThat(port.sent).hasSize(sent);
        assertThat(result(disconnect).failure()).isNull();
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause()).isEmpty());
        assertThat(result(operation).failure()).isInstanceOf(MqttConnectionLostException.class);
    }

    @ParameterizedTest(name = "{0}, after PUBREC={1}, receives {2}")
    @MethodSource("stageMismatches")
    void a_persistent_session_resumes_an_exchange_closed_by_a_stage_mismatch(
            Qos qos,
            boolean received,
            String response
    ) {
        // Arrange
        connect(false);
        publish(message().withQos(qos));
        int id = port.accepted.getLast().packetId();
        if (received) {
            receive(pubRec(id));
        }
        receive(response(response, id));
        port.outgoingIds.clear();

        // Act
        connect(true);

        // Assert: the broker may still hold the identifier, so the exchange is not dropped.
        assertThat(port.outgoingIds).containsExactly(id);
    }

    @ParameterizedTest(name = "disconnect pending={0}")
    @ValueSource(booleans = {false, true})
    void a_puback_for_a_qos_two_publication_fails_only_that_publication(boolean disconnecting) {
        // Arrange: a broker without QoS 2 accepts QoS 2 at QoS 1 and answers PUBACK.
        connect(false);
        long downgraded = publish(message());
        int id = port.accepted.getLast().packetId();
        long other = publish(message().withQos(Qos.AT_LEAST_ONCE));
        int otherId = port.accepted.getLast().packetId();
        if (disconnecting) {
            sut.disconnect(++nextOperation);
        }

        // Act
        receive(pubAck(id));

        // Assert
        assertThat(result(downgraded).failure())
                .isInstanceOfSatisfying(MqttProtocolException.class, failure ->
                        assertThat(failure.reasonCode()).isEqualTo(ReasonCode.PROTOCOL_ERROR));
        assertThat(sut.state()).isEqualTo(disconnecting ? DISCONNECTING : CONNECTED);
        assertThat(port.ends).isEmpty();
        assertThat(port.results).doesNotContainKey(other);

        // Act & Assert
        receive(pubAck(otherId));
        assertThat(result(other).value()).isEqualTo(PublishResult.of());
    }

    @Test
    void a_resumed_qos_two_publication_answered_by_puback_is_dropped_with_its_byte_budget() {
        // Arrange: the publication failed with its first connection and is resumed.
        connect(false);
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();
        sut.failed(generation, new IOException("connection lost"));
        Result failure = result(operation);
        connect(true);

        // Act
        receive(pubAck(id));
        sut.failed(generation, new IOException("connection lost"));
        port.outgoingIds.clear();
        connect(true);
        long large = publish(messageWithWireBytes(32 * 1024 * 1024).withQos(Qos.EXACTLY_ONCE));

        // Assert: nothing is resumed again, and the whole byte budget is available.
        assertThat(port.outgoingIds).containsExactly(port.accepted.getLast().packetId());
        assertThat(port.results).doesNotContainKey(large);
        assertThat(result(operation)).isSameAs(failure);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void deferred_pubrels_keep_pubrec_order_and_do_not_block_new_publications() {
        // Arrange
        connect(false);
        publish(message());
        int first = port.accepted.getLast().packetId();
        publish(message());
        int second = port.accepted.getLast().packetId();
        port.releaseAllowance = 0;

        // Act: an unsent repeated response keeps its original place in the FIFO.
        receive(pubRec(second));
        receive(pubRec(first));
        receive(pubRec(second));
        long admitted = publish(message());

        // Assert
        assertThat(port.releases).isEmpty();
        assertThat(port.accepted).hasSize(3);
        assertThat(port.results).doesNotContainKey(admitted);

        // Act
        port.releaseAllowance = 1;
        sut.sendCapacityAvailable(generation);

        // Assert
        assertThat(port.releases).containsExactly(pubRel(second));

        // Act
        port.releaseAllowance = 1;
        sut.sendCapacityAvailable(generation);
        sut.sendCapacityAvailable(generation);

        // Assert
        assertThat(port.releases).containsExactly(pubRel(second), pubRel(first));
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void pubcomp_removes_a_repeated_pubrel_still_waiting_for_capacity() {
        // Arrange
        connect(false);
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();
        receive(pubRec(id));
        port.releaseAllowance = 0;
        receive(pubRec(id));

        // Act
        receive(pubComp(id));
        port.releaseAllowance = 1;
        sut.sendCapacityAvailable(generation);

        // Assert
        assertThat(port.releases).containsExactly(pubRel(id));
        assertThat(result(operation).value()).isEqualTo(PublishResult.of());
    }

    @Test
    void a_closed_pubrel_queue_waits_for_the_original_connection_failure() {
        // Arrange
        connect(false);
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();
        port.releaseResult = SendQueue.OfferResult.CLOSED;

        // Act
        receive(pubRec(id));

        // Assert
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.results).doesNotContainKey(operation);

        // Act
        var cause = new IOException("PUBREL transport closed");
        sut.failed(generation, cause);

        // Assert
        assertThat(result(operation).failure()).hasCause(cause);
        assertThat(port.ends).singleElement().satisfies(end ->
                assertThat(end.cause()).containsSame(cause));
    }

    @Test
    void pubcomp_during_disconnect_completes_publish_without_sending_another_packet() {
        // Arrange
        connect(false);
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();
        receive(pubRec(id));
        long disconnect = ++nextOperation;
        sut.disconnect(disconnect);
        long ticket = port.ticket;
        int sent = port.sent.size();

        // Act
        receive(pubComp(id));

        // Assert
        assertThat(result(operation).value()).isEqualTo(PublishResult.of());
        assertThat(port.sent).hasSize(sent);
        assertThat(port.results).doesNotContainKey(disconnect);

        // Act & Assert
        sut.sent(generation, ticket, 0);
        assertThat(result(disconnect).failure()).isNull();
        assertThat(sut.state()).isEqualTo(DISCONNECTED);
    }

    @Test
    void pubrec_during_disconnect_leaves_only_pubrel_to_resend() {
        // Arrange
        connect(false);
        long operation = publish(message());
        int first = port.accepted.getLast().packetId();
        publish(message());
        int second = port.accepted.getLast().packetId();
        sut.disconnect(++nextOperation);
        long ticket = port.ticket;
        int sent = port.sent.size();

        // Act
        receive(pubRec(first));
        receive(pubRec(first));
        receive(pubRec(30_000));
        sut.sendCapacityAvailable(generation);

        // Assert
        assertThat(port.releases).isEmpty();
        assertThat(port.sent).hasSize(sent);
        assertThat(port.results).doesNotContainKey(operation);

        // Act
        sut.sent(generation, ticket, 0);
        Result failed = result(operation);
        port.outgoingIds.clear();
        connect(true);

        // Assert: the resent PUBREL goes before the resent PUBLISH.
        assertThat(failed.failure()).isInstanceOf(MqttConnectionLostException.class);
        assertThat(port.outgoingIds).containsExactly(first, second);
        assertThat(port.releases).containsExactly(pubRel(first));
        assertThat(result(operation)).isSameAs(failed);

        // Act & Assert
        receive(pubComp(first));
        assertThat(result(operation)).isSameAs(failed);
    }

    @ParameterizedTest(name = "disconnect at {0}")
    @EnumSource(value = Stage.class, names = {"BEFORE_PUBLISH", "BEFORE_PUBREC"})
    void a_continued_session_resends_publish_with_its_identifier_and_receipt_before_pubrec(
            Stage stage
    ) {
        // Arrange
        connect(false);
        long operation = interruptAt(stage);
        Publication original = port.accepted.getLast();
        Result failure = result(operation);

        // Act
        connect(true);

        // Assert
        Publication replay = port.accepted.getLast();
        assertThat(port.accepted).hasSize(2);
        assertThat(replay.packetId()).isEqualTo(original.packetId());
        assertThat(replay.receipt()).isSameAs(original.receipt());
        assertThat(requireNonNull(replay.receipt()).accepted()).isTrue();
        assertThat(port.releases).isEmpty();
        assertThat(failure.failure()).isInstanceOf(MqttConnectionLostException.class);

        // Act & Assert: completing the resumed exchange leaves the failed future unchanged.
        receive(pubRec(original.packetId()));
        receive(pubComp(original.packetId()));
        assertThat(result(operation)).isSameAs(failure);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "disconnect at {0}")
    @EnumSource(value = Stage.class, names = {"BEFORE_PUBREL", "BEFORE_PUBCOMP"})
    void a_continued_session_resends_only_pubrel_after_pubrec(Stage stage) {
        // Arrange
        connect(false);
        long operation = interruptAt(stage);
        Publication original = port.accepted.getLast();
        Result failure = result(operation);
        int releases = port.releases.size();

        // Act
        connect(true);

        // Assert
        assertThat(port.accepted).containsExactly(original);
        assertThat(port.releases).hasSize(releases + 1);
        assertThat(port.releases.getLast()).isEqualTo(pubRel(original.packetId()));
        assertThat(failure.failure()).isInstanceOf(MqttConnectionLostException.class);

        // Act & Assert: completing the resumed exchange leaves the failed future unchanged.
        receive(pubComp(original.packetId()));
        assertThat(result(operation)).isSameAs(failure);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "disconnect at {0}")
    @EnumSource(Stage.class)
    void a_lost_session_discards_the_exchange_at_every_send_stage(Stage stage) {
        // Arrange
        connect(false);
        long operation = interruptAt(stage);
        Result failure = result(operation);
        int publications = port.accepted.size();
        int releases = port.releases.size();

        // Act
        connect(false);

        // Assert
        assertThat(port.accepted).hasSize(publications);
        assertThat(port.releases).hasSize(releases);
        assertThat(failure.failure()).isInstanceOf(MqttConnectionLostException.class);
        assertThat(result(operation)).isSameAs(failure);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "disconnect at {0}")
    @EnumSource(Stage.class)
    void a_scheduled_reconnection_holds_a_qos_two_future_until_a_continued_session_completes_it(
            Stage stage
    ) {
        // Arrange
        long operation = holdAcrossReconnection(stage);
        int id = port.accepted.getLast().packetId();

        // Act: the broker answers the stages that the cut left open.
        reconnect(true);
        if (!stage.afterPubrec()) {
            receive(pubRec(id));
        }

        // Assert: the future succeeds once, at PUBCOMP.
        assertThat(port.results).doesNotContainKey(operation);
        receive(pubComp(id));
        assertThat(result(operation).value()).isEqualTo(PublishResult.of());
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "disconnect at {0}")
    @EnumSource(Stage.class)
    void a_scheduled_reconnection_that_loses_the_session_fails_the_held_qos_two_future(
            Stage stage
    ) {
        // Arrange
        long operation = holdAcrossReconnection(stage);

        // Act
        reconnect(false);

        // Assert
        assertThat(result(operation).failure()).isInstanceOf(MqttConnectionLostException.class);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void mixed_replay_resends_pubrel_in_pubrec_order_before_publish_in_send_order() {
        // Arrange
        connect(false);
        publish(message().withQos(Qos.AT_LEAST_ONCE));
        int first = port.accepted.getLast().packetId();
        publish(message());
        int second = port.accepted.getLast().packetId();
        publish(message());
        int third = port.accepted.getLast().packetId();
        publish(message().withQos(Qos.AT_LEAST_ONCE));
        int fourth = port.accepted.getLast().packetId();
        receive(pubRec(third));
        receive(pubRec(second));
        receive(pubRec(third));
        sut.failed(generation, new IOException("connection lost"));
        port.outgoingIds.clear();

        // Act
        connect(true);

        // Assert: duplicate PUBREC does not move an existing PUBCOMP wait to the end.
        assertThat(port.outgoingIds).containsExactly(third, second, first, fourth);
        assertThat(port.accepted.subList(4, port.accepted.size()))
                .extracting(Publication::packetId)
                .containsExactly(first, fourth);
    }

    @Test
    void pubrec_for_a_waiting_publish_replay_replaces_it_without_resending_its_payload() {
        // Arrange
        connect(false);
        publish(message());
        int first = port.accepted.getLast().packetId();
        publish(message());
        int second = port.accepted.getLast().packetId();
        sut.failed(generation, new IOException("connection lost"));
        port.publishAllowance = 0;
        port.outgoingIds.clear();
        connect(true);

        // Act
        receive(pubRec(first));
        port.publishAllowance = 1;
        sut.sendCapacityAvailable(generation);

        // Assert
        assertThat(port.accepted.subList(2, port.accepted.size()))
                .extracting(Publication::packetId)
                .containsExactly(second);
        assertThat(port.releases).containsExactly(pubRel(first));
        assertThat(port.outgoingIds).containsExactly(second, first);
    }

    @Test
    void a_duplicate_pubrec_shares_a_pubrel_replay_still_waiting_for_capacity() {
        // Arrange: the PUBREL that the continued session resends waits for send capacity.
        connect(false);
        publish(message());
        int first = port.accepted.getLast().packetId();
        publish(message());
        int second = port.accepted.getLast().packetId();
        receive(pubRec(first));
        sut.failed(generation, new IOException("connection lost"));
        port.releaseAllowance = 0;
        port.outgoingIds.clear();
        connect(true);

        // Act: room for two PUBRELs returns after the duplicate.
        receive(pubRec(first));
        port.releaseAllowance = 2;
        sut.sendCapacityAvailable(generation);

        // Assert
        assertThat(port.outgoingIds).containsExactly(first, second);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_waiting_pubrel_replay_blocks_new_publish_and_stale_capacity_cannot_resume_it(
            boolean completedWhileWaiting
    ) {
        // Arrange
        connect(false);
        publish(message());
        int id = port.accepted.getLast().packetId();
        receive(pubRec(id));
        sut.failed(generation, new IOException("connection lost"));
        long oldGeneration = generation;
        port.releaseAllowance = 0;
        connect(true);
        long refused = publish(message());
        assertThat(result(refused).failure()).isInstanceOf(MqttCapacityException.class);
        if (completedWhileWaiting) {
            receive(pubComp(id));
        }

        // Act
        port.releaseAllowance = 1;
        sut.sendCapacityAvailable(oldGeneration);

        // Assert
        assertThat(port.releases).containsExactly(pubRel(id));

        // Act
        sut.sendCapacityAvailable(generation);
        long admitted = publish(message());

        // Assert
        assertThat(port.releases).hasSize(completedWhileWaiting ? 1 : 2);
        assertThat(port.results).doesNotContainKey(admitted);
        assertThat(port.accepted).hasSize(2);
    }

    @Test
    void pubrec_releases_payload_bytes_once_while_the_exchange_count_waits_for_pubcomp() {
        // Arrange
        connect(false);
        MqttMessage large = messageWithWireBytes(32 * 1024 * 1024).withQos(Qos.EXACTLY_ONCE);
        publish(large);
        int first = port.accepted.getLast().packetId();
        long full = publish(message());
        assertThat(result(full).failure()).isInstanceOf(MqttCapacityException.class);

        // Act
        receive(pubRec(first));
        long admitted = publish(large);
        int second = port.accepted.getLast().packetId();
        receive(pubRec(first));
        receive(pubComp(first));
        long stillFull = publish(message());

        // Assert
        assertThat(port.results).doesNotContainKey(admitted);
        assertThat(result(stillFull).failure()).isInstanceOf(MqttCapacityException.class);

        // Act
        receive(pubRec(second));
        for (int i = 1; i < 64; i++) {
            publish(message());
            receive(pubRec(port.accepted.getLast().packetId()));
        }
        long exhausted = publish(message());

        // Assert
        assertThat(result(exhausted).failure()).isInstanceOf(MqttCapacityException.class);

        // Act & Assert
        receive(pubComp(second));
        long resumed = publish(message());
        assertThat(port.results).doesNotContainKey(resumed);
    }

    @Test
    void an_identifier_is_held_until_pubcomp_and_then_reused_across_allocator_wrap() {
        // Arrange
        connect(false);
        long operation = publish(message());
        int retained = port.accepted.getLast().packetId();
        receive(pubRec(retained));
        MqttMessage following = message().withQos(Qos.AT_LEAST_ONCE);

        // Act & Assert: one complete wrap must skip the retained identifier.
        for (int i = 0; i < 65_535; i++) {
            publish(following);
            int id = port.accepted.getLast().packetId();
            assertThat(id).isNotEqualTo(retained);
            receive(pubAck(id));
        }
        assertThat(port.results).doesNotContainKey(operation);
        receive(pubComp(retained));

        // Act: after completion a subsequent wrap must make the identifier available again.
        boolean reused = false;
        for (int i = 0; i < 65_535 && !reused; i++) {
            publish(following);
            int id = port.accepted.getLast().packetId();
            reused = id == retained;
            receive(pubAck(id));
        }

        // Assert
        assertThat(reused).isTrue();
        assertThat(result(operation).value()).isEqualTo(PublishResult.of());
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @ParameterizedTest(name = "events: {0}")
    @MethodSource("eventOrders")
    void send_acknowledgements_and_reconnect_complete_once_in_every_valid_event_order(
            List<Event> order
    ) {
        // Arrange
        connect(false);
        long operation = publish(message());
        Publication original = port.accepted.getLast();
        requireNonNull(original.receipt()).markAccepted();
        long originalGeneration = generation;

        // Act: peer acknowledgements belong to the original connection even when delivered late.
        for (Event event : order) {
            switch (event) {
                case SENT -> sut.sent(originalGeneration, original.ticket(), 0);
                case PUBREC -> sut.received(
                        originalGeneration,
                        pubRec(original.packetId()),
                        0,
                        0
                );
                case PUBCOMP -> sut.received(
                        originalGeneration,
                        pubComp(original.packetId()),
                        0,
                        0
                );
                case FAILURE -> sut.failed(originalGeneration, new IOException("connection lost"));
                case RECONNECT -> connect(true);
            }
        }

        // Assert
        Result completed = result(operation);
        boolean succeeded = order.indexOf(Event.PUBCOMP) < order.indexOf(Event.FAILURE);
        boolean received = order.indexOf(Event.PUBREC) < order.indexOf(Event.FAILURE);
        if (succeeded) {
            assertThat(completed.value()).isEqualTo(PublishResult.of());
            assertThat(port.accepted).containsExactly(original);
            assertThat(port.releases).containsExactly(pubRel(original.packetId()));
        } else {
            assertThat(completed.failure()).isInstanceOf(MqttConnectionLostException.class);
            assertThat(port.accepted).hasSize(received ? 1 : 2);
            assertThat(port.releases).hasSize(received ? 2 : 0);
            if (!received) {
                assertThat(port.accepted.getLast().packetId()).isEqualTo(original.packetId());
                assertThat(port.accepted.getLast().receipt()).isSameAs(original.receipt());
                receive(pubRec(original.packetId()));
            }
            receive(pubComp(original.packetId()));
        }
        assertThat(result(operation)).isSameAs(completed);
        assertThat(sut.state()).isEqualTo(CONNECTED);
        assertThat(port.timers).isEmpty();
    }

    @ParameterizedTest(name = "PUBREC {0}")
    @EnumSource(
            value = ReasonCode.class,
            names = {"UNSPECIFIED_ERROR", "NOT_AUTHORIZED", "QUOTA_EXCEEDED"}
    )
    void an_mqtt_5_refusing_pubrec_ends_the_exchange_without_pubrel(ReasonCode reason) {
        // Arrange: the session holds as many exchanges as it allows.
        sut = core(MqttVersion.V5, true, ReconnectBackoff.disabled());
        connect(false);
        long refused = publish(message());
        int id = port.accepted.getLast().packetId();
        for (int i = 1; i < 64; i++) {
            publish(message());
        }
        assertThat(result(publish(message())).failure())
                .isInstanceOf(MqttCapacityException.class);

        // Act
        receive(new Packet.PubRec(id, reason, EMPTY));

        // Assert: the publication fails, nothing answers the refusal, and the exchange is over.
        assertThat(result(refused).failure()).isInstanceOfSatisfying(
                MqttPublicationRefusedException.class,
                refusal -> assertThat(refusal.reasonCode()).isEqualTo(reason)
        );
        assertThat(port.releases).isEmpty();
        long admitted = publish(message());
        assertThat(port.results).doesNotContainKey(admitted);
        assertThat(sut.state()).isEqualTo(CONNECTED);
    }

    @Test
    void an_mqtt_5_pubcomp_completes_the_publication_whatever_its_reason_code() {
        // Arrange
        sut = core(MqttVersion.V5, true, ReconnectBackoff.disabled());
        connect(false);
        long operation = publish(message());
        int id = port.accepted.getLast().packetId();
        receive(pubRec(id));

        // Act: a broker that lost the exchange's state took the message at PUBREC.
        receive(new Packet.PubComp(id, ReasonCode.PACKET_IDENTIFIER_NOT_FOUND, EMPTY));

        // Assert
        assertThat(result(operation).value()).isEqualTo(PublishResult.of());
    }

    private SessionCore core(boolean cleanStart) {
        return core(cleanStart, ReconnectBackoff.disabled());
    }

    private SessionCore core(boolean cleanStart, ReconnectBackoff backoff) {
        return core(MqttVersion.V3_1_1, cleanStart, backoff);
    }

    private SessionCore core(MqttVersion version, boolean cleanStart, ReconnectBackoff backoff) {
        return new SessionCore(
                port,
                "client",
                Duration.ofSeconds(30),
                Duration.ZERO,
                version,
                cleanStart ? Duration.ZERO : Duration.ofHours(1),
                16 * 1024 * 1024,
                null,
                false,
                backoff,
                false,
                Duration.ofSeconds(60)
        );
    }

    // Interrupts a QoS 2 publication at `stage` in a persistent session that reconnects
    // automatically, and returns its operation, which stays pending.
    private long holdAcrossReconnection(Stage stage) {
        sut = core(false, new ReconnectBackoff(
                Duration.ofNanos(3),
                Duration.ofNanos(3),
                new ReconnectBackoffTest.MaximumRandom()
        ));
        connect(false);
        long operation = interruptAt(stage);
        assertThat(port.results).doesNotContainKey(operation);
        return operation;
    }

    private void reconnect(boolean present) {
        sut.timer(0, SessionCore.Timer.RECONNECT, 3);
        generation = port.generation;
        receive(new Packet.ConnAck(present, ReasonCode.SUCCESS, EMPTY));
    }

    private void connect(boolean present) {
        long operation = ++nextOperation;
        sut.connect(new ConnectionAttempt(operation), 0);
        generation = port.generation;
        receive(new Packet.ConnAck(present, ReasonCode.SUCCESS, EMPTY));
        assertThat(result(operation).value())
                .isEqualTo(ConnAck.of(present, port.attempts.getLast()));
    }

    /// Leaves one of 17 subscriptions to re-create behind the 16 SUBSCRIBE requests that await
    /// SUBACK after a connection without Session Present.
    private void restoreMoreSubscriptionsThanTheSubackWindow() {
        connect(false);
        for (int i = 0; i < 17; i++) {
            subscribe("restored/" + i);
        }
        sut.failed(generation, new IOException("connection lost"));
        port.sent.clear();
        connect(false);
        assertThat(port.sent).hasSize(16).allMatch(Packet.Subscribe.class::isInstance);
    }

    /// Publishes, advances the exchange to `stage`, and ends the connection there. Acceptance
    /// and write completion are separate from the peer's receipt.
    private long interruptAt(Stage stage) {
        long operation = publish(message());
        Publication original = port.accepted.getLast();
        requireNonNull(original.receipt()).markAccepted();
        if (stage != Stage.BEFORE_PUBLISH) {
            sut.sent(generation, original.ticket(), 0);
        }
        if (stage == Stage.BEFORE_PUBREL) {
            port.releaseAllowance = 0;
        }
        if (stage.afterPubrec()) {
            receive(pubRec(original.packetId()));
        }
        if (stage == Stage.BEFORE_PUBCOMP) {
            sut.sent(generation, port.releaseTickets.getLast(), 0);
        }
        sut.failed(generation, new IOException("connection lost"));
        port.releaseAllowance = Integer.MAX_VALUE;
        return operation;
    }

    private long publish(MqttMessage message) {
        long operation = ++nextOperation;
        sut.publish(operation, message);
        return operation;
    }

    private long subscribe(String filter) {
        long operation = ++nextOperation;
        sut.subscribe(operation, Subscribe.of(new TopicFilter(filter), Qos.AT_LEAST_ONCE));
        var request = (Packet.Subscribe) port.sent.getLast();
        receive(new Packet.SubAck(request.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
        return operation;
    }

    private void receive(Packet packet) {
        sut.received(generation, packet, 0, 0);
    }

    private Result result(long operation) {
        return requireNonNull(port.results.get(operation));
    }

    private static MqttMessage message() {
        return messageWithWireBytes(16).withQos(Qos.EXACTLY_ONCE);
    }

    private static Packet.Publish qosOnePublish(int id) {
        MqttMessage incoming = message();
        return new Packet.Publish(
                incoming.topic(),
                incoming.payload(),
                Qos.AT_LEAST_ONCE,
                false,
                false,
                id,
                EMPTY
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

    private static Packet response(String type, int id) {
        return switch (type) {
            case "PUBACK" -> pubAck(id);
            case "PUBREC" -> pubRec(id);
            case "PUBCOMP" -> pubComp(id);
            default -> throw new AssertionError(type);
        };
    }

    private static Stream<Arguments> stageMismatches() {
        return Stream.of(
                Arguments.of(Qos.AT_LEAST_ONCE, false, "PUBREC"),
                Arguments.of(Qos.AT_LEAST_ONCE, false, "PUBCOMP"),
                Arguments.of(Qos.EXACTLY_ONCE, false, "PUBCOMP"),
                Arguments.of(Qos.EXACTLY_ONCE, true, "PUBACK")
        );
    }

    private static Stream<List<Event>> eventOrders() {
        List<List<Event>> orders = permutations(List.of(Event.values()))
                .filter(order -> order.indexOf(Event.PUBREC) < order.indexOf(Event.PUBCOMP))
                .filter(order -> order.indexOf(Event.FAILURE) < order.indexOf(Event.RECONNECT))
                .toList();
        assertThat(orders).hasSize(30).doesNotHaveDuplicates();
        return orders.stream();
    }

    static <T> Stream<List<T>> permutations(List<T> remaining) {
        if (remaining.isEmpty()) {
            return Stream.of(List.of());
        }
        return remaining.stream().flatMap(event -> {
            var tail = new ArrayList<>(remaining);
            tail.remove(event);
            return permutations(tail).map(suffix -> {
                var order = new ArrayList<T>();
                order.add(event);
                order.addAll(suffix);
                return List.copyOf(order);
            });
        });
    }

    private enum Stage {
        BEFORE_PUBLISH,
        BEFORE_PUBREC,
        BEFORE_PUBREL,
        BEFORE_PUBCOMP;

        boolean afterPubrec() {
            return this == BEFORE_PUBREL || this == BEFORE_PUBCOMP;
        }
    }

    private enum Event {
        SENT,
        PUBREC,
        PUBCOMP,
        FAILURE,
        RECONNECT
    }
}
