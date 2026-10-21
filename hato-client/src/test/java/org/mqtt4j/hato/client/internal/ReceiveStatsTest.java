package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.ClientStats.OverloadCause;
import org.mqtt4j.hato.ClientStats.Usage;
import org.mqtt4j.hato.MqttCapacityException;
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
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class ReceiveStatsTest {

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
            Duration.ofSeconds(60),
            ClientLimits.DEFAULT
    );
    private long generation;

    @Test
    void each_unfinished_reception_counts_in_the_one_stage_that_it_has_reached() {
        // Arrange: in receive order, one reception reaches each stage.
        connect();
        Sent completion = released(1);
        pubrecWrittenAt(2, 0, 0);
        receive(publish(3, Qos.AT_LEAST_ONCE), 0);
        ready(port.deliveries.getLast());
        Sent acknowledgement = lastAcknowledgement();
        receive(publish(4, Qos.AT_LEAST_ONCE), 0);
        DeliveryToken unready = port.deliveries.getLast();
        receive(publish(5, Qos.AT_LEAST_ONCE), 0);
        ready(port.deliveries.getLast());

        // Act
        Receptions.Values values = sut.values(0).receiving();

        // Assert: the ready fifth reception waits behind the fourth, in receive order.
        assertThat(stages(values)).containsExactly(1L, 1L, 1L, 1L, 1L);
        assertThat(values.receptions()).isEqualTo(new Usage(5, 4096));

        // Act: the fourth meets its condition, so both of the last PUBACKs reach the queue.
        ready(unready);

        // Assert
        assertThat(stages(sut.values(0).receiving())).containsExactly(0L, 0L, 3L, 1L, 1L);

        // Act: the writes end three exchanges, and PUBREL moves the second one on.
        sut.written(generation, acknowledgement.ticket(), 0);
        sut.written(generation, completion.ticket(), 0);
        receive(pubRel(2), 0);

        // Assert
        Receptions.Values remaining = sut.values(0).receiving();
        assertThat(stages(remaining)).containsExactly(0L, 0L, 2L, 0L, 1L);
        assertThat(remaining.receptions()).isEqualTo(new Usage(3, 4096));
    }

    @Test
    void a_superseded_reception_counts_once_until_its_acknowledgement_is_written() {
        // Arrange
        connect();
        receive(publish(6, Qos.AT_LEAST_ONCE), 0);
        ready(port.deliveries.getLast());
        Sent superseded = lastAcknowledgement();

        // Act: a QoS 1 publication without DUP on the same id starts a new reception.
        receive(publish(6, Qos.AT_LEAST_ONCE), 0);

        // Assert
        Receptions.Values values = sut.values(0).receiving();
        assertThat(stages(values)).containsExactly(1L, 0L, 1L, 0L, 0L);
        assertThat(values.receptions().used()).isEqualTo(2);

        // Act
        sut.written(generation, superseded.ticket(), 0);

        // Assert
        Receptions.Values remaining = sut.values(0).receiving();
        assertThat(stages(remaining)).containsExactly(1L, 0L, 0L, 0L, 0L);
        assertThat(remaining.receptions().used()).isEqualTo(1);
    }

    @Test
    void the_oldest_waits_for_conditions_and_pubrel_are_measured_on_the_clients_clock() {
        // Arrange
        connect();
        Receptions.Values none = sut.values(50).receiving();
        pubrecWrittenAt(1, 100, 200);
        pubrecWrittenAt(2, 300, 400);
        receive(publish(3, Qos.AT_LEAST_ONCE), 500);
        DeliveryToken oldest = port.deliveries.getLast();
        receive(publish(4, Qos.AT_LEAST_ONCE), 700);

        // Act
        Receptions.Values values = sut.values(1000).receiving();

        // Assert
        assertThat(none.oldestAwaitingConditions()).isNull();
        assertThat(none.oldestAwaitingRelease()).isNull();
        assertThat(values.oldestAwaitingConditions()).isEqualTo(Duration.ofNanos(500));
        assertThat(values.oldestAwaitingRelease()).isEqualTo(Duration.ofNanos(800));

        // Act: the oldest of each kind moves on.
        ready(oldest);
        receive(pubRel(1), 900);

        // Assert
        Receptions.Values later = sut.values(1000).receiving();
        assertThat(later.oldestAwaitingConditions()).isEqualTo(Duration.ofNanos(300));
        assertThat(later.oldestAwaitingRelease()).isEqualTo(Duration.ofNanos(600));
    }

    @Test
    void received_publishes_are_counted_by_qos_and_those_without_a_subscription_apart() {
        // Arrange
        connect();

        // Act: a retransmission counts as a PUBLISH of its own.
        receive(publish("data", 1, Qos.EXACTLY_ONCE, false), 0);
        receive(publish("data", 1, Qos.EXACTLY_ONCE, true), 0);
        receive(publish("data", 2, Qos.AT_LEAST_ONCE, false), 0);
        receive(publish("data", 0, Qos.AT_MOST_ONCE, false), 0);
        receive(publish("other", 3, Qos.AT_LEAST_ONCE, false), 0);

        // Assert
        Receptions.Values values = sut.values(0).receiving();
        assertThat(values.publishes()).isEqualTo(Map.of(
                Qos.AT_MOST_ONCE, 1L,
                Qos.AT_LEAST_ONCE, 2L,
                Qos.EXACTLY_ONCE, 2L
        ));
        assertThat(values.unmatched()).isOne();
    }

    @Test
    void a_reception_beyond_the_receive_table_ends_the_connection_as_an_overload() {
        // Arrange: 4,096 QoS 1 receptions await their handlers.
        connect();
        for (int id = 1; id <= 4096; id++) {
            receive(publish(id, Qos.AT_LEAST_ONCE), 0);
        }

        // Act
        receive(publish(4097, Qos.AT_LEAST_ONCE), 0);

        // Assert
        assertThat(port.ends).singleElement().satisfies(end -> assertThat(end.cause())
                .containsInstanceOf(MqttCapacityException.class));
        assertThat(sut.values(0).overloads()).containsEntry(OverloadCause.RECEPTIONS, 1L);
    }

    /// Receives the QoS 2 publication `id` at `receivedAt`, writes its PUBREC at `writtenAt`,
    /// and returns that write, after which the reception awaits PUBREL.
    private Sent pubrecWrittenAt(int id, long receivedAt, long writtenAt) {
        receive(publish(id, Qos.EXACTLY_ONCE), receivedAt);
        ready(port.deliveries.getLast());
        Sent received = lastAcknowledgement();
        requireNonNull(received.receipt()).markAccepted();
        sut.written(generation, received.ticket(), writtenAt);
        return received;
    }

    /// Takes the QoS 2 publication `id` through PUBREL and returns the write of its PUBCOMP,
    /// which has not completed.
    private Sent released(int id) {
        pubrecWrittenAt(id, 0, 0);
        receive(pubRel(id), 0);
        return lastAcknowledgement();
    }

    /// The counts of the five stages, in their order: awaiting conditions, awaiting the turn,
    /// awaiting the write of PUBACK or PUBREC, awaiting PUBREL, and awaiting the write of
    /// PUBCOMP.
    private static List<Long> stages(Receptions.Values values) {
        return List.of(
                values.awaitingConditions(),
                values.awaitingTurn(),
                values.awaitingAcknowledgementWrite(),
                values.awaitingRelease(),
                values.awaitingCompletionWrite()
        );
    }

    private void connect() {
        sut.connect(new ConnectionAttempt(1), 0);
        generation = port.generation;
        sut.opened(generation);
        receive(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY), 0);
        sut.subscribe(2, Subscribe.of(new TopicFilter("data"), Qos.EXACTLY_ONCE));
        var request = (Packet.Subscribe) port.sent.getLast().packet();
        receive(new Packet.SubAck(request.packetId(), List.of(ReasonCode.GRANTED_QOS_2), EMPTY), 0);
    }

    private void ready(DeliveryToken token) {
        port.handed.computeIfPresent(token.acknowledgement(), (_, _) -> true);
        sut.deliveryReady(token.acknowledgement());
    }

    private void receive(Packet packet, long now) {
        sut.received(generation, packet, now, 16);
    }

    private Sent lastAcknowledgement() {
        return port.sent.getLast();
    }

    private static Packet.Publish publish(int id, Qos qos) {
        return publish("data", id, qos, false);
    }

    private static Packet.Publish publish(String topic, int id, Qos qos, boolean dup) {
        return new Packet.Publish(new Topic(topic), new byte[] {1}, qos, false, dup, id, EMPTY);
    }

    private static Packet.PubRel pubRel(int id) {
        return new Packet.PubRel(id, ReasonCode.SUCCESS, EMPTY);
    }
}
