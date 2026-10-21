package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.ClientStats.OverloadCause;
import org.mqtt4j.hato.ClientStats.Usage;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.Publication;
import org.mqtt4j.hato.client.internal.SessionPersistenceTest.RecordingPort;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.client.internal.common.protocol.Property;
import org.mqtt4j.hato.client.internal.common.protocol.PropertyId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.SessionPersistenceTest.messageWithWireBytes;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class SendStatsTest {

    private final RecordingPort port = new RecordingPort();
    private final SessionCore sut = new SessionCore(
            port,
            "client",
            Duration.ofSeconds(30),
            Duration.ZERO,
            MqttVersion.V5,
            Duration.ofHours(1),
            16 * 1024 * 1024,
            null,
            false,
            ReconnectBackoff.disabled(),
            RetryRules.DEFAULT,
            Duration.ofSeconds(60),
            ClientLimits.DEFAULT
    );
    private long nextOperation;
    private long generation;

    @Test
    void each_unfinished_publication_counts_in_the_stage_that_awaits_the_brokers_answer() {
        // Arrange: the broker takes four publications at once, and the fifth waits to be sent.
        connect(4);
        Publication first = publish(Qos.AT_LEAST_ONCE);
        publish(Qos.EXACTLY_ONCE);
        Publication released = publish(Qos.EXACTLY_ONCE);
        receive(new Packet.PubRec(released.packetId(), ReasonCode.SUCCESS, EMPTY));
        publish(Qos.AT_LEAST_ONCE);
        publish(Qos.AT_LEAST_ONCE);

        // Act
        SessionCore.Values values = sut.values(0);

        // Assert: a persistent session keeps each PUBLISH until PUBACK or PUBREC.
        Publications.Values sending = values.sending();
        assertThat(port.accepted).hasSize(4);
        assertThat(sending.awaitingPubAck()).isEqualTo(3);
        assertThat(sending.awaitingPubRec()).isOne();
        assertThat(sending.awaitingPubComp()).isOne();
        assertThat(sending.publications()).isEqualTo(new Usage(5, 64));
        assertThat(sending.keptBytes()).isEqualTo(new Usage(4 * 64, 32L * 1024 * 1024));
        assertThat(values.quota()).isZero();

        // Act
        receive(new Packet.PubAck(first.packetId(), ReasonCode.SUCCESS, EMPTY));

        // Assert: the waiting publication takes the room that PUBACK returned.
        SessionCore.Values answered = sut.values(0);
        assertThat(port.accepted).hasSize(5);
        assertThat(answered.sending().awaitingPubAck()).isEqualTo(2);
        assertThat(answered.sending().publications().used()).isEqualTo(4);
        assertThat(answered.quota()).isZero();
    }

    @Test
    void the_quota_shows_the_room_that_the_brokers_receive_maximum_leaves() {
        // Arrange
        SessionCore.Values unconnected = sut.values(0);
        connect(4);
        publish(Qos.AT_LEAST_ONCE);

        // Act
        SessionCore.Values values = sut.values(0);

        // Assert
        assertThat(unconnected.quota()).isNull();
        assertThat(values.quota()).isEqualTo(3);

        // Act: the publication stays in the persistent session after the connection ends.
        sut.failed(generation, new IOException("connection lost"));

        // Assert
        SessionCore.Values ended = sut.values(0);
        assertThat(ended.quota()).isNull();
        assertThat(ended.sending().awaitingPubAck()).isOne();
    }

    @Test
    void the_oldest_response_wait_counts_from_the_completion_of_its_write() {
        // Arrange
        connect(10);
        Publication first = publish(Qos.AT_LEAST_ONCE);
        Publication second = publish(Qos.EXACTLY_ONCE);
        SessionCore.Values unwritten = sut.values(1000);
        sut.sent(generation, second.ticket(), 120, 150);
        sut.sent(generation, first.ticket(), 200, 250);
        receive(new Packet.PubRec(second.packetId(), ReasonCode.SUCCESS, EMPTY));
        sut.sent(generation, port.releaseTickets.getLast(), 300, 350);

        // Act
        SessionCore.Values values = sut.values(1000);

        // Assert: PUBREC answered the oldest wait, so the PUBACK wait is the oldest now.
        assertThat(unwritten.oldestResponseWait()).isNull();
        assertThat(values.oldestResponseWait()).isEqualTo(Duration.ofNanos(800));

        // Act
        receive(new Packet.PubAck(first.packetId(), ReasonCode.SUCCESS, EMPTY));

        // Assert
        assertThat(sut.values(1000).oldestResponseWait()).isEqualTo(Duration.ofNanos(700));
    }

    @Test
    void a_disconnect_without_reserved_send_capacity_ends_the_connection_as_an_overload() {
        // Arrange
        connect(4);
        port.disconnectResult = SendQueue.OfferResult.FULL;

        // Act
        sut.disconnect(++nextOperation);

        // Assert
        assertThat(port.ends.getLast().cause()).containsInstanceOf(MqttCapacityException.class);
        assertThat(sut.values(0).overloads()).containsEntry(OverloadCause.RESERVED_SENDS, 1L);
    }

    @Test
    void an_overload_while_a_violation_is_reported_leaves_the_violation_as_the_cause() {
        // Arrange: the DISCONNECT that reports the violation has not been written.
        connect(4);
        sut.failed(generation, SessionCore.protocolError("Test violation"));

        // Act
        sut.overloaded(
                generation,
                OverloadCause.EVENTS,
                new MqttCapacityException("Incoming mailbox capacity")
        );

        // Assert
        assertThat(port.ends.getLast().cause())
                .hasValueSatisfying(cause -> assertThat(cause)
                        .isNotInstanceOf(MqttCapacityException.class));
        assertThat(sut.values(0).overloads()).allSatisfy((_, count) -> assertThat(count).isZero());
    }

    private void connect(int receiveMaximum) {
        long operation = ++nextOperation;
        sut.connect(new ConnectionAttempt(operation), 0);
        generation = port.generation;
        sut.opened(generation);
        receive(new Packet.ConnAck(
                false,
                ReasonCode.SUCCESS,
                new Properties(List.of(new Property.TwoByteValue(
                        PropertyId.RECEIVE_MAXIMUM,
                        receiveMaximum
                )))
        ));
    }

    /// Publishes a 64-byte PUBLISH at `qos` and returns the last publication that the send queue
    /// took.
    private Publication publish(Qos qos) {
        MqttMessage message = messageWithWireBytes(64, MqttVersion.V5).withQos(qos);
        sut.publish(++nextOperation, message);
        return port.accepted.getLast();
    }

    private void receive(Packet packet) {
        sut.received(generation, packet, 0, 0);
    }
}
