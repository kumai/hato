package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.client;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

@Timeout(30)
class Qos2ReceptionTest {

    @ParameterizedTest(name = "duplicate flag: {0}")
    @ValueSource(booleans = {false, true})
    void duplicate_publish_joins_the_condition_or_repeats_pubrec_without_redelivery(boolean dup)
            throws Exception {
        // Arrange: the manual condition prevents the initial PUBREC from being admitted.
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            PullSubscription subscription = peer.subscribePull(
                    sut,
                    "data",
                    Qos.EXACTLY_ONCE,
                    AckMode.MANUAL,
                    2
            );
            assertThat(subscription.grantedQos()).isEqualTo(Qos.EXACTLY_ONCE);
            peer.send(publication(7, (byte) 1, false));
            MqttDelivery original = received(subscription.receive(Duration.ofSeconds(5)));

            // Act & Assert: before admission, duplicate PUBLISH does not acknowledge early.
            peer.send(publication(7, (byte) 1, dup));
            peer.roundTripBarrier(sut);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            original.ack();
            assertThat(peer.receive()).isEqualTo(new Packet.PubRec(7, ReasonCode.SUCCESS, EMPTY));

            // After admission, the same duplicate gets another PUBREC and no delivery.
            peer.send(publication(7, (byte) 1, dup));
            assertThat(peer.receive()).isEqualTo(new Packet.PubRec(7, ReasonCode.SUCCESS, EMPTY));
            peer.send(new Packet.PubRel(7, ReasonCode.SUCCESS, EMPTY));
            assertThat(peer.receive()).isEqualTo(new Packet.PubComp(7, ReasonCode.SUCCESS, EMPTY));
            peer.roundTripBarrier(sut);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        }
    }

    @Test
    void repeated_and_unknown_pubrel_receive_pubcomp() throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            // Act & Assert: no receive entry is required, including for a repeated release.
            for (int id : new int[] {7, 7, 8}) {
                peer.send(new Packet.PubRel(id, ReasonCode.SUCCESS, EMPTY));
                assertThat(peer.receive())
                        .isEqualTo(new Packet.PubComp(id, ReasonCode.SUCCESS, EMPTY));
            }
            peer.roundTripBarrier(sut);
        }
    }

    @Test
    void pubcomp_passes_an_unacknowledged_delivery_while_pubrec_and_puback_keep_receive_order()
            throws Exception {
        // Arrange: the first manual delivery blocks the later PUBACK, but not PUBCOMP.
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            PullSubscription subscription = peer.subscribePull(
                    sut,
                    "data",
                    Qos.EXACTLY_ONCE,
                    AckMode.MANUAL,
                    2
            );
            peer.send(publication(7, (byte) 1, false));
            MqttDelivery first = received(subscription.receive(Duration.ofSeconds(5)));
            peer.publish("data", 8);
            MqttDelivery second = received(subscription.receive(Duration.ofSeconds(5)));
            peer.send(new Packet.PubRel(9, ReasonCode.SUCCESS, EMPTY));
            assertThat(peer.receive()).isEqualTo(new Packet.PubComp(9, ReasonCode.SUCCESS, EMPTY));
            second.ack();
            peer.roundTripBarrier(sut);

            // Act
            first.ack();

            // Assert: acknowledgement kind cannot change the receive order of PUBREC and PUBACK.
            assertThat(peer.receive()).isEqualTo(new Packet.PubRec(7, ReasonCode.SUCCESS, EMPTY));
            assertThat(peer.receive()).isEqualTo(new Packet.PubAck(8, ReasonCode.SUCCESS, EMPTY));
            peer.send(new Packet.PubRel(7, ReasonCode.SUCCESS, EMPTY));
            assertThat(peer.receive()).isEqualTo(new Packet.PubComp(7, ReasonCode.SUCCESS, EMPTY));
            peer.roundTripBarrier(sut);
        }
    }

    @Test
    void a_publish_reusing_a_released_identifier_is_a_new_delivery() throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            PullSubscription subscription = peer.subscribePull(
                    sut,
                    "data",
                    Qos.EXACTLY_ONCE,
                    AckMode.ON_RECEIVE,
                    2
            );
            peer.send(publication(7, (byte) 1, false));
            assertThat(received(subscription.receive(Duration.ofSeconds(5))).message().payload())
                    .containsExactly((byte) 1);
            assertThat(peer.receive()).isEqualTo(new Packet.PubRec(7, ReasonCode.SUCCESS, EMPTY));
            peer.send(new Packet.PubRel(7, ReasonCode.SUCCESS, EMPTY));
            assertThat(peer.receive()).isEqualTo(new Packet.PubComp(7, ReasonCode.SUCCESS, EMPTY));

            // Act: even DUP=1 is new after PUBREL has released the identifier.
            peer.send(publication(7, (byte) 2, true));

            // Assert
            assertThat(received(subscription.receive(Duration.ofSeconds(5))).message().payload())
                    .containsExactly((byte) 2);
            assertThat(peer.receive()).isEqualTo(new Packet.PubRec(7, ReasonCode.SUCCESS, EMPTY));
            peer.send(new Packet.PubRel(7, ReasonCode.SUCCESS, EMPTY));
            assertThat(peer.receive()).isEqualTo(new Packet.PubComp(7, ReasonCode.SUCCESS, EMPTY));
            peer.roundTripBarrier(sut);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        }
    }

    private static Packet.Publish publication(int id, byte value, boolean dup) {
        return new Packet.Publish(
                new Topic("data"),
                new byte[] {value},
                Qos.EXACTLY_ONCE,
                false,
                dup,
                id,
                EMPTY
        );
    }

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }
}
