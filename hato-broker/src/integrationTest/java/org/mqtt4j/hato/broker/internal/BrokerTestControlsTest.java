package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// The test controls on the wire: subscription rules in SUBACK, and held responses that keep
/// their order and their limit.
class BrokerTestControlsTest {

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void rules_refuse_one_filter_and_lower_the_qos_of_another_in_one_subscribe(
            MqttVersion version
    ) throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession client = (version == MqttVersion.V3_1_1)
                        ? connected(sut.port(), "subscriber")
                        : connectedV5(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            sut.refuseSubscriptions("refused/#");
            sut.grantAtMost("lowered/#", 1);

            // Act
            client.send(new Packet.Subscribe(
                    1,
                    List.of(
                            subscription("refused/#", EXACTLY_ONCE),
                            subscription("lowered/#", EXACTLY_ONCE),
                            subscription("plain/#", EXACTLY_ONCE)
                    ),
                    EMPTY
            ));

            // Assert: the refused filter subscribes to nothing; failure is 0x80 in both
            // versions.
            assertThat(client.expect(Packet.SubAck.class)).isEqualTo(new Packet.SubAck(
                    1,
                    List.of(
                            ReasonCode.UNSPECIFIED_ERROR,
                            ReasonCode.GRANTED_QOS_1,
                            ReasonCode.GRANTED_QOS_2
                    ),
                    EMPTY
            ));
            publisher.publish("refused/topic", "dropped", EXACTLY_ONCE, 1);
            publisher.publish("lowered/topic", "lowered", EXACTLY_ONCE, 2);
            assertThat(client.expect(Packet.Publish.class).qos()).isEqualTo(AT_LEAST_ONCE);
            client.ping();
        }
    }

    @Test
    void a_refused_filter_leaves_an_earlier_subscription_to_it_in_place() throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession client = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            client.subscribe("kept/#", AT_LEAST_ONCE);
            sut.refuseSubscriptions("kept/#");

            // Act
            client.send(new Packet.Subscribe(
                    2,
                    List.of(subscription("kept/#", AT_MOST_ONCE)),
                    EMPTY
            ));

            // Assert: the earlier subscription still receives at its QoS.
            assertThat(client.expect(Packet.SubAck.class)).isEqualTo(new Packet.SubAck(
                    2,
                    List.of(ReasonCode.UNSPECIFIED_ERROR),
                    EMPTY
            ));
            publisher.publish("kept/topic", "still delivered", AT_LEAST_ONCE, 1);
            assertThat(client.expect(Packet.Publish.class).qos()).isEqualTo(AT_LEAST_ONCE);
        }
    }

    @Test
    void released_responses_arrive_in_the_order_held_ahead_of_later_responses() throws Exception {
        // More than the 128 responses that the send queue keeps room for, so that the release
        // waits for room.
        int count = 200;
        try (BrokerServer sut = BrokerServer.start(loopback());
                RawSession client = connected(sut.port(), "holding")) {
            // Arrange
            sut.holdResponses(Packet.PubAck.class, "holding");
            for (int id = 1; id <= count; id++) {
                client.send(publication("held", "message", AT_LEAST_ONCE, id));
            }
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(sut.heldResponses()).hasSize(count));

            // Act
            sut.releaseResponses(Packet.PubAck.class, "holding");
            client.send(new Packet.PingReq());

            // Assert
            for (int id = 1; id <= count; id++) {
                assertThat(client.expect(Packet.PubAck.class))
                        .isEqualTo(new Packet.PubAck(id, SUCCESS, EMPTY));
            }
            client.expect(Packet.PingResp.class);
            assertThat(sut.heldResponses()).isEmpty();
        }
    }

    @Test
    void a_connection_that_has_more_responses_held_than_the_limit_is_closed() throws Exception {
        try (BrokerServer sut = BrokerServer.builder().maxHeld(2).start(loopback());
                RawSession client = connected(sut.port(), "holding")) {
            // Arrange: other kinds of response still go out while acknowledgements wait.
            sut.holdResponses(Packet.PubAck.class, "holding");
            client.send(publication("held", "first", AT_LEAST_ONCE, 1));
            client.send(publication("held", "second", AT_LEAST_ONCE, 2));
            client.publish("held", "exactly once", EXACTLY_ONCE, 3);
            client.ping();
            assertThat(sut.heldResponses()).hasSize(2);

            // Act
            client.send(publication("held", "third", AT_LEAST_ONCE, 4));

            // Assert
            client.expectClosed();
            assertThat(sut.heldResponses()).isEmpty();
        }
    }

    @Test
    void a_held_kind_of_response_waits_for_later_connections_of_the_client() throws Exception {
        try (BrokerServer sut = BrokerServer.start(loopback())) {
            // Arrange: the client subscribes in its first connection.
            sut.holdResponses(Packet.SubAck.class, "holding");
            try (RawSession first = connected(sut.port(), "holding")) {
                first.send(new Packet.Subscribe(
                        1,
                        List.of(subscription("first", AT_MOST_ONCE)),
                        EMPTY
                ));
                await().atMost(Duration.ofSeconds(5))
                        .untilAsserted(() -> assertThat(sut.heldResponses()).hasSize(1));
            }
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(sut.heldResponses()).isEmpty());

            // Act
            try (RawSession next = connected(sut.port(), "holding")) {
                next.send(new Packet.Subscribe(
                        2,
                        List.of(subscription("next", AT_MOST_ONCE)),
                        EMPTY
                ));

                // Assert: the hold outlives the connection, and its release answers the new one.
                await().atMost(Duration.ofSeconds(5))
                        .untilAsserted(() -> assertThat(sut.heldResponses()).hasSize(1));
                sut.releaseResponses(Packet.SubAck.class, "holding");
                assertThat(next.expect(Packet.SubAck.class))
                        .isEqualTo(new Packet.SubAck(2, List.of(SUCCESS), EMPTY));
            }
        }
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
