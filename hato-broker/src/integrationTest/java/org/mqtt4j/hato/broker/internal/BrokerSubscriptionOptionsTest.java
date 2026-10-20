package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.RawSession.subscription;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// The MQTT 5.0 subscription options that the broker honors: No Local and Retain As Published.
///
/// The broker routes a publication before it acknowledges it, and writes to each connection in
/// the order it queued the packets. So a PUBACK, or a PINGRESP after a delivery, shows that no
/// other delivery is on its way.
class BrokerSubscriptionOptionsTest {

    @Test
    void a_no_local_subscription_withholds_only_the_subscribers_own_publications()
            throws Exception {
        try (
                var sut = MqttBroker.start();
                RawSession subscriber = connectedV5(sut.port(), "subscriber");
                RawSession other = connectedV5(sut.port(), "other")
        ) {
            // Arrange
            subscriber.subscribe(subscription("local/#", AT_LEAST_ONCE, true, false));

            // Act: the PUBACK of the subscriber's own publication arrives before any delivery.
            subscriber.publish("local/topic", "own", AT_LEAST_ONCE, 1);
            other.publish("local/topic", "other", AT_LEAST_ONCE, 1);

            // Assert
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            assertThat(delivery).isEqualTo(
                    publication("local/topic", "other", AT_LEAST_ONCE, delivery.packetId())
            );
        }
    }

    @Test
    void an_overlapping_subscription_without_no_local_delivers_the_own_publication_once()
            throws Exception {
        try (var sut = MqttBroker.start(); RawSession client = connectedV5(sut.port(), "client")) {
            // Arrange
            client.subscribe(subscription("overlap/#", AT_LEAST_ONCE, true, false));
            client.subscribe("overlap/+", AT_LEAST_ONCE);

            // Act
            client.send(publication("overlap/topic", "own", AT_MOST_ONCE, 0));

            // Assert: the PINGRESP follows the only copy.
            assertThat(client.expect(Packet.Publish.class))
                    .isEqualTo(publication("overlap/topic", "own", AT_MOST_ONCE, 0));
            client.ping();
        }
    }

    @ParameterizedTest(name = "published with retain {0}")
    @ValueSource(booleans = {true, false})
    void only_a_retain_as_published_subscription_keeps_the_retain_flag(boolean retain)
            throws Exception {
        try (
                var sut = MqttBroker.start();
                RawSession keeping = connectedV5(sut.port(), "keeping");
                RawSession clearing = connectedV5(sut.port(), "clearing");
                RawSession publisher = connectedV5(sut.port(), "publisher")
        ) {
            // Arrange
            keeping.subscribe(subscription("flags/#", AT_MOST_ONCE, false, true));
            clearing.subscribe("flags/#", AT_MOST_ONCE);

            // Act
            publish(publisher, "flags/topic", retain);

            // Assert
            assertThat(keeping.expect(Packet.Publish.class).retain()).isEqualTo(retain);
            assertThat(clearing.expect(Packet.Publish.class).retain()).isFalse();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("overlaps")
    void overlapping_subscriptions_get_one_copy_for_each_retain_flag_they_ask_for(Overlap overlap)
            throws Exception {
        try (
                var sut = MqttBroker.start();
                RawSession subscriber = connectedV5(sut.port(), "subscriber");
                RawSession publisher = connectedV5(sut.port(), "publisher")
        ) {
            // Arrange: the wider filter keeps the publisher's retain flag at QoS 1, and the
            // narrower one clears it at QoS 0.
            subscriber.subscribe(subscription("mixed/#", AT_LEAST_ONCE, false, true));
            subscriber.subscribe(subscription("mixed/+", AT_MOST_ONCE, false, false));

            // Act
            publish(publisher, "mixed/topic", overlap.retain());

            // Assert: the PINGRESP follows the last copy.
            var copies = new ArrayList<Copy>();
            for (int i = 0; i < overlap.copies().size(); i++) {
                Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
                copies.add(new Copy(delivery.qos(), delivery.retain()));
            }
            subscriber.ping();
            assertThat(copies).containsExactlyInAnyOrderElementsOf(overlap.copies());
        }
    }

    static Stream<Overlap> overlaps() {
        return Stream.of(
                new Overlap(
                        "a retained publication",
                        true,
                        List.of(new Copy(AT_LEAST_ONCE, true), new Copy(AT_MOST_ONCE, false))
                ),
                new Overlap(
                        "a publication without retain",
                        false,
                        List.of(new Copy(AT_LEAST_ONCE, false))
                )
        );
    }

    /// A publication with its retain flag, and the copies that one subscriber gets of it.
    record Overlap(String name, boolean retain, List<Copy> copies) {

        @Override
        public String toString() {
            return name;
        }
    }

    record Copy(Qos qos, boolean retain) {}

    /// Publishes at QoS 1 with the retain flag `retain`, and checks the acknowledgement.
    private static void publish(RawSession publisher, String topic, boolean retain)
            throws Exception {
        publisher.send(new Packet.Publish(
                new Topic(topic),
                "payload".getBytes(UTF_8),
                AT_LEAST_ONCE,
                retain,
                false,
                1,
                EMPTY
        ));
        assertThat(publisher.expect(Packet.PubAck.class))
                .isEqualTo(new Packet.PubAck(1, SUCCESS, EMPTY));
    }
}
