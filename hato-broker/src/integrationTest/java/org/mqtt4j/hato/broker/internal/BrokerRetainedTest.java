package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.value.Qos;
import org.mqtt4j.hato.broker.internal.common.value.Topic;
import org.mqtt4j.hato.broker.internal.common.value.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.connectedV5;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// Retained messages: the last one of each topic for new subscriptions, removed by an empty
/// payload, sent as Retain Handling chooses, and bounded by the broker's limits.
class BrokerRetainedTest {

    @ParameterizedTest
    @EnumSource(Qos.class)
    void a_new_subscription_receives_the_last_retained_message_of_each_matching_topic(Qos qos)
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            // Arrange: the second message on kept/a replaces the first. PINGRESP shows that the
            // broker has handled the publications, which QoS 0 does not acknowledge.
            publisher.publish(retained("kept/a", "first", qos));
            publisher.publish(retained("kept/a", "second", qos));
            publisher.publish(retained("kept/b", "other", qos));
            publisher.ping();

            // Act
            subscriber.subscribe("kept/#", EXACTLY_ONCE);

            // Assert: each arrives with RETAIN 1 at its own QoS, and nothing else follows.
            List<Packet.Publish> received = List.of(
                    subscriber.expect(Packet.Publish.class),
                    subscriber.expect(Packet.Publish.class)
            );
            assertThat(received)
                    .extracting(
                            publish -> publish.topic().name(),
                            publish -> new String(publish.payload(), UTF_8),
                            Packet.Publish::qos,
                            Packet.Publish::retain
                    )
                    .containsExactlyInAnyOrder(
                            tuple("kept/a", "second", qos, true),
                            tuple("kept/b", "other", qos, true)
                    );
            subscriber.ping();
        }
    }

    @Test
    void a_retained_message_arrives_at_the_lower_of_its_qos_and_the_granted_qos() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            // Arrange
            publisher.publish(retained("downgraded", "kept", EXACTLY_ONCE));

            // Act
            subscriber.subscribe("downgraded", AT_LEAST_ONCE);

            // Assert
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(retained("downgraded", "kept", AT_LEAST_ONCE));
        }
    }

    @Test
    void an_empty_retained_publication_reaches_current_subscribers_and_removes_the_message()
            throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession current = connected(sut.port(), "current")) {
            // Arrange: a current subscriber receives a retained publication with RETAIN 0.
            current.subscribe("removed", AT_MOST_ONCE);
            publisher.publish(retained("removed", "kept", AT_LEAST_ONCE));
            assertThat(current.expect(Packet.Publish.class))
                    .isEqualTo(publication("removed", "kept", AT_MOST_ONCE, 0));

            // Act
            publisher.publish(retained("removed", "", AT_LEAST_ONCE));

            // Assert: the removal reaches current subscribers like any publication, and a new
            // subscription receives nothing.
            assertThat(current.expect(Packet.Publish.class))
                    .isEqualTo(publication("removed", "", AT_MOST_ONCE, 0));
            try (RawSession later = connected(sut.port(), "later")) {
                later.subscribe("removed", AT_MOST_ONCE);
                later.ping();
            }
        }
    }

    @Test
    void a_publication_without_retain_leaves_the_retained_message_in_place() throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            // Arrange
            publisher.publish(retained("unchanged", "kept", AT_LEAST_ONCE));

            // Act: neither a message nor an empty payload without RETAIN changes it.
            publisher.publish("unchanged", "passing", AT_LEAST_ONCE, 2);
            publisher.publish("unchanged", "", AT_LEAST_ONCE, 3);

            // Assert
            subscriber.subscribe("unchanged", AT_LEAST_ONCE);
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(retained("unchanged", "kept", AT_LEAST_ONCE));
            subscriber.ping();
        }
    }

    @ParameterizedTest(name = "Retain Handling {0}")
    @CsvSource({"0, true, true", "1, true, false", "2, false, false"})
    void retain_handling_chooses_which_subscribe_receives_the_retained_message(
            int handling,
            boolean sentForNew,
            boolean sentForRepeated
    ) throws Exception {
        try (var sut = MqttBroker.start();
                RawSession publisher = connectedV5(sut.port(), "publisher");
                RawSession subscriber = connectedV5(sut.port(), "subscriber")) {
            // Arrange
            publisher.publish(retained("handled", "kept", AT_LEAST_ONCE));
            var subscription = new Packet.Subscription(
                    new TopicFilter("handled"),
                    AT_LEAST_ONCE,
                    false,
                    false,
                    handling
            );

            // Act: the first SUBSCRIBE makes the subscription, and the second repeats it.
            subscriber.subscribe(subscription);
            boolean receivedForNew = retainedArrives(subscriber);
            subscriber.subscribe(subscription);
            boolean receivedForRepeated = retainedArrives(subscriber);

            // Assert
            assertThat(receivedForNew).isEqualTo(sentForNew);
            assertThat(receivedForRepeated).isEqualTo(sentForRepeated);
        }
    }

    @Test
    void retained_messages_for_a_new_subscription_wait_for_the_window_like_other_deliveries()
            throws Exception {
        try (BrokerServer sut = BrokerServer.builder().maxInflight(1).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "subscriber")) {
            // Arrange
            publisher.publish(retained("window/a", "a", AT_LEAST_ONCE));
            publisher.publish(retained("window/b", "b", AT_LEAST_ONCE));

            // Act
            subscriber.subscribe("window/#", AT_LEAST_ONCE);

            // Assert: the second message waits until the first is acknowledged.
            Packet.Publish first = subscriber.expect(Packet.Publish.class);
            subscriber.ping();
            subscriber.send(new Packet.PubAck(first.packetId(), SUCCESS, EMPTY));
            Packet.Publish second = subscriber.expect(Packet.Publish.class);
            assertThat(List.of(first, second))
                    .extracting(publish -> publish.topic().name())
                    .containsExactlyInAnyOrder("window/a", "window/b");
        }
    }

    @Test
    void a_retained_message_beyond_the_count_limit_is_delivered_but_not_kept() throws Exception {
        try (BrokerServer sut = BrokerServer.builder().maxRetained(2).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession current = connected(sut.port(), "current")) {
            // Arrange
            current.subscribe("limited/#", AT_MOST_ONCE);

            // Act
            publisher.publish(retained("limited/a", "a", AT_LEAST_ONCE));
            publisher.publish(retained("limited/b", "b", AT_LEAST_ONCE));
            publisher.publish(retained("limited/c", "c", AT_LEAST_ONCE));

            // Assert: the current subscriber receives all three, and a new subscription only the
            // two that the limit keeps, until removing one makes room.
            for (String name : List.of("a", "b", "c")) {
                assertThat(current.expect(Packet.Publish.class))
                        .isEqualTo(publication("limited/" + name, name, AT_MOST_ONCE, 0));
            }
            assertThat(retainedTopics(sut.port(), "first", "limited/#", 2))
                    .containsExactlyInAnyOrder("limited/a", "limited/b");
            publisher.publish(retained("limited/a", "", AT_LEAST_ONCE));
            publisher.publish(retained("limited/c", "c", AT_LEAST_ONCE));
            assertThat(retainedTopics(sut.port(), "second", "limited/#", 2))
                    .containsExactlyInAnyOrder("limited/b", "limited/c");
        }
    }

    @Test
    void a_retained_message_beyond_the_size_limit_leaves_its_topic_without_one() throws Exception {
        // The limit holds the 17-byte MQTT 5.0 PUBLISH of the small message, but not two of them.
        try (BrokerServer sut = BrokerServer.builder().maxRetainedBytes(20).start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: replacing the small message frees its share of the limit.
            publisher.publish(retained("sized", "small", AT_LEAST_ONCE));
            publisher.publish(retained("sized", "small", AT_LEAST_ONCE));

            // Act
            publisher.publish(retained("sized", "x".repeat(100), AT_LEAST_ONCE));

            // Assert: the large message replaced the small one, so neither is kept, and the small
            // message fits again.
            assertThat(retainedTopics(sut.port(), "first", "sized", 0)).isEmpty();
            publisher.publish(retained("sized", "small", AT_LEAST_ONCE));
            assertThat(retainedTopics(sut.port(), "second", "sized", 1)).containsExactly("sized");
        }
    }

    private static Packet.Publish retained(String topic, String payload, Qos qos) {
        return new Packet.Publish(
                new Topic(topic),
                payload.getBytes(UTF_8),
                qos,
                true,
                false,
                (qos == AT_MOST_ONCE) ? 0 : 1,
                EMPTY
        );
    }

    /// Whether a retained message arrives before the answer to a PINGREQ, which follows
    /// whatever a SUBSCRIBE before it sent. An arriving message is acknowledged.
    private static boolean retainedArrives(RawSession subscriber) throws Exception {
        subscriber.send(new Packet.PingReq());
        Packet first = subscriber.expect(Packet.class);
        if (first instanceof Packet.Publish delivery) {
            assertThat(delivery.retain()).isTrue();
            subscriber.send(new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY));
            subscriber.expect(Packet.PingResp.class);
            return true;
        }
        assertThat(first).isInstanceOf(Packet.PingResp.class);
        return false;
    }

    /// The topics of the `count` retained messages that a new QoS 0 subscription to `filter`
    /// receives, after checking that no other message follows them.
    private static List<String> retainedTopics(int port, String clientId, String filter, int count)
            throws Exception {
        try (RawSession subscriber = connected(port, clientId)) {
            subscriber.subscribe(filter, AT_MOST_ONCE);
            var topics = new ArrayList<String>();
            for (int received = 0; received < count; received++) {
                topics.add(subscriber.expect(Packet.Publish.class).topic().name());
            }
            subscriber.ping();
            return topics;
        }
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
