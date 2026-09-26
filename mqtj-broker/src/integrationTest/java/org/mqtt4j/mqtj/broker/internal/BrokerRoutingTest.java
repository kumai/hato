package org.mqtt4j.mqtj.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.mqtj.broker.MqttBroker;
import org.mqtt4j.mqtj.broker.internal.common.concurrent.TimerService;
import org.mqtt4j.mqtj.broker.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.broker.internal.common.value.Qos;
import org.mqtt4j.mqtj.broker.internal.common.value.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.mqtj.broker.internal.RawSession.connected;
import static org.mqtt4j.mqtj.broker.internal.RawSession.publication;
import static org.mqtt4j.mqtj.broker.internal.RawSession.subscription;
import static org.mqtt4j.mqtj.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.mqtj.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.mqtj.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.mqtj.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.mqtj.broker.internal.common.value.ReasonCode.GRANTED_QOS_1;
import static org.mqtt4j.mqtj.broker.internal.common.value.ReasonCode.SUCCESS;

class BrokerRoutingTest {

    @Test
    void subscriptions_are_granted_in_request_order_with_a_maximum_qos_of_one() throws Exception {
        try (var sut = MqttBroker.start(); var client = connected(sut.port(), "grants")) {
            client.send(new Packet.Subscribe(7, List.of(
                    subscription("zero", AT_MOST_ONCE),
                    subscription("one", AT_LEAST_ONCE),
                    subscription("two", EXACTLY_ONCE)
            ), EMPTY));
            // [MQTT-3.9.3-1], [MQTT-3.8.4-5].
            assertThat(client.expect(Packet.SubAck.class))
                    .isEqualTo(new Packet.SubAck(
                            7, List.of(SUCCESS, GRANTED_QOS_1, GRANTED_QOS_1), EMPTY
                    ));
        }
    }

    @ParameterizedTest
    @CsvSource({"0, 0", "0, 1", "1, 0", "1, 1"})
    void deliveries_use_the_smaller_of_publication_and_granted_qos(int sentQos, int grantedQos)
            throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("sensors/+", Qos.of(grantedQos));
            publisher.publish("sensors/temperature", "21", Qos.of(sentQos), sentQos == 0 ? 0 : 7);
            // [MQTT-3.8.4-6]. The broker allocates its own outgoing identifier.
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            Qos expected = Qos.of(Math.min(sentQos, grantedQos));
            assertThat(delivery).isEqualTo(publication(
                    "sensors/temperature", "21", expected, delivery.packetId()
            ));
            if (expected == AT_LEAST_ONCE) {
                subscriber.send(new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY));
            }
            subscriber.ping();
            publisher.ping();
        }
    }

    @Test
    void a_qos_one_publication_is_acknowledged_after_its_deliveries_are_queued()
            throws Exception {
        try (var sut = MqttBroker.start(); var client = connected(sut.port(), "echo")) {
            client.subscribe("echo", AT_MOST_ONCE);
            // The publisher's own delivery shares its send FIFO, so the order is observable.
            client.send(publication("echo", "self", AT_LEAST_ONCE, 7));
            assertThat(client.expect(Packet.Publish.class))
                    .isEqualTo(publication("echo", "self", AT_MOST_ONCE, 0));
            assertThat(client.expect(Packet.PubAck.class))
                    .isEqualTo(new Packet.PubAck(7, SUCCESS, EMPTY));
        }
    }

    @Test
    void unsupported_qos_two_closes_the_publisher_before_routing() throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("unsupported", AT_MOST_ONCE);
            publisher.send(publication("unsupported", "qos two", EXACTLY_ONCE, 7));
            publisher.expectClosed();
            subscriber.ping();
        }
    }

    @Test
    void retain_publications_are_forwarded_without_retaining_or_replaying_them() throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("transient", AT_MOST_ONCE);
            Packet.Publish message = publication("transient", "current", AT_LEAST_ONCE, 7);
            publisher.send(new Packet.Publish(
                    message.topic(), message.payload(), message.qos(), true, false, 7, EMPTY
            ));
            assertThat(publisher.expect(Packet.PubAck.class))
                    .isEqualTo(new Packet.PubAck(7, SUCCESS, EMPTY));
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("transient", "current", AT_MOST_ONCE, 0));

            // Version 0.2 forwards the message but intentionally has no retained storage.
            try (var later = connected(sut.port(), "later")) {
                later.subscribe("transient", AT_MOST_ONCE);
                later.ping();
            }
        }
    }

    @Test
    void overlapping_subscriptions_receive_one_copy_at_the_highest_matching_qos() throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("sensors/#", AT_MOST_ONCE);
            subscriber.subscribe("sensors/+", AT_LEAST_ONCE);
            publisher.publish("sensors/temperature", "21", AT_LEAST_ONCE, 9);
            // [MQTT-3.3.5-1]: one copy, at the highest matching subscription QoS.
            Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
            assertThat(delivery.qos()).isEqualTo(AT_LEAST_ONCE);
            subscriber.send(new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY));
            subscriber.ping();
        }
    }

    @Test
    void subscribing_again_replaces_the_existing_grant() throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("sensors/#", AT_LEAST_ONCE);
            subscriber.subscribe("sensors/#", AT_MOST_ONCE);
            publisher.publish("sensors/temperature", "21", AT_LEAST_ONCE, 9);
            // [MQTT-3.8.4-3]: the former QoS 1 subscription no longer applies.
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("sensors/temperature", "21", AT_MOST_ONCE, 0));
            subscriber.ping();
        }
    }

    @Test
    void unsubscribe_stops_new_deliveries_but_allows_acknowledging_an_existing_delivery()
            throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("sensors/#", AT_LEAST_ONCE);
            publisher.publish("sensors/temperature", "before", AT_LEAST_ONCE, 9);
            Packet.Publish pending = subscriber.expect(Packet.Publish.class);
            subscriber.send(new Packet.Unsubscribe(
                    23, List.of(new TopicFilter("sensors/#"), new TopicFilter("absent")), EMPTY
            ));
            assertThat(subscriber.expect(Packet.UnsubAck.class))
                    .isEqualTo(new Packet.UnsubAck(23, List.of(), EMPTY));
            // [MQTT-3.10.4-3]: an already-started QoS 1 delivery can still complete.
            subscriber.send(new Packet.PubAck(pending.packetId(), SUCCESS, EMPTY));
            publisher.publish("sensors/temperature", "after", AT_LEAST_ONCE, 9);
            subscriber.ping();
        }
    }

    @Test
    void share_prefixes_are_literal_topic_levels_in_the_minimal_broker() throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("$share/team/sensors/#", AT_MOST_ONCE);
            publisher.publish("sensors/temperature", "unmatched", AT_LEAST_ONCE, 1);
            subscriber.ping();
            publisher.publish("$share/team/sensors/temperature", "matched", AT_LEAST_ONCE, 2);
            assertThat(subscriber.expect(Packet.Publish.class)).isEqualTo(publication(
                    "$share/team/sensors/temperature", "matched", AT_MOST_ONCE, 0
            ));
        }
    }

    @Test
    void messages_from_one_publisher_reach_each_subscriber_in_order() throws Exception {
        try (var sut = MqttBroker.start();
                var first = connected(sut.port(), "first");
                var second = connected(sut.port(), "second");
                var publisher = connected(sut.port(), "publisher")) {
            first.subscribe("ordered", AT_MOST_ONCE);
            second.subscribe("ordered", AT_MOST_ONCE);
            for (int i = 0; i < 20; i++) {
                publisher.publish("ordered", Integer.toString(i), AT_MOST_ONCE, 0);
            }
            // [MQTT-4.6.0-6].
            for (int i = 0; i < 20; i++) {
                Packet.Publish expected = publication(
                        "ordered", Integer.toString(i), AT_MOST_ONCE, 0
                );
                assertThat(first.expect(Packet.Publish.class)).isEqualTo(expected);
                assertThat(second.expect(Packet.Publish.class)).isEqualTo(expected);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_reused_incoming_identifier_after_puback_is_a_new_publication(boolean dup)
            throws Exception {
        try (var sut = MqttBroker.start();
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("reuse", AT_MOST_ONCE);
            publisher.publish("reuse", "first", AT_LEAST_ONCE, 7);
            assertThat(subscriber.expect(Packet.Publish.class).payload())
                    .isEqualTo("first".getBytes(UTF_8));
            Packet.Publish next = publication("reuse", "second", AT_LEAST_ONCE, 7);
            // [MQTT-4.3.2-2]: after PUBACK even DUP does not make it the old publication.
            publisher.send(new Packet.Publish(
                    next.topic(), next.payload(), next.qos(), false, dup, next.packetId(), EMPTY
            ));
            assertThat(publisher.expect(Packet.PubAck.class))
                    .isEqualTo(new Packet.PubAck(7, SUCCESS, EMPTY));
            assertThat(subscriber.expect(Packet.Publish.class))
                    .isEqualTo(publication("reuse", "second", AT_MOST_ONCE, 0));
        }
    }

    @Test
    void puback_releases_inflight_capacity_for_the_next_publication() throws Exception {
        try (var sut = limitedBroker(1024 * 1024, 1);
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("bounded", AT_LEAST_ONCE);
            for (int i = 0; i < 3; i++) {
                publisher.publish("bounded", Integer.toString(i), AT_LEAST_ONCE, 7);
                Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
                subscriber.send(new Packet.PubAck(delivery.packetId(), SUCCESS, EMPTY));
                // This barrier ensures the release precedes the next publication.
                subscriber.ping();
            }
        }
    }

    @Test
    void inflight_identifiers_are_distinct_and_only_the_overloaded_subscriber_is_closed()
            throws Exception {
        try (var sut = limitedBroker(1024 * 1024, 2);
                var subscriber = connected(sut.port(), "subscriber");
                var unaffected = connected(sut.port(), "unaffected");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("bounded", AT_LEAST_ONCE);
            unaffected.subscribe("bounded", AT_MOST_ONCE);
            publisher.publish("bounded", "first", AT_LEAST_ONCE, 7);
            Packet.Publish first = subscriber.expect(Packet.Publish.class);
            publisher.publish("bounded", "second", AT_LEAST_ONCE, 7);
            Packet.Publish second = subscriber.expect(Packet.Publish.class);
            // [MQTT-2.3.1-2]: outstanding identifiers cannot be reused.
            assertThat(second.packetId()).isNotEqualTo(first.packetId());
            publisher.publish("bounded", "overflow", AT_LEAST_ONCE, 7);
            subscriber.expectClosed();
            for (String message : List.of("first", "second", "overflow")) {
                assertThat(unaffected.expect(Packet.Publish.class))
                        .isEqualTo(publication("bounded", message, AT_MOST_ONCE, 0));
            }
            unaffected.ping();
            publisher.ping();
        }
    }

    @Test
    void a_delivery_larger_than_the_send_byte_budget_disconnects_its_subscriber() throws Exception {
        try (var sut = limitedBroker(64, 128);
                var subscriber = connected(sut.port(), "subscriber");
                var publisher = connected(sut.port(), "publisher")) {
            subscriber.subscribe("bounded", AT_MOST_ONCE);
            // A single oversized frame deterministically exceeds capacity without filling
            // kernel socket buffers or depending on when the writer is scheduled.
            publisher.publish("bounded", "x".repeat(128), AT_LEAST_ONCE, 7);
            subscriber.expectClosed();
            publisher.ping();
        }
    }

    private static BrokerServer limitedBroker(long sendBytes, int maxInflight) throws IOException {
        return BrokerServer.start(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                Duration.ofSeconds(5), sendBytes, maxInflight, TimerService.instance
        );
    }
}
