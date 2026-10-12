package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.BrokerQos2Test.acknowledge;
import static org.mqtt4j.hato.broker.internal.BrokerQos2Test.receive;
import static org.mqtt4j.hato.broker.internal.BrokerSessionRaceTest.persistent;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

class BrokerQos2OrderTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("orders")
    void qos_two_stages_and_queued_order_survive_legal_event_orders(List<Event> events)
            throws Exception {
        // Arrange: repeated ACK events advance the original exchange in legal protocol order.
        var timer = new BrokerDeadlineTest.ManualTimer();
        RawSession subscriber = null;
        try (
                BrokerServer sut = BrokerServer.builder()
                        .maxInflight(1)
                        .scheduler(timer)
                        .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession publisher = connected(sut.port(), "publisher")
        ) {
            subscriber = persistent(sut.port(), false);
            subscriber.subscribe("ordered", EXACTLY_ONCE);
            publisher.publish("ordered", "original", EXACTLY_ONCE, 7);
            Packet.Publish original = expectDelivery(subscriber, "original");
            Packet.Publish inflight = original;
            boolean received = false;
            boolean completed = false;
            boolean published = false;

            // Act & Assert
            for (Event event : events) {
                switch (event) {
                    case ACK -> {
                        if (!received) {
                            receive(subscriber, original.packetId());
                            received = true;
                        } else {
                            subscriber.send(new Packet.PubComp(
                                    original.packetId(),
                                    SUCCESS,
                                    EMPTY
                            ));
                            completed = true;
                            inflight = published ? expectDelivery(subscriber, "second") : null;
                        }
                    }
                    case PUBLISH -> {
                        publisher.publish("ordered", "second", EXACTLY_ONCE, 8);
                        publisher.publish("ordered", "third", EXACTLY_ONCE, 9);
                        published = true;
                        if (completed) {
                            inflight = expectDelivery(subscriber, "second");
                        }
                    }
                    case TAKEOVER, TIMEOUT -> {
                        try (RawSession old = subscriber) {
                            if (event == Event.TIMEOUT) {
                                timer.clock.advance(Duration.ofMillis(1500));
                                old.expectClosed();
                            }
                            subscriber = persistent(sut.port(), true);
                            if (event == Event.TAKEOVER) {
                                old.expectClosed();
                            }
                            if (!completed && received) {
                                // Ownership already moved; this exchange cannot replay PUBLISH.
                                assertThat(subscriber.expect(Packet.PubRel.class))
                                        .isEqualTo(new Packet.PubRel(
                                                original.packetId(),
                                                SUCCESS,
                                                EMPTY
                                        ));
                            } else if (inflight != null) {
                                assertThat(subscriber.expect(Packet.Publish.class))
                                        .isEqualTo(new Packet.Publish(
                                                inflight.topic(),
                                                inflight.payload(),
                                                EXACTLY_ONCE,
                                                false,
                                                true,
                                                inflight.packetId(),
                                                EMPTY
                                        ));
                            }
                        }
                    }
                }
                // Extra replays, completed exchanges, and early queued deliveries fail here.
                subscriber.ping();
                assertThat(timer.clock.pendingTaskCount()).isEqualTo(1);
            }

            assertThat(inflight).isNotNull();
            assertThat(inflight).isEqualTo(publication(
                    "ordered",
                    "second",
                    EXACTLY_ONCE,
                    inflight.packetId()
            ));
            acknowledge(subscriber, inflight);
            acknowledge(subscriber, expectDelivery(subscriber, "third"));
            subscriber.ping();
            subscriber.disconnect();
            assertThat(timer.clock.pendingTaskCount()).isZero();
        } finally {
            if (subscriber != null) {
                subscriber.close();
            }
        }
    }

    private static Packet.Publish expectDelivery(RawSession subscriber, String payload)
            throws Exception {
        Packet.Publish delivery = subscriber.expect(Packet.Publish.class);
        assertThat(delivery.packetId()).isPositive();
        assertThat(delivery)
                .isEqualTo(publication("ordered", payload, EXACTLY_ONCE, delivery.packetId()));
        return delivery;
    }

    /// Every distinct order of the events. The two ACKs are alike, so there are 5!/2! = 60.
    private static Stream<List<Event>> orders() {
        return orders(List.of(
                Event.ACK,
                Event.ACK,
                Event.PUBLISH,
                Event.TAKEOVER,
                Event.TIMEOUT
        ));
    }

    private static Stream<List<Event>> orders(List<Event> remaining) {
        if (remaining.isEmpty()) {
            return Stream.of(List.of());
        }
        return remaining.stream().distinct().flatMap(first -> {
            var rest = new ArrayList<>(remaining);
            rest.remove(first);
            return orders(rest)
                    .map(tail -> Stream.concat(Stream.of(first), tail.stream()).toList());
        });
    }

    private enum Event { ACK, PUBLISH, TAKEOVER, TIMEOUT }
}
