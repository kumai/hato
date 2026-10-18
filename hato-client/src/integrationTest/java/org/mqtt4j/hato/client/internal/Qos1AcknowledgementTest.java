package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.client;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.endpoint;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// A raw TCP peer makes the acknowledgement boundary visible independently of broker policy.
class Qos1AcknowledgementTest {

    @Test
    @Timeout(30)
    void closing_a_deep_pull_queue_abandons_queued_work_but_keeps_a_handed_off_manual_ack()
            throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Subscription subscription = peer.subscribePull(sut, "data", AckMode.MANUAL, 512);
            peer.publish("data", 1);
            MqttDelivery first = received(subscription.receive(Duration.ofSeconds(5)));
            for (int queued = 0; queued < 320; queued += 64) {
                peer.publishBurst("data", 2 + queued, 64, false);
                peer.roundTripBarrier(sut);
            }

            // Act
            subscription.close();

            // Assert: no queued message is handed off, and the first manual ack still gates all.
            assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
            var unsubscribe = (Packet.Unsubscribe) peer.receive();
            peer.expectPublicationBarrier(sut);
            first.ack();
            peer.expectAcknowledgements(1, 321);
            peer.send(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            subscription.unsubscribe().get(5, SECONDS);
            peer.publish("data", 322);
            peer.expectAcknowledgements(322, 1);
            peer.expectPublicationBarrier(sut);
        }
    }

    @Test
    @Timeout(30)
    void pull_buffers_pre_suback_input_and_diagnostics_do_not_consume_or_acknowledge_it()
            throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("data"), Qos.AT_LEAST_ONCE)
            );
            var subscribe = (Packet.Subscribe) peer.receive();
            peer.publish("data", 7);
            peer.send(new Packet.SubAck(subscribe.packetId(),
                    List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
            subscription.established().get(5, SECONDS);

            // Act & Assert: formatting a failed assertion must leave the queued delivery alone.
            assertThat(subscription).isNotInstanceOf(Iterable.class);
            assertThatThrownBy(() -> assertThat(subscription).isNull())
                    .isInstanceOf(AssertionError.class);
            peer.expectPublicationBarrier(sut);
            assertThat(received(subscription.receive()).message().payload())
                    .containsExactly((byte) 1);
            peer.expectAcknowledgements(7, 1);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            peer.expectPublicationBarrier(sut);
        }
    }

    @Test
    void manual_pull_acknowledgements_follow_wire_receive_order_after_reverse_completion()
            throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Subscription subscription = peer.subscribePull(sut, "data", AckMode.MANUAL, 128);
            peer.publishBurst("data", 1, 100, false);
            var deliveries = new ArrayList<MqttDelivery>();
            for (int i = 0; i < 100; i++) {
                deliveries.add(received(subscription.receive(Duration.ofSeconds(5))));
            }
            peer.expectPublicationBarrier(sut);

            // Act & Assert: all later completions still wait for the first publication.
            for (int i = 99; i > 0; i--) {
                deliveries.get(i).ack();
            }
            peer.expectPublicationBarrier(sut);
            deliveries.getFirst().ack();
            peer.expectAcknowledgements(1, 100);
            deliveries.forEach(MqttDelivery::ack);
            peer.expectPublicationBarrier(sut);
        }
    }

    @Test
    void a_slow_ack_head_does_not_hold_completed_pull_payload_capacity() throws Exception {
        // Arrange: a two-slot pull queue drains while another subscription holds all PUBACKs.
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Subscription head = peer.subscribePull(sut, "head", AckMode.MANUAL, 1);
            Subscription tail = peer.subscribePull(sut, "tail", AckMode.ON_RECEIVE, 2);
            peer.publish("head", 1);
            MqttDelivery first = received(head.receive(Duration.ofSeconds(5)));

            // Act
            for (int id = 2; id <= 201; id++) {
                peer.publish("tail", id);
                assertThat(tail.receive(Duration.ofSeconds(5)))
                        .isInstanceOf(ReceiveResult.Received.class);
            }

            // Assert: receive capacity is reusable, but wire order remains intact.
            peer.expectPublicationBarrier(sut);
            first.ack();
            peer.expectAcknowledgements(1, 201);
            peer.expectPublicationBarrier(sut);
        }
    }

    @Test
    void an_inflight_retransmission_does_not_fill_a_manual_pull_queue() throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            Subscription subscription = peer.subscribePull(sut, "data", AckMode.MANUAL, 2);
            peer.publish("data", 7);
            MqttDelivery first = received(subscription.receive(Duration.ofSeconds(5)));

            // Act: keep the message unacknowledged across several retransmissions.
            for (int i = 0; i < 32; i++) {
                peer.publishBurst("data", 7, 1, true);
            }
            peer.send(new Packet.Publish(new Topic("data"), new byte[] {9}, Qos.AT_MOST_ONCE,
                    false, false, 0, EMPTY));

            // Assert: ordered input reaches the sentinel without any duplicate handoff or ACK.
            assertThat(received(subscription.receive(Duration.ofSeconds(5))).message().qos())
                    .isEqualTo(Qos.AT_MOST_ONCE);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            peer.expectPublicationBarrier(sut);
            first.ack();
            peer.expectAcknowledgements(7, 1);
        }
    }

    @ParameterizedTest
    @EnumSource(AckMode.class)
    void a_hundred_contiguous_publications_are_acknowledged_without_disconnect(AckMode ackMode)
            throws Exception {
        // Arrange
        var handled = new CountDownLatch(100);
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                peer.subscribe(sut, "data", ackMode, 128, delivery -> {
                    delivery.ack();
                    handled.countDown();
                });

                // Act: one socket write presents more acknowledgements than the send reserve.
                peer.publishBurst("data", 1, 100, false);

                // Assert
                peer.expectAcknowledgements(1, 100);
                assertThat(handled.await(5, SECONDS)).isTrue();
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void a_hundred_unmatched_publications_are_acknowledged_without_disconnect() throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                // Act
                peer.publishBurst("unmatched", 1, 100, false);

                // Assert
                peer.expectAcknowledgements(1, 100);
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void repeated_inflight_retransmissions_do_not_add_deliveries_or_acknowledgements()
            throws Exception {
        // Arrange
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var outstanding = new ArrayList<MqttDelivery>();
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                peer.subscribe(sut, "data", AckMode.MANUAL, 64, received::add);
                peer.publishBurst("data", 1, 32, false);
                for (int i = 0; i < 32; i++) {
                    outstanding.add(requireNonNull(received.poll(5, SECONDS)));
                }

                for (int retry = 0; retry < 5; retry++) {
                    // Act: repeat an entire broker inflight window before any manual ack.
                    peer.publishBurst("data", 1, 32, true);
                    peer.send(new Packet.Publish(
                            new Topic("data"), new byte[] {0}, Qos.AT_MOST_ONCE,
                            false, false, 0, EMPTY
                    ));

                    // Assert: the lane sentinel follows every preceding publication callback.
                    assertThat(requireNonNull(received.poll(5, SECONDS)).message().qos())
                            .isEqualTo(Qos.AT_MOST_ONCE);
                    peer.expectPublicationBarrier(sut);
                    assertThat(received).isEmpty();
                }

                // Act
                outstanding.forEach(MqttDelivery::ack);

                // Assert: each original transaction produces exactly one ordered PUBACK.
                peer.expectAcknowledgements(1, 32);
                outstanding.forEach(MqttDelivery::ack);
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void a_packet_id_reused_after_puback_is_new_and_old_handles_cannot_ack_it()
            throws Exception {
        // Arrange
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                peer.subscribe(sut, "data", AckMode.MANUAL, received::add);
                peer.publish("data", 7);
                MqttDelivery first = requireNonNull(received.poll(5, SECONDS));
                first.ack();
                peer.expectAcknowledgements(7, 1);

                // Act: reuse immediately after observing the PUBACK, possibly before the client
                // has processed its own write completion.
                peer.send(new Packet.Publish(
                        new Topic("data"), new byte[] {2}, Qos.AT_LEAST_ONCE,
                        false, false, 7, EMPTY
                ));
                MqttDelivery next = requireNonNull(received.poll(5, SECONDS));

                // Assert: a publication without DUP is new even while the old write is unreported.
                assertThat(next.message().payload()).containsExactly((byte) 2);
                assertThat(next.dup()).isFalse();
                first.ack();
                peer.expectPublicationBarrier(sut);
                next.ack();
                peer.expectAcknowledgements(7, 1);
                first.ack();
                next.ack();
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void unsubscribing_a_deep_queue_releases_acknowledgements_without_mailbox_overflow()
            throws Exception {
        // Arrange: retain one running handler while 320 more deliveries queue behind it.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var handled = new AtomicInteger();
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                Subscription subscription = peer.subscribe(
                        sut, "data", AckMode.ON_RECEIVE, 512, _ -> {
                            handled.incrementAndGet();
                            entered.countDown();
                            waitFor(release);
                        }
                );
                peer.publish("data", 1);
                assertThat(entered.await(5, SECONDS)).isTrue();
                peer.expectAcknowledgements(1, 1);
                for (int queued = 0; queued < 320; queued += 64) {
                    peer.publishBurst("data", 2 + queued, 64, false);
                    peer.roundTripBarrier(sut);
                }

                // Act: one UNSUBACK discards every queued recipient in the same actor turn.
                CompletableFuture<Void> removed = subscription.unsubscribe();
                var unsubscribe = (Packet.Unsubscribe) peer.receive();
                peer.send(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
                removed.get(5, SECONDS);

                // Assert: readiness coalescing and send admission both drain the full queue.
                peer.expectAcknowledgements(2, 320);
                peer.expectPublicationBarrier(sut);
                assertThat(handled).hasValue(1);
                release.countDown();
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void on_receive_acknowledges_while_the_handler_is_still_running() throws Exception {
        // Arrange
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                peer.subscribe(sut, "data", AckMode.ON_RECEIVE, _ -> {
                    entered.countDown();
                    waitFor(release);
                });

                // Act
                peer.publish("data", 7);
                assertThat(entered.await(5, SECONDS)).isTrue();

                // Assert: PUBACK arrives while the handler is still blocked.
                assertThat(peer.receive())
                        .isEqualTo(new Packet.PubAck(7, ReasonCode.SUCCESS, EMPTY));
                release.countDown();
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void after_handler_acknowledges_only_after_the_handler_returns() throws Exception {
        // Arrange
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                peer.subscribe(sut, "data", AckMode.AFTER_HANDLER, _ -> {
                    entered.countDown();
                    waitFor(release);
                });
                peer.publish("data", 7);
                assertThat(entered.await(5, SECONDS)).isTrue();

                // Act & Assert: a later outgoing publication cannot be preceded by PUBACK yet.
                peer.expectPublicationBarrier(sut);

                // Act
                release.countDown();

                // Assert
                assertThat(peer.receive())
                        .isEqualTo(new Packet.PubAck(7, ReasonCode.SUCCESS, EMPTY));
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void manual_acknowledgements_leave_the_wire_in_receive_order() throws Exception {
        // Arrange
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                peer.subscribe(sut, "data", AckMode.MANUAL, received::add);
                peer.publish("data", 7);
                peer.publish("data", 2);
                MqttDelivery first = requireNonNull(received.poll(5, SECONDS));
                MqttDelivery second = requireNonNull(received.poll(5, SECONDS));

                // Act & Assert: acknowledging the second delivery cannot overtake the first.
                second.ack();
                peer.expectPublicationBarrier(sut);

                // Act
                first.ack();

                // Assert: wire order follows reception, not packet ids or application completion.
                assertThat(peer.receive())
                        .isEqualTo(new Packet.PubAck(7, ReasonCode.SUCCESS, EMPTY));
                assertThat(peer.receive())
                        .isEqualTo(new Packet.PubAck(2, ReasonCode.SUCCESS, EMPTY));

                // Act & Assert: repeated acknowledgement does not add another PUBACK.
                first.ack();
                second.ack();
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void a_failed_after_handler_delivery_closes_without_puback() throws Exception {
        // Arrange
        var failure = new IllegalStateException("Application did not finish processing");
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (
                ServerSocket server = listen();
                MqttClient sut = MqttClient.builder()
                        .server(endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .connectionListener(ended::add)
                        .build()
        ) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                peer.subscribe(sut, "data", AckMode.AFTER_HANDLER, _ -> {
                    throw failure;
                });

                // Act
                peer.publish("data", 7);

                // Assert: EOF is the first response to the failed delivery.
                peer.expectClosed();
                assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            }
        }
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, SECONDS)).isTrue();
        } catch (InterruptedException cause) {
            Thread.currentThread().interrupt();
            throw new AssertionError(cause);
        }
    }

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }
}
