package org.mqtt4j.mqtj.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.mqtj.*;
import org.mqtt4j.mqtj.client.internal.common.io.FrameReader;
import org.mqtt4j.mqtj.client.internal.common.io.FrameWriter;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketCodec;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;

/// A raw TCP peer makes the acknowledgement boundary visible independently of broker policy.
class Qos1AcknowledgementTest {

    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    @ParameterizedTest
    @EnumSource(AckMode.class)
    void a_hundred_contiguous_publications_are_acknowledged_without_disconnect(AckMode ackMode)
            throws Exception {
        // Arrange
        var handled = new CountDownLatch(100);
        try (ServerSocket server = listen(); MqttClient sut = client(server)) {
            try (Peer peer = Peer.connect(server, sut)) {
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
            try (Peer peer = Peer.connect(server, sut)) {
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
            try (Peer peer = Peer.connect(server, sut)) {
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
            try (Peer peer = Peer.connect(server, sut)) {
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
            try (Peer peer = Peer.connect(server, sut)) {
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
            try (Peer peer = Peer.connect(server, sut)) {
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
            try (Peer peer = Peer.connect(server, sut)) {
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
            try (Peer peer = Peer.connect(server, sut)) {
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
                        .connectionListener(ended::add)
                        .build()
        ) {
            try (Peer peer = Peer.connect(server, sut)) {
                peer.subscribe(sut, "data", AckMode.AFTER_HANDLER, _ -> {
                    throw failure;
                });

                // Act
                peer.publish("data", 7);

                // Assert: EOF is the first response to the failed delivery.
                assertThat(peer.reader.read()).isEmpty();
                assertThat(ended.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));
            }
        }
    }

    private static MqttClient client(ServerSocket server) {
        return MqttClient.builder()
                .server(endpoint(server))
                .version(MqttVersion.V3_1_1)
                .build();
    }

    private static ServerSocket listen() throws IOException {
        var server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        server.setSoTimeout(5000);
        return server;
    }

    private static URI endpoint(ServerSocket server) {
        return URI.create("mqtt://127.0.0.1:" + server.getLocalPort());
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, SECONDS)).isTrue();
        } catch (InterruptedException cause) {
            Thread.currentThread().interrupt();
            throw new AssertionError(cause);
        }
    }

    private static final class Peer implements AutoCloseable {

        private final Socket socket;
        private final FrameReader reader;
        private final FrameWriter writer;

        private Peer(Socket socket) throws IOException {
            this.socket = socket;
            socket.setSoTimeout(5000);
            reader = new FrameReader(socket.getInputStream(), 1024 * 1024);
            writer = new FrameWriter(socket.getOutputStream());
        }

        static Peer connect(ServerSocket server, MqttClient sut) throws Exception {
            CompletableFuture<ConnAck> connected = sut.connect();
            var peer = new Peer(server.accept());
            try {
                assertThat(peer.receive()).isInstanceOf(Packet.Connect.class);
                peer.send(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));
                connected.get(5, SECONDS);
                return peer;
            } catch (Exception | AssertionError failure) {
                peer.close();
                throw failure;
            }
        }

        Subscription subscribe(
                MqttClient sut,
                String topic,
                AckMode ackMode,
                Consumer<MqttDelivery> handler
        ) throws Exception {
            return subscribe(sut, topic, ackMode, 256, handler);
        }

        Subscription subscribe(
                MqttClient sut,
                String topic,
                AckMode ackMode,
                int capacity,
                Consumer<MqttDelivery> handler
        ) throws Exception {
            CompletableFuture<Subscription> subscribed = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
                            .withAckMode(ackMode)
                            .withBufferCapacity(capacity),
                    handler
            );
            var request = (Packet.Subscribe) receive();
            send(new Packet.SubAck(request.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
            return subscribed.get(5, SECONDS);
        }

        void publish(String topic, int packetId) throws IOException {
            send(new Packet.Publish(
                    new Topic(topic), new byte[] {1}, Qos.AT_LEAST_ONCE,
                    false, false, packetId, EMPTY
            ));
        }

        void publishBurst(String topic, int first, int count, boolean dup) throws IOException {
            var bytes = new ByteArrayOutputStream();
            for (int id = first; id < first + count; id++) {
                bytes.writeBytes(codec.encode(new Packet.Publish(
                        new Topic(topic), new byte[] {(byte) id}, Qos.AT_LEAST_ONCE,
                        false, dup, id, EMPTY
                )));
            }
            socket.getOutputStream().write(bytes.toByteArray());
        }

        void expectAcknowledgements(int first, int count) throws IOException {
            for (int id = first; id < first + count; id++) {
                assertThat(receive()).isEqualTo(new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY));
            }
        }

        void roundTripBarrier(MqttClient sut) throws Exception {
            CompletableFuture<PublishResult> published = sut.publish(new MqttMessage(
                    new Topic("barrier"), new byte[] {0}, Qos.AT_LEAST_ONCE, false
            ));
            var publication = (Packet.Publish) receive();
            assertThat(publication.topic()).isEqualTo(new Topic("barrier"));
            send(new Packet.PubAck(publication.packetId(), ReasonCode.SUCCESS, EMPTY));
            // This PUBACK follows all preceding input in the same byte stream and mailbox.
            published.get(5, SECONDS);
        }

        void expectPublicationBarrier(MqttClient sut) throws Exception {
            sut.publish(MessagingTest.message("barrier", (byte) 0)).get(5, SECONDS);
            assertThat(receive()).isInstanceOfSatisfying(
                    Packet.Publish.class,
                    publication -> assertThat(publication.topic()).isEqualTo(new Topic("barrier"))
            );
        }

        Packet receive() throws IOException {
            return codec.decode(reader.read().orElseThrow(() -> new AssertionError(
                    "The client closed the connection before sending the expected packet"
            )));
        }

        void send(Packet packet) throws IOException {
            writer.write(codec.encode(packet));
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
