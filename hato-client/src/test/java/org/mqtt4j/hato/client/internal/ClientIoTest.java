package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.io.SendQueue;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.spi.Transport;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.client.internal.common.io.SendQueue.OfferResult.ACCEPTED;
import static org.mqtt4j.hato.client.internal.common.io.SendQueue.OfferResult.CLOSED;
import static org.mqtt4j.hato.client.internal.common.io.SendQueue.OfferResult.FULL;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class ClientIoTest {

    private final List<SingleWriteTransport> transports = new ArrayList<>();

    private final RecordingEvents events = new RecordingEvents();

    private final RecordingCodec codec = new RecordingCodec();

    private final ClientIo sut = new ClientIo(
            URI.create("mqtt://localhost"),
            "io-test",
            Duration.ofSeconds(30),
            16 * 1024 * 1024,
            _ -> {
                var transport = new SingleWriteTransport();
                transports.add(transport);
                return transport;
            },
            events,
            codec
    );

    @AfterEach
    void close() {
        sut.shutdown();
    }

    @Test
    void debug_describes_packets_without_payloads_and_trace_contains_wire_bytes() {
        // Arrange: capture the same System.Logger backend as applications using JUL.
        var jul = Logger.getLogger(ClientIo.class.getName());
        Level previousLevel = jul.getLevel();
        boolean previousParents = jul.getUseParentHandlers();
        var records = new CopyOnWriteArrayList<LogRecord>();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        jul.addHandler(handler);
        jul.setUseParentHandlers(false);
        var packet = new Packet.Publish(
                new Topic("a"),
                "payload-marker".getBytes(StandardCharsets.UTF_8),
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
        byte[] bytes = PacketCodec.of(MqttVersion.V3_1_1).encode(packet);
        String hex = HexFormat.of().formatHex(bytes);
        try {
            SingleWriteTransport transport = open(1);
            transport.completeInline = true;

            // Act: DEBUG describes admission, write completion, and decoded input.
            jul.setLevel(Level.FINE);
            sut.send(1, 1, packet, null);
            await().untilAsserted(() -> assertThat(events.tickets).containsExactly(1L));
            requireNonNull(transport.listener).onBytes(bytes);

            // Assert
            assertThat(records).extracting(LogRecord::getMessage).containsExactly(
                    "Queued Publish ticket 1 (" + bytes.length + " bytes)",
                    "Written ticket 1",
                    "Received Publish (" + bytes.length + " bytes)"
            );
            assertThat(records).allSatisfy(record -> {
                assertThat(record.getLevel()).isEqualTo(Level.FINE);
                assertThat(record.getMessage()).doesNotContain("payload-marker", hex);
            });

            // Act: TRACE additionally records attempted writes and incoming byte chunks.
            records.clear();
            jul.setLevel(Level.FINER);
            sut.send(1, 2, packet, null);
            await().untilAsserted(() -> assertThat(events.tickets).containsExactly(1L, 2L));
            requireNonNull(transport.listener).onBytes(bytes);

            // Assert
            assertThat(records.stream().filter(record -> record.getLevel() == Level.FINER))
                    .extracting(LogRecord::getMessage)
                    .containsExactly(
                            "Write attempt for ticket 2: " + hex,
                            "Received bytes: " + hex
                    );

            // Act & Assert: disabled levels emit neither packet summaries nor bytes.
            records.clear();
            jul.setLevel(Level.OFF);
            sut.send(1, 3, packet, null);
            await().untilAsserted(() -> assertThat(events.tickets).containsExactly(1L, 2L, 3L));
            requireNonNull(transport.listener).onBytes(bytes);
            assertThat(records).isEmpty();
        } finally {
            jul.removeHandler(handler);
            jul.setLevel(previousLevel);
            jul.setUseParentHandlers(previousParents);
        }
    }

    @ParameterizedTest
    @MethodSource("sendCapacities")
    void admission_counts_the_active_write_and_returns_full_without_ending_the_connection(
            Packet packet,
            int limit
    ) {
        // Arrange
        SingleWriteTransport transport = open(1);
        for (int i = 1; i <= limit; i++) {
            assertThat(sut.send(1, i, packet, null)).isEqualTo(ACCEPTED);
        }
        assertThat(events.failures).isEmpty();

        // Act
        SendQueue.OfferResult result = sut.send(1, limit + 1, packet, null);

        // Assert: the transport receives only one write and has no capacity policy.
        assertThat(result).isEqualTo(FULL);
        await().untilAsserted(() -> assertThat(transport.attempts).containsExactly(1L));
        assertThat(events.failures).isEmpty();
        assertThat(events.ends).isEmpty();
        assertThat(transport.rejectSend).isFalse();

        // Act & Assert: completing one write permits a new send after the refusal.
        transport.complete();
        await().untilAsserted(() -> assertThat(transport.attempts).containsExactly(1L, 2L));
        assertThat(sut.send(1, limit + 2, packet, null)).isEqualTo(ACCEPTED);
    }

    @Test
    void required_sends_keep_their_reservation_and_fifo_when_regular_operations_fill_the_queue() {
        // Arrange
        SingleWriteTransport transport = open(1);
        for (int i = 1; i <= 64; i++) {
            assertThat(sut.send(1, i, subscribe(), null)).isEqualTo(ACCEPTED);
        }
        for (int i = 65; i <= 128; i++) {
            assertThat(sut.send(1, i, unsubscribe(), null)).isEqualTo(ACCEPTED);
        }

        // Act
        assertThat(sut.send(1, 129, publish(), null)).isEqualTo(FULL);
        assertThat(sut.send(1, 130, new Packet.PubAck(1, ReasonCode.SUCCESS, EMPTY), null))
                .isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 131, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 132, new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY), null))
                .isEqualTo(ACCEPTED);
        transport.awaitActive();
        transport.completeInline = true;
        transport.complete();
        await().untilAsserted(() -> assertThat(events.tickets).hasSize(131));

        // Assert
        assertThat(events.failures).isEmpty();
        assertThat(transport.attempts).containsExactlyElementsOf(
                LongStream.rangeClosed(1, 132).filter(ticket -> ticket != 129).boxed().toList()
        );
        assertThat(events.tickets).containsExactlyElementsOf(transport.attempts);
    }

    @Test
    void pubacks_leave_capacity_for_connection_control_without_reordering_writes() {
        // Arrange
        SingleWriteTransport transport = open(1);
        var acknowledgement = new Packet.PubAck(1, ReasonCode.SUCCESS, EMPTY);
        for (int i = 1; i <= 14; i++) {
            assertThat(sut.send(1, i, acknowledgement, null)).isEqualTo(ACCEPTED);
        }

        // Act
        assertThat(sut.send(1, 15, acknowledgement, null)).isEqualTo(FULL);
        assertThat(sut.send(1, 16, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 17, new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY), null))
                .isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 18, acknowledgement, null)).isEqualTo(FULL);
        transport.awaitActive();
        transport.completeInline = true;
        transport.complete();

        // Assert
        await().untilAsserted(() -> assertThat(events.tickets).hasSize(16));
        assertThat(transport.attempts).containsExactlyElementsOf(
                LongStream.rangeClosed(1, 17).filter(ticket -> ticket != 15).boxed().toList()
        );
        assertThat(events.ends).isEmpty();
        assertThat(events.failures).isEmpty();
    }

    @Test
    void puback_admission_keeps_byte_headroom_while_a_large_connect_is_active() {
        // Arrange: the CONNECT consumes just over half of the reserved byte budget.
        SingleWriteTransport transport = open(1);
        var connect = new Packet.Connect(
                "c".repeat(65_535), MqttVersion.V3_1_1, true, 60, null, null, null, EMPTY
        );
        var acknowledgement = new Packet.PubAck(1, ReasonCode.SUCCESS, EMPTY);
        assertThat(sut.send(1, 1, connect, null)).isEqualTo(ACCEPTED);
        transport.awaitActive();

        // Act & Assert: PUBACK waits while connection control still has room.
        assertThat(sut.send(1, 2, acknowledgement, null)).isEqualTo(FULL);
        assertThat(sut.send(1, 3, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);
        transport.complete();
        await().untilAsserted(() -> assertThat(events.tickets).containsExactly(1L));
        assertThat(events.capacityGenerations).containsExactly(1L);
        assertThat(sut.send(1, 4, acknowledgement, null)).isEqualTo(ACCEPTED);
        assertThat(events.ends).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(OldWriteRelease.class)
    void old_active_pubacks_share_headroom_and_return_capacity_to_the_current_generation(
            OldWriteRelease release
    ) {
        // Arrange: the old connection keeps one active reservation after close.
        var acknowledgement = new Packet.PubAck(1, ReasonCode.SUCCESS, EMPTY);
        SingleWriteTransport old = open(1);
        assertThat(sut.send(1, 1, acknowledgement, null)).isEqualTo(ACCEPTED);
        old.awaitActive();
        sut.close();
        SingleWriteTransport current = open(2);
        for (int i = 2; i <= 14; i++) {
            assertThat(sut.send(2, i, acknowledgement, null)).isEqualTo(ACCEPTED);
        }
        current.awaitActive();
        assertThat(sut.send(2, 15, acknowledgement, null)).isEqualTo(FULL);
        assertThat(events.capacityGenerations).isEmpty();

        // Act: either completion or final closure can release the old buffer.
        release.apply.accept(old);

        // Assert: no old protocol completion is needed to wake the new connection.
        assertThat(events.capacityGenerations).containsExactly(2L);
        assertThat(events.written).isEmpty();
        assertThat(events.tickets).isEmpty();
        assertThat(sut.send(2, 16, acknowledgement, null)).isEqualTo(ACCEPTED);
        assertThat(sut.send(2, 17, acknowledgement, null)).isEqualTo(FULL);
        assertThat(sut.send(2, 18, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);
        assertThat(sut.send(2, 19, new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY), null))
                .isEqualTo(ACCEPTED);
        assertThat(events.ends).isEmpty();
    }

    @ParameterizedTest(name = "payload bytes={0}, admitted={1}")
    @CsvSource({"8388608,1", "16777216,0"})
    void the_byte_limit_includes_headers_and_refusal_leaves_capacity_for_other_sends(
            int payloadSize,
            int admitted
    ) {
        // Arrange: two 8 MiB payloads, or one 16 MiB payload, exceed the encoded byte budget.
        SingleWriteTransport transport = open(1);
        MqttMessage message = message(payloadSize);
        for (int i = 1; i <= admitted; i++) {
            assertThat(sut.publish(1, i, message, 0, null)).isEqualTo(ACCEPTED);
        }

        // Act & Assert
        assertThat(sut.publish(1, 2, message, 0, null)).isEqualTo(FULL);
        assertThat(sut.send(1, 3, publish(), null)).isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 4, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);
        transport.awaitActive();
        transport.completeInline = true;
        transport.complete();
        await().untilAsserted(() -> assertThat(events.tickets).hasSize(admitted + 2));
        assertThat(transport.attempts).doesNotContain(2L).endsWith(3L, 4L);
        assertThat(events.failures).isEmpty();
        assertThat(events.ends).isEmpty();
        assertThat(transport.rejectSend).isFalse();
    }

    @Test
    void refused_publications_are_not_encoded_and_use_only_the_regular_budget() {
        // Arrange
        SingleWriteTransport transport = open(1);
        MqttMessage tooLarge = message(16 * 1024 * 1024);

        // Act & Assert: payload plus its header does not fit even in an empty queue.
        assertThat(sut.publish(1, 1, tooLarge, 0, null)).isEqualTo(FULL);
        assertThat(codec.prepared.get()).isZero();
        for (int i = 1; i <= 128; i++) {
            assertThat(sut.publish(1, i, message(0), 0, null)).isEqualTo(ACCEPTED);
        }
        transport.awaitActive();
        assertThat(sut.publish(1, 129, message(0), 0, null)).isEqualTo(FULL);
        assertThat(codec.prepared.get()).isEqualTo(1);
        assertThat(sut.send(1, 130, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);

        // Act & Assert: closure takes precedence and keeps the transport's cause.
        var cause = new IOException("Connection ended");
        transport.end(cause);
        assertThat(sut.publish(1, 131, tooLarge, 0, null)).isEqualTo(CLOSED);
        assertThat(events.ends).containsExactly(cause);
        assertThat(events.failures).isEmpty();
    }

    @Test
    void inline_completions_advance_writes_without_actor_work_or_recursive_sends() {
        // Arrange
        SingleWriteTransport transport = open(1);
        for (int i = 1; i <= 16; i++) {
            sut.send(1, i, new Packet.PingReq(), null);
        }
        transport.awaitActive();
        transport.completeInline = true;

        // Act: no actor consumes or forwards the recorded completion events.
        transport.complete();

        // Assert
        await().untilAsserted(() -> assertThat(events.tickets).hasSize(16));
        assertThat(transport.attempts).hasSize(16);
        assertThat(transport.maximumSendDepth).isEqualTo(1);
        assertThat(events.failures).isEmpty();
    }

    @Test
    void a_publication_without_a_receipt_keeps_its_fields_and_omits_dup() throws Exception {
        // Arrange
        SingleWriteTransport transport = open(1);
        var observed = new CompletableFuture<Packet.Publish>();
        transport.beforeReturn = bytes -> observed.complete((Packet.Publish) codec.decode(bytes));
        var message = MqttMessage.of(new Topic("a"), new byte[] {1, 2}, Qos.AT_LEAST_ONCE, true);

        // Act
        assertThat(sut.publish(1, 1, message, 7, null)).isEqualTo(ACCEPTED);

        // Assert
        Packet.Publish packet = observed.get(5, SECONDS);
        assertThat(packet.dup()).isFalse();
        assertThat(packet.packetId()).isEqualTo(7);
        assertThat(packet.qos()).isEqualTo(Qos.AT_LEAST_ONCE);
        assertThat(packet.retain()).isTrue();
        assertThat(packet.payload()).containsExactly(1, 2);
    }

    @Test
    void transport_admission_marks_a_retry_even_without_write_completion() throws Exception {
        // Arrange
        var receipt = new SendReceipt();
        var message = MqttMessage.of(new Topic("a"), new byte[] {1}, Qos.AT_LEAST_ONCE, false);
        SingleWriteTransport old = open(1);
        var first = new CompletableFuture<Packet.Publish>();
        old.beforeReturn = bytes -> first.complete((Packet.Publish) codec.decode(bytes));
        assertThat(sut.publish(1, 1, message, 7, receipt)).isEqualTo(ACCEPTED);
        await().until(receipt::accepted);

        // Act: the transport accepted the array, but it has not reported a successful write.
        sut.close();
        SingleWriteTransport current = open(2);
        var retry = new CompletableFuture<Packet.Publish>();
        current.beforeReturn = bytes -> retry.complete((Packet.Publish) codec.decode(bytes));
        assertThat(sut.publish(2, 2, message, 7, receipt)).isEqualTo(ACCEPTED);

        // Assert
        assertThat(first.get(5, SECONDS).dup()).isFalse();
        Packet.Publish packet = retry.get(5, SECONDS);
        assertThat(packet.dup()).isTrue();
        assertThat(packet.packetId()).isEqualTo(7);
        assertThat(packet.payload()).containsExactly(1);
        assertThat(events.tickets).isEmpty();
    }

    @Test
    void closing_during_encoding_leaves_active_and_queued_publications_untransmitted()
            throws Exception {
        // Arrange
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        codec.beforeEncode = () -> {
            entered.countDown();
            waitFor(release);
        };
        var active = new SendReceipt();
        var queued = new SendReceipt();
        var message = MqttMessage.of(new Topic("a"), new byte[0], Qos.AT_LEAST_ONCE, false);
        SingleWriteTransport old = open(1);
        try {
            assertThat(sut.publish(1, 1, message, 7, active)).isEqualTo(ACCEPTED);
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(sut.publish(1, 2, message, 8, queued)).isEqualTo(ACCEPTED);

            // Act: enqueue both retries before the old encoder returns.
            sut.close();
            old.end(new IOException("Old encoder stopped"));
            SingleWriteTransport current = open(2);
            var packets = new CopyOnWriteArrayList<Packet.Publish>();
            current.completeInline = true;
            current.beforeReturn = bytes -> packets.add((Packet.Publish) codec.decode(bytes));
            assertThat(sut.publish(2, 3, message, 7, active)).isEqualTo(ACCEPTED);
            assertThat(sut.publish(2, 4, message, 8, queued)).isEqualTo(ACCEPTED);

            // Assert: queue admission and active encoding did not transfer either publication.
            assertThat(active.accepted()).isFalse();
            assertThat(queued.accepted()).isFalse();
            release.countDown();
            await().untilAsserted(() -> assertThat(packets).hasSize(2));
            assertThat(packets).extracting(Packet.Publish::packetId).containsExactly(7, 8);
            assertThat(packets).extracting(Packet.Publish::dup).containsExactly(false, false);
            assertThat(old.attempts).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest(name = "send throws: {0}")
    @ValueSource(booleans = {false, true})
    void a_rejected_or_throwing_send_does_not_mark_a_retry(boolean throwsFailure)
            throws Exception {
        // Arrange
        var receipt = new SendReceipt();
        var message = MqttMessage.of(new Topic("a"), new byte[0], Qos.AT_LEAST_ONCE, false);
        SingleWriteTransport old = open(1);
        var failure = new IllegalStateException("Send refused");
        if (throwsFailure) {
            old.beforeSend = () -> {
                throw failure;
            };
        } else {
            old.rejectSend = true;
            old.endInsideSend = failure;
        }

        // Act
        assertThat(sut.publish(1, 1, message, 7, receipt)).isEqualTo(ACCEPTED);
        await().untilAsserted(() -> assertThat(events.ends).containsExactly(failure));
        assertThat(receipt.accepted()).isFalse();
        sut.close();
        SingleWriteTransport current = open(2);
        var retry = new CompletableFuture<Packet.Publish>();
        current.beforeReturn = bytes -> retry.complete((Packet.Publish) codec.decode(bytes));
        assertThat(sut.publish(2, 2, message, 7, receipt)).isEqualTo(ACCEPTED);

        // Assert
        assertThat(retry.get(5, SECONDS).dup()).isFalse();
        assertThat(events.failures).isEmpty();
        assertThat(events.fatals).isEmpty();
    }

    @Test
    void an_inline_end_before_send_returns_still_marks_the_next_connections_retry()
            throws Exception {
        // Arrange
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var receipt = new SendReceipt();
        var message = MqttMessage.of(new Topic("a"), new byte[0], Qos.AT_LEAST_ONCE, false);
        SingleWriteTransport old = open(1);
        var failure = new IOException("Inline termination");
        old.beforeReturn = _ -> {
            old.end(failure);
            entered.countDown();
            waitFor(release);
        };
        try {
            assertThat(sut.publish(1, 1, message, 7, receipt)).isEqualTo(ACCEPTED);
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(events.ends).containsExactly(failure);
            assertThat(receipt.accepted()).isFalse();

            // Act: the actor can replace the connection before ACCEPTED returns to the sender.
            sut.close();
            SingleWriteTransport current = open(2);
            var retry = new CompletableFuture<Packet.Publish>();
            current.beforeReturn = bytes -> retry.complete((Packet.Publish) codec.decode(bytes));
            assertThat(sut.publish(2, 2, message, 7, receipt)).isEqualTo(ACCEPTED);
            assertThat(current.attempts).isEmpty();
            release.countDown();

            // Assert: the sole sender records acceptance before encoding the queued retry.
            assertThat(retry.get(5, SECONDS).dup()).isTrue();
            assertThat(receipt.accepted()).isTrue();
            assertThat(events.tickets).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void an_acknowledgement_waiting_in_the_send_queue_has_no_transport_receipt() {
        // Arrange
        SingleWriteTransport transport = open(1);
        sut.send(1, 1, new Packet.PingReq(), null);
        transport.awaitActive();
        var receipt = new SendReceipt();
        var packet = new Packet.PubAck(7, ReasonCode.SUCCESS, EMPTY);

        // Act
        assertThat(sut.send(1, 2, packet, receipt)).isEqualTo(ACCEPTED);
        sut.close();
        transport.end(new IOException("Connection ended before the queued PUBACK"));

        // Assert
        assertThat(receipt.accepted()).isFalse();
        assertThat(transport.attempts).containsExactly(1L);
        assertThat(events.written).isEmpty();
    }

    @Test
    void transport_admission_records_an_acknowledgement_without_write_completion()
            throws Exception {
        // Arrange
        SingleWriteTransport transport = open(1);
        var observed = new CompletableFuture<Packet>();
        transport.beforeReturn = bytes -> observed.complete(codec.decode(bytes));
        var receipt = new SendReceipt();
        var packet = new Packet.PubAck(7, ReasonCode.SUCCESS, EMPTY);

        // Act
        assertThat(sut.send(1, 1, packet, receipt)).isEqualTo(ACCEPTED);
        await().until(receipt::accepted);
        transport.end(new IOException("Accepted without a successful write"));

        // Assert
        assertThat(receipt.accepted()).isTrue();
        assertThat(observed.get(5, SECONDS)).isEqualTo(packet);
        assertThat(events.written).isEmpty();
    }

    @ParameterizedTest(name = "acknowledgement send throws: {0}")
    @ValueSource(booleans = {false, true})
    void a_rejected_or_throwing_acknowledgement_has_no_transport_receipt(boolean throwsFailure) {
        // Arrange
        SingleWriteTransport transport = open(1);
        var failure = new IOException("PUBACK refused");
        if (throwsFailure) {
            transport.beforeSend = () -> {
                throw new IllegalStateException(failure);
            };
        } else {
            transport.rejectSend = true;
            transport.endInsideSend = failure;
        }
        var receipt = new SendReceipt();

        // Act
        assertThat(sut.send(1, 1, new Packet.PubAck(7, ReasonCode.SUCCESS, EMPTY), receipt))
                .isEqualTo(ACCEPTED);
        await().untilAsserted(() -> assertThat(events.ends).hasSize(1));

        // Assert
        assertThat(receipt.accepted()).isFalse();
        assertThat(events.written).isEmpty();
    }

    @Test
    void an_acknowledgement_receipt_can_arrive_after_its_connection_ends() throws Exception {
        // Arrange
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var receipt = new SendReceipt();
        SingleWriteTransport old = open(1);
        var failure = new IOException("Inline PUBACK termination");
        old.beforeReturn = _ -> {
            old.end(failure);
            entered.countDown();
            waitFor(release);
        };
        try {
            sut.send(1, 1, new Packet.PubAck(7, ReasonCode.SUCCESS, EMPTY), receipt);
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(events.ends).containsExactly(failure);
            assertThat(receipt.accepted()).isFalse();

            // Act: this connection's sends wait for the old send call to return.
            sut.close();
            SingleWriteTransport current = open(2);
            var observed = new CompletableFuture<Packet>();
            current.beforeReturn = bytes -> observed.complete(codec.decode(bytes));
            sut.send(2, 2, new Packet.PingReq(), null);
            assertThat(current.attempts).isEmpty();
            release.countDown();

            // Assert
            assertThat(observed.get(5, SECONDS)).isInstanceOf(Packet.PingReq.class);
            assertThat(receipt.accepted()).isTrue();
            assertThat(events.written).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_rejected_retry_keeps_an_earlier_transport_admission() throws Exception {
        // Arrange
        var receipt = new SendReceipt();
        var message = MqttMessage.of(new Topic("a"), new byte[0], Qos.AT_LEAST_ONCE, false);
        open(1);
        assertThat(sut.publish(1, 1, message, 7, receipt)).isEqualTo(ACCEPTED);
        await().until(receipt::accepted);
        sut.close();
        SingleWriteTransport rejected = open(2);
        var failure = new IOException("Retry refused");
        rejected.rejectSend = true;
        rejected.endInsideSend = failure;

        // Act
        assertThat(sut.publish(2, 2, message, 7, receipt)).isEqualTo(ACCEPTED);
        await().untilAsserted(() -> assertThat(events.ends).containsExactly(failure));
        sut.close();
        SingleWriteTransport current = open(3);
        var retry = new CompletableFuture<Packet.Publish>();
        current.beforeReturn = bytes -> retry.complete((Packet.Publish) codec.decode(bytes));
        assertThat(sut.publish(3, 3, message, 7, receipt)).isEqualTo(ACCEPTED);

        // Assert
        assertThat(receipt.accepted()).isTrue();
        assertThat(retry.get(5, SECONDS).dup()).isTrue();
    }

    @ParameterizedTest(name = "terminal callback inside send: {0}")
    @ValueSource(booleans = {false, true})
    void a_closed_send_discards_waiting_writes_and_preserves_the_terminal_cause(boolean inline) {
        // Arrange
        SingleWriteTransport transport = open(1);
        sut.send(1, 1, new Packet.PingReq(), null);
        sut.send(1, 2, new Packet.PingReq(), null);
        sut.send(1, 3, new Packet.PingReq(), null);
        var failure = new IOException("Connection reset");
        transport.awaitActive();
        transport.rejectSend = true;
        transport.endInsideSend = inline ? failure : null;

        // Act
        transport.complete();
        await().untilAsserted(() -> assertThat(transport.rejections.get()).isEqualTo(1));
        // A publication that never fits stays FULL until closure makes it CLOSED.
        MqttMessage tooLarge = message(16 * 1024 * 1024);
        await().untilAsserted(
                () -> assertThat(sut.publish(1, 4, tooLarge, 0, null)).isEqualTo(CLOSED)
        );
        if (!inline) {
            assertThat(events.ends).isEmpty();
            transport.end(failure);
        }

        // Assert
        assertThat(transport.attempts).containsExactly(1L, 2L);
        assertThat(events.tickets).containsExactly(1L);
        assertThat(events.failures).isEmpty();
        await().untilAsserted(() -> assertThat(events.ends).containsExactly(failure));
    }

    @Test
    void close_discards_waiting_writes_and_old_completions_do_not_advance_a_new_connection() {
        // Arrange
        SingleWriteTransport old = open(1);
        sut.send(1, 1, new Packet.PingReq(), null);
        sut.send(1, 2, new Packet.PingReq(), null);
        old.awaitActive();

        // Act: transport shutdown is asynchronous and still owns its active buffer.
        sut.close();
        SingleWriteTransport current = open(2);
        sut.send(2, 1, new Packet.PingReq(), null);
        sut.send(2, 2, new Packet.PingReq(), null);
        old.complete();
        old.end(new IOException("Old connection closed"));
        current.complete();
        await().untilAsserted(() -> assertThat(current.attempts).containsExactly(1L, 2L));

        // Assert: the old queue neither sends its pending item nor advances the new queue.
        assertThat(old.attempts).containsExactly(1L);
        assertThat(events.generations).containsExactly(2L);
        assertThat(events.ends).isEmpty();
        assertThat(events.failures).isEmpty();
    }

    @Test
    void closing_during_encoding_keeps_capacity_until_the_old_result_is_discarded()
            throws Exception {
        // Arrange: one large payload is being encoded while another publication waits.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        codec.beforeEncode = () -> {
            entered.countDown();
            waitFor(release);
        };
        MqttMessage large = message(8 * 1024 * 1024);
        SingleWriteTransport old = open(1);
        try {
            assertThat(sut.publish(1, 1, large, 0, null)).isEqualTo(ACCEPTED);
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(sut.publish(1, 2, message(0), 0, null)).isEqualTo(ACCEPTED);

            // Act: even onClosed cannot release the payload still used by the encoder.
            sut.close();
            old.end(new IOException("Old connection ended"));
            SingleWriteTransport current = open(2);
            assertThat(sut.publish(2, 3, large, 0, null)).isEqualTo(FULL);
            assertThat(sut.send(2, 4, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);

            // Assert: input and callbacks remain usable while the sender is occupied.
            requireNonNull(current.listener).onBytes(codec.encode(new Packet.PingResp()));
            assertThat(events.pingResponses).containsExactly(new Packet.PingResp());
            assertThat(current.attempts).isEmpty();
            assertThat(old.attempts).isEmpty();

            // Act: discard the old encoding, then use its returned capacity in the new connection.
            release.countDown();
            current.awaitActive();
            assertThat(sut.publish(2, 5, large, 0, null)).isEqualTo(ACCEPTED);
            current.completeInline = true;
            current.complete();

            // Assert
            await().untilAsserted(() -> assertThat(events.tickets).containsExactly(4L, 5L));
            assertThat(codec.prepared.get()).isEqualTo(2);
            assertThat(old.attempts).isEmpty();
            assertThat(current.attempts).containsExactly(4L, 5L);
            assertThat(events.failures).isEmpty();
            assertThat(events.fatals).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void an_old_transport_retains_capacity_without_stopping_the_new_connections_sender() {
        // Arrange
        MqttMessage large = message(8 * 1024 * 1024);
        SingleWriteTransport old = open(1);
        assertThat(sut.publish(1, 1, large, 0, null)).isEqualTo(ACCEPTED);
        old.awaitActive();

        // Act: close is asynchronous; the old transport still owns the encoded array.
        sut.close();
        SingleWriteTransport current = open(2);
        assertThat(sut.publish(2, 2, large, 0, null)).isEqualTo(FULL);
        assertThat(sut.send(2, 3, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);
        current.awaitActive();

        // Assert: the new connection progresses despite the old outstanding write.
        assertThat(current.attempts).containsExactly(3L);
        assertThat(events.tickets).isEmpty();

        // Act: termination returns only the old transport's capacity.
        old.end(new IOException("Old write released"));
        assertThat(sut.publish(2, 4, large, 0, null)).isEqualTo(ACCEPTED);
        current.completeInline = true;
        current.complete();

        // Assert
        await().untilAsserted(() -> assertThat(events.tickets).containsExactly(3L, 4L));
        assertThat(events.generations).containsExactly(2L, 2L);
        assertThat(old.attempts).containsExactly(1L);
        assertThat(events.ends).isEmpty();
        assertThat(events.failures).isEmpty();
    }

    @Test
    void inline_completion_keeps_capacity_until_the_send_call_returns() throws Exception {
        // Arrange: onSent completes inline, but the SPI call still retains its argument.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        MqttMessage large = message(8 * 1024 * 1024);
        SingleWriteTransport old = open(1);
        old.completeInline = true;
        old.afterSent = () -> {
            entered.countDown();
            waitFor(release);
        };
        try {
            assertThat(sut.publish(1, 1, large, 0, null)).isEqualTo(ACCEPTED);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            sut.close();
            SingleWriteTransport current = open(2);
            assertThat(sut.publish(2, 2, large, 0, null)).isEqualTo(FULL);
            assertThat(sut.send(2, 3, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);
            release.countDown();
            current.awaitActive();
            assertThat(sut.publish(2, 4, large, 0, null)).isEqualTo(ACCEPTED);

            // Assert
            assertThat(old.attempts).containsExactly(1L);
            assertThat(current.attempts).containsExactly(3L);
            assertThat(events.tickets).containsExactly(1L);
            assertThat(events.failures).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void an_inline_puback_write_is_reported_while_its_capacity_remains_owned()
            throws Exception {
        // Arrange: a transport completes the write inline but has not returned from send.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        SingleWriteTransport transport = open(1);
        transport.completeInline = true;
        transport.afterSent = () -> {
            transport.completeInline = false;
            entered.countDown();
            waitFor(release);
        };
        var acknowledgement = new Packet.PubAck(1, ReasonCode.SUCCESS, EMPTY);
        try {
            // Act
            assertThat(sut.send(1, 1, acknowledgement, null)).isEqualTo(ACCEPTED);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Assert: the write is reported at once, but the retained buffer keeps its capacity.
            assertThat(events.written).containsExactly(1L);
            assertThat(events.tickets).isEmpty();
            assertThat(events.capacityGenerations).isEmpty();
            for (int i = 2; i <= 14; i++) {
                assertThat(sut.send(1, i, acknowledgement, null)).isEqualTo(ACCEPTED);
            }
            assertThat(sut.send(1, 15, acknowledgement, null)).isEqualTo(FULL);

            // Act & Assert: release emits the capacity wake and user-visible completion.
            release.countDown();
            await().untilAsserted(() -> assertThat(events.tickets).containsExactly(1L));
            assertThat(events.capacityGenerations).containsExactly(1L);
            assertThat(events.written).containsExactly(1L);
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_write_completion_can_immediately_reuse_its_capacity(boolean inline) throws Exception {
        // Arrange: only one of these publications fits in the regular byte budget.
        MqttMessage large = message(9 * 1024 * 1024);
        var next = new CompletableFuture<SendQueue.OfferResult>();
        SingleWriteTransport transport = open(1);
        transport.completeInline = inline;
        events.beforeSent = () -> {
            events.beforeSent = () -> { };
            next.complete(sut.publish(1, 2, large, 0, null));
        };

        // Act: admit the second publication as soon as the first completion is posted.
        assertThat(sut.publish(1, 1, large, 0, null)).isEqualTo(ACCEPTED);
        if (!inline) {
            transport.complete();
        }

        // Assert
        assertThat(next.get(5, SECONDS)).isEqualTo(ACCEPTED);
        await().untilAsserted(() -> assertThat(transport.attempts).containsExactly(1L, 2L));
        assertThat(events.failures).isEmpty();
        assertThat(events.ends).isEmpty();
    }

    @Test
    void a_send_runtime_exception_fails_only_its_connection() {
        // Arrange
        var failure = new IllegalStateException("Send failed");
        SingleWriteTransport old = open(1);
        old.beforeSend = () -> {
            throw failure;
        };

        // Act
        sut.send(1, 1, new Packet.PingReq(), null);

        // Assert
        await().untilAsserted(() -> assertThat(events.ends).containsExactly(failure));
        assertThat(events.failures).isEmpty();
        assertThat(events.fatals).isEmpty();

        // Act: the actor can close the failed connection and start a new one.
        sut.close();
        old.end(failure);
        SingleWriteTransport current = open(2);
        current.completeInline = true;
        sut.send(2, 2, new Packet.PingReq(), null);

        // Assert
        await().untilAsserted(() -> assertThat(events.tickets).containsExactly(2L));
    }

    @Test
    void a_send_error_is_reported_as_fatal_to_the_client() {
        // Arrange
        var failure = new AssertionError("Send failed fatally");
        SingleWriteTransport transport = open(1);
        transport.beforeSend = () -> {
            throw failure;
        };

        // Act
        sut.send(1, 1, new Packet.PingReq(), null);

        // Assert
        await().untilAsserted(() -> assertThat(events.fatals).containsExactly(failure));
        assertThat(events.failures).isEmpty();
        assertThat(events.tickets).isEmpty();
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            assertThat(latch.await(30, SECONDS)).isTrue();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    @Test
    void only_a_pingresp_answering_a_pingreq_admitted_on_its_connection_goes_ahead() {
        // Arrange: the old connection admits a PINGREQ that is never answered.
        byte[] response = codec.encode(new Packet.PingResp());
        open(1);
        assertThat(sut.send(1, 1, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);
        sut.close();
        Transport.Listener listener = requireNonNull(open(2).listener);

        // Act: an unsolicited, an answering, and a repeated response on the new connection.
        listener.onBytes(response);
        assertThat(sut.send(2, 2, new Packet.PingReq(), null)).isEqualTo(ACCEPTED);
        listener.onBytes(response);
        listener.onBytes(response);

        // Assert: only the answer may overtake earlier input; the others keep wire order.
        assertThat(events.pingResponses).containsExactly(new Packet.PingResp());
        assertThat(events.received).containsExactly(new Packet.PingResp(), new Packet.PingResp());
    }

    private SingleWriteTransport open(long generation) {
        sut.open(generation);
        return transports.getLast();
    }

    private static MqttMessage message(int payloadSize) {
        return MqttMessage.of(new Topic("a"), new byte[payloadSize], Qos.AT_MOST_ONCE, false);
    }

    private static Packet.Publish publish() {
        return new Packet.Publish(
                new Topic("a"),
                new byte[0],
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
    }

    private static Packet.Subscribe subscribe() {
        return new Packet.Subscribe(
                1,
                List.of(new Packet.Subscription(
                        new TopicFilter("a"),
                        Qos.AT_MOST_ONCE,
                        false,
                        false,
                        0
                )),
                EMPTY
        );
    }

    private static Packet.Unsubscribe unsubscribe() {
        return new Packet.Unsubscribe(1, List.of(new TopicFilter("a")), EMPTY);
    }

    private static Stream<Arguments> sendCapacities() {
        return Stream.of(
                Arguments.of(publish(), 128),
                Arguments.of(subscribe(), 128),
                Arguments.of(unsubscribe(), 128),
                Arguments.of(new Packet.Connect(
                        "client", MqttVersion.V3_1_1, true, 60, null, null, null, EMPTY
                ), 16),
                Arguments.of(new Packet.PubAck(1, ReasonCode.SUCCESS, EMPTY), 14),
                Arguments.of(new Packet.PingReq(), 16),
                Arguments.of(new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY), 16)
        );
    }

    private enum OldWriteRelease {
        COMPLETION(SingleWriteTransport::complete),
        CLOSURE(transport -> transport.end(new IOException("Old active write released")));

        final Consumer<SingleWriteTransport> apply;

        OldWriteRelease(Consumer<SingleWriteTransport> apply) {
            this.apply = apply;
        }
    }

    private static final class SingleWriteTransport implements Transport {

        final List<Long> attempts = new CopyOnWriteArrayList<>();
        final AtomicInteger rejections = new AtomicInteger();
        @Nullable Listener listener;
        long activeTicket;
        volatile Runnable beforeSend = () -> { };
        volatile Runnable afterSent = () -> { };
        volatile Consumer<byte[]> beforeReturn = _ -> { };
        volatile boolean completeInline;
        volatile boolean rejectSend;
        volatile @Nullable Throwable endInsideSend;
        int sendDepth;
        int maximumSendDepth;

        @Override
        public void start(URI endpoint, Duration timeout, Listener receiver) {
            listener = receiver;
            receiver.onConnected();
        }

        @Override
        public synchronized SendResult send(long ticket, byte[] bytes) {
            beforeSend.run();
            assertThat(activeTicket).isZero();
            sendDepth++;
            maximumSendDepth = Math.max(maximumSendDepth, sendDepth);
            try {
                attempts.add(ticket);
                if (rejectSend) {
                    rejections.incrementAndGet();
                    if (endInsideSend != null) {
                        end(endInsideSend);
                    }
                    return SendResult.CLOSED;
                }
                activeTicket = ticket;
                if (completeInline) {
                    finish();
                    afterSent.run();
                }
                beforeReturn.accept(bytes);
                return SendResult.ACCEPTED;
            } finally {
                sendDepth--;
            }
        }

        void complete() {
            awaitActive();
            finish();
        }

        synchronized void finish() {
            assertThat(activeTicket).isNotZero();
            long ticket = activeTicket;
            activeTicket = 0;
            requireNonNull(listener).onSent(ticket);
        }

        void awaitActive() {
            await().until(this::hasActive);
        }

        private synchronized boolean hasActive() {
            return activeTicket != 0;
        }

        synchronized void end(Throwable cause) {
            activeTicket = 0;
            requireNonNull(listener).onClosed(cause);
        }

        @Override
        public void close() {
            rejectSend = true;
        }
    }

    private static final class RecordingCodec implements PacketCodec {

        private final PacketCodec delegate = PacketCodec.of(MqttVersion.V3_1_1);
        final AtomicInteger prepared = new AtomicInteger();
        volatile Runnable beforeEncode = () -> { };

        @Override
        public byte[] encode(Packet packet) {
            return delegate.encode(packet);
        }

        @Override
        public byte[] encodePublishHeader(
                Topic topic,
                Qos qos,
                boolean retain,
                boolean dup,
                int packetId,
                Properties properties,
                int payloadSize
        ) {
            return delegate.encodePublishHeader(
                    topic,
                    qos,
                    retain,
                    dup,
                    packetId,
                    properties,
                    payloadSize
            );
        }

        @Override
        public byte[] encodePublish(byte[] header, byte[] payload) {
            prepared.incrementAndGet();
            beforeEncode.run();
            return delegate.encodePublish(header, payload);
        }

        @Override
        public Packet decode(byte[] packet) {
            return delegate.decode(packet);
        }
    }

    private static final class RecordingEvents implements ClientIo.Events {

        final List<Long> generations = new CopyOnWriteArrayList<>();
        final List<Long> tickets = new CopyOnWriteArrayList<>();
        final List<Throwable> failures = new CopyOnWriteArrayList<>();
        final List<Throwable> ends = new CopyOnWriteArrayList<>();
        final List<Throwable> fatals = new CopyOnWriteArrayList<>();
        final List<Packet> received = new CopyOnWriteArrayList<>();
        final List<Packet> pingResponses = new CopyOnWriteArrayList<>();
        final List<Long> written = new CopyOnWriteArrayList<>();
        final List<Long> capacityGenerations = new CopyOnWriteArrayList<>();
        volatile Runnable beforeSent = () -> { };

        @Override
        public void connected(long generation) {
        }

        @Override
        public void received(long generation, Packet packet, int size) {
            received.add(packet);
        }

        @Override
        public void pingResponse(long generation, Packet packet, int size) {
            pingResponses.add(packet);
        }

        @Override
        public void written(long generation, long ticket) {
            written.add(ticket);
        }

        @Override
        public void sendCapacityAvailable(long generation) {
            capacityGenerations.add(generation);
        }

        @Override
        public Runnable prepareSent(long generation, long ticket) {
            return () -> {
                beforeSent.run();
                generations.add(generation);
                tickets.add(ticket);
            };
        }

        @Override
        public void ended(long generation, Throwable cause) {
            ends.add(cause);
        }

        @Override
        public void failed(long generation, Throwable cause) {
            failures.add(cause);
        }

        @Override
        public void fatal(Throwable cause) {
            fatals.add(cause);
        }
    }
}
