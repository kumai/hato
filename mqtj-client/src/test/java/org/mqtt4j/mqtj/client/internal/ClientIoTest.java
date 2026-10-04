package org.mqtt4j.mqtj.client.internal;

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

import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.ReasonCode;
import org.mqtt4j.mqtj.Topic;
import org.mqtt4j.mqtj.TopicFilter;
import org.mqtt4j.mqtj.client.internal.common.io.SendQueue;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.client.internal.common.protocol.Properties;
import org.mqtt4j.mqtj.spi.Transport;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.mqtj.client.internal.common.io.SendQueue.OfferResult.ACCEPTED;
import static org.mqtt4j.mqtj.client.internal.common.io.SendQueue.OfferResult.CLOSED;
import static org.mqtt4j.mqtj.client.internal.common.io.SendQueue.OfferResult.FULL;
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;

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
            sut.send(1, 1, packet);
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
            sut.send(1, 2, packet);
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
            sut.send(1, 3, packet);
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
            assertThat(sut.send(1, i, packet)).isEqualTo(ACCEPTED);
        }
        assertThat(events.failures).isEmpty();

        // Act
        SendQueue.OfferResult result = sut.send(1, limit + 1, packet);

        // Assert: the transport receives only one write and has no capacity policy.
        assertThat(result).isEqualTo(FULL);
        await().untilAsserted(() -> assertThat(transport.attempts).containsExactly(1L));
        assertThat(events.failures).isEmpty();
        assertThat(events.ends).isEmpty();
        assertThat(transport.rejectSend).isFalse();

        // Act & Assert: completing one write permits a new send after the refusal.
        transport.complete();
        await().untilAsserted(() -> assertThat(transport.attempts).containsExactly(1L, 2L));
        assertThat(sut.send(1, limit + 2, packet)).isEqualTo(ACCEPTED);
    }

    @Test
    void required_sends_keep_their_reservation_and_fifo_when_regular_operations_fill_the_queue() {
        // Arrange
        SingleWriteTransport transport = open(1);
        for (int i = 1; i <= 64; i++) {
            assertThat(sut.send(1, i, subscribe())).isEqualTo(ACCEPTED);
        }
        for (int i = 65; i <= 128; i++) {
            assertThat(sut.send(1, i, unsubscribe())).isEqualTo(ACCEPTED);
        }

        // Act
        assertThat(sut.send(1, 129, publish())).isEqualTo(FULL);
        assertThat(sut.send(1, 130, new Packet.PubAck(1, ReasonCode.SUCCESS, EMPTY)))
                .isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 131, new Packet.PingReq())).isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 132, new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY)))
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
            assertThat(sut.send(1, i, acknowledgement)).isEqualTo(ACCEPTED);
        }

        // Act
        assertThat(sut.send(1, 15, acknowledgement)).isEqualTo(FULL);
        assertThat(sut.send(1, 16, new Packet.PingReq())).isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 17, new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY)))
                .isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 18, acknowledgement)).isEqualTo(FULL);
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
        assertThat(sut.send(1, 1, connect)).isEqualTo(ACCEPTED);
        transport.awaitActive();

        // Act & Assert: PUBACK waits while connection control still has room.
        assertThat(sut.send(1, 2, acknowledgement)).isEqualTo(FULL);
        assertThat(sut.send(1, 3, new Packet.PingReq())).isEqualTo(ACCEPTED);
        transport.complete();
        await().untilAsserted(() -> assertThat(events.tickets).containsExactly(1L));
        assertThat(events.capacityGenerations).containsExactly(1L);
        assertThat(sut.send(1, 4, acknowledgement)).isEqualTo(ACCEPTED);
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
        assertThat(sut.send(1, 1, acknowledgement)).isEqualTo(ACCEPTED);
        old.awaitActive();
        sut.close();
        SingleWriteTransport current = open(2);
        for (int i = 2; i <= 14; i++) {
            assertThat(sut.send(2, i, acknowledgement)).isEqualTo(ACCEPTED);
        }
        current.awaitActive();
        assertThat(sut.send(2, 15, acknowledgement)).isEqualTo(FULL);
        assertThat(events.capacityGenerations).isEmpty();

        // Act: either completion or final closure can release the old buffer.
        release.apply.accept(old);

        // Assert: no old protocol completion is needed to wake the new connection.
        assertThat(events.capacityGenerations).containsExactly(2L);
        assertThat(events.written).isEmpty();
        assertThat(events.tickets).isEmpty();
        assertThat(sut.send(2, 16, acknowledgement)).isEqualTo(ACCEPTED);
        assertThat(sut.send(2, 17, acknowledgement)).isEqualTo(FULL);
        assertThat(sut.send(2, 18, new Packet.PingReq())).isEqualTo(ACCEPTED);
        assertThat(sut.send(2, 19, new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY)))
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
            assertThat(sut.publish(1, i, message, 0)).isEqualTo(ACCEPTED);
        }

        // Act & Assert
        assertThat(sut.publish(1, 2, message, 0)).isEqualTo(FULL);
        assertThat(sut.send(1, 3, publish())).isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 4, new Packet.PingReq())).isEqualTo(ACCEPTED);
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
        assertThat(sut.publish(1, 1, tooLarge, 0)).isEqualTo(FULL);
        assertThat(codec.prepared.get()).isZero();
        for (int i = 1; i <= 128; i++) {
            assertThat(sut.publish(1, i, message(0), 0)).isEqualTo(ACCEPTED);
        }
        transport.awaitActive();
        assertThat(sut.publish(1, 129, message(0), 0)).isEqualTo(FULL);
        assertThat(codec.prepared.get()).isEqualTo(1);
        assertThat(sut.send(1, 130, new Packet.PingReq())).isEqualTo(ACCEPTED);

        // Act & Assert: closure takes precedence and keeps the transport's cause.
        var cause = new IOException("Connection ended");
        transport.end(cause);
        assertThat(sut.publish(1, 131, tooLarge, 0)).isEqualTo(CLOSED);
        assertThat(events.ends).containsExactly(cause);
        assertThat(events.failures).isEmpty();
    }

    @Test
    void inline_completions_advance_writes_without_actor_work_or_recursive_sends() {
        // Arrange
        SingleWriteTransport transport = open(1);
        for (int i = 1; i <= 16; i++) {
            sut.send(1, i, new Packet.PingReq());
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

    @ParameterizedTest(name = "terminal callback inside send: {0}")
    @ValueSource(booleans = {false, true})
    void a_closed_send_discards_waiting_writes_and_preserves_the_terminal_cause(boolean inline) {
        // Arrange
        SingleWriteTransport transport = open(1);
        sut.send(1, 1, new Packet.PingReq());
        sut.send(1, 2, new Packet.PingReq());
        sut.send(1, 3, new Packet.PingReq());
        var failure = new IOException("Connection reset");
        transport.awaitActive();
        transport.rejectSend = true;
        transport.endInsideSend = inline ? failure : null;

        // Act
        transport.complete();
        await().untilAsserted(() -> assertThat(transport.rejections.get()).isEqualTo(1));
        // A publication that never fits stays FULL until closure makes it CLOSED.
        MqttMessage tooLarge = message(16 * 1024 * 1024);
        await().untilAsserted(() -> assertThat(sut.publish(1, 4, tooLarge, 0)).isEqualTo(CLOSED));
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
        sut.send(1, 1, new Packet.PingReq());
        sut.send(1, 2, new Packet.PingReq());
        old.awaitActive();

        // Act: transport shutdown is asynchronous and still owns its active buffer.
        sut.close();
        SingleWriteTransport current = open(2);
        sut.send(2, 1, new Packet.PingReq());
        sut.send(2, 2, new Packet.PingReq());
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
            assertThat(sut.publish(1, 1, large, 0)).isEqualTo(ACCEPTED);
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(sut.publish(1, 2, message(0), 0)).isEqualTo(ACCEPTED);

            // Act: even onClosed cannot release the payload still used by the encoder.
            sut.close();
            old.end(new IOException("Old connection ended"));
            SingleWriteTransport current = open(2);
            assertThat(sut.publish(2, 3, large, 0)).isEqualTo(FULL);
            assertThat(sut.send(2, 4, new Packet.PingReq())).isEqualTo(ACCEPTED);

            // Assert: input and callbacks remain usable while the sender is occupied.
            requireNonNull(current.listener).onBytes(codec.encode(new Packet.PingResp()));
            assertThat(events.pingResponses).containsExactly(new Packet.PingResp());
            assertThat(current.attempts).isEmpty();
            assertThat(old.attempts).isEmpty();

            // Act: discard the old encoding, then use its returned capacity in the new connection.
            release.countDown();
            current.awaitActive();
            assertThat(sut.publish(2, 5, large, 0)).isEqualTo(ACCEPTED);
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
        assertThat(sut.publish(1, 1, large, 0)).isEqualTo(ACCEPTED);
        old.awaitActive();

        // Act: close is asynchronous; the old transport still owns the encoded array.
        sut.close();
        SingleWriteTransport current = open(2);
        assertThat(sut.publish(2, 2, large, 0)).isEqualTo(FULL);
        assertThat(sut.send(2, 3, new Packet.PingReq())).isEqualTo(ACCEPTED);
        current.awaitActive();

        // Assert: the new connection progresses despite the old outstanding write.
        assertThat(current.attempts).containsExactly(3L);
        assertThat(events.tickets).isEmpty();

        // Act: termination returns only the old transport's capacity.
        old.end(new IOException("Old write released"));
        assertThat(sut.publish(2, 4, large, 0)).isEqualTo(ACCEPTED);
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
            assertThat(sut.publish(1, 1, large, 0)).isEqualTo(ACCEPTED);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            sut.close();
            SingleWriteTransport current = open(2);
            assertThat(sut.publish(2, 2, large, 0)).isEqualTo(FULL);
            assertThat(sut.send(2, 3, new Packet.PingReq())).isEqualTo(ACCEPTED);
            release.countDown();
            current.awaitActive();
            assertThat(sut.publish(2, 4, large, 0)).isEqualTo(ACCEPTED);

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
            assertThat(sut.send(1, 1, acknowledgement)).isEqualTo(ACCEPTED);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Assert: the write is reported at once, but the retained buffer keeps its capacity.
            assertThat(events.written).containsExactly(1L);
            assertThat(events.tickets).isEmpty();
            assertThat(events.capacityGenerations).isEmpty();
            for (int i = 2; i <= 14; i++) {
                assertThat(sut.send(1, i, acknowledgement)).isEqualTo(ACCEPTED);
            }
            assertThat(sut.send(1, 15, acknowledgement)).isEqualTo(FULL);

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
            next.complete(sut.publish(1, 2, large, 0));
        };

        // Act: admit the second publication as soon as the first completion is posted.
        assertThat(sut.publish(1, 1, large, 0)).isEqualTo(ACCEPTED);
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
        sut.send(1, 1, new Packet.PingReq());

        // Assert
        await().untilAsserted(() -> assertThat(events.ends).containsExactly(failure));
        assertThat(events.failures).isEmpty();
        assertThat(events.fatals).isEmpty();

        // Act: the actor can close the failed connection and start a new one.
        sut.close();
        old.end(failure);
        SingleWriteTransport current = open(2);
        current.completeInline = true;
        sut.send(2, 2, new Packet.PingReq());

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
        sut.send(1, 1, new Packet.PingReq());

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
        assertThat(sut.send(1, 1, new Packet.PingReq())).isEqualTo(ACCEPTED);
        sut.close();
        Transport.Listener listener = requireNonNull(open(2).listener);

        // Act: an unsolicited, an answering, and a repeated response on the new connection.
        listener.onBytes(response);
        assertThat(sut.send(2, 2, new Packet.PingReq())).isEqualTo(ACCEPTED);
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
        return new MqttMessage(new Topic("a"), new byte[payloadSize], Qos.AT_MOST_ONCE, false);
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
