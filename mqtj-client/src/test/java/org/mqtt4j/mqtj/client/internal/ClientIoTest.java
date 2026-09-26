package org.mqtt4j.mqtj.client.internal;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.ReasonCode;
import org.mqtt4j.mqtj.Topic;
import org.mqtt4j.mqtj.TopicFilter;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.spi.Transport;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.mqtj.client.internal.SendQueue.OfferResult.ACCEPTED;
import static org.mqtt4j.mqtj.client.internal.SendQueue.OfferResult.CLOSED;
import static org.mqtt4j.mqtj.client.internal.SendQueue.OfferResult.FULL;
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;

class ClientIoTest {

    private final List<SingleWriteTransport> transports = new ArrayList<>();

    private final RecordingEvents events = new RecordingEvents();

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
            events
    );

    @AfterEach
    void close() {
        sut.close();
    }

    @Test
    void debug_describes_packets_without_payloads_and_trace_contains_wire_bytes() {
        // Arrange: capture the same System.Logger backend as applications using JUL.
        var jul = Logger.getLogger(ClientIo.class.getName());
        Level previousLevel = jul.getLevel();
        boolean previousParents = jul.getUseParentHandlers();
        var records = new ArrayList<LogRecord>();
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
        assertThat(transport.attempts).containsExactly(1L);
        assertThat(events.failures).isEmpty();
        assertThat(events.ends).isEmpty();
        assertThat(transport.rejectSend).isFalse();

        // Act & Assert: completing one write permits a new send after the refusal.
        transport.complete();
        drainCompletions();
        assertThat(sut.send(1, limit + 2, packet)).isEqualTo(ACCEPTED);
        assertThat(transport.attempts).containsExactly(1L, 2L);
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
        transport.completeInline = true;
        transport.complete();
        drainCompletions();

        // Assert
        assertThat(events.failures).isEmpty();
        assertThat(transport.attempts).containsExactlyElementsOf(
                LongStream.rangeClosed(1, 132).filter(ticket -> ticket != 129).boxed().toList()
        );
        assertThat(events.tickets).containsExactlyElementsOf(transport.attempts);
    }

    @ParameterizedTest(name = "payload bytes={0}, admitted={1}")
    @CsvSource({"8388608,1", "16777216,0"})
    void the_byte_limit_includes_headers_and_refusal_leaves_capacity_for_other_sends(
            int payloadSize,
            int admitted
    ) {
        // Arrange: two 8 MiB payloads, or one 16 MiB payload, exceed the encoded byte budget.
        SingleWriteTransport transport = open(1);
        Packet packet = publish(payloadSize);
        for (int i = 1; i <= admitted; i++) {
            assertThat(sut.send(1, i, packet)).isEqualTo(ACCEPTED);
        }

        // Act & Assert
        assertThat(sut.send(1, 2, packet)).isEqualTo(FULL);
        assertThat(sut.send(1, 3, publish())).isEqualTo(ACCEPTED);
        assertThat(sut.send(1, 4, new Packet.PingReq())).isEqualTo(ACCEPTED);
        transport.completeInline = true;
        transport.complete();
        drainCompletions();
        assertThat(transport.attempts).doesNotContain(2L).endsWith(3L, 4L);
        assertThat(events.failures).isEmpty();
        assertThat(events.ends).isEmpty();
        assertThat(transport.rejectSend).isFalse();
    }

    @Test
    void inline_completions_do_not_start_another_write_inside_the_callback() {
        // Arrange
        SingleWriteTransport transport = open(1);
        for (int i = 1; i <= 16; i++) {
            sut.send(1, i, new Packet.PingReq());
        }
        transport.completeInline = true;

        // Act
        transport.complete();

        // Assert: draining waits for the actor's completion event.
        assertThat(transport.attempts).containsExactly(1L);
        drainCompletions();
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
        transport.rejectSend = true;
        transport.endInsideSend = inline ? failure : null;

        // Act
        transport.complete();
        drainCompletions();
        assertThat(sut.send(1, 4, new Packet.PingReq())).isEqualTo(CLOSED);
        if (!inline) {
            assertThat(events.ends).isEmpty();
            transport.end(failure);
        }

        // Assert
        assertThat(transport.attempts).containsExactly(1L, 2L);
        assertThat(events.tickets).containsExactly(1L);
        assertThat(events.failures).isEmpty();
        assertThat(events.ends).containsExactly(failure);
    }

    @Test
    void close_discards_waiting_writes_and_old_completions_do_not_advance_a_new_connection() {
        // Arrange
        SingleWriteTransport old = open(1);
        sut.send(1, 1, new Packet.PingReq());
        sut.send(1, 2, new Packet.PingReq());

        // Act: transport shutdown is asynchronous and still owns its active buffer.
        sut.close();
        SingleWriteTransport current = open(2);
        sut.send(2, 1, new Packet.PingReq());
        sut.send(2, 2, new Packet.PingReq());
        old.complete();
        old.end(new IOException("Old connection closed"));
        current.complete();
        sut.sendNext(1);

        // Assert: the old queue neither sends its pending item nor advances the new queue.
        assertThat(old.attempts).containsExactly(1L);
        assertThat(current.attempts).containsExactly(1L);
        assertThat(events.ready).containsExactly(2L);
        assertThat(events.ends).isEmpty();
        drainCompletions();
        assertThat(current.attempts).containsExactly(1L, 2L);
        assertThat(events.failures).isEmpty();
    }

    private SingleWriteTransport open(long generation) {
        sut.open(generation);
        return transports.getLast();
    }

    private void drainCompletions() {
        while (!events.ready.isEmpty()) {
            sut.sendNext(events.ready.removeFirst());
        }
    }

    private static Packet.Publish publish() {
        return publish(0);
    }

    private static Packet.Publish publish(int payloadSize) {
        return new Packet.Publish(
                new Topic("a"),
                new byte[payloadSize],
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
                Arguments.of(new Packet.PubAck(1, ReasonCode.SUCCESS, EMPTY), 16),
                Arguments.of(new Packet.PingReq(), 16),
                Arguments.of(new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY), 16)
        );
    }

    private static final class SingleWriteTransport implements Transport {

        final List<Long> attempts = new ArrayList<>();
        @Nullable Listener listener;
        long activeTicket;
        boolean completeInline;
        boolean rejectSend;
        @Nullable Throwable endInsideSend;
        int sendDepth;
        int maximumSendDepth;

        @Override
        public void start(URI endpoint, Duration timeout, Listener receiver) {
            listener = receiver;
            receiver.onConnected();
        }

        @Override
        public SendResult send(long ticket, byte[] bytes) {
            assertThat(activeTicket).isZero();
            sendDepth++;
            maximumSendDepth = Math.max(maximumSendDepth, sendDepth);
            try {
                attempts.add(ticket);
                if (rejectSend) {
                    if (endInsideSend != null) {
                        end(endInsideSend);
                    }
                    return SendResult.CLOSED;
                }
                activeTicket = ticket;
                if (completeInline) {
                    complete();
                }
                return SendResult.ACCEPTED;
            } finally {
                sendDepth--;
            }
        }

        void complete() {
            assertThat(activeTicket).isNotZero();
            long ticket = activeTicket;
            activeTicket = 0;
            requireNonNull(listener).onSent(ticket);
        }

        void end(Throwable cause) {
            activeTicket = 0;
            requireNonNull(listener).onClosed(cause);
        }

        @Override
        public void close() {
            rejectSend = true;
        }
    }

    private static final class RecordingEvents implements ClientIo.Events {

        final ArrayDeque<Long> ready = new ArrayDeque<>();
        final List<Long> tickets = new ArrayList<>();
        final List<Throwable> failures = new ArrayList<>();
        final List<Throwable> ends = new ArrayList<>();

        @Override
        public void connected(long generation) {
        }

        @Override
        public void received(long generation, Packet packet, int size) {
        }

        @Override
        public void sent(long generation, long ticket) {
            tickets.add(ticket);
            ready.addLast(generation);
        }

        @Override
        public void ended(long generation, Throwable cause) {
            ends.add(cause);
        }

        @Override
        public void failed(long generation, Throwable cause) {
            failures.add(cause);
        }
    }
}
