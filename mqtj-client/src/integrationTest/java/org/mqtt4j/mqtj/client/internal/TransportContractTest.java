package org.mqtt4j.mqtj.client.internal;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.LongConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.mqtj.Disconnection;
import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.client.internal.common.io.FrameReader;
import org.mqtt4j.mqtj.client.internal.common.io.FrameWriter;
import org.mqtt4j.mqtj.client.internal.common.io.TcpConnection;
import org.mqtt4j.mqtj.client.internal.common.io.TcpConnectionTestSupport;
import org.mqtt4j.mqtj.spi.Transport;
import org.mqtt4j.mqtj.testkit.FaultyTransport;
import org.mqtt4j.mqtj.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.mqtj.spi.Transport.SendResult.ACCEPTED;
import static org.mqtt4j.mqtj.spi.Transport.SendResult.CLOSED;

/// The same ownership, ordering, and termination contract for TCP and its fault wrapper.
@SuppressWarnings("try") // Explicit close is the behavior under test; try still cleans up failures.
class TransportContractTest {

    @ParameterizedTest(name = "{displayName}, fault injection wrapper: {0}")
    @ValueSource(booleans = {false, true})
    void disconnect_succeeds_when_peer_close_precedes_the_write_completion_notification(
            boolean faulty
    ) throws Exception {
        // Arrange: hold a successful write's return until the reader has observed peer EOF.
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (
                ServerSocket server = listen();
                Socket socket = socketWithDelayedDisconnectCompletion(false)
        ) {
            socket.connect(server.getLocalSocketAddress(), 5000);
            try (
                    TcpConnection connection = TcpConnectionTestSupport.wrap(socket);
                    Socket peer = server.accept();
                    MqttClient sut = MqttClient.builder()
                            .server(endpoint(server))
                            .version(MqttVersion.V3_1_1)
                            .connectionListener(ended::add)
                            .transportFactory(_ -> transport(faulty, connection))
                            .build()
            ) {
                peer.setSoTimeout(5000);
                var reader = new FrameReader(peer.getInputStream(), 1024);
                var writer = new FrameWriter(peer.getOutputStream());
                var connected = sut.connect();
                assertThat(reader.read().orElseThrow()[0]).isEqualTo((byte) 0x10);
                writer.write(new byte[] {0x20, 0x02, 0x00, 0x00});
                connected.get(5, SECONDS);

                // Act: close immediately after receiving the entire DISCONNECT frame.
                var disconnected = sut.disconnect();
                assertThat(reader.read().orElseThrow()).containsExactly((byte) 0xE0, (byte) 0x00);
                peer.close();

                // Assert: onSent must reach the core before onClosed for this to succeed.
                disconnected.get(5, SECONDS);
                assertThat(ended.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
                assertThat(ended).isEmpty();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, wrapper: {0}, local close: {1}, write fails: {2}")
    @CsvSource({
            "false, false, false", "false, false, true",
            "false, true, false", "false, true, true",
            "true, false, false", "true, false, true",
            "true, true, false", "true, true, true"
    })
    void shutdown_preserves_the_local_write_result(
            boolean faulty,
            boolean localClose,
            boolean writeFails
    ) throws Exception {
        // Arrange: local writing returns or fails only after the socket has been closed.
        byte[] packet = {(byte) 0xE0, 0x00};
        try (
                ServerSocket server = listen();
                Socket socket = socketWithDelayedDisconnectCompletion(writeFails)
        ) {
            socket.connect(server.getLocalSocketAddress(), 5000);
            try (
                    TcpConnection connection = TcpConnectionTestSupport.wrap(socket);
                    Socket peer = server.accept();
                    Transport sut = transport(faulty, connection)
            ) {
                peer.setSoTimeout(5000);
                var receiver = new Receiver();
                sut.start(endpoint(server), Duration.ofSeconds(5), receiver);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                assertThat(sut.send(1, packet)).isEqualTo(ACCEPTED);
                assertThat(peer.getInputStream().readNBytes(packet.length)).isEqualTo(packet);

                // Act: terminate after peer receipt, before the local write's result is known.
                if (localClose) {
                    sut.close();
                } else {
                    peer.close();
                }

                // Assert: only successful local writing precedes the one final event with onSent.
                if (!writeFails) {
                    assertThat(receiver.events.poll(5, SECONDS)).isEqualTo(1L);
                }
                assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(IOException.class);
                assertThat(sut.send(2, packet)).isEqualTo(CLOSED);
                sut.close();
                assertThat(receiver.events).isEmpty();
            }
        }
    }

    @Test
    void a_connection_returned_after_close_is_closed_without_connected_notification()
            throws Exception {
        // Arrange: hold the TCP result until after shutdown has been requested.
        var result = new CompletableFuture<TcpConnection>();
        var entered = new CompletableFuture<Void>();
        try (
                ServerSocket server = listen();
                var sut = new TcpTransport(
                        "late",
                        new Semaphore(1),
                        InetAddress::getByName,
                        (_, _) -> {
                            entered.complete(null);
                            return result.join();
                        }
                )
        ) {
            var receiver = new Receiver();
            sut.start(endpoint(server), Duration.ofSeconds(5), receiver);
            entered.get(5, SECONDS);
            assertThatThrownBy(() -> sut.send(1, new byte[] {1}))
                    .isInstanceOf(IllegalStateException.class);

            // Act
            sut.close();

            // Assert
            assertThat(sut.send(2, new byte[] {2})).isEqualTo(CLOSED);
            assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(Throwable.class);
            try (
                    var late = TcpConnection.connect(
                            "127.0.0.1",
                            server.getLocalPort(),
                            Duration.ofSeconds(5)
                    );
                    Socket peer = server.accept()
            ) {
                peer.setSoTimeout(5000);

                // Act: release the late connection result.
                result.complete(late);

                // Assert
                assertThat(peer.getInputStream().read()).isEqualTo(-1);
                assertThat(receiver.events).isEmpty();
            }
        } finally {
            result.completeExceptionally(new IllegalStateException("Test cleanup"));
        }
    }

    @ParameterizedTest(name = "{displayName}, fault injection wrapper: {0}")
    @ValueSource(booleans = {false, true})
    void ordered_bytes_write_completions_and_a_single_terminal_event(boolean faulty)
            throws Exception {
        // Arrange
        try (
                ServerSocket server = listen();
                Transport sut = transport(faulty)
        ) {
            var receiver = new Receiver();
            sut.start(endpoint(server), Duration.ofSeconds(5), receiver);
            try (Socket socket = server.accept()) {
                socket.setSoTimeout(5000);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");

                // Act: the completion callback may start the next write immediately.
                receiver.sent = ticket -> {
                    if (ticket == 1) {
                        assertThat(sut.send(2, new byte[] {4, 5})).isEqualTo(ACCEPTED);
                    }
                };
                byte[] first = {1, 2, 3};
                assertThat(sut.send(1, first)).isEqualTo(ACCEPTED);

                // Assert
                assertThat(socket.getInputStream().readNBytes(5)).containsExactly(1, 2, 3, 4, 5);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo(1L);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo(2L);
                assertThat(first).containsExactly(1, 2, 3);

                // Act: retain a received chunk while subsequent input arrives.
                socket.getOutputStream().write(new byte[] {6, 7});
                socket.getOutputStream().flush();
                byte[] received = requireNonNull(receiver.bytes.poll(5, SECONDS));
                byte[] saved = received.clone();
                socket.getOutputStream().write(new byte[] {8, 9});
                socket.getOutputStream().flush();
                var all = new ArrayList<Byte>();
                for (byte value : received) {
                    all.add(value);
                }
                while (all.size() < 4) {
                    for (byte value : requireNonNull(receiver.bytes.poll(5, SECONDS))) {
                        all.add(value);
                    }
                }

                // Assert: receiving again must not mutate the transferred buffer.
                assertThat(all).containsExactly((byte) 6, (byte) 7, (byte) 8, (byte) 9);
                assertThat(received).containsExactly(saved);
                socket.shutdownOutput();
                assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(Throwable.class);
                sut.close();
                sut.close();
                assertThat(sut.send(3, new byte[] {1})).isEqualTo(CLOSED);
                assertThat(socket.getInputStream().read()).isEqualTo(-1);
                assertThat(receiver.events).isEmpty();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, fault injection wrapper: {0}")
    @ValueSource(booleans = {false, true})
    void stalled_writes_allow_reads_and_close_without_waiting(boolean faulty)
            throws Exception {
        // Arrange
        try (
                ServerSocket server = listen();
                Transport sut = transport(faulty)
        ) {
            server.setReceiveBufferSize(1024);
            var receiver = new Receiver();
            sut.start(endpoint(server), Duration.ofSeconds(5), receiver);
            try (Socket socket = server.accept()) {
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");

                // Arrange: exceed the old transport byte limit in one outstanding write.
                assertThat(sut.send(1, new byte[16 * 1024 * 1024 + 1])).isEqualTo(ACCEPTED);
                assertThatThrownBy(() -> sut.send(2, new byte[] {1}))
                        .isInstanceOf(IllegalStateException.class);

                // Assert: the peer can send while deliberately refusing to drain our write.
                socket.setSoTimeout(5000);
                assertThat(socket.getInputStream().read()).isZero();
                socket.getOutputStream().write(42);
                assertThat(receiver.bytes.poll(5, SECONDS)).containsExactly((byte) 42);
                assertThat(receiver.events).isEmpty();

                // Act
                sut.close();

                // Assert
                Object event;
                do {
                    event = receiver.events.poll(5, SECONDS);
                    assertThat(event).isNotNull();
                } while (event instanceof Long);
                assertThat(event).isInstanceOf(Throwable.class);
                assertThat(sut.send(101, new byte[] {1})).isEqualTo(CLOSED);
                assertThat(receiver.events).isEmpty();
                assertThat(socket.isClosed()).isFalse();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, fault injection wrapper: {0}")
    @ValueSource(booleans = {false, true})
    void a_large_write_completes_for_a_peer_that_only_reads(boolean faulty) throws Exception {
        // Arrange: the write exceeds the socket buffers, and the peer never sends anything.
        byte[] payload = new byte[8 * 1024 * 1024];
        try (
                ServerSocket server = listen();
                Transport sut = transport(faulty)
        ) {
            server.setReceiveBufferSize(64 * 1024);
            var receiver = new Receiver();
            sut.start(endpoint(server), Duration.ofSeconds(5), receiver);
            try (Socket socket = server.accept()) {
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                socket.setSoTimeout(5000);

                // Act
                assertThat(sut.send(1, payload)).isEqualTo(ACCEPTED);

                // Assert: draining alone lets the transport finish the write.
                assertThat(socket.getInputStream().readNBytes(payload.length).length)
                        .isEqualTo(payload.length);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo(1L);
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, fault injection wrapper: {0}")
    @ValueSource(booleans = {false, true})
    void close_before_start_never_establishes_a_connection(boolean faulty) throws Exception {
        // Arrange
        try (Transport sut = transport(faulty)) {
            var receiver = new Receiver();
            assertThatThrownBy(() -> sut.send(1, new byte[] {1}))
                    .isInstanceOf(IllegalStateException.class);

            // Act
            sut.close();
            assertThat(sut.send(2, new byte[] {2})).isEqualTo(CLOSED);
            sut.start(URI.create("mqtt://127.0.0.1:1"), Duration.ofSeconds(1), receiver);

            // Assert
            assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(Throwable.class);
            assertThat(receiver.events).isEmpty();
        }
    }

    private static Transport transport(boolean faulty) {
        Transport tcp = Transport.tcp("contract");
        return faulty ? new FaultyTransport(tcp, new ManualTimerScheduler()) : tcp;
    }

    private static Transport transport(boolean faulty, TcpConnection connection) {
        var tcp = new TcpTransport(
                "contract",
                new Semaphore(1),
                InetAddress::getByName,
                (_, _) -> connection
        );
        return faulty ? new FaultyTransport(tcp, new ManualTimerScheduler()) : tcp;
    }

    private static Socket socketWithDelayedDisconnectCompletion(boolean writeFails) {
        var closed = new CompletableFuture<Void>();
        return new Socket() {
            @Override
            public OutputStream getOutputStream() throws IOException {
                return new FilterOutputStream(super.getOutputStream()) {
                    private boolean disconnect;

                    @Override
                    public void write(byte[] bytes, int offset, int length) throws IOException {
                        out.write(bytes, offset, length);
                        disconnect = length == 2 && bytes[offset] == (byte) 0xE0;
                    }

                    @Override
                    public void flush() throws IOException {
                        super.flush();
                        if (disconnect) {
                            closed.join();
                            if (writeFails) {
                                throw new IOException("Injected write failure");
                            }
                        }
                    }
                };
            }

            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    closed.complete(null);
                }
            }
        };
    }

    private static ServerSocket listen() throws Exception {
        var server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        server.setSoTimeout(5000);
        return server;
    }

    private static URI endpoint(ServerSocket server) {
        return URI.create("mqtt://127.0.0.1:" + server.getLocalPort());
    }

    private static final class Receiver implements Transport.Listener {

        final LinkedBlockingQueue<Object> events = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<byte[]> bytes = new LinkedBlockingQueue<>();
        volatile LongConsumer sent = _ -> { };

        @Override
        public void onConnected() {
            events.add("connected");
        }

        @Override
        public void onBytes(byte[] value) {
            bytes.add(value);
        }

        @Override
        public void onSent(long ticket) {
            events.add(ticket);
            sent.accept(ticket);
        }

        @Override
        public void onClosed(Throwable cause) {
            events.add(cause);
        }
    }
}
