package org.mqtt4j.hato.client.internal;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.LongConsumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.client.internal.common.io.FrameReader;
import org.mqtt4j.hato.client.internal.common.io.FrameWriter;
import org.mqtt4j.hato.client.internal.common.io.TcpConnection;
import org.mqtt4j.hato.spi.Transport;
import org.mqtt4j.hato.testkit.FaultyTransport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;
import org.mqtt4j.hato.testsupport.Tls;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mqtt4j.hato.spi.Transport.SendResult.ACCEPTED;
import static org.mqtt4j.hato.spi.Transport.SendResult.CLOSED;

/// The same ownership, ordering, and termination contract for TCP, TLS, and their fault wrappers.
@SuppressWarnings("try") // Explicit close is the behavior under test; try still cleans up failures.
class TransportContractTest {

    @ParameterizedTest(name = "{displayName}, TLS: {0}, fault injection wrapper: {1}")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void disconnect_succeeds_when_peer_close_precedes_the_write_completion_notification(
            boolean tls,
            boolean faulty
    ) throws Exception {
        // Arrange: hold a successful write's return until the reader has observed peer EOF.
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (
                ServerSocket server = listen(tls);
                Transport transport = delayedTransport(
                        tls,
                        faulty,
                        false,
                        new ManualTimerScheduler()
                );
                MqttClient sut = MqttClient.builder()
                        .server(endpoint(server, tls))
                        .version(MqttVersion.V3_1_1)
                        .connectionListener(ended::add)
                        .transportFactory(_ -> transport)
                        .build()
        ) {
            CompletableFuture<ConnAck> connected = sut.connect();
            try (Socket peer = accept(server)) {
                var reader = new FrameReader(peer.getInputStream(), 1024);
                var writer = new FrameWriter(peer.getOutputStream());
                assertThat(reader.read().orElseThrow()[0]).isEqualTo((byte) 0x10);
                writer.write(new byte[] {0x20, 0x02, 0x00, 0x00});
                connected.get(5, SECONDS);

                // Act: close immediately after receiving the entire DISCONNECT frame.
                CompletableFuture<Void> disconnected = sut.disconnect();
                assertThat(reader.read().orElseThrow()).containsExactly((byte) 0xE0, (byte) 0x00);
                peer.close();

                // Assert: onSent must reach the core before onClosed for this to succeed.
                disconnected.get(5, SECONDS);
                assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true));
                assertThat(ended).isEmpty();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, TLS: {0}, wrapper: {1}, local close: {2}, "
            + "write fails: {3}, delayed: {4}")
    @CsvSource({
            "false, false, false, false, false", "false, false, false, true, false",
            "false, false, true, false, false", "false, false, true, true, false",
            "false, true, false, false, false", "false, true, false, true, false",
            "false, true, true, false, false", "false, true, true, true, false",
            "false, true, false, false, true", "false, true, false, true, true",
            "false, true, true, false, true", "false, true, true, true, true",
            "true, false, false, false, false", "true, false, false, true, false",
            "true, false, true, false, false", "true, false, true, true, false",
            "true, true, false, false, false", "true, true, false, true, false",
            "true, true, true, false, false", "true, true, true, true, false",
            "true, true, false, false, true", "true, true, false, true, true",
            "true, true, true, false, true", "true, true, true, true, true"
    })
    void shutdown_preserves_the_local_write_result(
            boolean tls,
            boolean faulty,
            boolean localClose,
            boolean writeFails,
            boolean delayed
    ) throws Exception {
        // Arrange: local writing returns or fails only after the socket has been closed.
        byte[] packet = {(byte) 0xE0, 0x00};
        var clock = new ManualTimerScheduler();
        try (
                ServerSocket server = listen(tls);
                Transport sut = delayedTransport(tls, faulty, writeFails, clock)
        ) {
            var receiver = new Receiver();
            sut.start(endpoint(server, tls), Duration.ofSeconds(5), receiver);
            try (Socket peer = accept(server)) {
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                if (delayed) {
                    ((FaultyTransport) sut).sendDelay(Duration.ofSeconds(1));
                }
                assertThat(sut.send(1, packet)).isEqualTo(ACCEPTED);
                if (delayed) {
                    assertThat(clock.pendingTaskCount()).isEqualTo(1);
                    clock.advance(Duration.ofSeconds(1));
                }
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
                assertThat(clock.pendingTaskCount()).isZero();
                assertThat(receiver.events).isEmpty();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, TLS: {0}")
    @ValueSource(booleans = {false, true})
    void a_connection_returned_after_close_is_closed_without_connected_notification(boolean tls)
            throws Exception {
        // Arrange: hold the socket result until after shutdown has been requested.
        var result = new CompletableFuture<TcpTransport.Connection>();
        var entered = new CompletableFuture<Void>();
        SSLContext context = Tls.clientContext();
        try (
                ServerSocket server = listen();
                var sut = new TcpTransport(
                        "late",
                        tls ? "mqtts" : "mqtt",
                        new Semaphore(1),
                        InetAddress::getByName,
                        (_, _, _) -> {
                            entered.complete(null);
                            return result.join();
                        },
                        DefaultTimerScheduler.instance
                )
        ) {
            var receiver = new Receiver();
            sut.start(endpoint(server, tls), Duration.ofSeconds(5), receiver);
            entered.get(5, SECONDS);
            assertThatThrownBy(() -> sut.send(1, new byte[] {1}))
                    .isInstanceOf(IllegalStateException.class);

            // Act
            sut.close();

            // Assert
            assertThat(sut.send(2, new byte[] {2})).isEqualTo(CLOSED);
            assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(Throwable.class);
            try (
                    TcpTransport.Connection late = tls
                            ? TcpTransport.tls(
                                    "localhost",
                                    new InetSocketAddress("localhost", server.getLocalPort()),
                                    Duration.ofSeconds(5),
                                    context,
                                    context.getDefaultSSLParameters()
                            )
                            : TcpTransport.plain(TcpConnection.connect(
                                    "127.0.0.1",
                                    server.getLocalPort(),
                                    Duration.ofSeconds(5)
                            ));
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

    @ParameterizedTest(name = "{displayName}, TLS: {0}, wrapper: {1}, delayed: {2}")
    @CsvSource({
            "false, false, false", "false, true, false", "false, true, true",
            "true, false, false", "true, true, false", "true, true, true"
    })
    void ordered_bytes_write_completions_and_a_single_terminal_event(
            boolean tls,
            boolean faulty,
            boolean delayed
    ) throws Exception {
        // Arrange
        var clock = new ManualTimerScheduler();
        try (
                ServerSocket server = listen(tls);
                Transport sut = transport(tls, faulty, clock)
        ) {
            var receiver = new Receiver();
            sut.start(endpoint(server, tls), Duration.ofSeconds(5), receiver);
            try (Socket socket = accept(server)) {
                socket.setSoTimeout(5000);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                if (delayed) {
                    ((FaultyTransport) sut).sendDelay(Duration.ofSeconds(1));
                }

                // Act: the completion callback may start the next write immediately.
                receiver.sent = ticket -> {
                    if (ticket == 1) {
                        assertThat(sut.send(2, new byte[] {4, 5})).isEqualTo(ACCEPTED);
                    }
                };
                byte[] first = {1, 2, 3};
                assertThat(sut.send(1, first)).isEqualTo(ACCEPTED);
                if (delayed) {
                    assertThatThrownBy(() -> sut.send(3, new byte[] {9}))
                            .isInstanceOf(IllegalStateException.class);
                    assertThat(clock.pendingTaskCount()).isEqualTo(1);
                    clock.advance(Duration.ofMillis(999));
                    assertThat(receiver.events).isEmpty();
                    clock.advance(Duration.ofMillis(1));
                    // TCP reports completion on its worker; wait for its reentrant send's timer.
                    await().untilAsserted(() -> assertThat(clock.pendingTaskCount()).isEqualTo(1));
                    clock.advance(Duration.ofMillis(999));
                    assertThat(receiver.events).containsExactly(1L);
                    clock.advance(Duration.ofMillis(1));
                }

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
                assertThat(clock.pendingTaskCount()).isZero();
                assertThat(receiver.events).isEmpty();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, TLS: {0}, ending: {1}")
    @CsvSource({
            "false, close", "false, disconnect", "false, peer EOF",
            "true, close", "true, disconnect", "true, peer EOF"
    })
    void ending_a_delayed_send_returns_ownership_without_writing(boolean tls, String ending)
            throws Exception {
        // Arrange: the wrapper owns the bytes, but TCP has not received a send yet.
        var clock = new ManualTimerScheduler();
        var failure = new IOException("Injected disconnect before writing");
        byte[] bytes = {1, 2, 3};
        try (
                ServerSocket server = listen(tls);
                FaultyTransport sut = new FaultyTransport(transport(tls, false, clock), clock)
                        .sendDelay(Duration.ofSeconds(1))
        ) {
            var receiver = new Receiver();
            sut.start(endpoint(server, tls), Duration.ofSeconds(5), receiver);
            try (Socket peer = accept(server)) {
                peer.setSoTimeout(5000);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                assertThat(sut.send(1, bytes)).isEqualTo(ACCEPTED);
                assertThat(clock.pendingTaskCount()).isEqualTo(1);

                // Act: peer EOF uses a half-close so its input remains readable afterward.
                switch (ending) {
                    case "close" -> sut.close();
                    case "disconnect" -> sut.disconnect(failure);
                    case "peer EOF" -> peer.shutdownOutput();
                    default -> throw new AssertionError(ending);
                }

                // Assert: termination alone returns ownership; there was no successful write.
                Object terminal = receiver.events.poll(5, SECONDS);
                assertThat(terminal).isInstanceOf(IOException.class);
                if (ending.equals("disconnect")) {
                    assertThat(terminal).isSameAs(failure);
                }
                assertThat(clock.pendingTaskCount()).isZero();
                bytes[0] = 9;
                clock.advance(Duration.ofSeconds(1));
                assertThat(peer.getInputStream().read()).isEqualTo(-1);
                assertThat(sut.send(2, bytes)).isEqualTo(CLOSED);
                sut.close();
                assertThat(receiver.events).isEmpty();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, TLS: {0}, fault injection wrapper: {1}")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void stalled_writes_allow_reads_and_close_without_waiting(boolean tls, boolean faulty)
            throws Exception {
        // Arrange
        try (
                ServerSocket server = listen(tls);
                Transport sut = transport(tls, faulty, new ManualTimerScheduler())
        ) {
            server.setReceiveBufferSize(1024);
            var receiver = new Receiver();
            sut.start(endpoint(server, tls), Duration.ofSeconds(5), receiver);
            try (Socket socket = accept(server)) {
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
                assertTimeoutPreemptively(Duration.ofSeconds(2), sut::close);

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

    @ParameterizedTest(name = "{displayName}, TLS: {0}, fault injection wrapper: {1}")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void a_large_write_completes_for_a_peer_that_only_reads(boolean tls, boolean faulty)
            throws Exception {
        // Arrange: the write exceeds the socket buffers, and the peer never sends anything.
        var payload = new byte[8 * 1024 * 1024];
        try (
                ServerSocket server = listen(tls);
                Transport sut = transport(tls, faulty, new ManualTimerScheduler())
        ) {
            server.setReceiveBufferSize(64 * 1024);
            var receiver = new Receiver();
            sut.start(endpoint(server, tls), Duration.ofSeconds(5), receiver);
            try (Socket socket = accept(server)) {
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

    @ParameterizedTest(name = "{displayName}, TLS: {0}, fault injection wrapper: {1}")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void close_before_start_never_establishes_a_connection(boolean tls, boolean faulty)
            throws Exception {
        // Arrange
        try (Transport sut = transport(tls, faulty, new ManualTimerScheduler())) {
            var receiver = new Receiver();
            assertThatThrownBy(() -> sut.send(1, new byte[] {1}))
                    .isInstanceOf(IllegalStateException.class);

            // Act
            sut.close();
            assertThat(sut.send(2, new byte[] {2})).isEqualTo(CLOSED);
            sut.start(
                    URI.create((tls ? "mqtts" : "mqtt") + "://127.0.0.1:1"),
                    Duration.ofSeconds(1),
                    receiver
            );

            // Assert
            assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(Throwable.class);
            assertThat(receiver.events).isEmpty();
        }
    }

    private static Transport transport(
            boolean tls,
            boolean faulty,
            ManualTimerScheduler clock
    ) throws Exception {
        Transport socket;
        if (tls) {
            SSLContext context = Tls.clientContext();
            SSLParameters parameters = context.getDefaultSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            socket = new TcpTransport("contract", context, parameters);
        } else {
            socket = Transport.tcp("contract");
        }
        return faulty ? new FaultyTransport(socket, clock) : socket;
    }

    private static Transport delayedTransport(
            boolean tls,
            boolean faulty,
            boolean writeFails,
            ManualTimerScheduler clock
    ) throws Exception {
        SSLContext context = Tls.clientContext();
        SSLParameters parameters = context.getDefaultSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        var transport = new TcpTransport(
                "contract",
                tls ? "mqtts" : "mqtt",
                new Semaphore(1),
                InetAddress::getByName,
                (host, address, timeout) -> delayedConnection(
                        tls ? TcpTransport.tls(host, address, timeout, context, parameters)
                                : TcpTransport.plain(TcpConnection.connect(address, timeout)),
                        writeFails
                ),
                DefaultTimerScheduler.instance
        );
        return faulty ? new FaultyTransport(transport, clock) : transport;
    }

    private static TcpTransport.Connection delayedConnection(
            TcpTransport.Connection connection,
            boolean writeFails
    ) {
        var closed = new CompletableFuture<Void>();
        return new TcpTransport.Connection() {
            @Override
            public InputStream input() throws IOException {
                return connection.input();
            }

            @Override
            public void handshake() throws IOException {
                connection.handshake();
            }

            @Override
            public boolean closesGracefully() {
                return connection.closesGracefully();
            }

            @Override
            public OutputStream output() throws IOException {
                return new FilterOutputStream(connection.output()) {
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
                    connection.close();
                } finally {
                    closed.complete(null);
                }
            }

            @Override
            public void abort() throws IOException {
                try {
                    connection.abort();
                } finally {
                    closed.complete(null);
                }
            }
        };
    }

    private static ServerSocket listen(boolean tls) throws Exception {
        return tls ? Tls.serverSocket() : listen();
    }

    private static Socket accept(ServerSocket server) throws IOException {
        Socket socket = server.accept();
        socket.setSoTimeout(5000);
        if (socket instanceof SSLSocket secure) {
            secure.startHandshake();
        }
        return socket;
    }

    private static URI endpoint(ServerSocket server, boolean tls) {
        return tls ? URI.create("mqtts://localhost:" + server.getLocalPort()) : endpoint(server);
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
