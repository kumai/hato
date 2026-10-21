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
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.LongConsumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReconnectPolicy;
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

/// The same ownership, ordering, and termination contract for every transport kind and its fault
/// wrapper. Each kind makes its transports and the local peer that they connect to, so a test
/// sees the peer only as the byte stream that carries MQTT packets.
@SuppressWarnings("try") // Explicit close is the behavior under test; try still cleans up failures.
class TransportContractTest {

    // The HttpClients of the WebSocket transports, released after every case has run.
    private static final List<WebSocketTransports> webSocketOwners = new CopyOnWriteArrayList<>();

    private static final WebSocketTransports webSockets = owned(new WebSocketTransports(
            "contract",
            DefaultTimerScheduler.instance
    ));

    @AfterAll
    static void releaseHttpClients() throws InterruptedException {
        var released = new CountDownLatch(webSocketOwners.size());
        for (WebSocketTransports owner : webSocketOwners) {
            owner.release(released::countDown);
        }
        assertThat(released.await(10, SECONDS)).isTrue();
    }

    @ParameterizedTest(name = "{displayName}, {0}, fault injection wrapper: {1}")
    @CsvSource({"TCP, false", "TCP, true", "TLS, false", "TLS, true", "WS, false", "WS, true"})
    void disconnect_succeeds_when_peer_close_precedes_the_write_completion_notification(
            Kind kind,
            boolean faulty
    ) throws Exception {
        // Arrange: hold a successful write's return until the reader has observed peer EOF.
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (
                PeerServer server = kind.listen();
                Transport transport = kind.delayedTransport(
                        faulty,
                        false,
                        new ManualTimerScheduler()
                );
                MqttClient sut = MqttClient.builder()
                        .server(server.endpoint())
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .connectionListener(ended::add)
                        .transportFactory(_ -> transport)
                        .build()
        ) {
            CompletableFuture<ConnAck> connected = sut.connect();
            try (Peer peer = server.accept()) {
                var reader = new FrameReader(peer.input(), 1024);
                var writer = new FrameWriter(peer.output());
                assertThat(reader.read().orElseThrow()[0]).isEqualTo((byte) 0x10);
                writer.write(new byte[] {0x20, 0x02, 0x00, 0x00});
                connected.get(5, SECONDS);

                // Act: close immediately after receiving the entire DISCONNECT frame.
                CompletableFuture<Void> disconnected = sut.disconnect();
                assertThat(reader.read().orElseThrow()).containsExactly((byte) 0xE0, (byte) 0x00);
                peer.close();

                // Assert: onSent must reach the core before onClosed for this to succeed.
                disconnected.get(5, SECONDS);
                assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
                assertThat(ended).isEmpty();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, {0}, wrapper: {1}, local close: {2}, "
            + "write fails: {3}, delayed: {4}")
    @CsvSource({
            "TCP, false, false, false, false", "TCP, false, false, true, false",
            "TCP, false, true, false, false", "TCP, false, true, true, false",
            "TCP, true, false, false, false", "TCP, true, false, true, false",
            "TCP, true, true, false, false", "TCP, true, true, true, false",
            "TCP, true, false, false, true", "TCP, true, false, true, true",
            "TCP, true, true, false, true", "TCP, true, true, true, true",
            "TLS, false, false, false, false", "TLS, false, false, true, false",
            "TLS, false, true, false, false", "TLS, false, true, true, false",
            "TLS, true, false, false, false", "TLS, true, false, true, false",
            "TLS, true, true, false, false", "TLS, true, true, true, false",
            "TLS, true, false, false, true", "TLS, true, false, true, true",
            "TLS, true, true, false, true", "TLS, true, true, true, true",
            "WS, false, false, false, false", "WS, false, false, true, false",
            "WS, false, true, false, false", "WS, false, true, true, false",
            "WS, true, false, false, false", "WS, true, false, true, false",
            "WS, true, true, false, false", "WS, true, true, true, false",
            "WS, true, false, false, true", "WS, true, false, true, true",
            "WS, true, true, false, true", "WS, true, true, true, true"
    })
    void shutdown_preserves_the_local_write_result(
            Kind kind,
            boolean faulty,
            boolean localClose,
            boolean writeFails,
            boolean delayed
    ) throws Exception {
        // Arrange: local writing returns or fails only after the socket has been closed.
        byte[] packet = {(byte) 0xE0, 0x00};
        var clock = new ManualTimerScheduler();
        try (
                PeerServer server = kind.listen();
                Transport sut = kind.delayedTransport(faulty, writeFails, clock)
        ) {
            var receiver = new Receiver();
            sut.start(server.endpoint(), Duration.ofSeconds(5), receiver);
            try (Peer peer = server.accept()) {
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                if (delayed) {
                    ((FaultyTransport) sut).sendDelay(Duration.ofSeconds(1));
                }
                assertThat(sut.send(1, packet)).isEqualTo(ACCEPTED);
                if (delayed) {
                    assertThat(clock.pendingTaskCount()).isEqualTo(1);
                    clock.advance(Duration.ofSeconds(1));
                }
                assertThat(peer.input().readNBytes(packet.length)).isEqualTo(packet);

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

    @ParameterizedTest(name = "{displayName}, {0}")
    @EnumSource(value = Kind.class, names = {"TCP", "TLS"})
    void a_connection_returned_after_close_is_closed_without_connected_notification(Kind kind)
            throws Exception {
        // Arrange: hold the socket result until after shutdown has been requested. The case holds
        // TcpTransport's connection step, so only the kinds built on it run it.
        var result = new CompletableFuture<TcpTransport.Connection>();
        var entered = new CompletableFuture<Void>();
        TcpTransport.Connector connector = kind.connector();
        try (
                ServerSocket server = loopbackServer();
                var sut = new TcpTransport(
                        "late",
                        kind.scheme,
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
            sut.start(kind.endpoint(server.getLocalPort()), Duration.ofSeconds(5), receiver);
            entered.get(5, SECONDS);
            assertThatThrownBy(() -> sut.send(1, new byte[] {1}))
                    .isInstanceOf(IllegalStateException.class);

            // Act
            sut.close();

            // Assert
            assertThat(sut.send(2, new byte[] {2})).isEqualTo(CLOSED);
            assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(Throwable.class);
            try (
                    TcpTransport.Connection late = connector.connect(
                            kind.host,
                            new InetSocketAddress(kind.host, server.getLocalPort()),
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

    @ParameterizedTest(name = "{displayName}, {0}, wrapper: {1}, delayed: {2}")
    @CsvSource({
            "TCP, false, false", "TCP, true, false", "TCP, true, true",
            "TLS, false, false", "TLS, true, false", "TLS, true, true",
            "WS, false, false", "WS, true, false", "WS, true, true"
    })
    void ordered_bytes_write_completions_and_a_single_terminal_event(
            Kind kind,
            boolean faulty,
            boolean delayed
    ) throws Exception {
        // Arrange
        var clock = new ManualTimerScheduler();
        try (
                PeerServer server = kind.listen();
                Transport sut = kind.transport(faulty, clock)
        ) {
            var receiver = new Receiver();
            sut.start(server.endpoint(), Duration.ofSeconds(5), receiver);
            try (Peer peer = server.accept()) {
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
                assertThat(peer.input().readNBytes(5)).containsExactly(1, 2, 3, 4, 5);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo(1L);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo(2L);
                assertThat(first).containsExactly(1, 2, 3);

                // Act: retain a received chunk while subsequent input arrives.
                peer.output().write(new byte[] {6, 7});
                peer.output().flush();
                byte[] received = requireNonNull(receiver.bytes.poll(5, SECONDS));
                byte[] saved = received.clone();
                peer.output().write(new byte[] {8, 9});
                peer.output().flush();
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
                peer.shutdownOutput();
                assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(Throwable.class);
                sut.close();
                sut.close();
                assertThat(sut.send(3, new byte[] {1})).isEqualTo(CLOSED);
                assertThat(peer.input().read()).isEqualTo(-1);
                assertThat(clock.pendingTaskCount()).isZero();
                assertThat(receiver.events).isEmpty();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, {0}, ending: {1}")
    @CsvSource({
            "TCP, close", "TCP, disconnect", "TCP, peer EOF",
            "TLS, close", "TLS, disconnect", "TLS, peer EOF",
            "WS, close", "WS, disconnect", "WS, peer EOF"
    })
    void ending_a_delayed_send_returns_ownership_without_writing(Kind kind, String ending)
            throws Exception {
        // Arrange: the wrapper owns the bytes, but TCP has not received a send yet.
        var clock = new ManualTimerScheduler();
        var failure = new IOException("Injected disconnect before writing");
        byte[] bytes = {1, 2, 3};
        try (
                PeerServer server = kind.listen();
                FaultyTransport sut = new FaultyTransport(kind.transport(false, clock), clock)
                        .sendDelay(Duration.ofSeconds(1))
        ) {
            var receiver = new Receiver();
            sut.start(server.endpoint(), Duration.ofSeconds(5), receiver);
            try (Peer peer = server.accept()) {
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
                assertThat(peer.input().read()).isEqualTo(-1);
                assertThat(sut.send(2, bytes)).isEqualTo(CLOSED);
                sut.close();
                assertThat(receiver.events).isEmpty();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, {0}, fault injection wrapper: {1}")
    @CsvSource({"TCP, false", "TCP, true", "TLS, false", "TLS, true", "WS, false", "WS, true"})
    void stalled_writes_allow_reads_and_close_without_waiting(Kind kind, boolean faulty)
            throws Exception {
        // Arrange
        try (
                PeerServer server = kind.listen();
                Transport sut = kind.transport(faulty, new ManualTimerScheduler())
        ) {
            server.receiveBufferSize(1024);
            var receiver = new Receiver();
            sut.start(server.endpoint(), Duration.ofSeconds(5), receiver);
            try (Peer peer = server.accept()) {
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");

                // Arrange: exceed the old transport byte limit in one outstanding write.
                assertThat(sut.send(1, new byte[16 * 1024 * 1024 + 1])).isEqualTo(ACCEPTED);
                assertThatThrownBy(() -> sut.send(2, new byte[] {1}))
                        .isInstanceOf(IllegalStateException.class);

                // Assert: the peer can send while deliberately refusing to drain our write.
                assertThat(peer.input().read()).isZero();
                peer.output().write(42);
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
                assertThat(peer.isClosed()).isFalse();
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, {0}, fault injection wrapper: {1}")
    @CsvSource({"TCP, false", "TCP, true", "TLS, false", "TLS, true", "WS, false", "WS, true"})
    void a_large_write_completes_for_a_peer_that_only_reads(Kind kind, boolean faulty)
            throws Exception {
        // Arrange: the write exceeds the socket buffers, and the peer never sends anything.
        var payload = new byte[8 * 1024 * 1024];
        try (
                PeerServer server = kind.listen();
                Transport sut = kind.transport(faulty, new ManualTimerScheduler())
        ) {
            server.receiveBufferSize(64 * 1024);
            var receiver = new Receiver();
            sut.start(server.endpoint(), Duration.ofSeconds(5), receiver);
            try (Peer peer = server.accept()) {
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");

                // Act
                assertThat(sut.send(1, payload)).isEqualTo(ACCEPTED);

                // Assert: draining alone lets the transport finish the write.
                assertThat(peer.input().readNBytes(payload.length).length)
                        .isEqualTo(payload.length);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo(1L);
            }
        }
    }

    @ParameterizedTest(name = "{displayName}, {0}, fault injection wrapper: {1}")
    @CsvSource({"TCP, false", "TCP, true", "TLS, false", "TLS, true", "WS, false", "WS, true"})
    void close_before_start_never_establishes_a_connection(Kind kind, boolean faulty)
            throws Exception {
        // Arrange
        try (Transport sut = kind.transport(faulty, new ManualTimerScheduler())) {
            var receiver = new Receiver();
            assertThatThrownBy(() -> sut.send(1, new byte[] {1}))
                    .isInstanceOf(IllegalStateException.class);

            // Act
            sut.close();
            assertThat(sut.send(2, new byte[] {2})).isEqualTo(CLOSED);
            sut.start(
                    URI.create(kind.scheme + "://127.0.0.1:1"),
                    Duration.ofSeconds(1),
                    receiver
            );

            // Assert
            assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(Throwable.class);
            assertThat(receiver.events).isEmpty();
        }
    }

    /// The transport kinds under the contract. Each kind makes the transports that the tests
    /// exercise and the local peer that they connect to.
    enum Kind {
        TCP("mqtt", "127.0.0.1"),
        // The test server certificate names localhost, so TLS connects by that name.
        TLS("mqtts", "localhost"),
        WS("ws", "127.0.0.1");

        final String scheme;

        final String host;

        Kind(String scheme, String host) {
            this.scheme = scheme;
            this.host = host;
        }

        URI endpoint(int port) {
            return URI.create(scheme + "://" + host + ":" + port);
        }

        /// The kind's transport, alone or behind the fault injection wrapper.
        Transport transport(boolean faulty, ManualTimerScheduler clock) throws Exception {
            Transport transport = switch (this) {
                case TCP -> Transport.tcp("contract");
                case TLS -> {
                    SSLContext context = Tls.clientContext();
                    yield new TcpTransport(
                            "contract",
                            context,
                            verifyingParameters(context),
                            DefaultTimerScheduler.instance
                    );
                }
                case WS -> webSockets.create("contract");
            };
            return faulty ? new FaultyTransport(transport, clock) : transport;
        }

        /// A transport whose DISCONNECT write returns only after its connection closes, and then
        /// fails if `writeFails`.
        Transport delayedTransport(
                boolean faulty,
                boolean writeFails,
                ManualTimerScheduler clock
        ) throws Exception {
            Transport transport = switch (this) {
                case TCP, TLS -> {
                    TcpTransport.Connector connector = connector();
                    yield new TcpTransport(
                            "contract",
                            scheme,
                            new Semaphore(1),
                            InetAddress::getByName,
                            (host, address, timeout) -> delayedConnection(
                                    connector.connect(host, address, timeout),
                                    writeFails
                            ),
                            DefaultTimerScheduler.instance
                    );
                }
                case WS -> owned(new WebSocketTransports(
                        "contract",
                        DefaultTimerScheduler.instance,
                        socket -> delayedWebSocket(socket, writeFails)
                )).create("contract");
            };
            return faulty ? new FaultyTransport(transport, clock) : transport;
        }

        /// Opens the kind's connections the way its TcpTransport does, before any handshake.
        TcpTransport.Connector connector() throws Exception {
            return switch (this) {
                case TCP -> (_, address, timeout) -> TcpTransport.plain(
                        TcpConnection.connect(address, timeout)
                );
                case TLS -> {
                    SSLContext context = Tls.clientContext();
                    SSLParameters parameters = verifyingParameters(context);
                    yield (host, address, timeout) -> TcpTransport.tls(
                            host,
                            address,
                            timeout,
                            context,
                            parameters
                    );
                }
                case WS -> throw new UnsupportedOperationException(
                        "A WebSocket transport has no TCP connector"
                );
            };
        }

        /// A listening peer for the kind's transports.
        PeerServer listen() throws Exception {
            return switch (this) {
                case TCP -> socketPeers(loopbackServer());
                case TLS -> socketPeers(Tls.serverSocket());
                case WS -> new WebSocketPeerServer(WebSocketPeer.listen());
            };
        }

        private PeerServer socketPeers(ServerSocket server) {
            return new SocketPeerServer(server, endpoint(server.getLocalPort()));
        }
    }

    /// A listening peer, and the endpoint where the kind's transports find it.
    interface PeerServer extends AutoCloseable {

        URI endpoint();

        /// Limits the receive buffer of the connections accepted afterward, so that a peer that
        /// does not read stalls the transport's writes.
        void receiveBufferSize(int bytes) throws IOException;

        /// Waits for the next connection, whose reads then wait at most five seconds.
        Peer accept() throws IOException;

        @Override
        void close() throws IOException;
    }

    /// The peer side of one connection, seen as the byte stream that carries MQTT packets.
    interface Peer extends AutoCloseable {

        InputStream input() throws IOException;

        OutputStream output() throws IOException;

        /// Ends the peer's stream while its input stays readable.
        void shutdownOutput() throws IOException;

        boolean isClosed();

        /// Drops the connection.
        @Override
        void close() throws IOException;
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

    // The default parameters of `context`, verifying the server's name as HTTPS does.
    private static SSLParameters verifyingParameters(SSLContext context) {
        SSLParameters parameters = context.getDefaultSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        return parameters;
    }

    private static ServerSocket loopbackServer() throws IOException {
        var server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        server.setSoTimeout(5000);
        return server;
    }

    private static WebSocketTransports owned(WebSocketTransports owner) {
        webSocketOwners.add(owner);
        return owner;
    }

    // A WebSocket whose DISCONNECT write reports its result only once the transport aborts it,
    // and then fails if `writeFails`, as delayedConnection does for TCP.
    private static WebSocket delayedWebSocket(WebSocket socket, boolean writeFails) {
        var aborted = new CompletableFuture<Void>();
        return new WebSocket() {
            @Override
            public CompletableFuture<WebSocket> sendText(CharSequence data, boolean last) {
                return socket.sendText(data, last);
            }

            @Override
            public CompletableFuture<WebSocket> sendBinary(ByteBuffer data, boolean last) {
                boolean disconnect = data.remaining() == 2
                        && data.get(data.position()) == (byte) 0xE0;
                CompletableFuture<WebSocket> written = socket.sendBinary(data, last);
                if (!disconnect) {
                    return written;
                }
                return written.thenCombine(aborted, (open, _) -> open)
                        .thenCompose(open -> writeFails
                                ? CompletableFuture.failedFuture(
                                        new IOException("Injected write failure")
                                )
                                : CompletableFuture.completedFuture(open));
            }

            @Override
            public CompletableFuture<WebSocket> sendPing(ByteBuffer message) {
                return socket.sendPing(message);
            }

            @Override
            public CompletableFuture<WebSocket> sendPong(ByteBuffer message) {
                return socket.sendPong(message);
            }

            @Override
            public CompletableFuture<WebSocket> sendClose(int statusCode, String reason) {
                return socket.sendClose(statusCode, reason);
            }

            @Override
            public void request(long n) {
                socket.request(n);
            }

            @Override
            public String getSubprotocol() {
                return socket.getSubprotocol();
            }

            @Override
            public boolean isOutputClosed() {
                return socket.isOutputClosed();
            }

            @Override
            public boolean isInputClosed() {
                return socket.isInputClosed();
            }

            @Override
            public void abort() {
                socket.abort();
                aborted.complete(null);
            }
        };
    }

    private record WebSocketPeerServer(WebSocketPeer server) implements PeerServer {

        @Override
        public URI endpoint() {
            return server.uri("/mqtt");
        }

        @Override
        public void receiveBufferSize(int bytes) throws IOException {
            server.receiveBufferSize(bytes);
        }

        @Override
        public Peer accept() throws IOException {
            WebSocketPeer.Connection connection = server.accept();
            connection.open("mqtt");
            return new WebSocketPeerConnection(connection);
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    // The peer's end of its stream is its Close frame.
    private record WebSocketPeerConnection(WebSocketPeer.Connection connection) implements Peer {

        @Override
        public InputStream input() {
            return connection.binaryInput();
        }

        @Override
        public OutputStream output() {
            return connection.binaryOutput();
        }

        @Override
        public void shutdownOutput() throws IOException {
            connection.sendClose(1000, "");
        }

        @Override
        public boolean isClosed() {
            return connection.isClosed();
        }

        @Override
        public void close() throws IOException {
            connection.close();
        }
    }

    private record SocketPeerServer(ServerSocket server, URI endpoint) implements PeerServer {

        @Override
        public void receiveBufferSize(int bytes) throws IOException {
            server.setReceiveBufferSize(bytes);
        }

        @Override
        public Peer accept() throws IOException {
            Socket socket = server.accept();
            socket.setSoTimeout(5000);
            if (socket instanceof SSLSocket secure) {
                secure.startHandshake();
            }
            return new SocketPeer(socket);
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    private record SocketPeer(Socket socket) implements Peer {

        @Override
        public InputStream input() throws IOException {
            return socket.getInputStream();
        }

        @Override
        public OutputStream output() throws IOException {
            return socket.getOutputStream();
        }

        @Override
        public void shutdownOutput() throws IOException {
            socket.shutdownOutput();
        }

        @Override
        public boolean isClosed() {
            return socket.isClosed();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
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
