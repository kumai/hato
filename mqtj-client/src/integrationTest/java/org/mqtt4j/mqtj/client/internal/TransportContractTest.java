package org.mqtt4j.mqtj.client.internal;

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
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.mqtj.client.internal.common.io.TcpConnection;
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
