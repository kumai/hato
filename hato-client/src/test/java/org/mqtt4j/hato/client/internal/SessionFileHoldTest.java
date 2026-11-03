package org.mqtt4j.hato.client.internal;

import java.io.EOFException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Transport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/// How long a client object holds its session file against the stores of other client objects.
class SessionFileHoldTest {

    @TempDir
    Path directory;

    @Test
    void a_closed_client_holds_its_session_file_until_its_connection_has_closed()
            throws Exception {
        // Arrange: the client has taken the session over, and its CONNECT has not left yet.
        Path file = directory.resolve("session");
        var transport = new PendingTransport();
        MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .clientId("client")
                .sessionExpiry(Duration.ofHours(1))
                .sessionFile(file)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .transportFactory(_ -> transport)
                .build();
        CompletableFuture<ConnAck> _ = sut.connect();
        transport.connect.get(5, SECONDS);
        var other = new FileSessionStore(file);

        // Act: the client closes, and its transport takes a while to close.
        sut.close();
        transport.closing.get(5, SECONDS);
        boolean heldWhileClosing = other.hold();
        transport.reportClosed();

        // Assert: another client object's store holds the file only once the connection has
        // closed, by the time the client has terminated.
        assertThat(heldWhileClosing).isFalse();
        sut.terminated().get(5, SECONDS);
        assertThat(other.hold()).isTrue();
        other.release();
    }

    @Test
    void a_save_after_a_failed_attempt_keeps_the_file_until_that_connection_has_closed()
            throws Exception {
        // Arrange: the file holds a saved session, which the client takes over. The CONNECT of
        // its attempt has not left when the attempt runs out of time.
        Path file = directory.resolve("session");
        save(file, new SessionState(List.of(), 1, List.of()));
        var transport = new PendingTransport();
        var clock = new ManualTimerScheduler();
        MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .clientId("client")
                .sessionExpiry(Duration.ofHours(1))
                .sessionFile(file)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .timerScheduler(clock)
                .transportFactory(_ -> transport)
                .build();
        CompletableFuture<ConnAck> connecting = sut.connect();
        transport.connect.get(5, SECONDS);
        clock.advance(Duration.ofSeconds(30));
        assertThat(connecting.handle((_, failure) -> failure).get(5, SECONDS))
                .isInstanceOf(MqttConnectException.class);
        transport.closing.get(5, SECONDS);
        var other = new FileSessionStore(file);

        // Act: disconnect() saves the session while the transport of the attempt still closes.
        sut.disconnect().get(5, SECONDS);
        boolean heldWhileClosing = other.hold();
        transport.reportClosed();

        // Assert: another client object's store holds the file only once that transport has
        // closed.
        assertThat(heldWhileClosing).isFalse();
        await().atMost(5, SECONDS).until(other::hold);
        other.release();
        sut.close();
    }

    /// Saves `state` in `file`, as an earlier client object would have.
    private static void save(Path file, SessionState state) throws Exception {
        var store = new FileSessionStore(file);
        assertThat(store.hold()).isTrue();
        try {
            boolean _ = store.replace(
                    0,
                    1,
                    new SessionCodec("client", MqttVersion.V5).encode(state)
            );
        } finally {
            store.release();
        }
    }

    /// Connects at once, keeps every write pending, as a CONNECT that has not left yet, and
    /// reports its close only when the test says so.
    private static final class PendingTransport implements Transport {

        final CompletableFuture<byte[]> connect = new CompletableFuture<>();

        final CompletableFuture<Void> closing = new CompletableFuture<>();

        private @Nullable Listener listener;

        @Override
        public synchronized void start(URI endpoint, Duration timeout, Listener receiver) {
            listener = receiver;
            receiver.onConnected();
        }

        @Override
        public SendResult send(long ticket, byte[] bytes) {
            connect.complete(bytes);
            return SendResult.ACCEPTED;
        }

        @Override
        public void close() {
            closing.complete(null);
        }

        /// Reports that the connection has closed, which releases the pending write.
        synchronized void reportClosed() {
            requireNonNull(listener).onClosed(new EOFException("Closed"));
        }
    }
}
