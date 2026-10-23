package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocketHandshakeException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.mqtt4j.hato.Transport;
import org.mqtt4j.hato.client.internal.WebSocketPeer.Frame;
import org.mqtt4j.hato.testsupport.Tls;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.Transport.SendResult.ACCEPTED;
import static org.mqtt4j.hato.client.internal.WebSocketPeer.BINARY;
import static org.mqtt4j.hato.client.internal.WebSocketPeer.CLOSE;

/// What only the WebSocket transport has, beyond the transport contract that it shares with TCP
/// in TransportContractTest: the handshake, the kinds of frames, the closing handshake, the
/// deadline of the upgrade, the HttpClient that a client's transports share, and the TLS
/// settings of `wss`.
@Timeout(30)
@SuppressWarnings("try") // Explicit close is the behavior under test; try still cleans up failures.
class WebSocketTransportTest {

    // The owners whose HttpClients each test releases at its end.
    private final List<WebSocketTransports> owners = new ArrayList<>();

    private final WebSocketTransports owner = owned(new WebSocketTransports(
            "ws-test",
            DefaultTimerScheduler.instance
    ));

    private final Receiver receiver = new Receiver();

    @AfterEach
    void releaseHttpClients() throws InterruptedException {
        var released = new CountDownLatch(owners.size());
        for (WebSocketTransports each : owners) {
            each.release(released::countDown);
        }
        assertThat(released.await(10, SECONDS)).isTrue();
    }

    @ParameterizedTest(name = "{0} transport rejects {1}")
    @CsvSource({"ws, wss", "ws, mqtt", "wss, ws", "wss, mqtts"})
    void unsupported_schemes_are_rejected_before_starting_a_connection(
            String transportScheme,
            String endpointScheme
    ) throws Exception {
        // Arrange
        SSLContext context = Tls.clientContext();
        WebSocketTransports transports = transportScheme.equals("ws")
                ? owner
                : secureOwner(context, context.getDefaultSSLParameters());
        try (Transport sut = transports.create("wrong-scheme")) {
            // Act & Assert
            assertThatThrownBy(() -> sut.start(
                    URI.create(endpointScheme + "://127.0.0.1/mqtt"),
                    Duration.ofSeconds(5),
                    receiver
            )).isInstanceOf(IllegalArgumentException.class);
            assertThat(receiver.events).isEmpty();
        }
    }

    @Test
    void the_transport_offers_mqtt_on_the_given_target_and_writes_binary_frames()
            throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange
            sut.start(server.uri("/mqtt?tenant=a"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");

                // Act
                assertThat(sut.send(1, new byte[] {1, 2})).isEqualTo(ACCEPTED);

                // Assert
                Frame frame = peer.receive();
                assertThat(peer.target()).isEqualTo("/mqtt?tenant=a");
                assertThat(peer.subprotocols()).containsExactly("mqtt");
                assertThat(frame.opcode()).isEqualTo(BINARY);
                assertThat(frame.fin()).isTrue();
                assertThat(frame.payload()).containsExactly(1, 2);
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo(1L);
            }
        }
    }

    @Test
    void a_broker_that_chooses_no_subprotocol_still_connects() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange
            sut.start(server.uri("/"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                // Act
                peer.open();

                // Assert: the client warns once and continues.
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
            }
        }
    }

    @Test
    void a_broker_that_chooses_another_subprotocol_fails_the_attempt() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange
            sut.start(server.uri("/"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                // Act
                peer.open("mqttv3.1");

                // Assert: the JDK refuses the answer, and the transport never connects.
                assertThat(receiver.events.poll(5, SECONDS))
                        .isInstanceOf(WebSocketHandshakeException.class);
                assertThat(receiver.events).isEmpty();
            }
        }
    }

    @Test
    void a_text_message_from_the_broker_closes_the_connection() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange
            sut.start(server.uri("/"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");

                // Act
                peer.sendText("not mqtt");

                // Assert: the transport aborts at once, without a closing handshake.
                assertThat(receiver.events.poll(5, SECONDS))
                        .isInstanceOf(ProtocolException.class);
                assertThat(peer.binaryInput().read()).isEqualTo(-1);
                assertThat(receiver.bytes).isEmpty();
            }
        }
    }

    @Test
    void a_close_from_the_broker_ends_the_transport_with_its_code_and_reason() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange
            sut.start(server.uri("/"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");

                // Act
                peer.sendClose(4000, "maintenance");

                // Assert
                assertThat(receiver.events.poll(5, SECONDS))
                        .isInstanceOfSatisfying(IOException.class, cause -> assertThat(cause)
                                .hasMessageContaining("4000")
                                .hasMessageContaining("maintenance"));
            }
        }
    }

    @Test
    void a_close_ends_as_soon_as_the_broker_answers_it() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange
            sut.start(server.uri("/"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");

                // Act
                sut.close();
                Frame close = peer.receive();
                assertThat(receiver.events.poll(300, MILLISECONDS)).isNull();
                peer.sendClose(1000, "");

                // Assert: the end comes well before the one-second grace period has passed.
                assertThat(close.opcode()).isEqualTo(CLOSE);
                assertThat(close.closeCode()).isEqualTo(1000);
                assertThat(receiver.events.poll(500, MILLISECONDS))
                        .isInstanceOf(IOException.class);
            }
        }
    }

    @Test
    void a_paused_transport_takes_no_more_messages_until_it_resumes() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange: the client pauses in the callback for each message, as it does once it
            // has fallen behind.
            receiver.received = ((PausableTransport) sut)::pauseReading;
            sut.start(server.uri("/"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                peer.sendBinary(new byte[] {1});
                assertThat(receiver.bytes.poll(5, SECONDS)).containsExactly(1);

                // Act
                peer.sendBinary(new byte[] {2});

                // Assert
                assertThat(receiver.bytes.poll(300, MILLISECONDS)).isNull();
                ((PausableTransport) sut).resumeReading();
                assertThat(receiver.bytes.poll(5, SECONDS)).containsExactly(2);
            }
        }
    }

    @Test
    void a_close_while_paused_still_ends_as_soon_as_the_broker_answers_it() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange: a message has paused reading.
            receiver.received = ((PausableTransport) sut)::pauseReading;
            sut.start(server.uri("/"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                peer.sendBinary(new byte[] {1});
                assertThat(receiver.bytes.poll(5, SECONDS)).containsExactly(1);

                // Act
                sut.close();
                assertThat(peer.receive().opcode()).isEqualTo(CLOSE);
                peer.sendClose(1000, "");

                // Assert: the broker's Close ends the close well before its grace period.
                assertThat(receiver.events.poll(500, MILLISECONDS))
                        .isInstanceOf(IOException.class);
            }
        }
    }

    @Test
    void a_close_aborts_after_its_grace_period_when_the_broker_does_not_answer()
            throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange
            sut.start(server.uri("/"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");

                // Act
                sut.close();

                // Assert: the end waits for the grace period and then drops the connection.
                assertThat(peer.receive().opcode()).isEqualTo(CLOSE);
                assertThat(receiver.events.poll(500, MILLISECONDS)).isNull();
                assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(IOException.class);
                assertThat(peer.binaryInput().read()).isEqualTo(-1);
            }
        }
    }

    @Test
    void a_close_inside_the_handshake_ends_at_once_and_aborts_a_later_websocket()
            throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange: the broker reads the upgrade request and does not answer it yet.
            sut.start(server.uri("/"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                // Act
                sut.close();

                // Assert: the upgrade itself goes on, and its WebSocket is aborted once it opens.
                assertThat(receiver.events.poll(500, MILLISECONDS))
                        .isInstanceOf(IOException.class);
                peer.open("mqtt");
                assertThat(peer.binaryInput().read()).isEqualTo(-1);
                assertThat(receiver.events.poll(300, MILLISECONDS)).isNull();
            }
        }
    }

    @Test
    void a_broker_that_never_answers_the_upgrade_fails_the_attempt_at_its_deadline()
            throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange
            long started = System.nanoTime();

            // Act
            sut.start(server.uri("/"), Duration.ofMillis(500), receiver);
            try (WebSocketPeer.Connection _ = server.accept()) {
                // Assert
                assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(IOException.class);
                assertThat(Duration.ofNanos(System.nanoTime() - started))
                        .isLessThan(Duration.ofMillis(1500));
            }
        }
    }

    @Test
    void a_broker_that_stops_inside_its_answer_fails_the_attempt_at_its_deadline()
            throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange
            long started = System.nanoTime();
            sut.start(server.uri("/"), Duration.ofMillis(500), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                // Act
                peer.openPartially();

                // Assert
                assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(IOException.class);
                assertThat(Duration.ofNanos(System.nanoTime() - started))
                        .isLessThan(Duration.ofMillis(1500));
            }
        }
    }

    @Test
    void a_request_that_the_jdk_sends_again_ends_at_the_first_deadline() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange: the broker holds the first request for most of the deadline and then
            // drops it. The JDK sends it once more on a new connection, which the broker holds
            // without an answer, and measures its own deadline again from there.
            long started = System.nanoTime();
            sut.start(server.uri("/"), Duration.ofSeconds(1), receiver);
            WebSocketPeer.Connection first = server.accept();
            Thread.sleep(700);
            first.close();
            var retried = new ArrayList<WebSocketPeer.Connection>();
            Thread holder = Thread.ofVirtual().start(() -> {
                try {
                    retried.add(server.accept());
                } catch (IOException _) {
                    // No second request, or the server closed.
                }
            });

            // Act & Assert: the transport's deadline bounds both requests together, where the
            // JDK alone would end the attempt only after about 1.7 seconds.
            assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(IOException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .isLessThan(Duration.ofMillis(1500));
            server.close();
            holder.join();
            for (WebSocketPeer.Connection connection : retried) {
                connection.close();
            }
        }
    }

    @Test
    void a_websocket_that_opens_after_the_deadline_is_aborted_without_connecting()
            throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange
            sut.start(server.uri("/"), Duration.ofMillis(300), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(IOException.class);

                // Act: the broker answers only after the transport has given the attempt up.
                peer.open("mqtt");

                // Assert
                assertThat(peer.binaryInput().read()).isEqualTo(-1);
                assertThat(receiver.events.poll(300, MILLISECONDS)).isNull();
            }
        }
    }

    @Test
    void callbacks_never_overlap_when_writes_and_messages_cross() throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen(); Transport sut = owner.create("ws")) {
            // Arrange: each finished write starts the next, while the broker sends messages.
            int count = 200;
            sut.start(server.uri("/"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                receiver.sent = ticket -> {
                    if (ticket < count) {
                        assertThat(sut.send(ticket + 1, new byte[] {(byte) ticket}))
                                .isEqualTo(ACCEPTED);
                    }
                };
                Thread sender = Thread.ofVirtual().start(() -> {
                    try {
                        for (int i = 0; i < count; i++) {
                            peer.sendBinary(new byte[] {(byte) i});
                        }
                    } catch (IOException failure) {
                        throw new IllegalStateException(failure);
                    }
                });

                // Act
                assertThat(sut.send(1, new byte[] {0})).isEqualTo(ACCEPTED);
                for (int i = 0; i < count; i++) {
                    assertThat(peer.receive().opcode()).isEqualTo(BINARY);
                }
                sender.join();

                // Assert
                var received = new ArrayList<Byte>();
                while (received.size() < count) {
                    byte[] bytes = receiver.bytes.poll(5, SECONDS);
                    assertThat(bytes).isNotNull();
                    for (byte value : bytes) {
                        received.add(value);
                    }
                }
                var expected = new ArrayList<Byte>();
                for (int i = 0; i < count; i++) {
                    expected.add((byte) i);
                }
                assertThat(received).isEqualTo(expected);
                for (long ticket = 1; ticket <= count; ticket++) {
                    assertThat(receiver.events.poll(5, SECONDS)).isEqualTo(ticket);
                }
                assertThat(receiver.overlapped).isFalse();
            }
        }
    }

    @Test
    void the_transports_of_one_client_share_its_http_client_until_it_is_released()
            throws Exception {
        try (WebSocketPeer server = WebSocketPeer.listen()) {
            // Arrange: two connections, one after the other.
            HttpClient first = connectAndClose(server);
            HttpClient second = connectAndClose(server);
            var released = new CountDownLatch(1);

            // Act
            owner.release(released::countDown);

            // Assert
            assertThat(second).isSameAs(first);
            assertThat(released.await(10, SECONDS)).isTrue();
            assertThat(first.isTerminated()).isTrue();
        }
    }

    @Test
    void a_wss_transport_presents_its_client_certificate_to_a_broker_that_requires_one()
            throws Exception {
        // Arrange: the broker trusts the test CA, which signed the client's certificate.
        SSLContext context = Tls.clientContextWithCertificate();
        WebSocketTransports secure = secureOwner(context, context.getDefaultSSLParameters());
        SSLServerSocket socket = Tls.serverSocket();
        socket.setNeedClientAuth(true);
        try (WebSocketPeer server = WebSocketPeer.listen(socket);
                Transport sut = secure.create("wss")) {
            // Act
            sut.start(server.uri("/mqtt"), Duration.ofSeconds(5), receiver);
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");

                // Assert
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                assertThat(peer.tlsSession().getPeerPrincipal().getName())
                        .isEqualTo("CN=Hato Test client,O=Hato Tests");
            }
        }
    }

    @Test
    void a_wss_transport_rejects_a_certificate_that_does_not_name_the_host() throws Exception {
        // Arrange: the server's certificate names localhost and no IP address.
        SSLContext context = Tls.clientContext();
        WebSocketTransports secure = secureOwner(context, context.getDefaultSSLParameters());
        try (WebSocketPeer server = WebSocketPeer.listen(Tls.serverSocket());
                Transport sut = secure.create("wss")) {
            // Act: the server's side of the handshake fails when the client drops it.
            sut.start(
                    URI.create("wss://127.0.0.1:" + server.port() + "/mqtt"),
                    Duration.ofSeconds(5),
                    receiver
            );
            assertThatThrownBy(server::accept).isInstanceOf(IOException.class);

            // Assert
            assertThat(assertInstanceOf(Throwable.class, receiver.events.poll(5, SECONDS)))
                    .hasRootCauseInstanceOf(CertificateException.class);
        }
    }

    @Test
    void a_wss_transport_applies_the_protocols_and_server_names_of_its_parameters()
            throws Exception {
        // Arrange: with an IP address host, the JDK takes SNI from the parameters, and the
        // client verifies the certificate against that name.
        SSLContext context = Tls.clientContext();
        SSLParameters parameters = context.getDefaultSSLParameters();
        parameters.setProtocols(new String[] {"TLSv1.2"});
        parameters.setServerNames(List.of(new SNIHostName("test.hato.invalid")));
        WebSocketTransports secure = secureOwner(context, parameters);
        try (WebSocketPeer server = WebSocketPeer.listen(Tls.serverSocket());
                Transport sut = secure.create("wss")) {
            // Act
            sut.start(
                    URI.create("wss://127.0.0.1:" + server.port() + "/mqtt"),
                    Duration.ofSeconds(5),
                    receiver
            );
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");

                // Assert
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                var session = (ExtendedSSLSession) peer.tlsSession();
                assertThat(session.getProtocol()).isEqualTo("TLSv1.2");
                assertThat(session.getRequestedServerNames())
                        .containsExactly(new SNIHostName("test.hato.invalid"));
            }
        }
    }

    // The wss transports of a client with `context` and `parameters`, released at the end of the
    // test.
    private WebSocketTransports secureOwner(SSLContext context, SSLParameters parameters) {
        return owned(new WebSocketTransports(
                "wss-test",
                DefaultTimerScheduler.instance,
                context,
                parameters
        ));
    }

    private WebSocketTransports owned(WebSocketTransports value) {
        owners.add(value);
        return value;
    }

    // Connects once, and closes after the broker has answered the closing handshake.
    private HttpClient connectAndClose(WebSocketPeer server) throws Exception {
        var events = new Receiver();
        try (Transport transport = owner.create("ws")) {
            transport.start(server.uri("/"), Duration.ofSeconds(5), events);
            try (WebSocketPeer.Connection peer = server.accept()) {
                peer.open("mqtt");
                assertThat(events.events.poll(5, SECONDS)).isEqualTo("connected");
                transport.close();
                assertThat(peer.receive().opcode()).isEqualTo(CLOSE);
                peer.sendClose(1000, "");
                assertThat(events.events.poll(5, SECONDS)).isInstanceOf(IOException.class);
            }
        }
        return owner.httpClient();
    }

    /// Records the transport's callbacks, and whether two of them ever ran at the same time.
    private static final class Receiver implements Transport.Listener {

        final LinkedBlockingQueue<Object> events = new LinkedBlockingQueue<>();

        final LinkedBlockingQueue<byte[]> bytes = new LinkedBlockingQueue<>();

        final AtomicBoolean overlapped = new AtomicBoolean();

        private final AtomicInteger running = new AtomicInteger();

        volatile LongConsumer sent = _ -> { };

        volatile Runnable received = () -> { };

        @Override
        public void onConnected() {
            enter();
            events.add("connected");
            leave();
        }

        @Override
        public void onBytes(byte[] value) {
            enter();
            bytes.add(value);
            received.run();
            leave();
        }

        @Override
        public void onSent(long ticket) {
            enter();
            try {
                events.add(ticket);
                sent.accept(ticket);
            } finally {
                leave();
            }
        }

        @Override
        public void onClosed(Throwable cause) {
            enter();
            events.add(cause);
            leave();
        }

        private void enter() {
            if (running.incrementAndGet() > 1) {
                overlapped.set(true);
            }
        }

        private void leave() {
            running.decrementAndGet();
        }
    }
}
