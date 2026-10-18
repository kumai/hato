package org.mqtt4j.hato.client.internal;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.client.internal.common.io.FrameReader;
import org.mqtt4j.hato.client.internal.common.io.FrameWriter;
import org.mqtt4j.hato.spi.Transport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;
import org.mqtt4j.hato.testsupport.Tls;
import org.mqtt4j.hato.testsupport.TlsRelay;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@SuppressWarnings("try") // Explicit close verifies cancellation and graceful TLS shutdown.
class TlsConnectionTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void hostname_verification_rejects_an_ip_unless_explicitly_disabled(boolean disable)
            throws Exception {
        // Arrange: the certificate covers localhost and a DNS name, but no IP literal.
        try (
                MqttBroker broker = MqttBroker.start();
                var relay = new TlsRelay(URI.create("mqtt://localhost:" + broker.port()));
                MqttClient sut = MqttClient.builder()
                        .server(URI.create("mqtts://127.0.0.1:" + relay.uri().getPort()))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .sslContext(Tls.clientContext())
                        .sslParameters(parameters -> {
                            if (disable) {
                                parameters.setEndpointIdentificationAlgorithm(null);
                            }
                        })
                        .build()
        ) {
            // Act & Assert
            if (disable) {
                assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
                sut.disconnect().get(5, SECONDS);
            } else {
                assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                        .hasCauseInstanceOf(MqttConnectException.class)
                        .hasStackTraceContaining("SSLHandshakeException");
            }
        }
    }

    @Test
    void default_jdk_trust_rejects_the_private_test_ca() throws Exception {
        // Arrange
        try (
                MqttBroker broker = MqttBroker.start();
                var relay = new TlsRelay(URI.create("mqtt://localhost:" + broker.port()));
                MqttClient sut = MqttClient.builder()
                        .server(relay.uri())
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .build()
        ) {
            // Act & Assert
            assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .hasStackTraceContaining("SSLHandshakeException");
        }
    }

    @Test
    void publications_round_trip_through_tls_and_the_embedded_broker() throws Exception {
        // Arrange
        var ended = new LinkedBlockingQueue<Disconnection>();
        var publication = MqttMessage.of(
                new Topic("tls/round-trip"),
                new byte[] {1, 2, 3},
                Qos.AT_LEAST_ONCE,
                false
        );
        try (
                MqttBroker broker = MqttBroker.start();
                var relay = new TlsRelay(URI.create("mqtt://localhost:" + broker.port()));
                MqttClient sut = MqttClient.builder()
                        .server(relay.uri())
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .sslContext(Tls.clientContext())
                        .connectionListener(ended::add)
                        .build()
        ) {
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("tls/round-trip"), Qos.AT_LEAST_ONCE)
            );
            subscription.established().get(5, SECONDS);

            // Act
            sut.publish(publication).get(5, SECONDS);
            ReceiveResult.Received received = assertInstanceOf(
                    ReceiveResult.Received.class,
                    subscription.receive(Duration.ofSeconds(5))
            );

            // Assert
            assertThat(received.delivery().message()).isEqualTo(publication);
            subscription.unsubscribe().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            assertThat(ended).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"test.hato.invalid", "localhost", "127.0.0.1"})
    void automatic_sni_preserves_the_original_dns_name(String host) throws Exception {
        // Arrange: the fake DNS resolver keeps the test independent of external name resolution.
        SSLContext context = Tls.clientContext();
        SSLParameters parameters = context.getDefaultSSLParameters();
        parameters.setEndpointIdentificationAlgorithm(host.equals("127.0.0.1") ? null : "HTTPS");
        var receiver = new Receiver();
        try (
                SSLServerSocket server = Tls.serverSocket();
                var sut = new TcpTransport(
                        "sni",
                        "mqtts",
                        new Semaphore(1),
                        _ -> InetAddress.getByName("127.0.0.1"),
                        (name, address, timeout) -> TcpTransport.tls(
                                name,
                                address,
                                timeout,
                                context,
                                parameters
                        ),
                        DefaultTimerScheduler.instance
                )
        ) {
            // Act
            sut.start(
                    URI.create("mqtts://" + host + ":" + server.getLocalPort()),
                    Duration.ofSeconds(5),
                    receiver
            );
            try (var peer = (SSLSocket) server.accept()) {
                peer.setSoTimeout(5000);
                peer.startHandshake();

                // Assert
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                var session = (ExtendedSSLSession) peer.getSession();
                if (host.equals("test.hato.invalid")) {
                    assertThat(session.getRequestedServerNames())
                            .containsExactly(new SNIHostName(host));
                } else {
                    assertThat(session.getRequestedServerNames()).isEmpty();
                }
            }
        }
    }

    @Test
    void tls_parameters_are_copied_at_build_and_reused_for_later_connections() throws Exception {
        // Arrange: application code retains and then mutates the customizable JDK value.
        var customized = new AtomicReference<SSLParameters>();
        var calls = new AtomicInteger();
        try (
                MqttBroker broker = MqttBroker.start();
                var relay = new TlsRelay(URI.create("mqtt://localhost:" + broker.port()));
                MqttClient sut = MqttClient.builder()
                        .server(relay.uri())
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .sslContext(Tls.clientContext())
                        .sslParameters(parameters -> {
                            customized.set(parameters);
                            calls.incrementAndGet();
                        })
                        .build()
        ) {
            requireNonNull(customized.get()).setProtocols(new String[] {"invalid-protocol"});

            // Act & Assert: each connection keeps the validated snapshot, without another callback.
            for (int attempt = 1; attempt <= 2; attempt++) {
                assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, attempt));
                sut.disconnect().get(5, SECONDS);
            }
            assertThat(calls).hasValue(1);
        }
    }

    @Test
    void explicit_sni_and_alpn_reach_the_peer() throws Exception {
        // Arrange
        try (SSLServerSocket server = Tls.serverSocket()) {
            SSLParameters parameters = server.getSSLParameters();
            parameters.setApplicationProtocols(new String[] {"mqtt"});
            server.setSSLParameters(parameters);
            try (MqttClient sut = MqttClient.builder()
                    .server(URI.create("mqtts://localhost:" + server.getLocalPort()))
                    .version(MqttVersion.V3_1_1)
                    .reconnectPolicy(ReconnectPolicy.disabled())
                    .sslContext(Tls.clientContext())
                    .sslParameters(clientParameters -> {
                        clientParameters.setServerNames(
                                List.of(new SNIHostName("test.hato.invalid"))
                        );
                        clientParameters.setApplicationProtocols(new String[] {"mqtt"});
                    }).build()) {
                // Act
                CompletableFuture<ConnAck> connected = sut.connect();
                try (var peer = (SSLSocket) server.accept()) {
                    peer.setSoTimeout(5000);
                    peer.startHandshake();

                    // Assert
                    assertThat(((ExtendedSSLSession) peer.getSession()).getRequestedServerNames())
                            .containsExactly(new SNIHostName("test.hato.invalid"));
                    assertThat(peer.getApplicationProtocol()).isEqualTo("mqtt");
                    var reader = new FrameReader(peer.getInputStream(), 1024);
                    assertThat(reader.read().orElseThrow()[0]).isEqualTo((byte) 0x10);
                    new FrameWriter(peer.getOutputStream()).write(new byte[] {0x20, 2, 0, 0});
                    assertThat(connected.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
                    CompletableFuture<Void> disconnected = sut.disconnect();
                    assertThat(reader.read().orElseThrow()).containsExactly((byte) 0xE0, (byte) 0);
                    disconnected.get(5, SECONDS);
                }
            }
        }
    }

    @Test
    void an_untrusted_certificate_only_notifies_failure() throws Exception {
        // Arrange: hostname validation is also enabled, while the CA is deliberately untrusted.
        var context = SSLContext.getDefault();
        SSLParameters parameters = context.getDefaultSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        var receiver = new Receiver();
        try (
                SSLServerSocket server = Tls.serverSocket();
                var sut = new TcpTransport(
                        "trust",
                        context,
                        parameters,
                        DefaultTimerScheduler.instance
                )
        ) {
            // Act
            sut.start(
                    URI.create("mqtts://localhost:" + server.getLocalPort()),
                    Duration.ofSeconds(5),
                    receiver
            );
            try (var peer = (SSLSocket) server.accept()) {
                peer.setSoTimeout(5000);
                assertThatThrownBy(peer::startHandshake).isInstanceOf(IOException.class);

                // Assert: handshake failure cannot be mistaken for a connected byte stream.
                assertThat(receiver.events.poll(5, SECONDS))
                        .isInstanceOf(SSLHandshakeException.class);
                sut.close();
                assertThat(receiver.events).isEmpty();
                assertThat(receiver.bytes).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_stalled_handshake_can_expire_or_be_closed_without_connected_notification(boolean close)
            throws Exception {
        // Arrange: this TCP peer accepts the ClientHello but never sends a TLS response.
        // The transport's clock is manual, so only advancing it can expire the deadline.
        SSLContext context = Tls.clientContext();
        var clock = new ManualTimerScheduler();
        var receiver = new Receiver();
        try (
                var server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
                var sut = new TcpTransport(
                        "handshake",
                        "mqtts",
                        new Semaphore(1),
                        _ -> InetAddress.getByName("127.0.0.1"),
                        (host, address, timeout) -> TcpTransport.tls(
                                host,
                                address,
                                timeout,
                                context,
                                context.getDefaultSSLParameters()
                        ),
                        clock
                )
        ) {
            server.setSoTimeout(5000);
            sut.start(
                    URI.create("mqtts://localhost:" + server.getLocalPort()),
                    Duration.ofSeconds(5),
                    receiver
            );
            try (Socket peer = server.accept()) {
                peer.setSoTimeout(5000);
                assertThat(peer.getInputStream().read()).isNotEqualTo(-1);
                assertThat(clock.pendingTaskCount()).isEqualTo(1);

                // Act
                if (close) {
                    assertThat(receiver.events).isEmpty();
                    assertTimeoutPreemptively(Duration.ofSeconds(2), sut::close);
                } else {
                    clock.advance(Duration.ofSeconds(5));
                }

                // Assert
                assertThat(receiver.events.poll(5, SECONDS))
                        .isInstanceOf(close ? IOException.class : SocketTimeoutException.class);
                sut.close();
                assertThat(receiver.events).isEmpty();
                assertThat(receiver.bytes).isEmpty();
                assertThat(peer.getInputStream().readAllBytes()).isNotEmpty();
                assertThat(clock.pendingTaskCount()).isZero();
            }
        }
    }

    @Test
    void the_client_connection_deadline_cancels_an_unfinished_tls_handshake() throws Exception {
        // Arrange: hold the TLS response. The transport measures its own 60-second deadline in
        // real time, so only the client's frozen clock can expire during the test.
        var clock = new ManualTimerScheduler();
        var ended = new LinkedBlockingQueue<Disconnection>();
        SSLContext context = Tls.clientContext();
        try (
                var server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
                MqttClient sut = MqttClient.builder()
                        .server(URI.create("mqtts://localhost:" + server.getLocalPort()))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .transportFactory(id -> new TcpTransport(
                                id,
                                context,
                                context.getDefaultSSLParameters(),
                                DefaultTimerScheduler.instance
                        ))
                        .connectionTimeout(Duration.ofSeconds(60))
                        .timerScheduler(clock)
                        .connectionListener(ended::add)
                        .build()
        ) {
            server.setSoTimeout(5000);
            CompletableFuture<ConnAck> connected = sut.connect();
            try (Socket peer = server.accept()) {
                peer.setSoTimeout(5000);
                assertThat(peer.getInputStream().read()).isNotEqualTo(-1);
                await().untilAsserted(() -> assertThat(clock.pendingTaskCount()).isEqualTo(1));

                // Act
                clock.advance(Duration.ofSeconds(60));

                // Assert: the client's deadline fails connect and closes the TCP peer as well.
                assertThatThrownBy(() -> connected.get(5, SECONDS))
                        .hasCauseInstanceOf(MqttConnectException.class)
                        .hasRootCauseExactlyInstanceOf(TimeoutException.class);
                assertThat(peer.getInputStream().readAllBytes()).isNotEmpty();
                assertThat(ended).isEmpty();
                assertThat(clock.pendingTaskCount()).isZero();
            }
        }
    }

    @Test
    void closing_an_idle_connection_sends_a_tls_close_notification() throws Exception {
        // Arrange: JSSE reads end of stream both after close_notify and after a bare TCP close,
        // so the peer counts the bytes that arrive below TLS once the handshake is complete.
        SSLContext context = Tls.clientContext();
        var receiver = new Receiver();
        try (
                var server = new CountingServerSocket();
                var sut = new TcpTransport(
                        "close",
                        context,
                        context.getDefaultSSLParameters(),
                        DefaultTimerScheduler.instance
                )
        ) {
            sut.start(
                    URI.create("mqtts://localhost:" + server.getLocalPort()),
                    Duration.ofSeconds(5),
                    receiver
            );
            try (SSLSocket peer = server.acceptTls()) {
                assertThat(receiver.events.poll(5, SECONDS)).isEqualTo("connected");
                long handshakeBytes = server.received.get();

                // Act
                assertTimeoutPreemptively(Duration.ofSeconds(2), sut::close);

                // Assert: a close_notify record, not a bare TCP close, ends the peer's stream.
                assertThat(peer.getInputStream().read()).isEqualTo(-1);
                assertThat(server.received.get()).isGreaterThan(handshakeBytes);
                assertThat(receiver.events.poll(5, SECONDS)).isInstanceOf(IOException.class);
                assertThat(receiver.events).isEmpty();
            }
        }
    }

    /// Accepts sockets that count the bytes they receive, then layers server-side TLS on them.
    private static final class CountingServerSocket extends ServerSocket {

        final AtomicLong received = new AtomicLong();

        CountingServerSocket() throws IOException {
            super(0, 1, InetAddress.getLoopbackAddress());
            setSoTimeout(5000);
        }

        SSLSocket acceptTls() throws Exception {
            Socket raw = accept();
            var tls = (SSLSocket) Tls.serverContext().getSocketFactory().createSocket(
                    raw,
                    "localhost",
                    raw.getPort(),
                    true
            );
            tls.setUseClientMode(false);
            tls.setSoTimeout(5000);
            tls.startHandshake();
            return tls;
        }

        @Override
        public Socket accept() throws IOException {
            var socket = new Socket() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return new FilterInputStream(super.getInputStream()) {
                        @Override
                        public int read() throws IOException {
                            int value = super.read();
                            if (value != -1) {
                                received.incrementAndGet();
                            }
                            return value;
                        }

                        @Override
                        public int read(byte[] bytes, int offset, int length) throws IOException {
                            int count = super.read(bytes, offset, length);
                            if (count > 0) {
                                received.addAndGet(count);
                            }
                            return count;
                        }
                    };
                }
            };
            implAccept(socket);
            return socket;
        }
    }

    private static final class Receiver implements Transport.Listener {

        final LinkedBlockingQueue<Object> events = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<byte[]> bytes = new LinkedBlockingQueue<>();

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
        }

        @Override
        public void onClosed(Throwable cause) {
            events.add(cause);
        }
    }
}
