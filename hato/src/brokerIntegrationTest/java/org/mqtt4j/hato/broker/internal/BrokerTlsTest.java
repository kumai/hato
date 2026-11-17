package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509ExtendedTrustManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.testsupport.Tls;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The TLS listener: clients that exchange messages through it, beside the plain one or alone,
/// and handshakes that fail, take long, or come again.
class BrokerTlsTest {

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void clients_exchange_every_qos_over_tls(MqttVersion version) throws Exception {
        // Arrange
        try (MqttBroker broker = MqttBroker.builder()
                .listenTls(loopback(), Tls.serverContext())
                .start();
                MqttClient sut = tlsClient(broker.tlsPort(), Tls.clientContext(), version)) {
            sut.connect().get(5, SECONDS);
            var received = new LinkedBlockingQueue<MqttMessage>();
            sut.subscribe(
                    Subscribe.of(new TopicFilter("tls/+"), Qos.EXACTLY_ONCE),
                    delivery -> received.add(delivery.message())
            ).established().get(5, SECONDS);
            // Act
            for (Qos qos : Qos.values()) {
                sut.publish(message("tls/" + qos.level(), new byte[] {(byte) qos.level()}, qos))
                        .get(5, SECONDS);
            }
            // Assert
            var levels = new ArrayList<Integer>();
            for (int i = 0; i < 3; i++) {
                MqttMessage message = received.poll(5, SECONDS);
                assertThat(message).isNotNull();
                levels.add(message.qos().level());
            }
            assertThat(levels).containsExactlyInAnyOrder(0, 1, 2);
        }
    }

    @Test
    void publications_larger_than_a_record_and_bursts_of_small_ones_cross_tls_whole()
            throws Exception {
        // Arrange
        try (MqttBroker broker = MqttBroker.builder()
                .listenTls(loopback(), Tls.serverContext())
                .start();
                MqttClient sut = tlsClient(broker.tlsPort(), Tls.clientContext(), MqttVersion.V5)) {
            sut.connect().get(5, SECONDS);
            var received = new LinkedBlockingQueue<MqttMessage>();
            sut.subscribe(
                    Subscribe.of(new TopicFilter("tls/#"), Qos.AT_LEAST_ONCE),
                    delivery -> received.add(delivery.message())
            ).established().get(5, SECONDS);
            var large = new byte[300_000];
            for (int i = 0; i < large.length; i++) {
                large[i] = (byte) (i * 31);
            }
            // Act: one publication of many records each way, and many that share records, sent
            // fifty at a time, within the operations that the client takes at once.
            sut.publish(message("tls/large", large, Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            for (int window = 0; window < 20; window++) {
                var sent = new ArrayList<CompletableFuture<?>>();
                for (int i = window * 50; i < (window + 1) * 50; i++) {
                    sent.add(sut.publish(message(
                            "tls/burst",
                            Integer.toString(i).getBytes(),
                            Qos.AT_LEAST_ONCE
                    )));
                }
                CompletableFuture.allOf(sent.toArray(CompletableFuture[]::new)).get(10, SECONDS);
            }
            // Assert
            MqttMessage first = received.poll(5, SECONDS);
            assertThat(first).isNotNull();
            assertThat(first.payload()).isEqualTo(large);
            var burst = new ArrayList<String>();
            for (int i = 0; i < 1_000; i++) {
                MqttMessage next = received.poll(5, SECONDS);
                assertThat(next).isNotNull();
                burst.add(new String(next.payload()));
            }
            assertThat(burst).containsExactlyInAnyOrderElementsOf(
                    IntStream.range(0, 1_000).mapToObj(Integer::toString).toList()
            );
        }
    }

    @Test
    void plain_and_tls_clients_reach_each_other_through_their_own_ports() throws Exception {
        // Arrange
        try (MqttBroker broker = MqttBroker.builder()
                .listen(loopback())
                .listenTls(loopback(), Tls.serverContext())
                .start();
                MqttClient plain = plainClient(broker.port());
                MqttClient secure = tlsClient(
                        broker.tlsPort(),
                        Tls.clientContext(),
                        MqttVersion.V5
                )) {
            plain.connect().get(5, SECONDS);
            secure.connect().get(5, SECONDS);
            var toPlain = new LinkedBlockingQueue<MqttMessage>();
            var toSecure = new LinkedBlockingQueue<MqttMessage>();
            plain.subscribe(
                    Subscribe.of(new TopicFilter("from/tls"), Qos.AT_LEAST_ONCE),
                    delivery -> toPlain.add(delivery.message())
            ).established().get(5, SECONDS);
            secure.subscribe(
                    Subscribe.of(new TopicFilter("from/plain"), Qos.AT_LEAST_ONCE),
                    delivery -> toSecure.add(delivery.message())
            ).established().get(5, SECONDS);
            // Act
            secure.publish(message("from/tls", new byte[] {1}, Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            plain.publish(message("from/plain", new byte[] {2}, Qos.AT_LEAST_ONCE)).get(5, SECONDS);
            // Assert
            assertThat(broker.port()).isNotEqualTo(broker.tlsPort());
            assertThat(toPlain.poll(5, SECONDS)).isNotNull();
            assertThat(toSecure.poll(5, SECONDS)).isNotNull();
        }
    }

    @Test
    void a_broker_that_listens_with_tls_alone_has_no_plain_port_and_frees_its_port_at_close()
            throws Exception {
        // Arrange
        MqttBroker sut = MqttBroker.builder().listenTls(loopback(), Tls.serverContext()).start();
        int port = sut.tlsPort();
        try (sut; MqttClient client = tlsClient(port, Tls.clientContext(), MqttVersion.V3_1_1)) {
            // Act
            client.connect().get(5, SECONDS);
            // Assert
            assertThatThrownBy(sut::port).isInstanceOf(IllegalStateException.class);
        }
        try (var reused = new ServerSocket(port, 50, InetAddress.getLoopbackAddress())) {
            assertThat(reused.getLocalPort()).isEqualTo(port);
        }
    }

    @Test
    void a_broker_without_a_tls_listener_has_no_tls_port() throws Exception {
        try (MqttBroker sut = MqttBroker.start()) {
            assertThatThrownBy(sut::tlsPort).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void a_plain_client_at_the_tls_port_loses_its_connection_and_its_place() throws Exception {
        // Arrange: room for one connection that waits for its CONNECT.
        SSLContext context = Tls.serverContext();
        try (BrokerServer broker = BrokerServer.builder()
                .maxAwaitingConnect(1)
                .listenTls(loopback(), context, serverParameters(context, _ -> { }))
                .start(null)) {
            try (var plain = new Socket(InetAddress.getLoopbackAddress(), broker.tlsPort())) {
                // Act
                plain.getOutputStream().write(RawSession.codec.encode(
                        RawSession.connect("plain-at-tls", true, 60)
                ));
                // Assert
                assertClosed(plain);
            }
            // The place that the plain client had goes to the next client.
            try (MqttClient next = tlsClient(
                    broker.tlsPort(),
                    Tls.clientContext(),
                    MqttVersion.V3_1_1
            )) {
                next.connect().get(5, SECONDS);
            }
        }
    }

    @Test
    void a_client_that_does_not_finish_its_handshake_loses_its_connection_at_the_connect_deadline()
            throws Exception {
        // Arrange
        SSLContext context = Tls.serverContext();
        try (BrokerServer broker = BrokerServer.builder()
                .connectTimeout(Duration.ofMillis(300))
                .listenTls(loopback(), context, serverParameters(context, _ -> { }))
                .start(null);
                var silent = new Socket(InetAddress.getLoopbackAddress(), broker.tlsPort())) {
            // Act & Assert: the client sends nothing.
            assertClosed(silent);
        }
    }

    @Test
    void a_trust_manager_that_waits_holds_up_no_other_client() throws Exception {
        // Arrange: a trust manager that holds the check of a client's certificate.
        var check = new HeldCheck();
        SSLContext context = Tls.serverContext(check::around);
        try (MqttBroker broker = MqttBroker.builder()
                .listen(loopback())
                .listenTls(loopback(), context)
                .sslParameters(parameters -> parameters.setNeedClientAuth(true))
                .start();
                MqttClient held = tlsClient(
                        broker.tlsPort(),
                        Tls.clientContextWithCertificate(),
                        MqttVersion.V5
                )) {
            CompletableFuture<ConnAck> connecting = held.connect();
            assertThat(check.started.await(5, SECONDS)).isTrue();
            // Act: another client while the check waits.
            try (RawSession other = RawSession.connected(broker.port(), "other")) {
                other.subscribe("other/topic", Qos.AT_LEAST_ONCE);
                other.publish("other/topic", "meanwhile", Qos.AT_MOST_ONCE, 0);
                other.expect(Packet.Publish.class);
                other.ping();
            }
            // Assert: the check ran on a thread for TLS, and the held client goes on.
            assertThat(check.thread.get()).contains("-tls-");
            check.release.countDown();
            connecting.get(5, SECONDS);
        }
    }

    @Test
    void close_interrupts_a_waiting_trust_manager_and_the_end_waits_for_it_to_return()
            throws Exception {
        // Arrange: a trust manager that holds a check until it is interrupted, and then until
        // the test lets it return.
        var check = new HeldCheck();
        SSLContext context = Tls.serverContext(check::around);
        MqttBroker broker = MqttBroker.builder()
                .listenTls(loopback(), context)
                .sslParameters(parameters -> parameters.setNeedClientAuth(true))
                .start();
        try (MqttClient held = tlsClient(
                broker.tlsPort(),
                Tls.clientContextWithCertificate(),
                MqttVersion.V5
        )) {
            CompletableFuture<ConnAck> _ = held.connect();
            assertThat(check.started.await(5, SECONDS)).isTrue();
            // Act
            CompletableFuture.runAsync(broker::close).get(5, SECONDS);
            CompletableFuture<Void> end = broker.terminated();
            // Assert
            assertThat(check.interrupted.await(5, SECONDS)).isTrue();
            assertThat(end).isNotDone();
            check.release.countDown();
            end.get(5, SECONDS);
        }
    }

    @Test
    void a_client_that_asks_for_a_new_handshake_loses_its_connection() throws Exception {
        // Arrange: a TLS 1.2 client, which can ask for one.
        try (MqttBroker broker = MqttBroker.builder()
                .listenTls(loopback(), Tls.serverContext())
                .start()) {
            SSLSocket socket = tlsSocket(broker.tlsPort(), "TLSv1.2");
            try (RawSession session = RawSession.over(socket, MqttVersion.V3_1_1)) {
                session.send(RawSession.connect("renegotiating", true, 60));
                session.expect(Packet.ConnAck.class);
                // Act
                try {
                    socket.startHandshake();
                } catch (IOException _) {
                    // The broker may close the connection before the handshake starts.
                }
                // Assert
                assertClosed(socket);
            }
        }
    }

    @Test
    void a_key_update_leaves_the_connection_open() throws Exception {
        // Arrange: a TLS 1.3 client, whose new handshake is a key update that asks for the
        // broker's.
        try (MqttBroker broker = MqttBroker.builder()
                .listenTls(loopback(), Tls.serverContext())
                .start()) {
            SSLSocket socket = tlsSocket(broker.tlsPort(), "TLSv1.3");
            try (RawSession sut = RawSession.over(socket, MqttVersion.V3_1_1)) {
                sut.send(RawSession.connect("updating", true, 60));
                sut.expect(Packet.ConnAck.class);
                // Act
                socket.startHandshake();
                // Assert
                sut.ping();
            }
        }
    }

    @Test
    void a_trust_manager_that_throws_an_error_fails_that_handshake_alone() throws Exception {
        // Arrange: a trust manager that breaks its contract with an Error, and a client whose
        // engine sends its certificate and the rest of its last flight in one record, which
        // leaves the broker's engine waiting for a task that it no longer gives.
        SSLContext context = Tls.serverContext(trust -> checking(trust, () -> {
            throw new AssertionError("a broken trust manager");
        }));
        try (MqttBroker broker = MqttBroker.builder()
                .listen(loopback())
                .listenTls(loopback(), context)
                .sslParameters(parameters -> parameters.setNeedClientAuth(true))
                .start();
                var socket = new Socket(InetAddress.getLoopbackAddress(), broker.tlsPort())) {
            SSLEngine client = Tls.clientContextWithCertificate().createSSLEngine(
                    "localhost",
                    broker.tlsPort()
            );
            client.setUseClientMode(true);
            // Act: the client's side of the handshake ends once it has sent its last flight.
            handshake(client, socket);
            // Assert: the broker closes the connection at once, rather than at its deadline,
            // and goes on with other clients.
            assertClosed(socket);
            try (RawSession other = RawSession.connected(broker.port(), "other")) {
                other.ping();
            }
        }
    }

    // Runs `client`'s side of a handshake over `socket` until the engine has no more to do, as
    // a client does that has sent its last flight, or until the broker ends the handshake with
    // an alert or by closing the connection.
    private static void handshake(SSLEngine client, Socket socket) throws IOException {
        socket.setSoTimeout(5_000);
        var empty = ByteBuffer.allocate(0);
        var in = ByteBuffer.allocate(client.getSession().getPacketBufferSize() * 4);
        var out = ByteBuffer.allocate(client.getSession().getPacketBufferSize());
        var app = ByteBuffer.allocate(client.getSession().getApplicationBufferSize());
        client.beginHandshake();
        try {
            runHandshake(client, socket, empty, in, out, app);
        } catch (SSLException _) {
            // The broker's alert.
        }
    }

    private static void runHandshake(
            SSLEngine client,
            Socket socket,
            ByteBuffer empty,
            ByteBuffer in,
            ByteBuffer out,
            ByteBuffer app
    ) throws IOException {
        while (true) {
            switch (client.getHandshakeStatus()) {
                case NEED_TASK -> client.getDelegatedTask().run();
                case NEED_WRAP -> {
                    out.clear();
                    SSLEngineResult _ = client.wrap(empty, out);
                    socket.getOutputStream().write(out.array(), 0, out.position());
                }
                case NEED_UNWRAP, NEED_UNWRAP_AGAIN -> {
                    in.flip();
                    SSLEngineResult result = client.unwrap(in, app);
                    in.compact();
                    if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                        int count = socket.getInputStream().read(
                                in.array(),
                                in.position(),
                                in.remaining()
                        );
                        if (count < 0) {
                            return;
                        }
                        in.position(in.position() + count);
                    }
                }
                case NOT_HANDSHAKING, FINISHED -> {
                    return;
                }
            }
        }
    }

    // A trust manager that runs `check` before `trust` checks the certificates of a client that
    // connects to a broker, and that leaves the rest to `trust`.
    private static X509ExtendedTrustManager checking(
            X509ExtendedTrustManager trust,
            Runnable check
    ) {
        return new X509ExtendedTrustManager() {

            @Override
            public void checkClientTrusted(
                    X509Certificate[] chain,
                    String authType,
                    SSLEngine engine
            ) throws CertificateException {
                check.run();
                trust.checkClientTrusted(chain, authType, engine);
            }

            @Override
            public void checkClientTrusted(
                    X509Certificate[] chain,
                    String authType,
                    Socket socket
            ) throws CertificateException {
                trust.checkClientTrusted(chain, authType, socket);
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType)
                    throws CertificateException {
                trust.checkClientTrusted(chain, authType);
            }

            @Override
            public void checkServerTrusted(
                    X509Certificate[] chain,
                    String authType,
                    SSLEngine engine
            ) throws CertificateException {
                trust.checkServerTrusted(chain, authType, engine);
            }

            @Override
            public void checkServerTrusted(
                    X509Certificate[] chain,
                    String authType,
                    Socket socket
            ) throws CertificateException {
                trust.checkServerTrusted(chain, authType, socket);
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType)
                    throws CertificateException {
                trust.checkServerTrusted(chain, authType);
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return trust.getAcceptedIssuers();
            }
        };
    }

    // A check of client certificates that tells when it starts and on which thread, and waits
    // until the test releases it. An interrupt does not end the wait, which goes on after it has
    // been counted, so that a test sees what waits for the check to return.
    private static final class HeldCheck {

        final CountDownLatch started = new CountDownLatch(1);

        final CountDownLatch interrupted = new CountDownLatch(1);

        final CountDownLatch release = new CountDownLatch(1);

        final AtomicReference<String> thread = new AtomicReference<>();

        X509ExtendedTrustManager around(X509ExtendedTrustManager trust) {
            return checking(trust, this::hold);
        }

        private void hold() {
            thread.set(Thread.currentThread().getName());
            started.countDown();
            while (true) {
                try {
                    // The limit keeps a failing test from holding the broker's thread for good.
                    boolean _ = release.await(10, SECONDS);
                    return;
                } catch (InterruptedException _) {
                    interrupted.countDown();
                }
            }
        }
    }

    private static MqttClient tlsClient(int port, SSLContext context, MqttVersion version) {
        return MqttClient.builder()
                .server(URI.create("mqtts://localhost:" + port))
                .sslContext(context)
                .version(version)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    private static MqttClient plainClient(int port) {
        return MqttClient.builder()
                .server(URI.create("mqtt://localhost:" + port))
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    // A client socket that has finished its handshake with the test CA's trust, over `protocol`
    // alone.
    private static SSLSocket tlsSocket(int port, String protocol) throws Exception {
        var socket = (SSLSocket) Tls.clientContext().getSocketFactory().createSocket(
                InetAddress.getLoopbackAddress(),
                port
        );
        socket.setEnabledProtocols(new String[] {protocol});
        socket.setSoTimeout(5_000);
        socket.startHandshake();
        return socket;
    }

    // The parameters of a server engine of `context`, as `customizer` changes them.
    private static SSLParameters serverParameters(
            SSLContext context,
            Consumer<SSLParameters> customizer
    ) {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        SSLParameters parameters = engine.getSSLParameters();
        customizer.accept(parameters);
        return parameters;
    }

    // Reads until the broker closes the connection: until the end of the stream, or a failure
    // such as a reset, which a closed TLS connection can also give. What the broker sent before,
    // such as an alert, is passed over. A read that times out finds the connection open.
    private static void assertClosed(Socket socket) throws IOException {
        socket.setSoTimeout(5_000);
        InputStream input = socket.getInputStream();
        try {
            while (input.read() >= 0) {
                // Passed over.
            }
        } catch (SocketTimeoutException failure) {
            throw new AssertionError("The broker kept the connection open", failure);
        } catch (IOException _) {
            // Closed with a failure.
        }
    }

    private static MqttMessage message(String topic, byte[] payload, Qos qos) {
        return MqttMessage.of(new Topic(topic), payload, qos, false);
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
