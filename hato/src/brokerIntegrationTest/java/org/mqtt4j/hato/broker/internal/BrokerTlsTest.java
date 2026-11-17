package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.ConnectException;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.protocol.Packet;
import org.mqtt4j.hato.internal.protocol.Properties;
import org.mqtt4j.hato.testsupport.Tls;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// The TLS listener: clients that exchange messages through it, beside the plain one or alone,
/// what the authenticator learns of them, and handshakes that fail, take long, or come again.
class BrokerTlsTest {

    // A whole TLS application record whose authentication fails: its header, then 32 bytes that no
    // key encrypted.
    private static final byte[] BROKEN_RECORD = brokenRecord();

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
    void the_authenticator_sees_the_certificate_that_a_client_presents() throws Exception {
        // Arrange
        var asked = new AtomicReference<Authenticator.Request>();
        try (MqttBroker broker = MqttBroker.builder()
                .listenTls(loopback(), Tls.serverContext())
                .sslParameters(parameters -> parameters.setNeedClientAuth(true))
                .authenticator(request -> {
                    asked.set(request);
                    return Authenticator.Decision.accept(Authenticator.Scope.all());
                })
                .start();
                MqttClient sut = tlsClient(
                        broker.tlsPort(),
                        Tls.clientContextWithCertificate(),
                        MqttVersion.V5
                )) {
            // Act
            sut.connect().get(5, SECONDS);
            // Assert
            Authenticator.Request request = asked.get();
            assertThat(request.overTls()).isTrue();
            List<X509Certificate> chain = request.clientCertificates();
            assertThat(chain).hasSize(2);
            assertThat(chain.getFirst().getSubjectX500Principal().getName())
                    .contains("CN=Hato Test client");
            assertThat(chain.get(1).getSubjectX500Principal().getName())
                    .contains("CN=Hato Test CA");
        }
    }

    @Test
    void the_authenticator_tells_a_tls_client_without_a_certificate_from_a_plain_one()
            throws Exception {
        // Arrange: a TLS listener that asks for a certificate without requiring one.
        var asked = new LinkedBlockingQueue<Authenticator.Request>();
        try (MqttBroker broker = MqttBroker.builder()
                .listen(loopback())
                .listenTls(loopback(), Tls.serverContext())
                .sslParameters(parameters -> parameters.setWantClientAuth(true))
                .authenticator(request -> {
                    asked.add(request);
                    return Authenticator.Decision.accept(Authenticator.Scope.all());
                })
                .start();
                MqttClient secure = tlsClient(
                        broker.tlsPort(),
                        Tls.clientContext(),
                        MqttVersion.V5
                );
                MqttClient plain = plainClient(broker.port())) {
            // Act
            secure.connect().get(5, SECONDS);
            Authenticator.Request overTls = asked.poll(5, SECONDS);
            plain.connect().get(5, SECONDS);
            Authenticator.Request overPlain = asked.poll(5, SECONDS);
            // Assert
            assertThat(overTls).isNotNull();
            assertThat(overTls.overTls()).isTrue();
            assertThat(overTls.clientCertificates()).isEmpty();
            assertThat(overPlain).isNotNull();
            assertThat(overPlain.overTls()).isFalse();
            assertThat(overPlain.clientCertificates()).isEmpty();
        }
    }

    @Test
    void a_required_certificate_keeps_a_client_without_one_away_from_the_authenticator()
            throws Exception {
        // Arrange
        var asked = new AtomicInteger();
        try (MqttBroker broker = MqttBroker.builder()
                .listenTls(loopback(), Tls.serverContext())
                .sslParameters(parameters -> parameters.setNeedClientAuth(true))
                .authenticator(_ -> {
                    asked.incrementAndGet();
                    return Authenticator.Decision.accept(Authenticator.Scope.all());
                })
                .start();
                MqttClient sut = tlsClient(broker.tlsPort(), Tls.clientContext(), MqttVersion.V5)) {
            // Act & Assert
            assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class);
            assertThat(asked).hasValue(0);
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
            // Assert: the check ran on a thread for TLS, named after the broker's port as its other
            // threads are, and the held client goes on.
            assertThat(check.thread.get()).startsWith("hato-broker-" + broker.port() + "-tls-");
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

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void a_broken_record_ends_the_connection_after_the_packets_before_it(String protocol)
            throws Exception {
        // Arrange: a selector that a PINGREQ holds while the client sends DISCONNECT and then a
        // record that fails its authentication, so that one read takes both.
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var disconnects = new AtomicInteger();
        SSLContext context = Tls.serverContext();
        try (BrokerServer broker = BrokerServer.builder()
                .listenTls(loopback(), context, serverParameters(context, _ -> { }))
                .beforeProcess(packet -> {
                    if (packet instanceof Packet.PingReq) {
                        held.countDown();
                        awaitUninterruptibly(release);
                    }
                    if (packet instanceof Packet.Disconnect) {
                        disconnects.incrementAndGet();
                    }
                })
                .start(null);
                var raw = new Socket(InetAddress.getLoopbackAddress(), broker.tlsPort())) {
            var client = (SSLSocket) Tls.clientContext().getSocketFactory().createSocket(
                    raw,
                    "localhost",
                    raw.getPort(),
                    false
            );
            client.setEnabledProtocols(new String[] {protocol});
            client.startHandshake();
            try (RawSession session = RawSession.over(client, MqttVersion.V3_1_1)) {
                session.send(RawSession.connect("disconnecting", true, 60));
                session.expect(Packet.ConnAck.class);
                session.send(new Packet.PingReq());
                assertThat(held.await(5, SECONDS)).isTrue();
                // Act
                try {
                    session.send(new Packet.Disconnect(ReasonCode.SUCCESS, Properties.EMPTY));
                    raw.getOutputStream().write(BROKEN_RECORD);
                } finally {
                    release.countDown();
                }
                // Assert: the broker ends the connection once its session has the DISCONNECT,
                // which keeps the Will from going out.
                assertClosed(raw);
                assertThat(disconnects).hasValue(1);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void a_broken_record_after_a_publication_ends_the_connection_and_sends_the_will(
            String protocol
    ) throws Exception {
        // Arrange: a selector that a QoS 0 publication to "hold" holds while a client without a
        // keep-alive sends another one and then a record that fails its authentication, so that
        // one read takes both, and nothing that needs an answer gives the connection another
        // turn; and a plain client that waits for the Will.
        var hold = new Topic("hold");
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        SSLContext context = Tls.serverContext();
        try (BrokerServer broker = BrokerServer.builder()
                .listenTls(loopback(), context, serverParameters(context, _ -> { }))
                .beforeProcess(packet -> {
                    if (packet instanceof Packet.Publish publication
                            && publication.topic().equals(hold)) {
                        held.countDown();
                        awaitUninterruptibly(release);
                    }
                })
                .start(loopback());
                RawSession watcher = RawSession.connected(broker.plainPort(), "watcher");
                var raw = new Socket(InetAddress.getLoopbackAddress(), broker.tlsPort())) {
            watcher.subscribe("wills/#", Qos.AT_MOST_ONCE);
            var client = (SSLSocket) Tls.clientContext().getSocketFactory().createSocket(
                    raw,
                    "localhost",
                    raw.getPort(),
                    false
            );
            client.setEnabledProtocols(new String[] {protocol});
            client.startHandshake();
            try (RawSession session = RawSession.over(client, MqttVersion.V3_1_1)) {
                session.send(new Packet.Connect(
                        "publishing",
                        MqttVersion.V3_1_1,
                        true,
                        0,
                        new Packet.Will(
                                new Topic("wills/publishing"),
                                new byte[] {1},
                                Qos.AT_MOST_ONCE,
                                false,
                                Properties.EMPTY
                        ),
                        null,
                        null,
                        Properties.EMPTY
                ));
                session.expect(Packet.ConnAck.class);
                session.send(publication(hold));
                assertThat(held.await(5, SECONDS)).isTrue();
                // Act
                try {
                    session.send(publication(new Topic("readings")));
                    raw.getOutputStream().write(BROKEN_RECORD);
                } finally {
                    release.countDown();
                }
                // Assert: the broker ends the connection without more input to wait for, and the
                // failure sends the Will.
                assertClosed(raw);
                assertThat(watcher.expect(Packet.Publish.class).topic())
                        .isEqualTo(new Topic("wills/publishing"));
            }
        }
    }

    @Test
    void a_long_stream_holds_the_selector_to_work_in_proportion_to_its_bytes() throws Exception {
        // Arrange: one selector, whose allocations show the work that each byte takes, and
        // publications of many records each, which leave records partly read.
        var bean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        assumeTrue(bean.isThreadAllocatedMemorySupported());
        int publications = 512;
        var processed = new CountDownLatch(publications);
        SSLContext context = Tls.serverContext();
        try (BrokerServer broker = BrokerServer.builder()
                .selectors(1)
                .listenTls(loopback(), context, serverParameters(context, _ -> { }))
                .beforeProcess(packet -> {
                    if (packet instanceof Packet.Publish) {
                        processed.countDown();
                    }
                })
                .start(null)) {
            SSLSocket socket = tlsSocket(broker.tlsPort(), "TLSv1.3");
            try (RawSession sut = RawSession.over(socket, MqttVersion.V3_1_1)) {
                sut.send(RawSession.connect("streaming", true, 60));
                sut.expect(Packet.ConnAck.class);
                byte[] publication = RawSession.codec.encode(new Packet.Publish(
                        new Topic("stream"),
                        new byte[64 * 1024],
                        Qos.AT_MOST_ONCE,
                        false,
                        false,
                        0,
                        Properties.EMPTY
                ));
                String name = "hato-broker-" + broker.port() + "-selector-0";
                long selector = Thread.getAllStackTraces().keySet().stream()
                        .filter(thread -> thread.getName().equals(name))
                        .findFirst()
                        .orElseThrow()
                        .threadId();
                long before = bean.getThreadAllocatedBytes(selector);
                // Act
                for (int i = 0; i < publications; i++) {
                    sut.sendRaw(publication);
                }
                assertThat(processed.await(30, SECONDS)).isTrue();
                // Assert: about four bytes a byte when ciphertext waits a record at most; a
                // selector that kept and copied what waited took over thirty at this length.
                long allocated = bean.getThreadAllocatedBytes(selector) - before;
                assertThat(allocated).isLessThan(12L * publications * publication.length);
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
    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_shutdown_frees_the_tls_port_at_once_and_ends_tls_with_close_notify(MqttVersion version)
            throws Exception {
        // Arrange: a TLS client that has yet to acknowledge a delivery, for which the shutdown
        // waits.
        try (MqttBroker broker = MqttBroker.builder()
                .listen(loopback())
                .listenTls(loopback(), Tls.serverContext())
                .start();
                RawSession app = RawSession.connected(broker.port(), "app");
                var raw = new Socket(InetAddress.getLoopbackAddress(), broker.tlsPort())) {
            var client = (SSLSocket) Tls.clientContext().getSocketFactory().createSocket(
                    raw,
                    "localhost",
                    raw.getPort(),
                    false
            );
            client.startHandshake();
            try (RawSession robot = RawSession.over(client, version)) {
                robot.send((version == MqttVersion.V5)
                        ? RawSession.connectV5("robot", true, 0, 0)
                        : RawSession.connect("robot", true, 0));
                robot.expect(Packet.ConnAck.class);
                robot.subscribe("orders", Qos.AT_LEAST_ONCE);
                app.publish("orders", "go", Qos.AT_LEAST_ONCE, 1);
                Packet.Publish order = robot.expect(Packet.Publish.class);
                // Act
                CompletableFuture<Boolean> shutdown = CompletableFuture.supplyAsync(
                        () -> broker.shutdown(Duration.ofSeconds(5))
                );
                // Assert: the TLS port refuses connections while the broker waits for the robot,
                // which then learns of the end: an MQTT 5.0 robot from DISCONNECT, and both from
                // close_notify, the one record that comes before the end of the stream.
                awaitRefused(broker.tlsPort());
                robot.send(new Packet.PubAck(
                        order.packetId(),
                        ReasonCode.SUCCESS,
                        Properties.EMPTY
                ));
                if (version == MqttVersion.V5) {
                    assertThat(robot.expect(Packet.Disconnect.class).reasonCode())
                            .isEqualTo(ReasonCode.SERVER_SHUTTING_DOWN);
                }
                raw.setSoTimeout(5_000);
                byte[] rest = raw.getInputStream().readAllBytes();
                assertThat(rest).hasSizeGreaterThan(5);
                assertThat(rest).hasSize(5 + ((rest[3] & 0xff) << 8 | (rest[4] & 0xff)));
                assertThat(shutdown).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(true);
            }
        }
    }

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

    // Waits until nothing listens at `port` any more, as once the broker that listened there
    // shuts down.
    private static void awaitRefused(int port) {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThatThrownBy(() -> {
            try (var socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
                socket.getInputStream();
            }
        }).isInstanceOf(ConnectException.class));
    }

    private static MqttMessage message(String topic, byte[] payload, Qos qos) {
        return MqttMessage.of(new Topic(topic), payload, qos, false);
    }

    // A QoS 0 publication of one byte to `topic`.
    private static Packet.Publish publication(Topic topic) {
        return new Packet.Publish(
                topic,
                new byte[] {2},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                Properties.EMPTY
        );
    }

    private static byte[] brokenRecord() {
        var record = new byte[5 + 32];
        // Application data, the record version of TLS 1.2 and 1.3, and the length.
        record[0] = 23;
        record[1] = 3;
        record[2] = 3;
        record[4] = 32;
        return record;
    }

    // Waits for `latch` on a thread of the broker, which a test holds this way, and keeps the
    // interrupt that comes meanwhile.
    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                boolean _ = latch.await(10, SECONDS);
                break;
            } catch (InterruptedException _) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
