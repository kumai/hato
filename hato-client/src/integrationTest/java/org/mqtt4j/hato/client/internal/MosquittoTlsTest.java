package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;
import org.mqtt4j.hato.testsupport.Tls;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The client's TLS against an independent implementation: Mosquitto's OpenSSL listeners, in
/// both protocol versions.
@EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
@ParameterizedClass(name = "{0}")
@EnumSource(MqttVersion.class)
class MosquittoTlsTest {

    private static final GenericContainer<?> broker = Mosquitto.tlsContainer();

    private final MqttVersion version;

    // Established connections and their ends, apart from attempts that never established one.
    private final LinkedBlockingQueue<Object> connections = new LinkedBlockingQueue<>();

    private final LinkedBlockingQueue<Disconnection> failures = new LinkedBlockingQueue<>();

    private final ConnectionListener listener = new ConnectionListener() {
        @Override
        public void onConnected(ConnAck connAck) {
            connections.add(connAck);
        }

        @Override
        public void onDisconnected(Disconnection disconnection) {
            connections.add(disconnection);
        }

        @Override
        public void onConnectFailed(Disconnection failure) {
            failures.add(failure);
        }
    };

    MosquittoTlsTest(MqttVersion version) {
        this.version = version;
    }

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void start() {
        Docker.start(broker);
    }

    @AfterAll
    static void stop() {
        broker.stop();
    }

    @Test
    void publications_round_trip_over_tls_with_server_authentication() throws Exception {
        // Arrange: the URI uses localhost, a name in the broker's certificate, so the default
        // hostname verification passes. TlsConnectionTest checks that a mismatch is refused.
        String topic = uniqueTopic();
        var publication = MqttMessage.of(
                new Topic(topic),
                new byte[] {1, 2, 3},
                Qos.EXACTLY_ONCE,
                false
        );
        try (MqttClient sut = MqttClient.builder()
                .server(endpoint(Mosquitto.MQTTS_PORT))
                .version(version)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .sslContext(Tls.clientContext())
                .connectionListener(listener)
                .build()) {
            ConnAck connAck = sut.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.EXACTLY_ONCE)
            );
            subscription.established().get(5, SECONDS);

            // Act
            sut.publish(publication).get(5, SECONDS);
            ReceiveResult.Received received = assertInstanceOf(
                    ReceiveResult.Received.class,
                    subscription.receive(Duration.ofSeconds(5))
            );
            subscription.unsubscribe().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);

            // Assert: the QoS 2 publication arrived unchanged, and the one connection lasted
            // until the requested disconnection.
            assertThat(received.delivery().message()).isEqualTo(publication);
            assertThat(connAck.sessionPresent()).isFalse();
            assertThat(connections.poll(5, SECONDS)).isEqualTo(connAck);
            assertThat(connections.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            assertThat(connections).isEmpty();
            assertThat(failures).isEmpty();
        }
    }

    @Test
    void a_context_with_a_client_certificate_connects_to_a_broker_that_requires_one()
            throws Exception {
        // Arrange
        try (MqttClient sut = mutualTlsClient(Tls.clientContextWithCertificate())) {
            // Act
            ConnAck connAck = sut.connect().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);

            // Assert
            assertThat(connAck.sessionPresent()).isFalse();
            assertThat(connections.poll(5, SECONDS)).isEqualTo(connAck);
            assertThat(connections.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            assertThat(connections).isEmpty();
            assertThat(failures).isEmpty();
        }
    }

    @Test
    void connecting_without_a_client_certificate_fails_and_reports_no_connection()
            throws Exception {
        // Arrange: the context trusts the test CA but has no client certificate to present.
        try (MqttClient sut = mutualTlsClient(Tls.clientContext())) {
            // Act
            CompletableFuture<ConnAck> connected = sut.connect();

            // Assert: the broker checks the certificate only after the client has finished
            // its TLS 1.3 handshake, so the refusal fails a read before CONNACK. That ends the
            // attempt with a communication failure, which the listener reports as the same
            // failure as connect().
            assertThatThrownBy(() -> connected.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .hasRootCauseInstanceOf(IOException.class);
            assertThat(failures.poll(5, SECONDS))
                    .isEqualTo(Disconnection.of(connected.exceptionNow(), false, 1));
            assertThat(failures).isEmpty();
            assertThat(connections).isEmpty();
        }
    }

    // Both tests of the pair pin TLS 1.3. A broker without it then fails the accepting test,
    // instead of quietly moving the refusal into the handshake.
    private MqttClient mutualTlsClient(SSLContext context) throws IOException {
        return MqttClient.builder()
                .server(endpoint(Mosquitto.MUTUAL_TLS_PORT))
                .version(version)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .sslContext(context)
                .sslParameters(parameters -> parameters.setProtocols(new String[] {"TLSv1.3"}))
                .connectionListener(listener)
                .build();
    }

    // The test certificate names localhost and no IP address, so the default hostname
    // verification needs a Docker daemon on this machine, reached as localhost.
    private static URI endpoint(int port) throws IOException {
        assertThat(InetAddress.getByName(broker.getHost()).isLoopbackAddress())
                .as("Docker host %s is not this machine", broker.getHost())
                .isTrue();
        return URI.create("mqtts://localhost:" + broker.getMappedPort(port));
    }
}
