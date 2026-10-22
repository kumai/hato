package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.Transport;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

@Timeout(30)
class ConnectionRequestsIntegrationTest {

    @Test
    void disconnect_without_a_connection_completes_locally() throws Exception {
        try (ServerSocket server = listen(); MqttClient sut = ScriptedBroker.client(server)) {
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void concurrent_connect_calls_share_one_attempt_and_one_result() throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = ScriptedBroker.client(server)) {
            CompletableFuture<ConnAck> first = sut.connect();
            try (ScriptedBroker peer = ScriptedBroker.accept(server)) {
                assertThat(peer.receive()).isInstanceOf(Packet.Connect.class);

                // Act: the second request joins while the broker still withholds CONNACK.
                CompletableFuture<ConnAck> second = sut.connect();
                peer.send(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));

                // Assert
                ConnAck ack = first.get(5, SECONDS);
                assertThat(second.get(5, SECONDS)).isSameAs(ack);
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void connecting_again_while_connected_returns_the_current_connack() throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = ScriptedBroker.client(server)) {
            CompletableFuture<ConnAck> connected = sut.connect();
            try (ScriptedBroker peer = ScriptedBroker.accept(server)) {
                assertThat(peer.receive()).isInstanceOf(Packet.Connect.class);
                peer.send(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));
                ConnAck ack = connected.get(5, SECONDS);

                // Act & Assert
                assertThat(sut.connect().get(5, SECONDS)).isSameAs(ack);
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void disconnect_during_an_attempt_waits_for_connack_and_preserves_registration()
            throws Exception {
        // Arrange
        try (ServerSocket server = listen(); MqttClient sut = ScriptedBroker.client(server)) {
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("data"), Qos.AT_LEAST_ONCE)
            );
            CompletableFuture<Qos> established = subscription.established();
            CompletableFuture<ConnAck> connected = sut.connect();
            try (ScriptedBroker peer = ScriptedBroker.accept(server)) {
                assertThat(peer.receive()).isInstanceOf(Packet.Connect.class);

                // Act: a successful CONNACK must be followed by DISCONNECT, without SUBSCRIBE.
                CompletableFuture<Void> disconnected = sut.disconnect();
                peer.send(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));

                // Assert
                assertThat(peer.receive()).isInstanceOf(Packet.Disconnect.class);
                disconnected.get(5, SECONDS);
                assertThatThrownBy(() -> connected.get(5, SECONDS))
                        .hasCauseInstanceOf(MqttConnectException.class);
                assertThat(established).isNotDone();
                peer.expectClosed();
            }

            // The same registration is established by a later explicit connection.
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                Packet.Subscribe packet = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                assertThat(packet.subscriptions()).singleElement().satisfies(request ->
                        assertThat(request.filter()).isEqualTo(subscription.filter()));
                peer.send(new Packet.SubAck(
                        packet.packetId(),
                        List.of(ReasonCode.GRANTED_QOS_1),
                        EMPTY
                ));
                assertThat(established.get(5, SECONDS)).isEqualTo(Qos.AT_LEAST_ONCE);
                peer.expectPublicationBarrier(sut);
            }
        }
    }

    @Test
    void repeated_disconnect_calls_observe_one_disconnect_packet() throws Exception {
        // Arrange
        try (ServerSocket server = listen();
                MqttClient sut = ScriptedBroker.client(server);
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            // Act
            CompletableFuture<Void> first = sut.disconnect();
            CompletableFuture<Void> second = sut.disconnect();

            // Assert
            assertThat(peer.receive()).isInstanceOf(Packet.Disconnect.class);
            first.get(5, SECONDS);
            second.get(5, SECONDS);
            peer.expectClosed();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void termination_waits_until_the_default_transport_reports_its_close() throws Exception {
        // Arrange: a wrapper around the default TCP transport holds its final close report.
        var reports = new LinkedBlockingQueue<Runnable>();
        try (ServerSocket server = listen();
                MqttClient sut = MqttClient.builder()
                        .server(ScriptedBroker.endpoint(server))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .transportFactory(id -> new ReportHoldingTransport(
                                Transport.tcp(id),
                                reports
                        ))
                        .build();
                ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
            var actor = (ClientActor) sut;
            CompletableFuture<Void> terminated = sut.terminated();

            // Act
            sut.close();

            // Assert: the peer sees the socket close, but termination waits for the report.
            peer.expectClosed();
            Runnable report = requireNonNull(reports.poll(5, SECONDS));
            await().until(() -> actor.terminationPending()
                    .equals(Set.of(ClientTermination.Pending.TRANSPORTS)));
            assertThat(terminated).isNotDone();
            report.run();
            terminated.get(5, SECONDS);
        }
    }

    /// Delegates to another transport, but holds its final close report until a test runs it.
    private static final class ReportHoldingTransport implements Transport {

        private final Transport delegate;

        private final LinkedBlockingQueue<Runnable> reports;

        ReportHoldingTransport(Transport delegate, LinkedBlockingQueue<Runnable> reports) {
            this.delegate = delegate;
            this.reports = reports;
        }

        @Override
        public void start(URI endpoint, Duration timeout, Listener listener) {
            delegate.start(endpoint, timeout, new Listener() {
                @Override
                public void onConnected() {
                    listener.onConnected();
                }

                @Override
                public void onBytes(byte[] bytes) {
                    listener.onBytes(bytes);
                }

                @Override
                public void onSent(long ticket) {
                    listener.onSent(ticket);
                }

                @Override
                public void onClosed(Throwable cause) {
                    reports.add(() -> listener.onClosed(cause));
                }
            });
        }

        @Override
        public SendResult send(long ticket, byte[] bytes) {
            return delegate.send(ticket, bytes);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
