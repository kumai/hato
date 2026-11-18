package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.UserProperty;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.internal.inproc.InprocBrokers;
import org.mqtt4j.hato.internal.inproc.InprocLink;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;

/// The broker's side of in-process connections, where a client's link meets the limits that
/// connections over TCP share.
class BrokerInprocConnectionTest {

    @Test
    void a_client_that_ends_before_its_acceptance_gives_its_place_back_at_once() throws Exception {
        // Arrange: room for one connection that waits for its CONNECT, and an in-process client
        // that ends before the broker accepts its link.
        try (BrokerServer sut = BrokerServer.builder()
                .name("ended-before-acceptance-test")
                .maxAwaitingConnect(1)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))) {
            var link = new InprocLink();
            link.endClient();
            // Act
            requireNonNull(InprocBrokers.find("ended-before-acceptance-test")).connect(link);
            // Assert: the broker ends that connection long before its CONNECT deadline, and a
            // TCP client takes the place that it had.
            await().atMost(Duration.ofSeconds(5)).until(link::brokerEnded);
            try (RawSession next = connected(sut.port(), "next")) {
                next.ping();
            }
        }
    }

    @Test
    void network_connections_that_wait_for_their_connect_leave_room_for_in_process_ones()
            throws Exception {
        // Arrange: room for one connection that waits for its CONNECT, taken by a TCP client whose
        // decision the authenticator holds.
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Authenticator authenticator = request -> {
            if (request.clientId().equals("network")) {
                held.countDown();
                boolean _ = release.await(10, SECONDS);
            }
            return Authenticator.Decision.accept(Authenticator.Scope.all());
        };
        try (BrokerServer broker = BrokerServer.builder()
                .name("network-holds-its-place-test")
                .maxAwaitingConnect(1)
                .authenticator(authenticator)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession network = RawSession.open(broker.port());
                MqttClient sut = MqttClient.builder()
                        .server(URI.create("inproc://network-holds-its-place-test"))
                        .clientId("application")
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .build()) {
            network.send(RawSession.connect("network", true, 0));
            assertThat(held.await(5, SECONDS)).isTrue();
            try {
                // Act & Assert: the broker accepts the in-process client.
                sut.connect().get(5, SECONDS);
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    @SuppressWarnings("try") // The client reaches the broker by its name alone.
    void an_in_process_publication_with_more_properties_than_a_packet_takes_ends_its_connection()
            throws Exception {
        // Arrange: an MQTT 5.0 client in the same JVM, whose packets the broker takes undecoded.
        try (BrokerServer broker = BrokerServer.builder()
                .name("many-properties-test")
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                MqttClient sut = MqttClient.builder()
                        .server(URI.create("inproc://many-properties-test"))
                        .version(MqttVersion.V5)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .build()) {
            sut.connect().get(5, SECONDS);
            MqttMessage message = MqttMessage.of(
                    new Topic("readings"),
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false
            ).withUserProperties(Collections.nCopies(65_537, new UserProperty("k", "v")));

            // Act & Assert: the broker ends the connection rather than pass on a packet that no
            // client over a network would decode.
            assertThatThrownBy(() -> sut.publish(message).get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class);
        }
    }
}
