package org.mqtt4j.hato.client.internal;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/// Connections to a broker in the same JVM by its `inproc` URI, beyond what the shared suites
/// check over them: finding the broker by name, its end and its start again, the thread that runs
/// an inline handler, a subscriber that falls behind, a takeover between TCP and memory, and what
/// the authenticator learns of such a client.
class InprocConnectionTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test
    void a_connection_attempt_finds_no_broker_of_an_unknown_name_as_it_finds_no_host()
            throws Exception {
        // Arrange
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("inproc://unknown-broker-test"))
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build()) {
            // Act
            CompletableFuture<ConnAck> attempt = sut.connect();
            // Assert
            assertThatThrownBy(() -> attempt.get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(MqttConnectException.class);
        }
    }

    @Test
    @SuppressWarnings("try") // The client reaches the broker by its name alone.
    void a_client_that_retries_its_first_connection_connects_once_its_broker_starts()
            throws Exception {
        // Arrange: a client that keeps trying while no broker of the name runs.
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("inproc://late-broker-test"))
                .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                        Duration.ofMillis(10),
                        Duration.ofMillis(100)
                ).withInitialRetry())
                .build()) {
            CompletableFuture<ConnAck> attempt = sut.connect();
            assertThat(attempt).isNotDone();
            // Act
            try (MqttBroker broker = MqttBroker.builder().name("late-broker-test").start()) {
                // Assert
                assertThat(attempt).succeedsWithin(WAIT);
            }
        }
    }

    @Test
    @SuppressWarnings("try") // The client reaches the second broker by its name alone.
    void a_client_reconnects_to_a_broker_that_starts_again_under_the_same_name()
            throws Exception {
        // Arrange
        var connected = new LinkedBlockingQueue<ConnAck>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        MqttBroker first = MqttBroker.builder().name("restarted-broker-test").start();
        try (MqttClient sut = MqttClient.builder()
                .server(first.uri())
                .reconnectPolicy(ReconnectPolicy.exponentialBackoff(
                        Duration.ofMillis(10),
                        Duration.ofMillis(100)
                ))
                .connectionListener(new ConnectionListener() {
                    @Override
                    public void onConnected(ConnAck connAck) {
                        connected.add(connAck);
                    }

                    @Override
                    public void onDisconnected(Disconnection disconnection) {
                        ended.add(disconnection);
                    }
                })
                .build()) {
            sut.connect().get(5, SECONDS);
            assertThat(connected.poll(5, SECONDS)).isNotNull();
            // Act: the broker ends, and another one starts under its name.
            first.close();
            assertThat(ended.poll(5, SECONDS)).isNotNull();
            try (MqttBroker second = MqttBroker.builder().name("restarted-broker-test").start()) {
                // Assert: the client is back, and publications go through.
                assertThat(connected.poll(5, SECONDS)).isNotNull();
                var topic = new Topic("restart/test");
                var received = new LinkedBlockingQueue<MqttDelivery>();
                sut.subscribe(
                        Subscribe.of(new TopicFilter("restart/test"), Qos.AT_LEAST_ONCE),
                        received::add
                ).established().get(5, SECONDS);
                sut.publish(MqttMessage.of(topic, new byte[] {1}, Qos.AT_LEAST_ONCE, false))
                        .get(5, SECONDS);
                assertThat(received.poll(5, SECONDS)).isNotNull();
            }
        }
    }

    @Test
    void an_inline_handler_runs_on_the_thread_that_reads_the_connection() throws Exception {
        // Arrange
        var threads = new LinkedBlockingQueue<String>();
        var topic = new Topic("inline/test");
        try (MqttBroker broker = MqttBroker.builder().name("inline-thread-test").start();
                MqttClient sut = MqttClient.builder()
                        .server(broker.uri())
                        .clientId("inline-reader")
                        .build();
                MqttClient publisher = MqttClient.builder().server(broker.uri()).build()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter("inline/test"), Qos.AT_LEAST_ONCE),
                    DeliveryOptions.inline(),
                    _ -> threads.add(Thread.currentThread().getName())
            ).established().get(5, SECONDS);
            // Act: one publication at a time, each received before the next.
            var seen = new ArrayList<String>();
            for (int i = 0; i < 20; i++) {
                publisher.publish(MqttMessage.of(
                        topic,
                        new byte[] {(byte) i},
                        Qos.AT_LEAST_ONCE,
                        false
                )).get(5, SECONDS);
                seen.add(threads.poll(5, SECONDS));
            }
            // Assert: the thread that reads the connection runs the handler, as it does over TCP
            // when nothing else waits.
            assertThat(seen).contains("hato-inline-reader-reader");
        }
    }

    @Test
    void a_subscriber_that_falls_behind_holds_the_broker_back_and_misses_no_qos_1_message()
            throws Exception {
        // Arrange: a slow handler, and publications of 32 KiB, so that more than the link holds
        // waits for the subscriber.
        int count = 200;
        var topic = new Topic("behind/test");
        var received = new CopyOnWriteArrayList<Integer>();
        try (MqttBroker broker = MqttBroker.builder().name("behind-test").start();
                MqttClient sut = MqttClient.builder().server(broker.uri()).build();
                MqttClient publisher = MqttClient.builder().server(broker.uri()).build()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter("behind/test"), Qos.AT_LEAST_ONCE),
                    delivery -> {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        received.add(delivery.message().payload()[0] & 0xFF);
                    }
            ).established().get(5, SECONDS);
            // Act: the publisher waits only for the broker, which keeps the deliveries that the
            // subscriber has yet to take.
            for (int i = 0; i < count; i++) {
                var payload = new byte[32 * 1024];
                payload[0] = (byte) i;
                publisher.publish(MqttMessage.of(topic, payload, Qos.AT_LEAST_ONCE, false))
                        .get(5, SECONDS);
            }
            // Assert: every publication arrives, in order.
            await().atMost(Duration.ofSeconds(30)).until(() -> received.size() >= count);
            var expected = new ArrayList<Integer>();
            for (int i = 0; i < count; i++) {
                expected.add(i);
            }
            assertThat(received).isEqualTo(expected);
        }
    }

    @Test
    void a_broker_that_waits_for_room_goes_on_as_the_subscriber_takes_its_qos_0_messages()
            throws Exception {
        // Arrange: an inline handler that holds up the reading of the connection for a moment,
        // and QoS 0 publications of 32 KiB, two of which fill the link. No acknowledgement
        // follows them, so only the room that the subscriber makes has the broker go on once the
        // publisher has stopped.
        int count = 100;
        var topic = new Topic("room/test");
        var received = new CopyOnWriteArrayList<Integer>();
        try (MqttBroker broker = MqttBroker.builder().name("room-test").start();
                MqttClient sut = MqttClient.builder().server(broker.uri()).build();
                MqttClient publisher = MqttClient.builder().server(broker.uri()).build()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter("room/test"), Qos.AT_MOST_ONCE),
                    DeliveryOptions.inline(),
                    delivery -> {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                        received.add(delivery.message().payload()[0] & 0xFF);
                    }
            ).established().get(5, SECONDS);
            // Act
            for (int i = 0; i < count; i++) {
                var payload = new byte[32 * 1024];
                payload[0] = (byte) i;
                publisher.publish(MqttMessage.of(topic, payload, Qos.AT_MOST_ONCE, false))
                        .get(5, SECONDS);
            }
            // Assert: the last publication arrives, after every one before it.
            await().atMost(Duration.ofSeconds(10)).until(() -> received.size() >= count);
            var expected = new ArrayList<Integer>();
            for (int i = 0; i < count; i++) {
                expected.add(i);
            }
            assertThat(received).isEqualTo(expected);
        }
    }

    @Test
    void a_client_identifier_moves_between_tcp_and_memory_as_between_any_connections()
            throws Exception {
        // Arrange
        var tcpEnded = new LinkedBlockingQueue<Disconnection>();
        var memoryEnded = new LinkedBlockingQueue<Disconnection>();
        try (MqttBroker broker = MqttBroker.builder().name("takeover-test").start();
                MqttClient overTcp = MqttClient.builder()
                        .server(URI.create("mqtt://localhost:" + broker.port()))
                        .clientId("moving")
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .connectionListener(tcpEnded::add)
                        .build();
                MqttClient inMemory = MqttClient.builder()
                        .server(broker.uri())
                        .clientId("moving")
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .connectionListener(memoryEnded::add)
                        .build()) {
            overTcp.connect().get(5, SECONDS);
            // Act & Assert: the connection in memory takes the identifier over, and back.
            inMemory.connect().get(5, SECONDS);
            assertThat(tcpEnded.poll(5, SECONDS)).isNotNull();
            overTcp.connect().get(5, SECONDS);
            assertThat(memoryEnded.poll(5, SECONDS)).isNotNull();
        }
    }

    @Test
    void the_authenticator_hears_of_an_in_process_client_without_a_remote_address()
            throws Exception {
        // Arrange
        var addresses = new LinkedBlockingQueue<Optional<InetSocketAddress>>();
        try (MqttBroker broker = MqttBroker.builder()
                .name("authenticated-test")
                .authenticator(request -> {
                    addresses.add(request.remoteAddress());
                    return Authenticator.Decision.accept(Authenticator.Scope.all());
                })
                .start();
                MqttClient sut = MqttClient.builder().server(broker.uri()).build()) {
            // Act
            sut.connect().get(5, SECONDS);
            // Assert
            assertThat(addresses.poll(5, SECONDS)).isEmpty();
        }
    }

    @Test
    void a_packet_beyond_the_broker_s_limit_ends_the_connection_as_over_tcp() throws Exception {
        // Arrange: an MQTT 3.1.1 client, which cannot learn the broker's limit.
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttBroker broker = MqttBroker.builder()
                .name("packet-limit-test")
                .maxPacketSize(1024)
                .start();
                MqttClient sut = MqttClient.builder()
                        .server(broker.uri())
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .connectionListener(ended::add)
                        .build()) {
            sut.connect().get(5, SECONDS);
            // Act
            sut.publish(MqttMessage.of(
                    new Topic("limit/test"),
                    new byte[2048],
                    Qos.AT_MOST_ONCE,
                    false
            )).get(5, SECONDS);
            // Assert
            assertThat(ended.poll(5, SECONDS)).isNotNull();
        }
    }

    @Test
    void a_payload_larger_than_the_link_holds_passes_whole() throws Exception {
        // Arrange
        var topic = new Topic("large/test");
        var payload = new byte[512 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttBroker broker = MqttBroker.builder().name("large-payload-test").start();
                MqttClient sut = MqttClient.builder().server(broker.uri()).build()) {
            sut.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter("large/test"), Qos.AT_LEAST_ONCE),
                    received::add
            ).established().get(5, SECONDS);
            // Act
            sut.publish(MqttMessage.of(topic, payload, Qos.AT_LEAST_ONCE, false)).get(5, SECONDS);
            // Assert
            MqttDelivery delivery = received.poll(5, SECONDS);
            assertThat(delivery).isNotNull();
            assertThat(delivery.message().payload()).isEqualTo(payload);
        }
    }
}
