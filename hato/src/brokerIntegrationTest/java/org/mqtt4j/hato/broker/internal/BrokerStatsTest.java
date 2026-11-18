package org.mqtt4j.hato.broker.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.Authenticator;
import org.mqtt4j.hato.broker.BrokerStats;
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.internal.protocol.Packet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.Qos.EXACTLY_ONCE;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.RawSession.publication;
import static org.mqtt4j.hato.internal.protocol.Properties.EMPTY;

/// What the application reads of a broker's counts: the clients, sessions, retained messages,
/// and waiting deliveries that it holds, and the publications that it has received, delivered,
/// dropped, and refused.
class BrokerStatsTest {

    @Test
    void the_broker_counts_the_publications_that_it_receives_and_delivers_by_their_qos()
            throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession subscriber = connected(sut.port(), "subscriber");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a subscription that takes publications at QoS 1 at most.
            subscriber.subscribe("counted", AT_LEAST_ONCE);

            // Act: one publication at each QoS.
            publisher.publish("counted", "zero", AT_MOST_ONCE, 0);
            publisher.publish("counted", "one", AT_LEAST_ONCE, 1);
            publisher.publish("counted", "two", EXACTLY_ONCE, 2);
            for (int received = 0; received < 3; received++) {
                subscriber.expect(Packet.Publish.class);
            }

            // Assert
            await().untilAsserted(() -> {
                BrokerStats stats = sut.stats();
                assertThat(countsOf(stats::received)).containsExactly(1L, 1L, 1L);
                assertThat(countsOf(stats::delivered)).containsExactly(1L, 2L, 0L);
            });
        }
    }

    @Test
    void the_broker_counts_its_clients_sessions_and_retained_messages() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            BrokerStats connected;
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                // Arrange: a client whose session outlives its connection, and a retained
                // message.
                publisher.publish(new Packet.Publish(
                        new Topic("status"),
                        new byte[] {1},
                        AT_LEAST_ONCE,
                        true,
                        false,
                        1,
                        EMPTY
                ));

                // Act
                connected = sut.stats();
                away.disconnect();
            }

            // Assert
            assertThat(connected.connections()).isEqualTo(2);
            assertThat(connected.persistentSessions()).isEqualTo(1);
            assertThat(connected.retainedMessages()).isEqualTo(1);
            await().untilAsserted(() -> {
                BrokerStats stats = sut.stats();
                assertThat(stats.connections()).isEqualTo(1);
                assertThat(stats.persistentSessions()).isEqualTo(1);
            });
        }
    }

    @Test
    void the_broker_counts_the_deliveries_that_wait_and_those_that_its_limits_drop()
            throws Exception {
        try (MqttBroker sut = MqttBroker.builder().maxQueuedDeliveries(2).start();
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a client that is away, whose session can keep two deliveries.
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("waiting", AT_LEAST_ONCE);
                away.disconnect();
            }

            // Act: three deliveries for it.
            for (int packetId = 1; packetId <= 3; packetId++) {
                publisher.publish("waiting", "message", AT_LEAST_ONCE, packetId);
            }

            // Assert
            await().untilAsserted(() -> {
                BrokerStats stats = sut.stats();
                assertThat(stats.queuedDeliveries()).isEqualTo(2);
                assertThat(countsOf(stats::dropped)).containsExactly(0L, 1L, 0L);
            });
        }
    }

    @Test
    void the_broker_counts_the_publications_outside_the_scope_of_their_client() throws Exception {
        // Arrange: a client that may publish to its own topics alone.
        Authenticator scoped = _ -> Authenticator.Decision.accept(Authenticator.Scope.of(
                List.of(new TopicFilter("own/#")),
                List.of(new TopicFilter("#"))
        ));
        try (MqttBroker sut = MqttBroker.builder().authenticator(scoped).start();
                RawSession client = connected(sut.port(), "robot")) {
            // Act
            client.publish(publication("own/state", "fine", AT_LEAST_ONCE, 1));
            client.publish(publication("others/order", "stop", AT_LEAST_ONCE, 2));

            // Assert
            await().untilAsserted(() -> assertThat(sut.stats().refusedPublications())
                    .isEqualTo(1));
        }
    }

    @Test
    @SuppressWarnings("try") // The test closes the broker itself; closing it again does nothing.
    void a_broker_that_has_ended_holds_nothing_and_keeps_its_lifetime_counts() throws Exception {
        try (MqttBroker sut = MqttBroker.start();
                RawSession client = connected(sut.port(), "robot")) {
            // Arrange
            client.publish("counted", "once", AT_MOST_ONCE, 0);
            await().until(() -> sut.stats().received(AT_MOST_ONCE) == 1);

            // Act
            sut.close();

            // Assert
            BrokerStats stats = sut.stats();
            assertThat(stats.connections()).isZero();
            assertThat(stats.received(AT_MOST_ONCE)).isEqualTo(1);
        }
    }

    @Test
    void the_counts_are_read_while_the_broker_holds_its_locks() throws Exception {
        // Arrange: room in the memory for deliveries for one delivery, which a session that is
        // away holds. A retained publication then needs that room, which the session hands over
        // under the topic lock and its inbox; there the broker waits until another thread has
        // read the counts, for two seconds at most.
        long room = RawSession.deliverySize(publication("kept", "0001", AT_LEAST_ONCE, 7));
        var server = new AtomicReference<BrokerServer>();
        var read = new CompletableFuture<BrokerServer.Counts>();
        BrokerServer.Builder holding = BrokerServer.builder()
                .maxDeliveryMemory(room)
                .deliveryMemoryUnit(0)
                .beforeHandOver(_ -> {
                    try {
                        read.complete(CompletableFuture.supplyAsync(server.get()::counts)
                                .get(2, SECONDS));
                    } catch (Exception failure) {
                        read.completeExceptionally(failure);
                    }
                });
        try (BrokerServer sut = holding.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher")) {
            server.set(sut);
            try (RawSession away = connected(sut.port(), "away", false, false)) {
                away.subscribe("kept", AT_LEAST_ONCE);
                away.disconnect();
            }
            publisher.publish("kept", "0001", AT_LEAST_ONCE, 7);

            // Act
            publisher.publish(new Packet.Publish(
                    new Topic("kept"),
                    "0002".getBytes(UTF_8),
                    AT_LEAST_ONCE,
                    true,
                    false,
                    7,
                    EMPTY
            ));

            // Assert: the counts came while the broker held its locks, with the retained
            // message and the delivery that waited.
            assertThat(read).succeedsWithin(Duration.ofSeconds(5)).satisfies(counts -> {
                assertThat(counts.retainedMessages()).isEqualTo(1);
                assertThat(counts.queuedDeliveries()).isEqualTo(1);
            });
        }
    }

    /// The counts of QoS 0, 1, and 2 that `count` reads.
    private static List<Long> countsOf(ToLongFunction<Qos> count) {
        return List.of(
                count.applyAsLong(AT_MOST_ONCE),
                count.applyAsLong(AT_LEAST_ONCE),
                count.applyAsLong(EXACTLY_ONCE)
        );
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }
}
