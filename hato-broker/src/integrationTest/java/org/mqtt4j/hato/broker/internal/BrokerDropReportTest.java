package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.broker.internal.RawSession.connected;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.ReasonCode.SUCCESS;

/// What the application hears of the QoS 1 and 2 deliveries that the broker's limits keep from
/// a session.
class BrokerDropReportTest {

    // What the listener has heard, in the order of its calls.
    private final List<Report> reports = new CopyOnWriteArrayList<>();

    @Test
    void the_application_hears_how_many_deliveries_a_session_lost_to_its_queue_limit()
            throws Exception {
        try (BrokerServer sut = broker(recording());
                RawSession subscriber = connected(sut.port(), "behind");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: one delivery takes the window, and two more fill the queue.
            Packet.Publish held = fill(subscriber, publisher);

            // Act: two more than the queue holds.
            publisher.publish("reported", "fourth", AT_LEAST_ONCE, 7);
            publisher.publish("reported", "fifth", AT_LEAST_ONCE, 7);

            // Assert: the session lost its two oldest queued deliveries, and goes on.
            await().untilAsserted(() -> assertThat(reported("behind")).isEqualTo(2));
            assertThat(reports).extracting(Report::clientId).containsOnly("behind");
            subscriber.send(new Packet.PubAck(held.packetId(), SUCCESS, EMPTY));
            assertThat(subscriber.expect(Packet.Publish.class).payload())
                    .isEqualTo("fourth".getBytes(UTF_8));
        }
    }

    @Test
    void qos_zero_deliveries_that_a_session_loses_are_not_reported() throws Exception {
        // Arrange: the listener is busy with a first report, so that the next one counts
        // everything that the session loses meanwhile.
        var release = new CountDownLatch(1);
        try (BrokerServer sut = broker(busyWithFirstCall(release));
                RawSession subscriber = connected(sut.port(), "behind");
                RawSession publisher = connected(sut.port(), "publisher")) {
            fill(subscriber, publisher);
            publisher.publish("reported", "fourth", AT_LEAST_ONCE, 7);
            await().untilAsserted(() -> assertThat(reports).hasSize(1));

            // Act: one QoS 0 delivery more than the queue holds, then an acknowledged one.
            for (String payload : List.of("a", "b", "c")) {
                publisher.publish("reported", payload, AT_MOST_ONCE, 0);
            }
            publisher.publish("reported", "fifth", AT_LEAST_ONCE, 7);
            release.countDown();

            // Assert: the second report counts the acknowledged delivery alone.
            await().untilAsserted(() -> assertThat(reports).containsExactly(
                    new Report("behind", 1),
                    new Report("behind", 1)
            ));
        }
    }

    @Test
    void the_application_hears_of_what_a_session_loses_while_it_is_away() throws Exception {
        try (BrokerServer sut = broker(recording());
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: a session that outlives its connection.
            try (RawSession previous = connected(sut.port(), "away", false, false)) {
                previous.subscribe("reported", AT_LEAST_ONCE);
                previous.disconnect();
            }

            // Act: one delivery more than the queue holds.
            for (String payload : List.of("first", "second", "third")) {
                publisher.publish("reported", payload, AT_LEAST_ONCE, 7);
            }

            // Assert
            await().untilAsserted(() -> assertThat(reported("away")).isEqualTo(1));
        }
    }

    @Test
    void the_application_hears_of_a_delivery_too_large_for_the_brokers_limits() throws Exception {
        try (BrokerServer sut = broker(recording());
                RawSession subscriber = connected(sut.port(), "behind");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange
            subscriber.subscribe("reported", AT_LEAST_ONCE);

            // Act: more bytes than the window ever takes.
            publisher.publish("reported", "x".repeat(2048), AT_LEAST_ONCE, 7);

            // Assert
            await().untilAsserted(() -> assertThat(reported("behind")).isEqualTo(1));
            subscriber.ping();
        }
    }

    @Test
    void the_application_hears_of_a_delivery_too_large_to_wait() throws Exception {
        BrokerServer.Builder limited = BrokerServer.builder()
                .maxInflight(1)
                .maxQueuedBytes(1024)
                .dropListener(recording());
        try (BrokerServer sut = limited.start(loopback());
                RawSession subscriber = connected(sut.port(), "behind");
                RawSession publisher = connected(sut.port(), "publisher")) {
            // Arrange: one delivery takes the window.
            subscriber.subscribe("reported", AT_LEAST_ONCE);
            publisher.publish("reported", "held", AT_LEAST_ONCE, 7);
            subscriber.expect(Packet.Publish.class);

            // Act: one that the window would take, but that is larger than the whole queue.
            publisher.publish("reported", "x".repeat(2048), AT_LEAST_ONCE, 7);

            // Assert
            await().untilAsserted(() -> assertThat(reported("behind")).isEqualTo(1));
        }
    }

    @Test
    void the_application_hears_of_retained_messages_that_a_session_could_not_wait_for()
            throws Exception {
        BrokerServer.Builder limited = BrokerServer.builder()
                .maxInflight(1)
                .maxRetained(2)
                .dropListener(recording());
        try (BrokerServer sut = limited.start(loopback());
                RawSession publisher = connected(sut.port(), "publisher");
                RawSession subscriber = connected(sut.port(), "behind")) {
            // Arrange: the broker retains two messages, and the first of them takes the window.
            publisher.publish(retained("kept/a"));
            publisher.publish(retained("kept/b"));
            subscriber.subscribe("kept/a", AT_LEAST_ONCE);
            subscriber.expect(Packet.Publish.class);

            // Act: three more subscriptions would leave five retained messages waiting.
            subscriber.subscribe("kept/+", AT_LEAST_ONCE);
            subscriber.subscribe("kept/#", AT_LEAST_ONCE);
            subscriber.subscribe("+/b", AT_LEAST_ONCE);

            // Assert: the session waits for no more than the broker retains.
            await().untilAsserted(() -> assertThat(reported("behind")).isEqualTo(3));
        }
    }

    @Test
    void drops_that_come_while_the_listener_is_busy_arrive_in_one_report() throws Exception {
        // Arrange: a listener that does not return from its first call until the test lets it.
        var release = new CountDownLatch(1);
        try (BrokerServer sut = broker(busyWithFirstCall(release));
                RawSession subscriber = connected(sut.port(), "behind");
                RawSession publisher = connected(sut.port(), "publisher")) {
            fill(subscriber, publisher);
            publisher.publish("reported", "fourth", AT_LEAST_ONCE, 7);
            await().untilAsserted(() -> assertThat(reports).hasSize(1));

            // Act: the broker goes on meanwhile, and the session loses three more deliveries.
            for (String payload : List.of("fifth", "sixth", "seventh")) {
                publisher.publish("reported", payload, AT_LEAST_ONCE, 7);
            }
            release.countDown();

            // Assert
            await().untilAsserted(() -> assertThat(reports).containsExactly(
                    new Report("behind", 1),
                    new Report("behind", 3)
            ));
        }
    }

    @Test
    void a_client_that_returns_with_new_sessions_while_the_listener_is_busy_gets_one_report()
            throws Exception {
        // Arrange: the listener is busy with the first drop of a client whose session has ended.
        var release = new CountDownLatch(1);
        try (BrokerServer sut = broker(busyWithFirstCall(release))) {
            loseOne(sut.port(), "returning");
            await().untilAsserted(() -> assertThat(reports).hasSize(1));

            // Act: the client comes back three times, each time with a new session that loses a
            // delivery and ends.
            for (int visit = 0; visit < 3; visit++) {
                loseOne(sut.port(), "returning");
            }
            release.countDown();

            // Assert: the three sessions are one client to the listener.
            await().untilAsserted(() -> assertThat(reports).containsExactly(
                    new Report("returning", 1),
                    new Report("returning", 3)
            ));
        }
    }

    @Test
    void drops_of_more_clients_than_the_broker_can_have_sessions_arrive_without_their_clients()
            throws Exception {
        // Arrange: a broker that can have two clients and one persistent session, so three
        // sessions at once, and a listener that is busy with a first drop.
        var release = new CountDownLatch(1);
        BrokerServer.Builder limited = settings(busyWithFirstCall(release))
                .maxClients(2)
                .maxSessions(1);
        try (BrokerServer sut = limited.start(loopback())) {
            loseOne(sut.port(), "first");
            await().untilAsserted(() -> assertThat(reports).hasSize(1));

            // Act: five more clients lose a delivery each, and then one of the first three of
            // them another.
            for (String clientId : List.of("a", "b", "c", "d", "e", "a")) {
                loseOne(sut.port(), clientId);
            }
            release.countDown();

            // Assert: three clients waited with their identifiers, and the listener hears of the
            // other two in one count.
            await().untilAsserted(() -> assertThat(reports).containsExactlyInAnyOrder(
                    new Report("first", 1),
                    new Report("a", 2),
                    new Report("b", 1),
                    new Report("c", 1),
                    new Report(null, 2)
            ));
        }
    }

    @Test
    void a_listener_that_fails_still_hears_of_later_drops() throws Exception {
        // Arrange: a listener whose first call fails.
        BrokerSettings.DropListener failing = recording(() -> {
            throw new IllegalStateException("injected listener failure");
        });
        try (BrokerServer sut = broker(failing);
                RawSession subscriber = connected(sut.port(), "behind");
                RawSession publisher = connected(sut.port(), "publisher")) {
            fill(subscriber, publisher);
            publisher.publish("reported", "fourth", AT_LEAST_ONCE, 7);
            await().untilAsserted(() -> assertThat(reports).hasSize(1));

            // Act
            publisher.publish("reported", "fifth", AT_LEAST_ONCE, 7);

            // Assert
            await().untilAsserted(() -> assertThat(reports).hasSize(2));
        }
    }

    @Test
    void a_closed_broker_reports_no_more_drops() throws Exception {
        // Arrange: the listener is busy with a first report, and the session has lost another
        // delivery since.
        var release = new CountDownLatch(1);
        BrokerServer sut = broker(busyWithFirstCall(release));
        Thread reporting;
        try (RawSession subscriber = connected(sut.port(), "behind");
                RawSession publisher = connected(sut.port(), "publisher")) {
            fill(subscriber, publisher);
            publisher.publish("reported", "fourth", AT_LEAST_ONCE, 7);
            await().untilAsserted(() -> assertThat(reports).hasSize(1));
            publisher.publish("reported", "fifth", AT_LEAST_ONCE, 7);
            reporting = reportingThread(sut.port());
        } finally {
            // Act: the broker closes while the listener is still busy, which then returns.
            sut.close();
            release.countDown();
        }

        // Assert: the thread that calls the listener ends without a second report.
        await().until(() -> !reporting.isAlive());
        assertThat(reports).containsExactly(new Report("behind", 1));
    }

    @Test
    void a_closed_broker_keeps_no_thread_for_reports_that_never_came() throws Exception {
        // Arrange
        Thread reporting;
        try (BrokerServer sut = broker(recording())) {
            reporting = reportingThread(sut.port());

            // Act: the broker closes as it leaves the block.
        }

        // Assert
        await().until(() -> !reporting.isAlive());
    }

    /// A listener that records its calls.
    private BrokerSettings.DropListener recording() {
        return recording(() -> { });
    }

    /// A listener that records its calls and does not return from the first one before
    /// `release` opens.
    private BrokerSettings.DropListener busyWithFirstCall(CountDownLatch release) {
        return recording(() -> awaitUninterruptibly(release));
    }

    /// A listener that records its calls, and runs `firstCall` in the first of them once it has
    /// recorded it.
    private BrokerSettings.DropListener recording(Runnable firstCall) {
        return new BrokerSettings.DropListener() {
            @Override
            public void dropped(String clientId, long count) {
                record(new Report(clientId, count));
            }

            @Override
            public void droppedUnattributed(long count) {
                record(new Report(null, count));
            }

            private void record(Report report) {
                boolean first = reports.isEmpty();
                reports.add(report);
                if (first) {
                    firstCall.run();
                }
            }
        };
    }

    /// The thread on which the broker on `port` calls its listener.
    private static Thread reportingThread(int port) {
        String name = "hato-broker-" + port + "-reports";
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().equals(name))
                .findFirst()
                .orElseThrow();
    }

    /// How many deliveries the listener has heard of for `clientId` so far.
    private long reported(String clientId) {
        return reports.stream()
                .filter(report -> clientId.equals(report.clientId()))
                .mapToLong(Report::count)
                .sum();
    }

    /// Connects `clientId` with a new session, which loses one delivery of its own publication,
    /// larger than its window ever takes, and ends with its connection.
    private static void loseOne(int port, String clientId) throws Exception {
        try (RawSession client = connected(port, clientId)) {
            client.subscribe("reported", AT_LEAST_ONCE);
            client.publish("reported", "x".repeat(2048), AT_LEAST_ONCE, 7);
            client.disconnect();
        }
    }

    /// Subscribes `subscriber` to `reported` at QoS 1 and has `publisher` fill its window with
    /// one delivery, which is returned unanswered, and its queue with two more.
    private static Packet.Publish fill(RawSession subscriber, RawSession publisher)
            throws Exception {
        subscriber.subscribe("reported", AT_LEAST_ONCE);
        publisher.publish("reported", "held", AT_LEAST_ONCE, 7);
        Packet.Publish held = subscriber.expect(Packet.Publish.class);
        publisher.publish("reported", "second", AT_LEAST_ONCE, 7);
        publisher.publish("reported", "third", AT_LEAST_ONCE, 7);
        return held;
    }

    /// A broker whose sessions take one unanswered delivery at a time and have two more wait
    /// for it, of each kind.
    private static BrokerServer broker(BrokerSettings.DropListener listener) throws IOException {
        return settings(listener).start(loopback());
    }

    /// The settings of such a broker, for a test that adds its own.
    private static BrokerServer.Builder settings(BrokerSettings.DropListener listener) {
        return BrokerServer.builder()
                .connectTimeout(Duration.ofSeconds(5))
                .sendBytes(64 * 1024)
                .maxInflight(1)
                .maxInflightBytes(1024)
                .maxQueued(2)
                .maxQueuedBytes(1024)
                .maxQueuedAtMostOnce(2)
                .maxQueuedAtMostOnceBytes(1024)
                .dropListener(listener);
    }

    private static InetSocketAddress loopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    /// A retained QoS 1 publication to `topic`.
    private static Packet.Publish retained(String topic) {
        return new Packet.Publish(
                new Topic(topic),
                topic.getBytes(UTF_8),
                AT_LEAST_ONCE,
                true,
                false,
                7,
                EMPTY
        );
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException _) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// One call of the listener; `clientId` is null in a call without a client.
    private record Report(@Nullable String clientId, long count) {
    }
}
