package org.mqtt4j.hato.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;
import org.mqtt4j.hato.broker.internal.common.value.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.broker.internal.RawSession.acceptedV5;
import static org.mqtt4j.hato.broker.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_LEAST_ONCE;
import static org.mqtt4j.hato.broker.internal.common.value.Qos.AT_MOST_ONCE;

/// How the broker holds back the Will of an MQTT 5.0 connection for its Will Delay Interval, on
/// a clock that the test advances.
///
/// A watcher receives the Wills of the clients that leave. The delay counts from when the broker
/// detaches the session from the connection that ended, and the broker sets the timer of the
/// Will as it does, so a test waits for that timer before it advances the clock.
class BrokerWillDelayTest {

    private final BrokerDeadlineTest.ManualTimer timer = new BrokerDeadlineTest.ManualTimer();

    @Test
    void a_will_with_a_delay_goes_out_once_the_delay_has_passed() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            leave(accepted(sut, connect("delayed", true, 60, delayed("delayed", 10)), false));
            timer.clock.advance(Duration.ofSeconds(10).minusNanos(1));
            expectNothing(watcher);

            // Act
            timer.clock.advance(Duration.ofNanos(1));

            // Assert
            expectWill(watcher, "delayed");
        }
    }

    @Test
    void a_disconnect_with_will_message_leaves_the_will_to_its_delay() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            RawSession client = accepted(
                    sut,
                    connect("asking", true, 60, delayed("asking", 10)),
                    false
            );

            // Act
            client.send(new Packet.Disconnect(ReasonCode.DISCONNECT_WITH_WILL_MESSAGE, EMPTY));
            leave(client);

            // Assert
            timer.clock.advance(Duration.ofSeconds(10).minusNanos(1));
            expectNothing(watcher);
            timer.clock.advance(Duration.ofNanos(1));
            expectWill(watcher, "asking");
        }
    }

    @Test
    void a_normal_disconnect_discards_a_delayed_will() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            RawSession client = accepted(
                    sut,
                    connect("leaving", true, 60, delayed("leaving", 10)),
                    false
            );

            // Act
            client.disconnect();
            client.close();

            // Assert: the broker ended the connection while it handled DISCONNECT, which the
            // watcher's PINGREQ waits for.
            watcher.ping();
            timer.clock.advance(Duration.ofSeconds(10));
            expectNothing(watcher);
        }
    }

    @Test
    void a_connection_that_continues_the_session_within_the_delay_cancels_the_will()
            throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            leave(accepted(sut, connect("returning", true, 60, delayed("returning", 10)), false));
            timer.clock.advance(Duration.ofSeconds(10).minusNanos(1));

            // Act
            accepted(sut, connect("returning", false, 60, null), true).close();

            // Assert: nothing even once the delay has long passed.
            timer.clock.advance(Duration.ofSeconds(30));
            expectNothing(watcher);
        }
    }

    @Test
    void a_takeover_that_continues_the_session_cancels_a_delayed_will() throws Exception {
        try (BrokerServer sut = broker();
                RawSession watcher = watching(sut);
                RawSession previous = accepted(
                        sut,
                        connect("moving", true, 60, delayed("moving", 10)),
                        false
                )) {
            // Act
            accepted(sut, connect("moving", false, 60, null), true).close();

            // Assert: nothing even once the delay has long passed.
            assertThat(previous.expect(Packet.Disconnect.class).reasonCode())
                    .isEqualTo(ReasonCode.SESSION_TAKEN_OVER);
            timer.clock.advance(Duration.ofSeconds(30));
            expectNothing(watcher);
        }
    }

    @Test
    void an_old_connection_that_ends_after_its_session_was_continued_publishes_no_delayed_will()
            throws Exception {
        // The old connection's writer, which ends the connection, stops at the hook.
        var holding = new AtomicBoolean();
        var heldWriter = new CompletableFuture<Thread>();
        var release = new CountDownLatch(1);
        try (BrokerServer sut = BrokerServer.builder()
                .connectTimeout(Duration.ofSeconds(5))
                .scheduler(timer)
                .beforeDeliveryWrite(() -> {
                    if (holding.get() && heldWriter.complete(Thread.currentThread())) {
                        BrokerSessionRaceTest.awaitRelease(release);
                    }
                })
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                RawSession watcher = watching(sut);
                RawSession publisher = RawSession.connectedV5(sut.port(), "publisher");
                RawSession previous = accepted(
                        sut,
                        connect("racing", true, 60, delayed("racing", 10)),
                        false
                )) {
            // Arrange: the broker ends the old connection, whose writer is held, and a delivery
            // finds it ended and detaches the session before the connection's end comes.
            previous.subscribe("race/#", AT_LEAST_ONCE);
            holding.set(true);
            publisher.publish("race/held", "held", AT_MOST_ONCE, 0);
            Thread writer = heldWriter.get(5, SECONDS);
            sut.disconnect("racing");
            publisher.publish("race/queued", "queued", AT_LEAST_ONCE, 1);

            // Act: a new connection continues the session, and then the old connection ends.
            try (RawSession resumed = accepted(sut, connect("racing", false, 60, null), true)) {
                assertThat(resumed.expect(Packet.Publish.class).topic())
                        .isEqualTo(new Topic("race/queued"));
                release.countDown();
                assertThat(writer.join(Duration.ofSeconds(5))).isTrue();

                // Assert: nothing even once the delay has long passed.
                timer.clock.advance(Duration.ofSeconds(30));
                expectNothing(watcher);
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_connection_after_the_delay_does_not_cancel_a_will_whose_timer_is_late()
            throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange: the timer comes a second after the delay has passed.
            timer.lateness = Duration.ofSeconds(1);
            leave(accepted(sut, connect("late", true, 60, delayed("late", 10)), false));
            timer.clock.advance(Duration.ofSeconds(10));

            // Act
            accepted(sut, connect("late", false, 60, null), true).close();

            // Assert
            expectWill(watcher, "late");
        }
    }

    @Test
    void a_connection_that_starts_a_new_session_publishes_the_waiting_will_at_once()
            throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            leave(accepted(sut, connect("restarted", true, 60, delayed("restarted", 60)), false));

            // Act
            accepted(sut, connect("restarted", true, 60, null), false).close();

            // Assert
            expectWill(watcher, "restarted");
        }
    }

    @Test
    void a_takeover_that_starts_a_new_session_publishes_a_delayed_will_at_once() throws Exception {
        try (BrokerServer sut = broker();
                RawSession watcher = watching(sut);
                RawSession previous = accepted(
                        sut,
                        connect("replaced", true, 60, delayed("replaced", 60)),
                        false
                )) {
            // Act
            accepted(sut, connect("replaced", true, 60, null), false).close();

            // Assert
            assertThat(previous.expect(Packet.Disconnect.class).reasonCode())
                    .isEqualTo(ReasonCode.SESSION_TAKEN_OVER);
            expectWill(watcher, "replaced");
        }
    }

    @Test
    void a_session_without_expiry_publishes_a_delayed_will_as_its_connection_ends()
            throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            RawSession client = accepted(
                    sut,
                    connect("brief", true, 0, delayed("brief", 60)),
                    false
            );

            // Act
            client.close();

            // Assert
            expectWill(watcher, "brief");
        }
    }

    @Test
    void a_session_that_expires_within_the_delay_publishes_the_will_as_it_ends() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            leave(accepted(sut, connect("expiring", true, 5, delayed("expiring", 60)), false));
            timer.clock.advance(Duration.ofSeconds(5).minusNanos(1));
            expectNothing(watcher);

            // Act
            timer.clock.advance(Duration.ofNanos(1));

            // Assert: the session ended with it.
            expectWill(watcher, "expiring");
            accepted(sut, connect("expiring", false, 5, null), false).close();
        }
    }

    @Test
    void a_session_that_overflows_while_away_publishes_its_waiting_will() throws Exception {
        try (BrokerServer sut = broker(1);
                RawSession watcher = watching(sut);
                RawSession publisher = RawSession.connectedV5(sut.port(), "publisher")) {
            // Arrange: a session that queues a single delivery while away.
            RawSession client = accepted(
                    sut,
                    connect("behind", true, 60, delayed("behind", 60)),
                    false
            );
            client.subscribe("queued/#", AT_LEAST_ONCE);
            leave(client);
            publisher.publish("queued/first", "first", AT_LEAST_ONCE, 1);

            // Act
            publisher.publish("queued/second", "second", AT_LEAST_ONCE, 2);

            // Assert
            expectWill(watcher, "behind");
        }
    }

    @Test
    void a_will_that_went_out_after_its_delay_does_not_go_out_again_as_the_session_ends()
            throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            leave(accepted(sut, connect("once", true, 10, delayed("once", 5)), false));
            timer.clock.advance(Duration.ofSeconds(5));
            expectWill(watcher, "once");

            // Act: the session expires, and a connection finds it gone.
            timer.clock.advance(Duration.ofSeconds(5));
            accepted(sut, connect("once", false, 10, null), false).close();

            // Assert
            expectNothing(watcher);
        }
    }

    @Test
    void a_timer_that_comes_after_its_cancellation_publishes_nothing() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange: the timer was dispatched before the connection cancelled the Will.
            leave(accepted(sut, connect("stale", true, 60, delayed("stale", 10)), false));
            Runnable stale = timer.actions.getLast();
            timer.clock.advance(Duration.ofSeconds(10).minusNanos(1));
            accepted(sut, connect("stale", false, 60, null), true).close();
            timer.clock.advance(Duration.ofNanos(1));

            // Act
            stale.run();

            // Assert
            expectNothing(watcher);
        }
    }

    @Test
    void an_expired_session_that_a_connection_finds_before_a_late_timer_publishes_its_will_once()
            throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange: the timer comes a second after the session's expiry.
            timer.lateness = Duration.ofSeconds(1);
            leave(accepted(sut, connect("lapsed", true, 5, delayed("lapsed", 60)), false));
            Runnable late = timer.actions.getLast();
            timer.clock.advance(Duration.ofSeconds(5));

            // Act: the connection discards the session and cancels the timer, which comes anyway.
            accepted(sut, connect("lapsed", false, 5, null), false).close();
            timer.clock.advance(Duration.ofSeconds(1));
            late.run();

            // Assert
            expectWill(watcher, "lapsed");
            expectNothing(watcher);
        }
    }

    @Test
    void a_delayed_will_counts_its_message_expiry_from_its_publication() throws Exception {
        var lifetime = new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, 30);
        var properties = new Properties(List.of(
                new Property.FourByteValue(PropertyId.WILL_DELAY_INTERVAL, 10),
                lifetime
        ));
        try (BrokerServer sut = broker();
                RawSession watcher = RawSession.connectedV5(sut.port(), "watcher")) {
            // Arrange
            watcher.subscribe("left/#", AT_MOST_ONCE);
            leave(accepted(sut, connect("timed", true, 60, will("timed", properties)), false));

            // Act
            timer.clock.advance(Duration.ofSeconds(10));

            // Assert: the whole lifetime is left, and the delay stays with the Will.
            assertThat(watcher.expect(Packet.Publish.class).properties())
                    .isEqualTo(new Properties(List.of(lifetime)));
        }
    }

    @Test
    void a_will_whose_timer_cannot_be_set_goes_out_at_once() throws Exception {
        try (BrokerServer sut = broker(); RawSession watcher = watching(sut)) {
            // Arrange
            RawSession client = accepted(
                    sut,
                    connect("untimed", true, 60, delayed("untimed", 60)),
                    false
            );
            timer.failNextSchedule.set(true);

            // Act
            client.close();

            // Assert
            expectWill(watcher, "untimed");
        }
    }

    @Test
    void closing_the_broker_cancels_the_timer_of_a_waiting_will() throws Exception {
        // Arrange
        BrokerServer sut = broker();
        leave(accepted(sut, connect("waiting", true, 60, delayed("waiting", 10)), false));

        // Act
        sut.close();

        // Assert
        assertThat(timer.clock.pendingTaskCount()).isZero();
    }

    private BrokerServer broker() throws IOException {
        return broker(16);
    }

    private BrokerServer broker(int maxQueued) throws IOException {
        return BrokerServer.builder()
                .connectTimeout(Duration.ofSeconds(5))
                .scheduler(timer)
                .maxQueued(maxQueued)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    /// A connection that receives the Wills of the clients that leave.
    private static RawSession watching(BrokerServer broker) throws Exception {
        RawSession watcher = RawSession.connected(broker.port(), "watcher");
        watcher.subscribe("left/#", AT_MOST_ONCE);
        return watcher;
    }

    /// An MQTT 5.0 connection that `request` opens, which the broker accepted with or without
    /// the session present.
    private static RawSession accepted(
            BrokerServer broker,
            Packet.Connect request,
            boolean present
    ) throws Exception {
        RawSession client = RawSession.open(broker.port(), MqttVersion.V5);
        client.send(request);
        assertThat(client.expect(Packet.ConnAck.class)).isEqualTo(acceptedV5(present));
        return client;
    }

    /// Ends the connection of `client` from the client's side and waits for the timer of its
    /// Will, which the broker sets as it detaches the session. No other timer is pending then,
    /// since the connections here have no keep-alive.
    private void leave(RawSession client) throws IOException {
        client.close();
        timer.awaitPendingTasks(1);
    }

    /// Checks that `watcher` has received nothing: the broker answers its PINGREQ after anything
    /// that it routed before.
    private static void expectNothing(RawSession watcher) throws Exception {
        watcher.ping();
    }

    private static void expectWill(RawSession watcher, String clientId) throws Exception {
        assertThat(watcher.expect(Packet.Publish.class).topic())
                .isEqualTo(new Topic("left/" + clientId));
    }

    /// An MQTT 5.0 CONNECT without keep-alive, with a Session Expiry Interval of `expiry`
    /// seconds and `will`, if any.
    private static Packet.Connect connect(
            String clientId,
            boolean cleanStart,
            long expiry,
            Packet.@Nullable Will will
    ) {
        var interval = new Property.FourByteValue(PropertyId.SESSION_EXPIRY_INTERVAL, expiry);
        return new Packet.Connect(
                clientId,
                MqttVersion.V5,
                cleanStart,
                0,
                will,
                null,
                null,
                new Properties(List.of(interval))
        );
    }

    /// The Will of `clientId` with a Will Delay Interval of `delay` seconds.
    private static Packet.Will delayed(String clientId, long delay) {
        return will(clientId, new Properties(List.of(
                new Property.FourByteValue(PropertyId.WILL_DELAY_INTERVAL, delay)
        )));
    }

    /// A QoS 0 Will of `clientId` on `left/` followed by the identifier.
    private static Packet.Will will(String clientId, Properties properties) {
        return new Packet.Will(
                new Topic("left/" + clientId),
                "gone".getBytes(UTF_8),
                AT_MOST_ONCE,
                false,
                properties
        );
    }
}
