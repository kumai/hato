package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.LongStream;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.Disconnection;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ListenerLaneTest {

    @Test
    void a_throwing_listener_and_logging_backend_cannot_strand_later_notifications()
            throws Exception {
        // Arrange
        var endings = new LinkedBlockingQueue<Disconnection>();
        var calls = new AtomicInteger();
        var listener = new ConnectionListener() {
            @Override
            public void onConnected(ConnAck ack) {
                throw new IllegalStateException("Listener failed");
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                endings.add(disconnection);
            }
        };
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                calls.incrementAndGet();
                throw new IllegalStateException("Logger failed");
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        Logger logger = Logger.getLogger(ListenerLane.class.getName());
        Level previousLevel = logger.getLevel();
        boolean previousParents = logger.getUseParentHandlers();
        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);
        logger.addHandler(handler);
        try {
            var sut = new ListenerLane(
                    URI.create("mqtt://localhost"),
                    "listener-test",
                    listener,
                    () -> { }
            );
            var ending = Disconnection.of(null, true, 1);

            // Act
            sut.connected(ConnAck.of(false, 1));
            sut.disconnected(ending);

            // Assert: the second callback still runs, and nothing remains pending.
            assertThat(endings.poll(5, SECONDS)).isSameAs(ending);
            await().pollInterval(Duration.ofMillis(1))
                    .untilAsserted(() -> assertThat(sut.usage()).isZero());
            assertThat(calls.get()).isEqualTo(1);
            assertThat(sut.dropped()).isZero();
        } finally {
            logger.removeHandler(handler);
            logger.setLevel(previousLevel);
            logger.setUseParentHandlers(previousParents);
        }
    }

    @Test
    void queued_callbacks_do_not_inherit_interrupt_state_from_the_previous_callback()
            throws Exception {
        // Arrange: each earlier callback leaves the lane thread interrupted, one returning
        // normally and one throwing, while the next notification is already queued.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var interruptStates = new LinkedBlockingQueue<Boolean>();
        var listener = new ConnectionListener() {
            @Override
            public void onConnected(ConnAck ack) {
                interruptStates.add(Thread.currentThread().isInterrupted());
                if (entered.getCount() > 0) {
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException failure) {
                        throw new AssertionError(failure);
                    }
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                interruptStates.add(Thread.currentThread().isInterrupted());
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Listener failed");
            }
        };
        var sut = new ListenerLane(
                URI.create("mqtt://localhost"),
                "listener-test",
                listener,
                () -> { }
        );
        sut.connected(ConnAck.of(false, 1));
        assertThat(entered.await(5, SECONDS)).isTrue();
        sut.disconnected(Disconnection.of(null, true, 1));
        sut.connected(ConnAck.of(true, 2));

        // Act: returning the first callback runs the queued ones on the same thread.
        release.countDown();

        // Assert
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        await().pollInterval(Duration.ofMillis(1))
                .untilAsserted(() -> assertThat(sut.usage()).isZero());
    }

    @Test
    void a_full_queue_drops_its_oldest_undelivered_notifications_and_counts_them()
            throws Exception {
        // Arrange: the first callback blocks while 70 more notifications arrive.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var attempts = new LinkedBlockingQueue<Long>();
        var listener = new ConnectionListener() {
            @Override
            public void onConnected(ConnAck ack) {
                attempts.add(ack.connectionAttempt());
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException failure) {
                    throw new AssertionError(failure);
                }
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
            }

            @Override
            public void onConnectFailed(Disconnection failure) {
                attempts.add(failure.connectionAttempt());
            }
        };
        var sut = new ListenerLane(
                URI.create("mqtt://localhost"),
                "listener-test",
                listener,
                () -> { }
        );
        sut.connected(ConnAck.of(false, 1));
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act
        for (long attempt = 2; attempt <= 71; attempt++) {
            var refusal = new IllegalStateException("refused");
            sut.connectFailed(Disconnection.of(refusal, false, attempt));
        }

        // Assert: 64 notifications wait behind the running one; the 6 oldest were dropped.
        assertThat(sut.usage()).isEqualTo(65);
        assertThat(sut.dropped()).isEqualTo(6);
        release.countDown();
        var delivered = new ArrayList<Long>();
        for (int i = 0; i < 65; i++) {
            delivered.add(attempts.poll(5, SECONDS));
        }
        var expected = new ArrayList<>(List.of(1L));
        LongStream.rangeClosed(8, 71).forEach(expected::add);
        assertThat(delivered).containsExactlyElementsOf(expected);
        await().pollInterval(Duration.ofMillis(1))
                .untilAsserted(() -> assertThat(sut.usage()).isZero());
        assertThat(attempts).isEmpty();
    }
}
