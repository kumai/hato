package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.Disconnection;

import static java.util.Objects.requireNonNull;
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
            var sut = new ListenerLane(URI.create("mqtt://localhost"), "listener-test", listener);
            ListenerLane.Reservation reservation = requireNonNull(sut.tryReserveConnection());
            var ending = Disconnection.of(null, true);

            // Act
            reservation.connected(ConnAck.of(false));
            reservation.disconnected(ending);

            // Assert: the second callback still runs and both reservations return.
            assertThat(endings.poll(5, SECONDS)).isSameAs(ending);
            await().pollInterval(Duration.ofMillis(1))
                    .untilAsserted(() -> assertThat(sut.usage()).isZero());
            assertThat(calls.get()).isEqualTo(1);
            for (int i = 0; i < 32; i++) {
                assertThat(sut.tryReserveConnection()).isNotNull();
            }
            assertThat(sut.tryReserveConnection()).isNull();
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
        var sut = new ListenerLane(URI.create("mqtt://localhost"), "listener-test", listener);
        ListenerLane.Reservation first = requireNonNull(sut.tryReserveConnection());
        ListenerLane.Reservation second = requireNonNull(sut.tryReserveConnection());
        first.connected(ConnAck.of(false));
        assertThat(entered.await(5, SECONDS)).isTrue();
        first.disconnected(Disconnection.of(null, true));
        second.connected(ConnAck.of(true));

        // Act: returning the first callback runs the queued ones on the same thread.
        release.countDown();

        // Assert: only the second attempt's unsent disconnection stays reserved.
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        await().pollInterval(Duration.ofMillis(1))
                .untilAsserted(() -> assertThat(sut.usage()).isEqualTo(1));
    }
}
