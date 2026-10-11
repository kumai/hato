package org.mqtt4j.mqtj.broker.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.mqtj.broker.internal.common.concurrent.Timer;
import org.mqtt4j.mqtj.broker.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.spi.TimerScheduler;
import org.mqtt4j.mqtj.testkit.ManualTimerScheduler;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.mqtj.broker.internal.RawSession.connect;
import static org.mqtt4j.mqtj.broker.internal.RawSession.connected;
import static org.mqtt4j.mqtj.broker.internal.RawSession.publication;
import static org.mqtt4j.mqtj.broker.internal.common.value.Qos.AT_MOST_ONCE;

class BrokerDeadlineTest {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration KEEP_ALIVE_TIMEOUT = Duration.ofMillis(1500);

    @Test
    void connect_is_accepted_just_before_the_deadline() throws Exception {
        var timer = new ManualTimer();
        try (BrokerServer sut = start(timer); var client = RawSession.open(sut.port())) {
            timer.awaitPendingTasks(1);
            timer.clock.advance(CONNECT_TIMEOUT.minusNanos(1));

            client.send(connect("just-in-time", true, 0));

            client.expect(Packet.ConnAck.class);
            client.ping();
            assertThat(timer.clock.pendingTaskCount()).isZero();
        }
    }

    @Test
    void an_unconnected_peer_closes_exactly_at_the_connect_deadline() throws Exception {
        var timer = new ManualTimer();
        try (BrokerServer sut = start(timer); var client = RawSession.open(sut.port())) {
            timer.awaitPendingTasks(1);
            timer.clock.advance(CONNECT_TIMEOUT.minusNanos(1));
            assertThat(timer.clock.pendingTaskCount()).isEqualTo(1);

            timer.clock.advance(Duration.ofNanos(1));

            client.expectClosed();
            assertThat(timer.clock.pendingTaskCount()).isZero();
        }
    }

    @Test
    void complete_packets_renew_keep_alive_until_the_client_becomes_silent() throws Exception {
        var timer = new ManualTimer();
        try (BrokerServer sut = start(timer); var client = RawSession.open(sut.port())) {
            client.send(connect("renewed-keep-alive", true, 1));
            client.expect(Packet.ConnAck.class);

            // PINGRESP is the I/O barrier: the complete PINGREQ was received at this time.
            for (int i = 0; i < 3; i++) {
                timer.clock.advance(KEEP_ALIVE_TIMEOUT.minusNanos(1));
                client.ping();
            }
            timer.clock.advance(KEEP_ALIVE_TIMEOUT.minusNanos(1));
            assertThat(timer.clock.pendingTaskCount()).isEqualTo(1);
            timer.clock.advance(Duration.ofNanos(1));

            client.expectClosed();
            assertThat(timer.clock.pendingTaskCount()).isZero();
        }
    }

    @Test
    void keep_alive_zero_cancels_the_connect_deadline() throws Exception {
        var timer = new ManualTimer();
        try (BrokerServer sut = start(timer); var client = RawSession.open(sut.port())) {
            timer.awaitPendingTasks(1);
            client.send(connect("no-keep-alive", true, 0));
            client.expect(Packet.ConnAck.class);
            assertThat(timer.clock.pendingTaskCount()).isZero();

            timer.clock.advance(CONNECT_TIMEOUT.multipliedBy(10));

            client.ping();
            assertThat(timer.clock.pendingTaskCount()).isZero();
        }
    }

    @Test
    void an_already_dispatched_connect_timer_cannot_replace_the_keep_alive_timer()
            throws Exception {
        var timer = new ManualTimer();
        try (BrokerServer sut = start(timer); var client = RawSession.open(sut.port())) {
            timer.awaitPendingTasks(1);
            Runnable stale = timer.actions.getFirst();
            timer.clock.advance(CONNECT_TIMEOUT.minusSeconds(1));
            client.send(connect("late-handshake", true, 1));
            client.expect(Packet.ConnAck.class);

            // Cancellation cannot retract a callback already dispatched by another scheduler.
            timer.clock.advance(Duration.ofSeconds(1));
            stale.run();

            assertThat(timer.clock.pendingTaskCount()).isEqualTo(1);
            client.ping();
            timer.clock.advance(KEEP_ALIVE_TIMEOUT);
            client.expectClosed();
            assertThat(timer.clock.pendingTaskCount()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void a_taken_over_connections_dispatched_timer_does_not_affect_its_replacement(
            boolean cleanSession
    ) throws Exception {
        var timer = new ManualTimer();
        try (BrokerServer sut = start(timer); var first = RawSession.open(sut.port())) {
            first.send(connect("same-id", cleanSession, 1));
            first.expect(Packet.ConnAck.class);
            Runnable stale = timer.actions.getLast();
            timer.clock.advance(Duration.ofSeconds(1));

            try (var replacement = connected(
                    sut.port(), "same-id", cleanSession, !cleanSession
            )) {
                first.expectClosed();
                assertThat(timer.clock.pendingTaskCount()).isZero();
                timer.clock.advance(Duration.ofMillis(500));
                stale.run();

                replacement.ping();
                assertThat(timer.clock.pendingTaskCount()).isZero();
            }
        }
    }

    @Test
    void expiry_preserves_the_session_and_its_old_timer_cannot_close_a_new_connection()
            throws Exception {
        var timer = new ManualTimer();
        try (BrokerServer sut = start(timer);
                var first = RawSession.open(sut.port());
                var publisher = connected(sut.port(), "publisher")) {
            first.send(connect("expired-session", false, 1));
            first.expect(Packet.ConnAck.class);
            first.subscribe("kept/#", AT_MOST_ONCE);
            Runnable stale = timer.actions.getLast();

            timer.clock.advance(KEEP_ALIVE_TIMEOUT);
            first.expectClosed();
            try (var replacement = connected(sut.port(), "expired-session", false, true)) {
                stale.run();
                publisher.publish("kept/topic", "after expiry", AT_MOST_ONCE, 0);
                assertThat(replacement.expect(Packet.Publish.class)).isEqualTo(publication(
                        "kept/topic", "after expiry", AT_MOST_ONCE, 0
                ));
                replacement.ping();
                assertThat(timer.clock.pendingTaskCount()).isZero();
            }
        }
    }

    @Test
    void connect_decoded_before_its_deadline_cannot_take_over_after_expiry() throws Exception {
        var timer = new ManualTimer();
        var armed = new AtomicBoolean();
        var heldReader = new CompletableFuture<Thread>();
        var release = new CountDownLatch(1);
        try (BrokerServer sut = start(timer, packet -> {
            if (packet instanceof Packet.Connect && armed.compareAndSet(true, false)) {
                heldReader.complete(Thread.currentThread());
                try {
                    assertThat(release.await(5, SECONDS)).isTrue();
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(failure);
                }
            }
        }); var existing = connected(sut.port(), "same-id", false, false)) {
            existing.subscribe("kept/#", AT_MOST_ONCE);
            armed.set(true);
            try (var expired = RawSession.open(sut.port())) {
                expired.send(connect("same-id", false, 0));
                Thread reader = heldReader.get(5, SECONDS);
                timer.clock.advance(CONNECT_TIMEOUT);
                expired.expectClosed();
                release.countDown();
                assertThat(reader.join(Duration.ofSeconds(5))).isTrue();

                existing.ping();
                existing.publish("kept/topic", "still attached", AT_MOST_ONCE, 0);
                assertThat(existing.expect(Packet.Publish.class)).isEqualTo(publication(
                        "kept/topic", "still attached", AT_MOST_ONCE, 0
                ));
                assertThat(timer.clock.pendingTaskCount()).isZero();
            }
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest
    @EnumSource(TimerFailure.class)
    void failed_keep_alive_startup_does_not_take_over_an_existing_session(TimerFailure failure)
            throws Exception {
        var timer = new ManualTimer();
        try (BrokerServer sut = start(timer);
                var existing = connected(sut.port(), "same-id", false, false);
                var failed = RawSession.open(sut.port())) {
            existing.subscribe("kept/#", AT_MOST_ONCE);
            timer.awaitPendingTasks(1);
            switch (failure) {
                case NANO_TIME -> timer.failNextClockRead.set(true);
                case SCHEDULE -> timer.failNextSchedule.set(true);
            }
            failed.send(connect("same-id", false, 1));
            failed.expectClosed();

            existing.ping();
            existing.publish("kept/topic", "still attached", AT_MOST_ONCE, 0);
            assertThat(existing.expect(Packet.Publish.class)).isEqualTo(publication(
                    "kept/topic", "still attached", AT_MOST_ONCE, 0
            ));
            assertThat(timer.clock.pendingTaskCount()).isZero();
        }
    }

    @Test
    void closing_the_broker_discards_all_deadlines_and_ignores_dispatched_callbacks()
            throws Exception {
        var timer = new ManualTimer();
        BrokerServer sut = start(timer);
        try (var connected = RawSession.open(sut.port());
                var unconnected = RawSession.open(sut.port())) {
            connected.send(connect("closing", true, 1));
            connected.expect(Packet.ConnAck.class);
            timer.awaitPendingTasks(2);

            sut.close();
            timer.clock.advance(CONNECT_TIMEOUT);
            timer.actions.forEach(Runnable::run);

            connected.expectClosed();
            unconnected.expectClosed();
            assertThat(timer.clock.pendingTaskCount()).isZero();
        } finally {
            sut.close();
        }
    }

    @Test
    void timer_cancellation_failure_does_not_prevent_closing_all_connections() throws Exception {
        var timer = new ManualTimer();
        BrokerServer sut = start(timer);
        try (var first = RawSession.open(sut.port());
                var second = RawSession.open(sut.port())) {
            timer.awaitPendingTasks(2);
            timer.cancellationFails = true;

            sut.close();

            first.expectClosed();
            second.expectClosed();
            assertThat(timer.clock.pendingTaskCount()).isZero();
        } finally {
            sut.close();
        }
    }

    @Test
    void a_timer_handle_returned_after_inline_expiry_is_cancelled() throws Exception {
        var timer = new ManualTimer();
        var reads = new AtomicInteger();
        var cancelled = new AtomicBoolean();
        var inline = new Timer() {
            @Override
            public long nanoTime() {
                if (reads.getAndIncrement() == 1) {
                    timer.clock.advance(CONNECT_TIMEOUT);
                }
                return timer.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                Cancellable pending = timer.schedule(delay, task);
                if (delay.isZero()) {
                    timer.clock.advance(Duration.ZERO);
                    return () -> {
                        pending.cancel();
                        cancelled.set(true);
                    };
                }
                return pending;
            }
        };
        try (BrokerServer sut = start(inline); var client = RawSession.open(sut.port())) {
            client.expectClosed();
            await().untilTrue(cancelled);
            assertThat(timer.clock.pendingTaskCount()).isZero();
            try (var replacement = RawSession.connected(sut.port(), "after-inline-expiry")) {
                replacement.ping();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(TimerFailure.class)
    void timer_startup_failure_only_closes_the_affected_connection(TimerFailure failure)
            throws Exception {
        var timer = new ManualTimer();
        try (BrokerServer sut = start(timer);
                var existing = RawSession.connected(sut.port(), "existing")) {
            switch (failure) {
                case NANO_TIME -> timer.failNextClockRead.set(true);
                case SCHEDULE -> timer.failNextSchedule.set(true);
            }
            try (var failed = RawSession.open(sut.port())) {
                failed.expectClosed();
            }

            existing.ping();
            try (var replacement = RawSession.connected(sut.port(), "after-failure")) {
                replacement.ping();
            }
        }
        assertThat(timer.clock.pendingTaskCount()).isZero();
    }

    private static BrokerServer start(Timer timer) throws IOException {
        return BrokerServer.builder()
                .connectTimeout(CONNECT_TIMEOUT)
                .scheduler(timer)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    private static BrokerServer start(Timer timer, Consumer<Packet> beforeProcess)
            throws IOException {
        return BrokerServer.builder()
                .connectTimeout(CONNECT_TIMEOUT)
                .scheduler(timer)
                .beforeProcess(beforeProcess)
                .start(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    }

    static final class ManualTimer implements Timer {

        final ManualTimerScheduler clock = new ManualTimerScheduler();
        final List<Runnable> actions = new CopyOnWriteArrayList<>();
        private final AtomicBoolean failNextClockRead = new AtomicBoolean();
        private final AtomicBoolean failNextSchedule = new AtomicBoolean();
        private volatile boolean cancellationFails;

        @Override
        public long nanoTime() {
            if (failNextClockRead.compareAndSet(true, false)) {
                throw new IllegalStateException("Injected clock failure");
            }
            return clock.nanoTime();
        }

        @Override
        public Cancellable schedule(Duration delay, Runnable task) {
            if (failNextSchedule.compareAndSet(true, false)) {
                throw new IllegalStateException("Injected scheduling failure");
            }
            actions.add(task);
            TimerScheduler.Cancellable pending = clock.schedule(delay, task);
            return () -> {
                pending.cancel();
                if (cancellationFails) {
                    throw new IllegalStateException("Injected cancellation failure");
                }
            };
        }

        void awaitPendingTasks(int expected) {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(clock.pendingTaskCount()).isEqualTo(expected));
        }
    }

    private enum TimerFailure { NANO_TIME, SCHEDULE }
}
