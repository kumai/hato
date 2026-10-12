package org.mqtt4j.hato.testkit;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.spi.TimerScheduler;
import org.mqtt4j.hato.spi.Transport;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.hato.spi.Transport.SendResult.ACCEPTED;
import static org.mqtt4j.hato.spi.Transport.SendResult.CLOSED;

class FaultyTransportTest {

    @ParameterizedTest
    @ValueSource(longs = {0, Long.MAX_VALUE - 1})
    void delayed_sends_reach_the_delegate_at_the_deadline_and_complete_only_when_written(
            long offset
    ) {
        // Arrange
        var clock = new ManualTimerScheduler();
        TimerScheduler shifted = new TimerScheduler() {
            @Override
            public long nanoTime() {
                return offset + clock.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                return clock.schedule(delay, task);
            }
        };
        var delegate = new Delegate();
        var bytes = new byte[] {42};
        try (var sut = new FaultyTransport(delegate, shifted)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            assertThat(sut.sendDelay(Duration.ofSeconds(5))).isSameAs(sut);

            // Act
            assertThat(sut.send(1, bytes)).isEqualTo(ACCEPTED);
            assertThat(clock.pendingTaskCount()).isEqualTo(1);
            clock.advance(Duration.ofSeconds(5).minusNanos(1));

            // Assert: admission retains the original buffer without claiming successful writing.
            assertThat(delegate.sent).isEmpty();
            assertThat(receiver.sent).isEmpty();
            clock.advance(Duration.ofNanos(1));
            assertThat(delegate.sent).hasSize(1);
            assertThat(delegate.sent.getFirst().ticket()).isEqualTo(1);
            assertThat(delegate.sent.getFirst().bytes()).isSameAs(bytes);
            assertThat(clock.pendingTaskCount()).isZero();
            assertThat(receiver.sent).isEmpty();
            delegate.listener.join().onSent(1);
            assertThat(receiver.sent).containsExactly(1L);
            clock.advance(Duration.ofSeconds(10));
            assertThat(delegate.sent).hasSize(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void default_and_explicit_zero_delay_delegate_immediately(boolean explicit) {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            if (explicit) {
                sut.sendDelay(Duration.ofSeconds(5)).sendDelay(Duration.ZERO);
            }

            // Act
            assertThat(sut.send(1, new byte[] {42})).isEqualTo(ACCEPTED);

            // Assert
            assertThat(delegate.sent).extracting(Send::ticket).containsExactly(1L);
            assertThat(clock.pendingTaskCount()).isZero();
            assertThat(receiver.sent).isEmpty();
            delegate.listener.join().onSent(1);
            assertThat(receiver.sent).containsExactly(1L);
        }
    }

    @Test
    void one_send_stays_outstanding_until_completion_and_delay_changes_apply_to_the_next_send() {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.sendDelay(Duration.ofSeconds(5));
            assertThat(sut.send(1, new byte[] {1})).isEqualTo(ACCEPTED);

            // Act: reducing the delay cannot release or overtake an admitted send.
            sut.sendDelay(Duration.ZERO);
            assertThatThrownBy(() -> sut.send(2, new byte[] {2}))
                    .isInstanceOf(IllegalStateException.class);
            clock.advance(Duration.ofSeconds(4));
            assertThat(delegate.sent).isEmpty();
            clock.advance(Duration.ofSeconds(1));
            assertThatThrownBy(() -> sut.send(2, new byte[] {2}))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(delegate.sent).extracting(Send::ticket).containsExactly(1L);
            delegate.listener.join().onSent(1);

            // Assert: the next admitted send uses the new delay.
            assertThat(sut.send(2, new byte[] {2})).isEqualTo(ACCEPTED);
            assertThat(delegate.sent).extracting(Send::ticket).containsExactly(1L, 2L);
            assertThat(clock.pendingTaskCount()).isZero();
        }
    }

    @Test
    void receive_and_send_delays_have_independent_deadlines_and_preserve_each_direction() {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.receiveDelay(Duration.ofSeconds(2)).sendDelay(Duration.ofSeconds(5));
            delegate.listener.join().onBytes(new byte[] {1});
            assertThat(sut.send(1, new byte[] {3})).isEqualTo(ACCEPTED);
            sut.receiveDelay(Duration.ZERO).sendDelay(Duration.ofSeconds(1));
            delegate.listener.join().onBytes(new byte[] {2});

            // Act & Assert
            assertThat(clock.pendingTaskCount()).isEqualTo(2);
            clock.advance(Duration.ofSeconds(1));
            assertThat(receiver.received).isEmpty();
            assertThat(delegate.sent).isEmpty();
            clock.advance(Duration.ofSeconds(1));
            assertThat(receiver.received).containsExactly((byte) 1, (byte) 2);
            assertThat(delegate.sent).isEmpty();
            clock.advance(Duration.ofSeconds(3));
            assertThat(delegate.sent).extracting(Send::ticket).containsExactly(1L);
            delegate.listener.join().onSent(1);
            assertThat(sut.send(2, new byte[] {4})).isEqualTo(ACCEPTED);
            assertThat(delegate.sent).hasSize(1);
            clock.advance(Duration.ofSeconds(1));
            assertThat(delegate.sent).extracting(Send::ticket).containsExactly(1L, 2L);
            delegate.listener.join().onSent(2);
            assertThat(receiver.sent).containsExactly(1L, 2L);
            assertThat(clock.pendingTaskCount()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("try") // Explicit close is the action under test.
    void shutdown_cancels_a_delayed_send_but_ownership_waits_for_termination(boolean close)
            throws Exception {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        delegate.terminateOnClose = false;
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.sendDelay(Duration.ofSeconds(5));
            assertThat(sut.send(1, new byte[] {42})).isEqualTo(ACCEPTED);
            var failure = new IOException("injected disconnect");

            // Act
            if (close) {
                sut.close();
            } else {
                sut.disconnect(failure);
            }
            assertThat(delegate.closingThread.get(5, SECONDS).join(Duration.ofSeconds(5))).isTrue();

            // Assert: neither the cancelled timer nor close returning releases the buffer.
            assertThat(clock.pendingTaskCount()).isZero();
            clock.advance(Duration.ofSeconds(10));
            assertThat(delegate.sent).isEmpty();
            assertThat(receiver.sent).isEmpty();
            assertThat(receiver.end).isNotDone();
            assertThat(sut.send(2, new byte[] {43})).isEqualTo(CLOSED);
            delegate.listener.join().onClosed(new IOException("underlying close completed"));
            Throwable ended = receiver.end.get(5, SECONDS);
            if (close) {
                assertThat(ended).isInstanceOf(IOException.class);
            } else {
                assertThat(ended).isSameAs(failure);
            }
            sut.close();
            assertThat(receiver.closures).isEqualTo(1);
        }
    }

    @Test
    void delegate_termination_cancels_sending_before_its_delayed_receive_notification()
            throws Exception {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.receiveDelay(Duration.ofSeconds(10)).sendDelay(Duration.ofSeconds(5));
            delegate.listener.join().onBytes(new byte[] {1});
            assertThat(sut.send(1, new byte[] {42})).isEqualTo(ACCEPTED);
            var failure = new IOException("peer closed");

            // Act
            delegate.listener.join().onClosed(failure);

            // Assert: only the incoming bytes and their final notification remain delayed.
            assertThat(sut.send(2, new byte[] {43})).isEqualTo(CLOSED);
            assertThat(clock.pendingTaskCount()).isEqualTo(1);
            clock.advance(Duration.ofSeconds(5));
            assertThat(delegate.sent).isEmpty();
            assertThat(receiver.end).isNotDone();
            clock.advance(Duration.ofSeconds(5));
            assertThat(receiver.received).containsExactly((byte) 1);
            assertThat(receiver.sent).isEmpty();
            assertThat(receiver.end.get(5, SECONDS)).isSameAs(failure);
            assertThat(receiver.closures).isEqualTo(1);
            assertThat(clock.pendingTaskCount()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 5})
    void inline_callbacks_can_send_again_without_holding_the_wrapper_monitor(int nextDelay) {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        delegate.completeInline = true;
        try (var sut = new FaultyTransport(delegate, clock)) {
            delegate.beforeStart = () -> assertThat(Thread.holdsLock(sut)).isFalse();
            delegate.beforeSend = () -> assertThat(Thread.holdsLock(sut)).isFalse();
            var receiver = new Receiver();
            receiver.connected = () ->
                    assertThat(sut.send(1, new byte[] {1})).isEqualTo(ACCEPTED);
            receiver.completed = ticket -> {
                if (ticket == 1) {
                    sut.sendDelay(Duration.ofSeconds(nextDelay));
                    assertThat(sut.send(2, new byte[] {2})).isEqualTo(ACCEPTED);
                }
            };

            // Act
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);

            // Assert: returning from the first send cannot clear its reentrant successor.
            if (nextDelay > 0) {
                assertThat(delegate.sent).extracting(Send::ticket).containsExactly(1L);
                assertThat(clock.pendingTaskCount()).isEqualTo(1);
            }
            clock.advance(Duration.ofSeconds(nextDelay));
            assertThat(delegate.sent).extracting(Send::ticket).containsExactly(1L, 2L);
            assertThat(receiver.sent).containsExactly(1L, 2L);
            assertThat(clock.pendingTaskCount()).isZero();
            assertThat(receiver.end).isNotDone();
        }
    }

    @Test
    void an_inline_timer_hands_off_the_send_outside_the_wrapper_monitor() {
        // Arrange: the scheduler reaches the deadline before schedule returns its cancellation.
        TimerScheduler clock = new TimerScheduler() {
            long now;

            @Override
            public long nanoTime() {
                return now;
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                now += delay.toNanos();
                task.run();
                return () -> { };
            }
        };
        var delegate = new Delegate();
        delegate.completeInline = true;
        try (var sut = new FaultyTransport(delegate, clock)) {
            delegate.beforeSend = () -> assertThat(Thread.holdsLock(sut)).isFalse();
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.sendDelay(Duration.ofSeconds(5));

            // Act
            assertThat(sut.send(1, new byte[] {42})).isEqualTo(ACCEPTED);

            // Assert
            assertThat(delegate.sent).extracting(Send::ticket).containsExactly(1L);
            assertThat(receiver.sent).containsExactly(1L);
            assertThat(receiver.end).isNotDone();
        }
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null delay contract.
    void invalid_send_delays_leave_the_previous_setting_unchanged() {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        try (var sut = new FaultyTransport(delegate, clock)) {
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), new Receiver());
            sut.sendDelay(Duration.ofSeconds(5));

            // Act & Assert
            assertThatThrownBy(() -> sut.sendDelay(null)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> sut.sendDelay(Duration.ofNanos(-1)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> sut.sendDelay(Duration.ofSeconds(Long.MAX_VALUE)))
                    .isInstanceOf(ArithmeticException.class);
            assertThat(sut.send(1, new byte[] {42})).isEqualTo(ACCEPTED);
            clock.advance(Duration.ofSeconds(4));
            assertThat(delegate.sent).isEmpty();
            clock.advance(Duration.ofSeconds(1));
            assertThat(delegate.sent).extracting(Send::ticket).containsExactly(1L);
        }
    }

    @Test
    void scheduling_failure_rejects_admission_without_leaving_a_send_outstanding() {
        // Arrange
        var clock = new ManualTimerScheduler();
        var failure = new IllegalStateException("cannot schedule");
        TimerScheduler rejecting = new TimerScheduler() {
            boolean reject = true;

            @Override
            public long nanoTime() {
                return clock.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                if (reject) {
                    reject = false;
                    throw failure;
                }
                return clock.schedule(delay, task);
            }
        };
        var delegate = new Delegate();
        try (var sut = new FaultyTransport(delegate, rejecting)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.sendDelay(Duration.ofSeconds(5));

            // Act & Assert: failed admission keeps ownership with the caller and permits retry.
            assertThatThrownBy(() -> sut.send(1, new byte[] {1})).isSameAs(failure);
            assertThat(delegate.sent).isEmpty();
            assertThat(clock.pendingTaskCount()).isZero();
            assertThat(receiver.end).isNotDone();
            assertThat(sut.send(2, new byte[] {2})).isEqualTo(ACCEPTED);
            clock.advance(Duration.ofSeconds(5));
            assertThat(delegate.sent).extracting(Send::ticket).containsExactly(2L);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 5})
    void delegate_send_failure_is_synchronous_before_admission_and_terminal_after_it(int delay)
            throws Exception {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        var failure = new IllegalStateException("delegate send failed");
        delegate.beforeSend = () -> {
            throw failure;
        };
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.sendDelay(Duration.ofSeconds(delay));

            // Act & Assert
            if (delay == 0) {
                assertThatThrownBy(() -> sut.send(1, new byte[] {1})).isSameAs(failure);
                assertThat(receiver.end).isNotDone();
                delegate.beforeSend = () -> { };
                assertThat(sut.send(2, new byte[] {2})).isEqualTo(ACCEPTED);
                assertThat(delegate.sent).extracting(Send::ticket).containsExactly(2L);
            } else {
                assertThat(sut.send(1, new byte[] {1})).isEqualTo(ACCEPTED);
                clock.advance(Duration.ofSeconds(delay));
                assertThat(receiver.end.get(5, SECONDS)).isSameAs(failure);
                assertThat(delegate.sent).isEmpty();
                assertThat(sut.send(2, new byte[] {2})).isEqualTo(CLOSED);
            }
            assertThat(receiver.sent).isEmpty();
            assertThat(clock.pendingTaskCount()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_closed_delegate_send_rejects_later_sends_and_preserves_its_terminal_cause(boolean inline)
            throws Exception {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        delegate.sendResult = CLOSED;
        var failure = new IOException("delegate ended before delayed send");
        if (inline) {
            delegate.beforeSend = () -> delegate.listener.join().onClosed(failure);
        }
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.sendDelay(Duration.ofSeconds(5));
            assertThat(sut.send(1, new byte[] {1})).isEqualTo(ACCEPTED);

            // Act
            clock.advance(Duration.ofSeconds(5));

            // Assert: CLOSED starts shutdown without inventing completion or a failure cause.
            assertThat(sut.send(2, new byte[] {2})).isEqualTo(CLOSED);
            assertThat(delegate.sent).extracting(Send::ticket).containsExactly(1L);
            assertThat(receiver.sent).isEmpty();
            if (!inline) {
                assertThat(receiver.end).isNotDone();
                delegate.listener.join().onClosed(failure);
            }
            assertThat(receiver.end.get(5, SECONDS)).isSameAs(failure);
            assertThat(receiver.closures).isEqualTo(1);
            assertThat(clock.pendingTaskCount()).isZero();
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "true, true"})
    void failed_or_late_cancellation_cannot_send_after_shutdown(
            boolean cancelFails,
            boolean loggingFails
    ) throws Exception {
        // Arrange: cancellation is best effort; the scheduler may already have dispatched work.
        var clock = new ManualTimerScheduler();
        TimerScheduler dispatched = new TimerScheduler() {
            @Override
            public long nanoTime() {
                return clock.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                clock.schedule(delay, task);
                return () -> {
                    if (cancelFails) {
                        throw new IllegalStateException("cannot cancel dispatched timer");
                    }
                };
            }
        };
        var delegate = new Delegate();
        var logCalls = new AtomicInteger();
        var jul = Logger.getLogger(FaultyTransport.class.getName());
        Level previousLevel = jul.getLevel();
        boolean previousParents = jul.getUseParentHandlers();
        Handler[] previousHandlers = jul.getHandlers();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                logCalls.incrementAndGet();
                if (loggingFails) {
                    throw new IllegalStateException("logging backend failed");
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        for (Handler previous : previousHandlers) {
            jul.removeHandler(previous);
        }
        jul.addHandler(handler);
        jul.setLevel(Level.ALL);
        jul.setUseParentHandlers(false);
        try (var sut = new FaultyTransport(delegate, dispatched)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.sendDelay(Duration.ofSeconds(5));
            assertThat(sut.send(1, new byte[] {42})).isEqualTo(ACCEPTED);
            var failure = new IOException("injected disconnect");

            // Act
            sut.disconnect(failure);
            assertThat(receiver.end.get(5, SECONDS)).isSameAs(failure);
            clock.advance(Duration.ofSeconds(5));

            // Assert
            assertThat(delegate.sent).isEmpty();
            assertThat(receiver.sent).isEmpty();
            assertThat(receiver.closures).isEqualTo(1);
            assertThat(clock.pendingTaskCount()).isZero();
            if (cancelFails) {
                assertThat(logCalls.get()).isPositive();
            } else {
                assertThat(logCalls.get()).isZero();
            }
        } finally {
            jul.removeHandler(handler);
            for (Handler previous : previousHandlers) {
                jul.addHandler(previous);
            }
            jul.setLevel(previousLevel);
            jul.setUseParentHandlers(previousParents);
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    @SuppressWarnings("try") // Explicit close is one of the reordered events.
    void reordered_deadlines_shutdown_and_write_completion_preserve_buffer_ownership(int seed)
            throws Exception {
        // Arrange: completion becomes eligible only after delegation, and EOF ends all callbacks.
        var random = new Random(seed);
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        delegate.terminateOnClose = false;
        var bytes = new byte[] {42};
        var failure = new IOException("injected disconnect");
        var eof = new IOException("delegate EOF");
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.sendDelay(Duration.ofSeconds(5));
            assertThat(sut.send(1, bytes)).isEqualTo(ACCEPTED);
            var ready = new ArrayList<>(List.of(
                    SendEvent.DEADLINE, SendEvent.CLOSE, SendEvent.EOF));
            boolean delegated = false;
            boolean completed = false;
            boolean requested = false;

            // Act: each selection changes the legal successors, rather than invoking stale events.
            while (!ready.isEmpty()) {
                Collections.shuffle(ready, random);
                switch (ready.removeFirst()) {
                    case DEADLINE -> {
                        clock.advance(Duration.ofSeconds(5));
                        delegated = true;
                        ready.add(SendEvent.COMPLETE);
                    }
                    case CLOSE -> {
                        if (seed % 2 == 0) {
                            sut.close();
                        } else {
                            sut.disconnect(failure);
                        }
                        requested = true;
                        ready.remove(SendEvent.DEADLINE);
                        Thread closing = delegate.closingThread.get(5, SECONDS);
                        assertThat(closing.join(Duration.ofSeconds(5))).isTrue();
                        assertThat(receiver.end).isNotDone();
                    }
                    case COMPLETE -> {
                        delegate.listener.join().onSent(1);
                        completed = true;
                    }
                    case EOF -> {
                        delegate.listener.join().onClosed(eof);
                        ready.clear();
                    }
                }
            }

            // Assert: cancelled work cannot acquire the buffer after its final ownership return.
            Throwable ended = receiver.end.get(5, SECONDS);
            if (!requested) {
                assertThat(ended).isSameAs(eof);
            } else if (seed % 2 != 0) {
                assertThat(ended).isSameAs(failure);
            } else {
                assertThat(ended).isInstanceOf(IOException.class).isNotSameAs(eof);
            }
            clock.advance(Duration.ofSeconds(10));
            assertThat(delegate.sent).hasSize(delegated ? 1 : 0);
            if (delegated) {
                assertThat(delegate.sent.getFirst().bytes()).isSameAs(bytes);
            }
            assertThat(receiver.events).isEqualTo(completed
                    ? List.of("connected", "sent:1", "closed")
                    : List.of("connected", "closed"));
            assertThat(receiver.closures).isEqualTo(1);
            assertThat(sut.send(2, new byte[] {43})).isEqualTo(CLOSED);
            assertThat(clock.pendingTaskCount()).isZero();
        }
    }

    static IntStream seeds() {
        return IntStream.range(0, 100);
    }

    @Test
    void terminal_notification_waits_until_a_racing_delegate_send_returns() throws Exception {
        // Arrange: the delegate's send invocation still retains the admitted array.
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        delegate.terminateOnClose = false;
        var bytes = new byte[] {42};
        var entered = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        var observed = new CompletableFuture<Byte>();
        delegate.beforeSend = () -> {
            observed.complete(bytes[0]);
            entered.complete(null);
            release.join();
        };
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            receiver.closed = () -> bytes[0] = 99;
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.sendDelay(Duration.ofSeconds(5));
            assertThat(sut.send(1, bytes)).isEqualTo(ACCEPTED);
            var advancing = CompletableFuture.runAsync(() -> clock.advance(Duration.ofSeconds(5)));
            var failure = new IOException("injected disconnect");

            try {
                // Act: shutdown finishes while the already-entered delegate call has not returned.
                entered.get(5, SECONDS);
                sut.disconnect(failure);
                Thread closing = delegate.closingThread.get(5, SECONDS);
                assertThat(closing.join(Duration.ofSeconds(5))).isTrue();
                delegate.listener.join().onClosed(new IOException("delegate ended"));

                // Assert: the caller cannot reclaim and mutate the buffer before send returns.
                assertThat(receiver.end).isNotDone();
                assertThat(bytes).containsExactly((byte) 42);
            } finally {
                release.complete(null);
            }
            advancing.get(5, SECONDS);
            assertThat(receiver.end.get(5, SECONDS)).isSameAs(failure);
            assertThat(observed.get(5, SECONDS)).isEqualTo((byte) 42);
            assertThat(bytes).containsExactly((byte) 99);
            assertThat(delegate.sent).hasSize(1);
            assertThat(delegate.sent.getFirst().bytes()).isSameAs(bytes);
            assertThat(receiver.events).containsExactly("connected", "closed");
            assertThat(clock.pendingTaskCount()).isZero();
        }
    }

    @Test
    void reducing_delay_does_not_reorder_chunks_and_cancellation_releases_them() throws Exception {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);

            // Act
            sut.receiveDelay(Duration.ofSeconds(10));
            delegate.listener.join().onBytes(new byte[] {1});
            sut.receiveDelay(Duration.ZERO);
            delegate.listener.join().onBytes(new byte[] {2});
            clock.advance(Duration.ofSeconds(9));

            // Assert
            assertThat(receiver.received).isEmpty();
            clock.advance(Duration.ofSeconds(1));
            assertThat(receiver.received).containsExactly((byte) 1, (byte) 2);
            sut.receiveDelay(Duration.ofSeconds(10));
            delegate.listener.join().onBytes(new byte[] {3});
            var failure = new IOException("injected disconnect");

            // Act
            sut.disconnect(failure);

            // Assert
            assertThat(receiver.end.get(5, SECONDS)).isSameAs(failure);
            assertThat(clock.pendingTaskCount()).isZero();
            clock.advance(Duration.ofSeconds(10));
            assertThat(receiver.received).containsExactly((byte) 1, (byte) 2);
        }
    }

    @Test
    void pending_send_ownership_is_released_by_delegate_termination_not_close_returning()
            throws Exception {
        // Arrange: closing requests termination; the delegate reports completion separately.
        var delegate = new Delegate();
        delegate.terminateOnClose = false;
        try (var sut = new FaultyTransport(delegate, new ManualTimerScheduler())) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            assertThat(sut.send(1, new byte[] {42})).isEqualTo(ACCEPTED);
            var failure = new IOException("injected disconnect");

            // Act
            sut.disconnect(failure);
            Thread closeWorker = delegate.closingThread.get(5, SECONDS);
            assertThat(closeWorker.join(Duration.ofSeconds(5))).isTrue();

            // Assert
            assertThat(receiver.end).isNotDone();
            assertThat(sut.send(2, new byte[] {43})).isEqualTo(CLOSED);
            delegate.listener.join().onClosed(new IOException("underlying close completed"));

            // Assert
            assertThat(receiver.end.get(5, SECONDS)).isSameAs(failure);
            assertThat(sut.send(3, new byte[] {44})).isEqualTo(CLOSED);
        }
    }

    @Test
    void delayed_receive_overload_ends_once_and_cancels_the_timer() throws Exception {
        // Arrange
        var clock = new ManualTimerScheduler();
        var delegate = new Delegate();
        try (var sut = new FaultyTransport(delegate, clock)) {
            var receiver = new Receiver();
            sut.start(URI.create("mqtt://localhost"), Duration.ofSeconds(1), receiver);
            sut.receiveDelay(Duration.ofSeconds(10));

            // Act: exceed the delayed-input budget without advancing the clock.
            for (int i = 0; i <= 256; i++) {
                delegate.listener.join().onBytes(new byte[] {1});
            }

            // Assert
            assertThat(receiver.end.get(5, SECONDS)).isInstanceOf(MqttCapacityException.class);
            assertThat(clock.pendingTaskCount()).isZero();
            assertThat(receiver.received).isEmpty();
        }
    }

    private static final class Delegate implements Transport {

        final CompletableFuture<Listener> listener = new CompletableFuture<>();
        final CompletableFuture<Thread> closingThread = new CompletableFuture<>();
        final List<Send> sent = new ArrayList<>();
        Runnable beforeStart = () -> { };
        Runnable beforeSend = () -> { };
        SendResult sendResult = ACCEPTED;
        boolean completeInline;
        boolean terminateOnClose = true;

        @Override
        public void start(URI endpoint, Duration timeout, Listener receiver) {
            beforeStart.run();
            listener.complete(receiver);
            receiver.onConnected();
        }

        @Override
        public SendResult send(long ticket, byte[] bytes) {
            beforeSend.run();
            sent.add(new Send(ticket, bytes));
            if (completeInline && sendResult == ACCEPTED) {
                listener.join().onSent(ticket);
            }
            return sendResult;
        }

        @Override
        public void close() {
            closingThread.complete(Thread.currentThread());
            if (terminateOnClose) {
                listener.join().onClosed(new IOException("Delegate closed"));
            }
        }
    }

    private static final class Receiver implements Transport.Listener {

        final List<Byte> received = new ArrayList<>();
        final List<Long> sent = new ArrayList<>();
        final List<String> events = new ArrayList<>();
        final CompletableFuture<Throwable> end = new CompletableFuture<>();
        Runnable connected = () -> { };
        Runnable closed = () -> { };
        LongConsumer completed = _ -> { };
        int closures;

        @Override
        public void onConnected() {
            events.add("connected");
            connected.run();
        }

        @Override
        public void onBytes(byte[] bytes) {
            events.add("bytes");
            for (byte value : bytes) {
                received.add(value);
            }
        }

        @Override
        public void onSent(long ticket) {
            events.add("sent:" + ticket);
            sent.add(ticket);
            completed.accept(ticket);
        }

        @Override
        public void onClosed(Throwable cause) {
            events.add("closed");
            closures++;
            closed.run();
            assertThat(end.complete(cause)).isTrue();
        }
    }

    private record Send(long ticket, byte[] bytes) {
    }

    private enum SendEvent {
        DEADLINE,
        CLOSE,
        COMPLETE,
        EOF
    }
}
