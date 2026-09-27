package org.mqtt4j.mqtj.client.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.mqtt4j.mqtj.MqttDelivery;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.Topic;
import org.mqtt4j.mqtj.TopicFilter;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class DeliveryLanesTest {

    private static final int BYTE_LIMIT = 32 * 1024 * 1024;

    private final LinkedBlockingQueue<Failure> failures = new LinkedBlockingQueue<>();

    private final List<DeliveryLanes> managers = new ArrayList<>();

    private final List<CountDownLatch> releases = new ArrayList<>();

    private final DeliveryLanes sut = lanes(BYTE_LIMIT);

    @AfterEach
    void close() {
        managers.forEach(DeliveryLanes::close);
        releases.forEach(CountDownLatch::countDown);
        await().untilAsserted(() -> managers.forEach(manager -> {
            assertThat(manager.retainedLanes()).isZero();
            assertThat(manager.retainedDeliveries()).isZero();
            assertThat(manager.retainedBytes()).isZero();
        }));
        assertThat(failures).isEmpty();
    }

    @Test
    void handlers_are_serial_virtual_and_counted_until_they_return() throws Exception {
        // Arrange
        var entered = new LinkedBlockingQueue<MqttDelivery>();
        var virtual = new AtomicBoolean();
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(2), delivery -> {
            virtual.set(Thread.currentThread().isVirtual());
            entered.add(delivery);
            waitFor(release);
        })).isTrue();
        MqttMessage first = message(1);
        MqttMessage second = message(2);

        // Act & Assert: a running handler consumes one of the two per-subscription slots.
        assertThat(sut.deliver(1, List.of(1L), first, false, 10)).isTrue();
        assertThat(requireNonNull(entered.poll(5, SECONDS)).message()).isEqualTo(first);
        assertThat(sut.deliver(1, List.of(1L), second, false, 20)).isTrue();
        assertThat(sut.deliver(1, List.of(1L), message(3), false, 30)).isFalse();

        // Assert: the second handler waits for the first, with both reservations retained.
        assertThat(virtual.get()).isTrue();
        assertThat(entered).isEmpty();
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        assertThat(sut.retainedBytes()).isEqualTo(30);

        // Act: returning the first handler lets the queued delivery run.
        release.countDown();

        // Assert
        MqttDelivery received = requireNonNull(entered.poll(5, SECONDS));
        assertThat(received.message()).isEqualTo(second);
        assertThat(received.dup()).isFalse();
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isZero());
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void a_normal_handler_return_clears_interrupt_state_before_the_next_queue_wait()
            throws Exception {
        // Arrange: each handler leaves the worker interrupted after returning normally.
        var workers = new LinkedBlockingQueue<Thread>();
        var interruptStates = new LinkedBlockingQueue<Boolean>();
        assertThat(sut.register(1, 1, request(1), _ -> {
            interruptStates.add(Thread.currentThread().isInterrupted());
            workers.add(Thread.currentThread());
            Thread.currentThread().interrupt();
        })).isTrue();

        // Act
        assertThat(sut.deliver(1, List.of(1L), message(1), false, 10)).isTrue();

        // Assert: the worker stays alive and waits for another delivery without reporting failure.
        Thread worker = requireNonNull(workers.poll(5, SECONDS));
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        await().until(
                () -> !worker.isAlive()
                        || sut.retainedDeliveries() == 0
                                && worker.getState() == Thread.State.WAITING
        );
        assertThat(worker.isAlive()).isTrue();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(failures).isEmpty();

        // Act & Assert: delivery resumes after the idle wait with a clear interrupt state.
        assertThat(sut.deliver(1, List.of(1L), message(2), false, 10)).isTrue();
        assertThat(workers.poll(5, SECONDS)).isSameAs(worker);
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isZero());
        assertThat(sut.abandonedDeliveries()).isZero();
    }

    @Test
    void queued_handlers_do_not_inherit_interrupt_state_from_the_previous_delivery()
            throws Exception {
        // Arrange: the next delivery is queued while the first handler is still running.
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        var first = new AtomicBoolean(true);
        var interruptStates = new LinkedBlockingQueue<Boolean>();
        assertThat(sut.register(1, 1, request(2), _ -> {
            interruptStates.add(Thread.currentThread().isInterrupted());
            if (first.getAndSet(false)) {
                entered.countDown();
                waitFor(release);
                Thread.currentThread().interrupt();
            }
        })).isTrue();
        assertThat(sut.deliver(1, List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        assertThat(sut.deliver(1, List.of(1L), message(2), false, 20)).isTrue();

        // Act: returning the first handler dispatches the queued delivery without an idle wait.
        release.countDown();

        // Assert
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isZero());
        assertThat(sut.abandonedDeliveries()).isZero();
        assertThat(failures).isEmpty();
    }

    @Test
    void waiting_lanes_are_signalled_for_delivery_and_removal() throws Exception {
        // Arrange: start each worker, then let it park on its empty queue.
        var entered = new LinkedBlockingQueue<Long>();
        Set<Thread> workers = ConcurrentHashMap.newKeySet();
        List<Long> ids = List.of(1L, 2L, 3L);
        for (long id : ids) {
            assertThat(sut.register(1, id, request(1), _ -> {
                workers.add(Thread.currentThread());
                entered.add(id);
            })).isTrue();
        }

        assertThat(sut.deliver(1, ids, message(1), false, 10)).isTrue();
        var first = new ArrayList<Long>();
        for (int i = 0; i < ids.size(); i++) {
            first.add(requireNonNull(entered.poll(5, SECONDS)));
        }
        assertThat(first).containsExactlyInAnyOrderElementsOf(ids);
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isZero());
        await().until(
                () -> workers.stream()
                        .allMatch(worker -> worker.getState() == Thread.State.WAITING)
        );

        // Act
        sut.remove(2);

        // Assert: stopping an idle lane wakes its worker and releases its thread reservation.
        await().untilAsserted(() -> assertThat(sut.retainedLanes()).isEqualTo(2));

        // Act & Assert: each surviving recipient wakes independently.
        assertThat(sut.deliver(1, List.of(3L), message(2), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(3L);
        assertThat(sut.deliver(1, List.of(1L, 2L), message(3), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(1L);
        assertThat(entered).isEmpty();
        assertThat(sut.abandonedDeliveries()).isZero();
    }

    @Test
    void fanout_reserves_all_recipients_or_none() throws Exception {
        // Arrange: the first lane is full and the second lane is empty.
        var entered = new LinkedBlockingQueue<Long>();
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(1), _ -> {
            entered.add(1L);
            waitFor(release);
        })).isTrue();
        assertThat(sut.register(1, 2, request(1), _ -> entered.add(2L))).isTrue();
        assertThat(sut.deliver(1, List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(1L);

        // Act & Assert: discovering the full lane second does not reserve the empty one.
        assertThat(sut.deliver(1, List.of(2L, 1L), message(2), false, 20)).isFalse();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(entered).isEmpty();

        // Act & Assert: refusing the fan-out leaves the empty lane's capacity available.
        assertThat(sut.deliver(1, List.of(2L), message(3), false, 30)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(2L);
    }

    @Test
    void overlapping_recipients_share_bytes_until_the_last_handler_returns() throws Exception {
        // Arrange: wire sizes are supplied by framing, so no large test payload is needed.
        var entered = new CountDownLatch(2);
        CountDownLatch firstRelease = release();
        CountDownLatch secondRelease = release();
        assertThat(sut.register(1, 1, request(2), _ -> {
            entered.countDown();
            waitFor(firstRelease);
        })).isTrue();
        assertThat(sut.register(1, 2, request(2), _ -> {
            entered.countDown();
            waitFor(secondRelease);
        })).isTrue();
        assertThat(sut.deliver(1, List.of(1L, 2L), message(1), false, BYTE_LIMIT)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();

        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);

        // Act & Assert: returning only one handler keeps the shared byte reservation.
        firstRelease.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isEqualTo(1));
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);
        assertThat(sut.deliver(1, List.of(1L), message(2), false, 1)).isFalse();

        // Act & Assert: the last handler returns the shared capacity.
        secondRelease.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        assertThat(sut.deliver(1, List.of(1L), message(2), false, 1)).isTrue();
    }

    @Test
    void a_configured_large_packet_fits_but_a_single_packet_above_budget_does_not() {
        // Arrange
        DeliveryLanes sut = lanes(BYTE_LIMIT + 1);
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(2), _ -> waitFor(release))).isTrue();

        // Act & Assert: rejection preserves capacity for a packet at the configured limit.
        assertThat(sut.deliver(1, List.of(1L), message(1), false, BYTE_LIMIT + 2))
                .isFalse();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.deliver(1, List.of(1L), message(1), false, BYTE_LIMIT + 1))
                .isTrue();
    }

    @Test
    void pausing_stops_admission_but_keeps_queued_work_and_can_be_rolled_back() throws Exception {
        // Arrange: one handler is running and another delivery is queued.
        var entered = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(2), delivery -> {
            entered.add(delivery);
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(1, List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(1, List.of(1L), message(2), false, 20)).isTrue();

        // Act & Assert: pausing rejects new work without discarding the existing deliveries.
        sut.pause(1);
        assertThat(sut.deliver(1, List.of(1L, 999L), message(3), false, 30)).isTrue();
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        release.countDown();
        assertThat(requireNonNull(entered.poll(5, SECONDS)).message()).isEqualTo(message(2));
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isZero());

        // Act & Assert: rolling back the pause permits another delivery.
        sut.resume(1);
        assertThat(sut.deliver(1, List.of(1L), message(4), false, 40)).isTrue();
        assertThat(requireNonNull(entered.poll(5, SECONDS)).message()).isEqualTo(message(4));
        assertThat(sut.abandonedDeliveries()).isZero();
    }

    @Test
    void removing_a_lane_discards_queued_work_and_keeps_the_running_handler() throws Exception {
        // Arrange: keep one handler running with two deliveries queued behind it.
        var entered = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(3), delivery -> {
            entered.add(delivery);
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(1, List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(1, List.of(1L), message(2), false, 20)).isTrue();
        assertThat(sut.deliver(1, List.of(1L), message(3), false, 30)).isTrue();

        // Act
        sut.remove(1);
        sut.remove(1);

        // Assert: removal is idempotent and retains only the executing handler's reservations.
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(sut.retainedLanes()).isEqualTo(1);
        assertThat(sut.abandonedDeliveries()).isEqualTo(2);

        // Act & Assert: the worker exits after the running handler returns.
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedLanes()).isZero());
        assertThat(entered).isEmpty();
    }

    @Test
    void connection_end_retains_running_bytes_and_ignores_stale_generations() throws Exception {
        // Arrange: one handler retains the entire byte budget.
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(2), _ -> {
            entered.countDown();
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(1, List.of(1L), message(1), false, BYTE_LIMIT)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act & Assert: a new generation cannot reuse the old handler's byte reservation.
        sut.close();
        assertThat(sut.register(2, 2, request(2), _ -> { })).isTrue();
        assertThat(sut.deliver(1, List.of(2L), message(2), false, 1)).isTrue();
        assertThat(sut.deliver(2, List.of(2L), message(2), false, 1)).isFalse();
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);

        // Act & Assert: returning the old handler permits delivery in the new generation.
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        assertThat(sut.deliver(2, List.of(2L), message(2), false, 1)).isTrue();
    }

    @Test
    void the_global_delivery_limit_counts_running_and_queued_recipients() throws Exception {
        // Arrange: fill the delivery budget behind one running handler.
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(5000), _ -> {
            entered.countDown();
            waitFor(release);
        })).isTrue();
        for (int i = 0; i < 4096; i++) {
            assertThat(sut.deliver(1, List.of(1L), message(1), false, 1)).isTrue();
        }
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act & Assert: the global limit applies even though the lane has spare capacity.
        assertThat(sut.deliver(1, List.of(1L), message(1), false, 1)).isFalse();
        assertThat(sut.retainedDeliveries()).isEqualTo(4096);

        // Act
        sut.close();

        // Assert: queued deliveries are abandoned; the executing handler keeps its reservation.
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(1);
        assertThat(sut.abandonedDeliveries()).isEqualTo(4095);
    }

    @Test
    void stopped_handlers_keep_the_lane_limit_across_reconnections() throws Exception {
        // Arrange: every lane has a running handler.
        var entered = new CountDownLatch(256);
        CountDownLatch release = release();
        for (long id = 1; id <= 256; id++) {
            assertThat(sut.register(1, id, request(1), _ -> {
                entered.countDown();
                waitFor(release);
            })).isTrue();
            assertThat(sut.deliver(1, List.of(id), message(1), false, 1)).isTrue();
        }
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act & Assert: closing does not release thread reservations for running handlers.
        sut.close();
        assertThat(sut.retainedLanes()).isEqualTo(256);
        assertThat(sut.register(2, 257, request(1), _ -> { })).isFalse();

        // Act & Assert: returning the handlers permits a new lane.
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedLanes()).isZero());
        assertThat(sut.register(2, 257, request(1), _ -> { })).isTrue();
    }

    @Test
    void interrupted_handler_errors_report_the_original_cause_and_abandon_the_remaining_work()
            throws Exception {
        // Arrange: one handler will leave its interrupt flag set and throw with a delivery queued.
        var entered = new CountDownLatch(1);
        var cause = new AssertionError("application failure");
        var calls = new AtomicInteger();
        CountDownLatch release = release();
        assertThat(sut.register(7, 1, request(2), _ -> {
            calls.incrementAndGet();
            entered.countDown();
            waitFor(release);
            Thread.currentThread().interrupt();
            throw cause;
        })).isTrue();
        assertThat(sut.deliver(7, List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(sut.deliver(7, List.of(1L), message(2), false, 20)).isTrue();

        // Act
        release.countDown();

        // Assert
        Failure failure = requireNonNull(failures.poll(5, SECONDS));
        assertThat(failure.generation()).isEqualTo(7);
        assertThat(failure.cause()).isSameAs(cause);
        await().untilAsserted(() -> assertThat(sut.retainedLanes()).isZero());
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.abandonedDeliveries()).isEqualTo(2);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void a_logging_failure_does_not_replace_the_handler_cause_or_repeat_notification()
            throws Exception {
        // Arrange: the JUL backend throws while recording the handler's failure.
        var cause = new AssertionError("application failure");
        var loggingFailure = new AssertionError("logging backend failure");
        var logCalls = new AtomicInteger();
        var jul = Logger.getLogger(DeliveryLanes.class.getName());
        Level previousLevel = jul.getLevel();
        boolean previousParents = jul.getUseParentHandlers();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                logCalls.incrementAndGet();
                throw loggingFailure;
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        jul.addHandler(handler);
        jul.setLevel(Level.WARNING);
        jul.setUseParentHandlers(false);
        try {
            assertThat(sut.register(7, 1, request(1), _ -> {
                throw cause;
            })).isTrue();

            // Act
            assertThat(sut.deliver(7, List.of(1L), message(1), false, 10)).isTrue();

            // Assert: the original cause is reported once and all reservations are released.
            Failure failure = requireNonNull(failures.poll(5, SECONDS));
            assertThat(failure.generation()).isEqualTo(7);
            assertThat(failure.cause()).isSameAs(cause);
            await().untilAsserted(() -> assertThat(sut.retainedLanes()).isZero());
            assertThat(failures).isEmpty();
            assertThat(logCalls.get()).isEqualTo(1);
            assertThat(sut.retainedDeliveries()).isZero();
            assertThat(sut.retainedBytes()).isZero();
            assertThat(sut.abandonedDeliveries()).isEqualTo(1);
        } finally {
            jul.removeHandler(handler);
            jul.setLevel(previousLevel);
            jul.setUseParentHandlers(previousParents);
        }
    }

    private DeliveryLanes lanes(long byteLimit) {
        var manager = new DeliveryLanes(
                "lane-test",
                byteLimit,
                (generation, cause) -> failures.add(new Failure(generation, cause))
        );
        managers.add(manager);
        return manager;
    }

    private CountDownLatch release() {
        var latch = new CountDownLatch(1);
        releases.add(latch);
        return latch;
    }

    private static Subscribe request(int capacity) {
        return new Subscribe(new TopicFilter("#"), Qos.AT_MOST_ONCE, capacity);
    }

    private static MqttMessage message(int value) {
        return new MqttMessage(new Topic("a"), new byte[] {(byte) value}, Qos.AT_MOST_ONCE, false);
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            if (!latch.await(30, SECONDS)) {
                throw new AssertionError("Test failed to release handler");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    private record Failure(long generation, Throwable cause) {
    }
}
