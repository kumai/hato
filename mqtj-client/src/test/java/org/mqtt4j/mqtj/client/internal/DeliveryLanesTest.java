package org.mqtt4j.mqtj.client.internal;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.LongStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.mqtj.AckMode;
import org.mqtt4j.mqtj.MqttConnectionLostException;
import org.mqtt4j.mqtj.MqttDelivery;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.ReceiveResult;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.Topic;
import org.mqtt4j.mqtj.TopicFilter;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@Timeout(10)
class DeliveryLanesTest {

    private static final int BYTE_LIMIT = 32 * 1024 * 1024;

    private final LinkedBlockingQueue<Failure> failures = new LinkedBlockingQueue<>();

    private final LinkedBlockingQueue<Ready> ready = new LinkedBlockingQueue<>();

    private final List<DeliveryLanes> managers = new ArrayList<>();

    private final List<CountDownLatch> releases = new ArrayList<>();

    private final DeliveryLanes sut = lanes(BYTE_LIMIT);

    @AfterEach
    void close() {
        managers.forEach(manager -> manager.close(null));
        releases.forEach(CountDownLatch::countDown);
        await().untilAsserted(() -> managers.forEach(manager -> {
            assertThat(manager.retainedLanes()).isZero();
            assertThat(manager.retainedDeliveries()).isZero();
            assertThat(manager.retainedBytes()).isZero();
        }));
        assertThat(failures).isEmpty();
    }

    @Test
    void connection_failure_abandons_queued_pull_work_and_retires_handed_off_acknowledgements()
            throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(
                1, 1, request(AckMode.MANUAL, 2)
        ).orElseThrow();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery handedOff = received(receiver.receive());
        assertThat(sut.deliver(1, 11, List.of(1L), qosOneMessage(), false, 20)).isTrue();
        var failure = new IOException("Connection ended");

        // Act
        sut.close(failure);

        // Assert
        assertThat(sut.abandonedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedLanes()).isZero();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(ready).isEmpty();
        assertThatThrownBy(receiver::receive)
                .isInstanceOf(MqttConnectionLostException.class).hasCause(failure);
        DeliveryLanes.Receiver fresh = sut.registerPull(
                2, 2, request(AckMode.MANUAL, 1)
        ).orElseThrow();
        assertThat(sut.deliver(2, 12, List.of(2L), qosOneMessage(), false, 30)).isTrue();
        MqttDelivery current = received(fresh.receive());
        handedOff.ack();
        assertThat(ready).isEmpty();
        assertThat(sut.retainedBytes()).isEqualTo(30);
        current.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(2, 12));
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void normal_connection_end_discards_pull_buffers_and_ends_receiving() throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, 1, request(1)).orElseThrow();
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 10)).isTrue();

        // Act
        sut.close(null);

        // Assert
        assertThat(receiver.receive()).isEqualTo(new ReceiveResult.Ended());
        assertThat(sut.abandonedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedLanes()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void pulling_releases_local_capacity_before_the_wire_ack_is_admitted() throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, 1, request(1)).orElseThrow();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Act
        MqttDelivery first = received(receiver.receive(Duration.ZERO));

        // Assert: a stalled sequencer retains metadata, not this subscription's payload budget.
        assertThat(first.message()).isEqualTo(qosOneMessage());
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.acknowledgingDeliveries()).isZero();
        assertThat(sut.deliver(1, 11, List.of(1L), qosOneMessage(), false, 20)).isTrue();
        sut.release(10);
        first.ack();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(20);
        assertThat(ready).isEmpty();
    }

    @Test
    void a_pulled_manual_delivery_keeps_capacity_until_ack_but_not_until_the_wire_write()
            throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(
                1, 1, request(AckMode.MANUAL, 1)
        ).orElseThrow();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Act & Assert
        MqttDelivery delivery = received(receiver.receive());
        assertThat(ready).isEmpty();
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(sut.deliver(1, 11, List.of(1L), qosOneMessage(), false, 20)).isFalse();
        delivery.ack();
        delivery.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        assertThat(ready).isEmpty();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.deliver(1, 11, List.of(1L), qosOneMessage(), false, 20)).isTrue();
    }

    @Test
    void overlapping_push_and_pull_views_share_bytes_but_have_independent_ack_conditions()
            throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, 1, request(1)).orElseThrow();
        var pushed = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, 2, request(AckMode.MANUAL, 1), pushed::add)).isTrue();
        assertThat(sut.deliver(1, 10, List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery push = requireNonNull(pushed.poll(5, SECONDS));

        // Act
        MqttDelivery pull = received(receiver.receive());

        // Assert
        assertThat(pull.message()).isSameAs(push.message());
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(ready).isEmpty();
        push.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        sut.release(10);
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
    }

    @Test
    void pulling_respects_the_shared_byte_limit_and_reuses_it_after_handoff() throws Exception {
        // Arrange
        DeliveryLanes sut = lanes(10);
        DeliveryLanes.Receiver first = sut.registerPull(1, 1, request(8)).orElseThrow();
        DeliveryLanes.Receiver second = sut.registerPull(1, 2, request(8)).orElseThrow();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 6)).isTrue();

        // Act & Assert: per-subscription slots remain, but shared bytes are exhausted.
        assertThat(sut.deliver(1, 11, List.of(2L), qosOneMessage(), false, 6)).isFalse();
        assertThat(sut.retainedBytes()).isEqualTo(6);
        assertThat(first.receive()).isInstanceOf(ReceiveResult.Received.class);
        assertThat(sut.deliver(1, 11, List.of(2L), qosOneMessage(), false, 6)).isTrue();
        assertThat(second.receive()).isInstanceOf(ReceiveResult.Received.class);
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void pull_timeouts_and_interrupts_do_not_consume_the_next_delivery() throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, 1, request(1)).orElseThrow();

        // Act & Assert
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(receiver.receive(Duration.ofMillis(10))).isEqualTo(new ReceiveResult.TimedOut());
        assertThatThrownBy(() -> receiver.receive(Duration.ofNanos(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 10)).isTrue();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(receiver::receive).isInstanceOf(InterruptedException.class);
        } finally {
            Thread.interrupted();
        }
        assertThat(received(receiver.receive(Duration.ofSeconds(Long.MAX_VALUE))).message())
                .isEqualTo(message(1));
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void a_timed_wait_reports_normal_end_without_waiting_for_its_deadline() throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, 1, request(1)).orElseThrow();
        var waiting = new FutureTask<>(() -> receiver.receive(Duration.ofMinutes(5)));
        Thread worker = Thread.ofVirtual().start(waiting);
        try {
            await().until(() -> worker.getState() == Thread.State.TIMED_WAITING);

            // Act
            sut.close(null);

            // Assert: end is distinct from timeout, including subsequent zero-duration polls.
            assertThat(waiting.get(5, SECONDS)).isEqualTo(new ReceiveResult.Ended());
            assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
        } finally {
            waiting.cancel(true);
        }
    }

    @Test
    void waiting_receivers_share_deliveries_and_all_end_when_closed() throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, 1, request(256)).orElseThrow();
        var received = new LinkedBlockingQueue<MqttMessage>();
        var jobs = new ArrayList<FutureTask<Void>>();
        var workers = new ArrayList<Thread>();
        try {
            for (int i = 0; i < 32; i++) {
                var job = new FutureTask<Void>(() -> {
                    while (true) {
                        ReceiveResult next = receiver.receive();
                        if (next instanceof ReceiveResult.Ended) {
                            return null;
                        }
                        received.add(received(next).message());
                    }
                });
                jobs.add(job);
                workers.add(Thread.ofVirtual().start(job));
            }
            await().until(() -> workers.stream()
                    .allMatch(worker -> worker.getState() == Thread.State.WAITING));

            // Act
            for (int i = 0; i < 256; i++) {
                assertThat(sut.deliver(1, 0, List.of(1L), message(i), false, 1)).isTrue();
            }

            // Assert: one receiver takes each item, and close ends every loop normally.
            await().untilAsserted(() -> assertThat(received).hasSize(256));
            assertThat(received).doesNotHaveDuplicates();
            sut.remove(1);
            for (FutureTask<Void> job : jobs) {
                assertThat(job.get(5, SECONDS)).isNull();
            }
        } finally {
            sut.remove(1);
            jobs.forEach(job -> job.cancel(true));
        }
    }

    @Test
    void a_paused_pull_lane_holds_qos_one_until_a_refused_removal_resumes_it() throws Exception {
        // Arrange: the pull subscription is awaiting removal.
        DeliveryLanes.Receiver receiver = sut.registerPull(1, 1, request(2)).orElseThrow();
        sut.pause(1);

        // Act
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 1)).isTrue();

        // Assert: QoS 1 is held without acknowledgement, and QoS 0 is dropped.
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(ready).isEmpty();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);

        // Act & Assert: a refused removal hands the held delivery over before acknowledging it.
        sut.resume(1);
        assertThat(received(receiver.receive(Duration.ZERO)).message()).isEqualTo(qosOneMessage());
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
    }

    @Test
    void resuming_a_paused_pull_lane_wakes_every_waiting_receiver() throws Exception {
        // Arrange: two receivers wait while the paused lane holds two QoS 1 deliveries.
        DeliveryLanes.Receiver receiver = sut.registerPull(1, 1, request(2)).orElseThrow();
        sut.pause(1);
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(1, 11, List.of(1L), qosOneMessage(), false, 10)).isTrue();
        var jobs = new ArrayList<FutureTask<ReceiveResult>>();
        var workers = new ArrayList<Thread>();
        try {
            for (int i = 0; i < 2; i++) {
                var job = new FutureTask<>(receiver::receive);
                jobs.add(job);
                workers.add(Thread.ofVirtual().start(job));
            }
            await().until(() -> workers.stream()
                    .allMatch(worker -> worker.getState() == Thread.State.WAITING));

            // Act
            sut.resume(1);

            // Assert: no held delivery stays queued while a receiver is still waiting.
            for (FutureTask<ReceiveResult> job : jobs) {
                assertThat(job.get(5, SECONDS)).isInstanceOf(ReceiveResult.Received.class);
            }
        } finally {
            jobs.forEach(job -> job.cancel(true));
        }
    }

    @Test
    void removing_a_paused_pull_lane_abandons_held_qos_one_without_handing_it_off()
            throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, 1, request(1)).orElseThrow();
        sut.pause(1);
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Act: UNSUBACK removes the lane.
        sut.remove(1);

        // Assert
        assertThat(receiver.receive()).isEqualTo(new ReceiveResult.Ended());
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        assertThat(sut.abandonedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedLanes()).isZero();
        assertThat(sut.retainedDeliveries()).isZero();
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
        assertThat(sut.deliver(1, 0, List.of(1L), first, false, 10)).isTrue();
        assertThat(requireNonNull(entered.poll(5, SECONDS)).message()).isEqualTo(first);
        assertThat(sut.deliver(1, 0, List.of(1L), second, false, 20)).isTrue();
        assertThat(sut.deliver(1, 0, List.of(1L), message(3), false, 30)).isFalse();

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
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 10)).isTrue();

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
        assertThat(sut.deliver(1, 0, List.of(1L), message(2), false, 10)).isTrue();
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
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        assertThat(sut.deliver(1, 0, List.of(1L), message(2), false, 20)).isTrue();

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

        assertThat(sut.deliver(1, 0, ids, message(1), false, 10)).isTrue();
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
        assertThat(sut.deliver(1, 0, List.of(3L), message(2), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(3L);
        assertThat(sut.deliver(1, 0, List.of(1L, 2L), message(3), false, 10)).isTrue();
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
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(1L);

        // Act & Assert: discovering the full lane second does not reserve the empty one.
        assertThat(sut.deliver(1, 0, List.of(2L, 1L), message(2), false, 20)).isFalse();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(entered).isEmpty();

        // Act & Assert: refusing the fan-out leaves the empty lane's capacity available.
        assertThat(sut.deliver(1, 0, List.of(2L), message(3), false, 30)).isTrue();
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
        assertThat(sut.deliver(1, 0, List.of(1L, 2L), message(1), false, BYTE_LIMIT)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();

        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);

        // Act & Assert: returning only one handler keeps the shared byte reservation.
        firstRelease.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isEqualTo(1));
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);
        assertThat(sut.deliver(1, 0, List.of(1L), message(2), false, 1)).isFalse();

        // Act & Assert: the last handler returns the shared capacity.
        secondRelease.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        assertThat(sut.deliver(1, 0, List.of(1L), message(2), false, 1)).isTrue();
    }

    @Test
    void a_configured_large_packet_fits_but_a_single_packet_above_budget_does_not() {
        // Arrange
        DeliveryLanes sut = lanes(BYTE_LIMIT + 1);
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(2), _ -> waitFor(release))).isTrue();

        // Act & Assert: rejection preserves capacity for a packet at the configured limit.
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, BYTE_LIMIT + 2))
                .isFalse();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, BYTE_LIMIT + 1))
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
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(1, 0, List.of(1L), message(2), false, 20)).isTrue();

        // Act & Assert: pausing rejects new work without discarding the existing deliveries.
        sut.pause(1);
        assertThat(sut.deliver(1, 0, List.of(1L, 999L), message(3), false, 30)).isTrue();
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        release.countDown();
        assertThat(requireNonNull(entered.poll(5, SECONDS)).message()).isEqualTo(message(2));
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isZero());

        // Act & Assert: rolling back the pause permits another delivery.
        sut.resume(1);
        assertThat(sut.deliver(1, 0, List.of(1L), message(4), false, 40)).isTrue();
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
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(1, 0, List.of(1L), message(2), false, 20)).isTrue();
        assertThat(sut.deliver(1, 0, List.of(1L), message(3), false, 30)).isTrue();

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
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, BYTE_LIMIT)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act & Assert: a new generation cannot reuse the old handler's byte reservation.
        sut.close(null);
        assertThat(sut.register(2, 2, request(2), _ -> { })).isTrue();
        assertThat(sut.deliver(1, 0, List.of(2L), message(2), false, 1)).isTrue();
        assertThat(sut.deliver(2, 0, List.of(2L), message(2), false, 1)).isFalse();
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);

        // Act & Assert: returning the old handler permits delivery in the new generation.
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        assertThat(sut.deliver(2, 0, List.of(2L), message(2), false, 1)).isTrue();
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
            assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 1)).isTrue();
        }
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act & Assert: the global limit applies even though the lane has spare capacity.
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 1)).isFalse();
        assertThat(sut.retainedDeliveries()).isEqualTo(4096);

        // Act
        sut.close(null);

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
            assertThat(sut.deliver(1, 0, List.of(id), message(1), false, 1)).isTrue();
        }
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act & Assert: closing does not release thread reservations for running handlers.
        sut.close(null);
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
        assertThat(sut.deliver(7, 0, List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(sut.deliver(7, 0, List.of(1L), message(2), false, 20)).isTrue();

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
            assertThat(sut.deliver(7, 0, List.of(1L), message(1), false, 10)).isTrue();

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

    @Test
    void manual_ack_is_once_only_and_keeps_capacity_until_the_sequencer_releases_it()
            throws Exception {
        // Arrange: the handler returns without acknowledging its delivery.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var workers = new LinkedBlockingQueue<Thread>();
        assertThat(sut.register(1, 1, request(AckMode.MANUAL, 1), delivery -> {
            workers.add(Thread.currentThread());
            received.add(delivery);
        })).isTrue();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), true, 10)).isTrue();
        MqttDelivery delivery = requireNonNull(received.poll(5, SECONDS));
        Thread worker = requireNonNull(workers.poll(5, SECONDS));
        await().until(() -> worker.getState() == Thread.State.WAITING);

        // Assert: normal return neither acknowledges nor releases MANUAL delivery capacity.
        assertThat(delivery.dup()).isTrue();
        assertThat(ready).isEmpty();
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(sut.deliver(1, 11, List.of(1L), qosOneMessage(), false, 10)).isFalse();

        // Act: repeated acknowledgements represent one completed recipient condition.
        delivery.ack();
        delivery.ack();

        // Assert: ordered acknowledgement release is a separate boundary.
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        assertThat(ready).isEmpty();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        sut.release(10);
        sut.release(10);
        delivery.ack();
        assertThat(ready).isEmpty();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void on_receive_ack_precedes_invocation_but_keeps_running_handler_capacity()
            throws Exception {
        // Arrange: SERIAL delivery queues a second message behind the running first handler.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var acknowledgementsAtEntry = new LinkedBlockingQueue<Integer>();
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(AckMode.ON_RECEIVE, 2), delivery -> {
            acknowledgementsAtEntry.add(ready.size());
            received.add(delivery);
            delivery.ack();
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(received.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(1, 11, List.of(1L), qosOneMessage(), false, 20)).isTrue();

        // Assert: the queued message is not yet acknowledged, and explicit ack is a no-op.
        assertThat(acknowledgementsAtEntry.poll(5, SECONDS)).isEqualTo(1);
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        assertThat(ready).isEmpty();
        sut.release(10);
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        assertThat(sut.retainedBytes()).isEqualTo(30);

        // Act: returning the first handler lets ON_RECEIVE reach the second invocation.
        release.countDown();

        // Assert
        assertThat(received.poll(5, SECONDS)).isNotNull();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 11));
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isEqualTo(1));
        sut.release(11);
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
    }

    @Test
    void overlapping_ack_modes_use_independent_views_and_share_bytes_until_all_work_finishes()
            throws Exception {
        // Arrange: each recipient has a different acknowledgement condition.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch receiveRelease = release();
        CountDownLatch handlerRelease = release();
        assertThat(sut.register(1, 1, request(AckMode.ON_RECEIVE, 1), delivery -> {
            received.add(delivery);
            waitFor(receiveRelease);
        })).isTrue();
        assertThat(sut.register(1, 2, request(AckMode.AFTER_HANDLER, 1), delivery -> {
            received.add(delivery);
            delivery.ack();
            waitFor(handlerRelease);
        })).isTrue();
        var manual = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, 3, request(AckMode.MANUAL, 1), manual::add)).isTrue();
        MqttMessage message = qosOneMessage();
        assertThat(sut.deliver(1, 10, List.of(1L, 2L, 3L), message, false, 10)).isTrue();
        MqttDelivery first = requireNonNull(received.poll(5, SECONDS));
        MqttDelivery second = requireNonNull(received.poll(5, SECONDS));
        MqttDelivery third = requireNonNull(manual.poll(5, SECONDS));

        // Assert: the payload is shared; acknowledgement identity belongs to each recipient.
        assertThat(first).isNotSameAs(second).isNotSameAs(third);
        assertThat(second).isNotSameAs(third);
        assertThat(first.message()).isSameAs(message);
        assertThat(second.message()).isSameAs(message);
        assertThat(third.message()).isSameAs(message);
        assertThat(ready).isEmpty();
        assertThat(sut.retainedBytes()).isEqualTo(10);

        // Act & Assert: MANUAL completes once, but AFTER_HANDLER still prevents PUBACK.
        third.ack();
        third.ack();
        assertThat(ready).isEmpty();
        handlerRelease.countDown();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        assertThat(sut.retainedDeliveries()).isEqualTo(3);
        sut.release(10);
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isEqualTo(1));
        assertThat(sut.retainedBytes()).isEqualTo(10);

        // Act & Assert: a running ON_RECEIVE handler keeps the final shared byte reservation.
        receiveRelease.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        assertThat(ready).isEmpty();
    }

    @Test
    void completed_handlers_behind_a_sequencer_head_keep_their_delivery_budget()
            throws Exception {
        // Arrange: both handlers complete, but the sequencer releases neither receipt yet.
        assertThat(sut.register(1, 1, request(AckMode.AFTER_HANDLER, 2), _ -> { })).isTrue();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(1, 11, List.of(1L), qosOneMessage(), false, 20)).isTrue();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 11));

        // Act & Assert
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        assertThat(sut.retainedBytes()).isEqualTo(30);
        assertThat(sut.deliver(1, 12, List.of(1L), qosOneMessage(), false, 1)).isFalse();
        sut.release(10);
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(20);
        sut.release(11);
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void removing_a_lane_discards_queued_ack_conditions_but_keeps_delivered_manual_work()
            throws Exception {
        // Arrange: one MANUAL delivery is running and another waits behind it.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(AckMode.MANUAL, 2), delivery -> {
            received.add(delivery);
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery delivery = requireNonNull(received.poll(5, SECONDS));
        assertThat(sut.deliver(1, 11, List.of(1L), qosOneMessage(), false, 20)).isTrue();

        // Act: successful removal abandons only the work not yet handed to the application.
        sut.remove(1);
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedLanes()).isZero());

        // Assert: the delivered view still requires its explicit ack after handler return.
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 11));
        assertThat(ready).isEmpty();
        assertThat(received).isEmpty();
        assertThat(sut.abandonedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        delivery.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        sut.release(10);
        sut.release(11);
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void connection_end_retires_manual_ack_waits_but_keeps_running_work_across_reconnect()
            throws Exception {
        // Arrange: the first handler returns without ack; the second still owns its payload.
        var completed = new LinkedBlockingQueue<MqttDelivery>();
        var workers = new LinkedBlockingQueue<Thread>();
        var running = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch release = release();
        assertThat(sut.register(1, 1, request(AckMode.MANUAL, 1), delivery -> {
            workers.add(Thread.currentThread());
            completed.add(delivery);
        })).isTrue();
        assertThat(sut.register(1, 2, request(AckMode.MANUAL, 1), delivery -> {
            running.add(delivery);
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery finished = requireNonNull(completed.poll(5, SECONDS));
        Thread worker = requireNonNull(workers.poll(5, SECONDS));
        await().until(() -> worker.getState() == Thread.State.WAITING);
        assertThat(sut.deliver(1, 11, List.of(2L), qosOneMessage(), false, 20)).isTrue();
        MqttDelivery executing = requireNonNull(running.poll(5, SECONDS));

        // Act & Assert: ending the connection releases only locally completed reservations.
        sut.close(null);
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(20);
        assertThat(sut.register(2, 3, request(AckMode.MANUAL, 1), completed::add)).isTrue();
        assertThat(sut.deliver(2, 12, List.of(3L), qosOneMessage(), false, 30)).isTrue();
        MqttDelivery fresh = requireNonNull(completed.poll(5, SECONDS));
        finished.ack();
        executing.ack();
        assertThat(ready).isEmpty();
        fresh.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(2, 12));
        sut.release(12);
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
    }

    @Test
    void absent_recipients_do_not_leave_an_ack_obligation() throws Exception {
        // Act & Assert: zero admitted recipients still reports readiness for this QoS 1 packet.
        assertThat(sut.deliver(1, 10, List.of(99L), qosOneMessage(), false, 10)).isTrue();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void a_paused_lane_keeps_qos_one_for_a_refused_removal_and_drops_qos_zero() throws Exception {
        // Arrange: the subscription is awaiting removal.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, 1, request(AckMode.AFTER_HANDLER, 2), received::add)).isTrue();
        sut.pause(1);

        // Act
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 1)).isTrue();

        // Assert: the QoS 1 delivery keeps its acknowledgement obligation.
        assertThat(ready).isEmpty();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);

        // Act: a refused removal resumes the lane.
        sut.resume(1);

        // Assert: the held delivery reaches the handler before it is acknowledged.
        MqttDelivery delivery = requireNonNull(received.poll(5, SECONDS));
        assertThat(delivery.message().qos()).isEqualTo(Qos.AT_LEAST_ONCE);
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        assertThat(received).isEmpty();
    }

    @Test
    void removal_abandons_qos_one_held_by_a_paused_lane_without_running_it() throws Exception {
        // Arrange
        var received = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, 1, request(AckMode.AFTER_HANDLER, 1), received::add)).isTrue();
        sut.pause(1);
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Act: UNSUBACK removes the lane.
        sut.remove(1);

        // Assert: the abandoned delivery releases its condition, and the worker never ran it.
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));
        await().untilAsserted(() -> assertThat(sut.retainedLanes()).isZero());
        assertThat(received).isEmpty();
        assertThat(sut.abandonedDeliveries()).isEqualTo(1);
        sut.release(10);
        assertThat(sut.retainedDeliveries()).isZero();
    }

    @Test
    void a_failing_after_handler_does_not_ack_even_if_application_calls_ack() throws Exception {
        // Arrange
        var cause = new IllegalStateException("handler failed");
        assertThat(sut.register(1, 1, request(AckMode.AFTER_HANDLER, 1), delivery -> {
            delivery.ack();
            throw cause;
        })).isTrue();

        // Act
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Assert: failure notification retires the connection; it is never a successful ack.
        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(1, cause));
        assertThat(ready).isEmpty();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        sut.close(null);
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void a_failed_handler_withholds_puback_even_after_acknowledging() throws Exception {
        // Arrange: the failing recipient acknowledges first; another recipient still runs.
        var cause = new IllegalStateException("handler failed after ack");
        var completed = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch otherRelease = release();
        assertThat(sut.register(1, 2, request(AckMode.AFTER_HANDLER, 2), delivery -> {
            if (delivery.message().qos() == Qos.AT_LEAST_ONCE) {
                waitFor(otherRelease);
            }
            completed.add(delivery);
        })).isTrue();
        assertThat(sut.register(1, 1, request(AckMode.MANUAL, 1), delivery -> {
            delivery.ack();
            throw cause;
        })).isTrue();

        // Act: the other recipient finishes only after the failure.
        assertThat(sut.deliver(1, 10, List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(1, cause));
        otherRelease.countDown();
        assertThat(sut.deliver(1, 0, List.of(2L), message(1), false, 1)).isTrue();

        // Assert: the later QoS 0 handler runs only after the shared delivery's completion.
        assertThat(completed.poll(5, SECONDS)).isNotNull();
        assertThat(completed.poll(5, SECONDS)).isNotNull();
        assertThat(ready).isEmpty();
    }

    @Test
    void a_failed_lane_keeps_its_queued_deliveries_unacknowledged() throws Exception {
        // Arrange: the failing lane has a shared delivery queued behind its running handler.
        var cause = new IllegalStateException("handler failed");
        var entered = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch failRelease = release();
        assertThat(sut.register(1, 1, request(AckMode.ON_RECEIVE, 2), delivery -> {
            entered.add(delivery);
            waitFor(failRelease);
            throw cause;
        })).isTrue();
        var completed = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch otherRelease = release();
        assertThat(sut.register(1, 2, request(AckMode.AFTER_HANDLER, 2), delivery -> {
            if (delivery.message().qos() == Qos.AT_LEAST_ONCE) {
                waitFor(otherRelease);
            }
            completed.add(delivery);
        })).isTrue();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(1, 11, List.of(1L, 2L), qosOneMessage(), false, 20)).isTrue();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(new Ready(1, 10));

        // Act: the first lane fails, then the other recipient finishes the shared delivery.
        failRelease.countDown();
        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(1, cause));
        otherRelease.countDown();
        assertThat(sut.deliver(1, 0, List.of(2L), message(1), false, 1)).isTrue();

        // Assert: the later QoS 0 handler runs only after the shared delivery's completion.
        assertThat(completed.poll(5, SECONDS)).isNotNull();
        assertThat(completed.poll(5, SECONDS)).isNotNull();
        assertThat(ready).isEmpty();
    }

    @Test
    void qos_zero_manual_delivery_does_not_wait_for_ack() throws Exception {
        // Arrange
        var received = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, 1, request(AckMode.MANUAL, 1), received::add)).isTrue();

        // Act
        assertThat(sut.deliver(1, 0, List.of(1L), message(1), false, 10)).isTrue();
        MqttDelivery delivery = requireNonNull(received.poll(5, SECONDS));

        // Assert
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        delivery.ack();
        assertThat(ready).isEmpty();
    }

    @Test
    void removing_a_deep_queue_records_one_snapshot_without_per_delivery_notifications()
            throws Exception {
        // Arrange: the first handler keeps 320 subsequent deliveries queued.
        var wakes = new LinkedBlockingQueue<Long>();
        DeliveryLanes sut = lanes(BYTE_LIMIT, wakes::add);
        CountDownLatch release = release();
        var entered = new CountDownLatch(1);
        assertThat(sut.register(1, 1, request(AckMode.ON_RECEIVE, 321), _ -> {
            entered.countDown();
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(1, 1, List.of(1L), qosOneMessage(), false, 1)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(wakes.poll(5, SECONDS)).isEqualTo(1L);
        assertThat(sut.takeReady(1)).containsExactly(1L);
        sut.release(1);
        for (long token = 2; token <= 321; token++) {
            assertThat(sut.deliver(1, token, List.of(1L), qosOneMessage(), false, 1)).isTrue();
        }

        // Act
        sut.remove(1);

        // Assert: reservations wait for send admission while a single wake covers the whole batch.
        assertThat(wakes).containsExactly(1L);
        assertThat(sut.takeReady(1)).isEqualTo(LongStream.rangeClosed(2, 321).boxed().toList());
        assertThat(sut.takeReady(1)).isEmpty();
        assertThat(sut.retainedDeliveries()).isEqualTo(321);
        for (long token = 2; token <= 321; token++) {
            sut.release(token);
        }
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
    }

    @Test
    void a_completion_after_a_snapshot_is_reported_again_and_double_ack_is_ignored()
            throws Exception {
        // Arrange
        var wakes = new LinkedBlockingQueue<Long>();
        DeliveryLanes sut = lanes(BYTE_LIMIT, wakes::add);
        var received = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, 1, request(AckMode.MANUAL, 2), received::add)).isTrue();
        assertThat(sut.deliver(1, 10, List.of(1L), qosOneMessage(), false, 1)).isTrue();
        assertThat(sut.deliver(1, 11, List.of(1L), qosOneMessage(), false, 1)).isTrue();
        MqttDelivery first = requireNonNull(received.poll(5, SECONDS));
        MqttDelivery second = requireNonNull(received.poll(5, SECONDS));

        // Act
        first.ack();
        assertThat(wakes.poll(5, SECONDS)).isEqualTo(1L);
        assertThat(sut.takeReady(1)).containsExactly(10L);
        second.ack();
        first.ack();
        second.ack();

        // Assert
        assertThat(wakes).containsExactly(1L);
        assertThat(sut.takeReady(1)).containsExactly(11L);
    }

    @Test
    void close_drops_pending_ready_tokens_without_signalling_and_old_snapshots_leave_new_work() {
        // Arrange: an empty recipient set records only a token, without retaining payload bytes.
        var wakes = new LinkedBlockingQueue<Long>();
        DeliveryLanes sut = lanes(1, wakes::add);
        for (long token = 1; token <= 4096; token++) {
            assertThat(sut.deliver(1, token, List.of(), qosOneMessage(), false, 10)).isTrue();
        }
        wakes.clear();
        assertThat(sut.retainedBytes()).isZero();

        // Act
        sut.close(null);

        // Assert
        assertThat(wakes).isEmpty();
        assertThat(sut.takeReady(1)).isEmpty();

        // Act
        assertThat(sut.deliver(2, 4097, List.of(), qosOneMessage(), false, 10)).isTrue();

        // Assert: a stale drain cannot consume a newer connection's readiness.
        assertThat(sut.takeReady(1)).isEmpty();
        assertThat(sut.takeReady(2)).containsExactly(4097L);
        assertThat(sut.retainedBytes()).isZero();
    }

    private static Subscribe request(AckMode ackMode, int capacity) {
        return Subscribe.of(new TopicFilter("#"), Qos.AT_LEAST_ONCE)
                .withAckMode(ackMode)
                .withBufferCapacity(capacity);
    }

    private static MqttMessage qosOneMessage() {
        return new MqttMessage(new Topic("a"), new byte[] {1}, Qos.AT_LEAST_ONCE, false);
    }

    private DeliveryLanes lanes(long byteLimit) {
        var owner = new AtomicReference<DeliveryLanes>();
        DeliveryLanes manager = lanes(byteLimit, generation ->
                requireNonNull(owner.get()).takeReady(generation)
                        .forEach(token -> ready.add(new Ready(generation, token))));
        owner.set(manager);
        return manager;
    }

    private DeliveryLanes lanes(long byteLimit, LongConsumer onReady) {
        var manager = new DeliveryLanes(
                "lane-test",
                byteLimit,
                (generation, cause) -> failures.add(new Failure(generation, cause)),
                onReady
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
        return Subscribe.of(new TopicFilter("#"), Qos.AT_MOST_ONCE).withBufferCapacity(capacity);
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

    private record Ready(long generation, long token) {
    }

    private record Failure(long generation, Throwable cause) {
    }

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }
}
