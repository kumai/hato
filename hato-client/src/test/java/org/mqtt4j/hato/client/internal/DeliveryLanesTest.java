package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.LongStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@Timeout(10)
class DeliveryLanesTest {

    private static final int BYTE_LIMIT = 32 * 1024 * 1024;

    private static final long SESSION = 1;

    private final LinkedBlockingQueue<Failure> failures = new LinkedBlockingQueue<>();

    private final LinkedBlockingQueue<Long> ready = new LinkedBlockingQueue<>();

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
    void connection_end_discards_queued_pull_work_and_retires_handed_off_acknowledgements()
            throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(
                1, request(AckMode.MANUAL, 2)
        ).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery handedOff = received(receiver.receive());
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 20)).isTrue();

        // Act
        sut.connectionEnded(false, false);

        // Assert
        assertThat(sut.abandonedDeliveries()).isZero();
        assertThat(sut.discardedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedLanes()).isEqualTo(1);
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(ready).isEmpty();
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(sut.deliver(token(2, 12), List.of(1L), qosOneMessage(), false, 30)).isTrue();
        MqttDelivery current = received(receiver.receive());
        handedOff.ack();
        assertThat(ready).isEmpty();
        assertThat(sut.retainedBytes()).isEqualTo(30);
        current.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(12L);
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void connection_end_discards_pull_buffers_and_keeps_receiving() throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(1)).orElseThrow();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();

        // Act
        sut.connectionEnded(false, false);

        // Assert
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(sut.abandonedDeliveries()).isZero();
        assertThat(sut.discardedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedLanes()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void a_continuing_session_keeps_handed_manual_conditions_but_discards_queued_receipts()
            throws Exception {
        DeliveryLanes.Receiver receiver = sut.registerPull(
                1,
                request(AckMode.MANUAL, 2)
        ).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery original = received(receiver.receive());
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 20)).isTrue();

        assertThat(sut.connectionEnded(true, false)).containsExactlyEntriesOf(Map.of(10L, false));
        assertThat(sut.discardedDeliveries()).isEqualTo(1);
        assertThat(sut.acknowledgingDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(sut.deliver(token(2, 10), List.of(1L), qosOneMessage(), true, 10)).isTrue();
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());

        original.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(sut.receiveState()).containsExactlyEntriesOf(Map.of(10L, true));
        assertThat(sut.isReady(10)).isTrue();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void retransmission_recreates_only_discarded_original_recipients() throws Exception {
        DeliveryLanes.Receiver completed = sut.registerPull(1, request(1)).orElseThrow();
        DeliveryLanes.Receiver queued = sut.registerPull(2, request(1)).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        assertThat(received(completed.receive()).dup()).isFalse();

        assertThat(sut.connectionEnded(true, false)).containsExactlyEntriesOf(Map.of(10L, false));
        assertThat(sut.retainedBytes()).isZero();
        DeliveryLanes.Receiver added = sut.registerPull(3, request(1)).orElseThrow();
        assertThat(sut.deliver(token(2, 10), List.of(1L, 2L, 3L), qosOneMessage(), true, 10))
                .isTrue();

        assertThat(completed.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(added.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(received(queued.receive()).dup()).isTrue();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(sut.receiveState()).containsExactlyEntriesOf(Map.of(10L, true));
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void a_duplicate_of_queued_work_does_not_reserve_or_queue_another_view() throws Exception {
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(1)).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();

        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), true, 10)).isTrue();

        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(received(receiver.receive()).dup()).isFalse();
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
    }

    @Test
    void queue_admission_releases_capacity_but_only_forget_retires_completed_receipt_metadata()
            throws Exception {
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(1)).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        received(receiver.receive());
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);

        sut.release(10);

        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.acknowledgingDeliveries()).isEqualTo(1);
        assertThat(sut.connectionEnded(true, false)).containsExactlyEntriesOf(Map.of(10L, true));
        assertThat(sut.deliver(token(2, 10), List.of(1L), qosOneMessage(), true, 10)).isTrue();
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(sut.isReady(10)).isTrue();

        sut.forget(10);
        sut.forget(10);

        assertThat(sut.receiveState()).isEmpty();
        assertThat(sut.acknowledgingDeliveries()).isZero();
        assertThat(sut.isReady(10)).isFalse();
    }

    @Test
    void discarding_the_session_retires_kept_conditions_and_their_push_capacity()
            throws Exception {
        // Arrange: a finished push delivery keeps its slot until its PUBACK enters the queue.
        var handled = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 1), handled::add)).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(handled.poll(5, SECONDS)).isNotNull();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(sut.connectionEnded(true, false)).containsExactlyEntriesOf(Map.of(10L, true));
        assertThat(sut.retainedDeliveries()).isEqualTo(1);

        // Act: Session Present 0 or permanent closure discards the continued session.
        sut.sessionDiscarded();

        // Assert
        assertThat(sut.receiveState()).isEmpty();
        assertThat(sut.acknowledgingDeliveries()).isZero();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void discarding_the_session_resumes_a_lane_paused_by_its_handler_failure() throws Exception {
        // Arrange: the accepted failure pauses the lane, as when it owns the termination slot.
        DeliveryLanes sut = lanes(BYTE_LIMIT, () -> { }, (token, cause) -> {
            failures.add(new Failure(token, cause));
            return true;
        });
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var cause = new IllegalStateException("old session handler failed");
        var first = new AtomicBoolean(true);
        assertThat(sut.register(1, request(1), delivery -> {
            received.add(delivery);
            if (first.getAndSet(false)) {
                throw cause;
            }
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(received.poll(5, SECONDS)).isNotNull();
        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(token(1, 0), cause));
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 10)).isTrue();

        // Act: that failure no longer ends a connection once its session is discarded.
        sut.sessionDiscarded();

        // Assert
        assertThat(requireNonNull(received.poll(5, SECONDS)).message()).isEqualTo(message(2));
    }

    @Test
    void stopping_invalidates_unready_handed_views_and_keeps_completed_recipient_conditions()
            throws Exception {
        DeliveryLanes.Receiver manual = sut.registerPull(
                1,
                request(AckMode.MANUAL, 1)
        ).orElseThrow();
        DeliveryLanes.Receiver completed = sut.registerPull(2, request(1)).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery original = received(manual.receive());
        received(completed.receive());

        assertThat(sut.connectionEnded(true, true)).containsExactlyEntriesOf(Map.of(10L, false));
        assertThat(sut.retainedBytes()).isZero();
        original.ack();
        assertThat(sut.isReady(10)).isFalse();
        assertThat(sut.deliver(token(2, 10), List.of(1L, 2L), qosOneMessage(), true, 10)).isTrue();
        MqttDelivery replacement = received(manual.receive());
        assertThat(replacement.dup()).isTrue();
        assertThat(completed.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        original.ack();
        assertThat(ready).isEmpty();

        replacement.ack();

        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(sut.receiveState()).containsExactlyEntriesOf(Map.of(10L, true));
    }

    @Test
    void a_running_invalidated_handler_cannot_ack_its_replacement_or_run_it_in_parallel()
            throws Exception {
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var workers = new LinkedBlockingQueue<Thread>();
        CountDownLatch originalRelease = release();
        CountDownLatch replacementRelease = release();
        var first = new AtomicBoolean(true);
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 2), delivery -> {
            workers.add(Thread.currentThread());
            received.add(delivery);
            waitFor(first.getAndSet(false) ? originalRelease : replacementRelease);
        })).isTrue();
        DeliveryLanes.Receiver completed = sut.registerPull(2, request(1)).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        assertThat(received.poll(5, SECONDS)).isNotNull();
        Thread worker = requireNonNull(workers.poll(5, SECONDS));
        received(completed.receive());

        assertThat(sut.connectionEnded(true, true)).containsExactlyEntriesOf(Map.of(10L, false));
        assertThat(sut.deliver(token(2, 10), List.of(1L, 2L), qosOneMessage(), true, 10)).isTrue();
        assertThat(received).isEmpty();
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        originalRelease.countDown();

        assertThat(requireNonNull(received.poll(5, SECONDS)).dup()).isTrue();
        assertThat(workers.poll(5, SECONDS)).isSameAs(worker);
        assertThat(sut.isReady(10)).isFalse();
        assertThat(ready).isEmpty();
        replacementRelease.countDown();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        sut.release(10);
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
    }

    @Test
    void failure_after_manual_ack_withdraws_stale_readiness_and_replays_only_the_failed_view()
            throws Exception {
        var wakes = new LinkedBlockingQueue<Boolean>();
        DeliveryLanes sut = lanes(BYTE_LIMIT, () -> wakes.add(true));
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var cause = new IllegalStateException("failed after acknowledgement");
        CountDownLatch failRelease = release();
        var first = new AtomicBoolean(true);
        assertThat(sut.register(1, request(AckMode.MANUAL, 1), delivery -> {
            received.add(delivery);
            delivery.ack();
            if (first.getAndSet(false)) {
                waitFor(failRelease);
                throw cause;
            }
        })).isTrue();
        DeliveryLanes.Receiver completed = sut.registerPull(2, request(1)).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery original = requireNonNull(received.poll(5, SECONDS));
        received(completed.receive());
        assertThat(wakes.poll(5, SECONDS)).isTrue();
        assertThat(sut.isReady(10)).isTrue();

        failRelease.countDown();

        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(token(1, 10), cause));
        assertThat(sut.takeReady()).isEmpty();
        assertThat(sut.isReady(10)).isFalse();
        assertThat(sut.connectionEnded(true, true)).containsExactlyEntriesOf(Map.of(10L, false));
        original.ack();
        assertThat(sut.deliver(token(2, 10), List.of(1L, 2L), qosOneMessage(), true, 10)).isTrue();
        assertThat(requireNonNull(received.poll(5, SECONDS)).dup()).isTrue();
        assertThat(completed.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(wakes.poll(5, SECONDS)).isTrue();
        assertThat(sut.takeReady()).containsExactly(10L);
        sut.release(10);
    }

    @Test
    void a_failure_while_disconnected_updates_the_next_snapshot_and_leaves_the_lane_usable()
            throws Exception {
        DeliveryLanes sut = lanes(BYTE_LIMIT, () -> { }, (token, cause) -> {
            failures.add(new Failure(token, cause));
            return false;
        });
        var received = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch failRelease = release();
        var cause = new IllegalStateException("offline handler failed");
        var first = new AtomicBoolean(true);
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 1), delivery -> {
            received.add(delivery);
            if (first.getAndSet(false)) {
                waitFor(failRelease);
                throw cause;
            }
        })).isTrue();
        DeliveryLanes.Receiver completed = sut.registerPull(2, request(1)).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        assertThat(received.poll(5, SECONDS)).isNotNull();
        received(completed.receive());
        assertThat(sut.connectionEnded(true, false)).containsExactlyEntriesOf(Map.of(10L, false));

        failRelease.countDown();

        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(token(1, 10), cause));
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        assertThat(sut.receiveState()).containsExactlyEntriesOf(Map.of(10L, false));
        assertThat(sut.deliver(token(2, 10), List.of(1L, 2L), qosOneMessage(), true, 10)).isTrue();
        assertThat(requireNonNull(received.poll(5, SECONDS)).dup()).isTrue();
        await().untilAsserted(() -> assertThat(sut.isReady(10)).isTrue());
        assertThat(completed.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        sut.release(10);
    }

    @Test
    void empty_recipient_retransmissions_report_readiness_once_per_snapshot() {
        DeliveryLanes sut = lanes(BYTE_LIMIT, () -> { });
        assertThat(sut.deliver(token(1, 10), List.of(), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(), qosOneMessage(), true, 10)).isTrue();

        assertThat(sut.isReady(10)).isTrue();
        assertThat(sut.takeReady()).containsExactly(10L);
        assertThat(sut.takeReady()).isEmpty();
        assertThat(sut.connectionEnded(true, false)).isEmpty();
        assertThat(sut.acknowledgingDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void pulling_releases_local_capacity_before_the_wire_ack_is_admitted() throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(1)).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Act
        MqttDelivery first = received(receiver.receive(Duration.ZERO));

        // Assert: a stalled sequencer retains metadata, not this subscription's payload budget.
        assertThat(first.message()).isEqualTo(qosOneMessage());
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.acknowledgingDeliveries()).isEqualTo(1);
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 20)).isTrue();
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
                1, request(AckMode.MANUAL, 1)
        ).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Act & Assert
        MqttDelivery delivery = received(receiver.receive());
        assertThat(ready).isEmpty();
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 20)).isFalse();
        delivery.ack();
        delivery.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(ready).isEmpty();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 20)).isTrue();
    }

    @Test
    void overlapping_push_and_pull_views_share_bytes_but_have_independent_ack_conditions()
            throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(1)).orElseThrow();
        var pushed = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(2, request(AckMode.MANUAL, 1), pushed::add)).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery push = requireNonNull(pushed.poll(5, SECONDS));

        // Act
        MqttDelivery pull = received(receiver.receive());

        // Assert
        assertThat(pull.message()).isSameAs(push.message());
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(ready).isEmpty();
        push.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        sut.release(10);
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
    }

    @Test
    void pulling_respects_the_shared_byte_limit_and_reuses_it_after_handoff() throws Exception {
        // Arrange
        DeliveryLanes sut = lanes(10);
        DeliveryLanes.Receiver first = sut.registerPull(1, request(8)).orElseThrow();
        DeliveryLanes.Receiver second = sut.registerPull(2, request(8)).orElseThrow();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 6)).isTrue();

        // Act & Assert: per-subscription slots remain, but shared bytes are exhausted.
        assertThat(sut.deliver(token(1, 11), List.of(2L), qosOneMessage(), false, 6)).isFalse();
        assertThat(sut.retainedBytes()).isEqualTo(6);
        assertThat(first.receive()).isInstanceOf(ReceiveResult.Received.class);
        assertThat(sut.deliver(token(1, 11), List.of(2L), qosOneMessage(), false, 6)).isTrue();
        assertThat(second.receive()).isInstanceOf(ReceiveResult.Received.class);
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void pull_timeouts_and_interrupts_do_not_consume_the_next_delivery() throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(1)).orElseThrow();

        // Act & Assert
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(receiver.receive(Duration.ofMillis(10))).isEqualTo(new ReceiveResult.TimedOut());
        assertThatThrownBy(() -> receiver.receive(Duration.ofNanos(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
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
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(1)).orElseThrow();
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
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(256)).orElseThrow();
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
                assertThat(sut.deliver(token(1, 0), List.of(1L), message(i), false, 1)).isTrue();
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
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(2)).orElseThrow();
        sut.pause(1);

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isTrue();

        // Assert: QoS 1 is held without acknowledgement, and QoS 0 is dropped.
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(ready).isEmpty();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);

        // Act & Assert: a refused removal hands the held delivery over before acknowledging it.
        sut.resume(1);
        assertThat(received(receiver.receive(Duration.ZERO)).message()).isEqualTo(qosOneMessage());
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
    }

    @Test
    void resuming_a_paused_pull_lane_wakes_every_waiting_receiver() throws Exception {
        // Arrange: two receivers wait while the paused lane holds two QoS 1 deliveries.
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(2)).orElseThrow();
        sut.pause(1);
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 10)).isTrue();
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
        DeliveryLanes.Receiver receiver = sut.registerPull(1, request(1)).orElseThrow();
        sut.pause(1);
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Act: UNSUBACK removes the lane.
        sut.remove(1);

        // Assert
        assertThat(receiver.receive()).isEqualTo(new ReceiveResult.Ended());
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
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
        assertThat(sut.register(1, request(2), delivery -> {
            virtual.set(Thread.currentThread().isVirtual());
            entered.add(delivery);
            waitFor(release);
        })).isTrue();
        MqttMessage first = message(1);
        MqttMessage second = message(2);

        // Act & Assert: a running handler consumes one of the two per-subscription slots.
        assertThat(sut.deliver(token(1, 0), List.of(1L), first, false, 10)).isTrue();
        assertThat(requireNonNull(entered.poll(5, SECONDS)).message()).isEqualTo(first);
        assertThat(sut.deliver(token(1, 0), List.of(1L), second, false, 20)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(3), false, 30)).isFalse();

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
        assertThat(sut.register(1, request(1), _ -> {
            interruptStates.add(Thread.currentThread().isInterrupted());
            workers.add(Thread.currentThread());
            Thread.currentThread().interrupt();
        })).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();

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
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 10)).isTrue();
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
        assertThat(sut.register(1, request(2), _ -> {
            interruptStates.add(Thread.currentThread().isInterrupted());
            if (first.getAndSet(false)) {
                entered.countDown();
                waitFor(release);
                Thread.currentThread().interrupt();
            }
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(interruptStates.poll(5, SECONDS)).isFalse();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 20)).isTrue();

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
            assertThat(sut.register(id, request(1), _ -> {
                workers.add(Thread.currentThread());
                entered.add(id);
            })).isTrue();
        }

        assertThat(sut.deliver(token(1, 0), ids, message(1), false, 10)).isTrue();
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
        assertThat(sut.deliver(token(1, 0), List.of(3L), message(2), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(3L);
        assertThat(sut.deliver(token(1, 0), List.of(1L, 2L), message(3), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(1L);
        assertThat(entered).isEmpty();
        assertThat(sut.abandonedDeliveries()).isZero();
    }

    @Test
    void fanout_reserves_all_recipients_or_none() throws Exception {
        // Arrange: the first lane is full and the second lane is empty.
        var entered = new LinkedBlockingQueue<Long>();
        CountDownLatch release = release();
        assertThat(sut.register(1, request(1), _ -> {
            entered.add(1L);
            waitFor(release);
        })).isTrue();
        assertThat(sut.register(2, request(1), _ -> entered.add(2L))).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(1L);

        // Act & Assert: discovering the full lane second does not reserve the empty one.
        assertThat(sut.deliver(token(1, 0), List.of(2L, 1L), message(2), false, 20)).isFalse();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(entered).isEmpty();

        // Act & Assert: refusing the fan-out leaves the empty lane's capacity available.
        assertThat(sut.deliver(token(1, 0), List.of(2L), message(3), false, 30)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(2L);
    }

    @Test
    void overlapping_recipients_share_bytes_until_the_last_handler_returns() throws Exception {
        // Arrange: wire sizes are supplied by framing, so no large test payload is needed.
        var entered = new CountDownLatch(2);
        CountDownLatch firstRelease = release();
        CountDownLatch secondRelease = release();
        assertThat(sut.register(1, request(2), _ -> {
            entered.countDown();
            waitFor(firstRelease);
        })).isTrue();
        assertThat(sut.register(2, request(2), _ -> {
            entered.countDown();
            waitFor(secondRelease);
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L, 2L), message(1), false, BYTE_LIMIT))
                .isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();

        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);

        // Act & Assert: returning only one handler keeps the shared byte reservation.
        firstRelease.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isEqualTo(1));
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 1)).isFalse();

        // Act & Assert: the last handler returns the shared capacity.
        secondRelease.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 1)).isTrue();
    }

    @Test
    void a_configured_large_packet_fits_but_a_single_packet_above_budget_does_not() {
        // Arrange
        DeliveryLanes sut = lanes(BYTE_LIMIT + 1);
        CountDownLatch release = release();
        assertThat(sut.register(1, request(2), _ -> waitFor(release))).isTrue();

        // Act & Assert: rejection preserves capacity for a packet at the configured limit.
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, BYTE_LIMIT + 2))
                .isFalse();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, BYTE_LIMIT + 1))
                .isTrue();
    }

    @Test
    void pausing_stops_admission_but_keeps_queued_work_and_can_be_rolled_back() throws Exception {
        // Arrange: one handler is running and another delivery is queued.
        var entered = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch release = release();
        assertThat(sut.register(1, request(2), delivery -> {
            entered.add(delivery);
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 20)).isTrue();

        // Act & Assert: pausing rejects new work without discarding the existing deliveries.
        sut.pause(1);
        assertThat(sut.deliver(token(1, 0), List.of(1L, 999L), message(3), false, 30)).isTrue();
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        release.countDown();
        assertThat(requireNonNull(entered.poll(5, SECONDS)).message()).isEqualTo(message(2));
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isZero());

        // Act & Assert: rolling back the pause permits another delivery.
        sut.resume(1);
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(4), false, 40)).isTrue();
        assertThat(requireNonNull(entered.poll(5, SECONDS)).message()).isEqualTo(message(4));
        assertThat(sut.abandonedDeliveries()).isZero();
    }

    @Test
    void removing_a_lane_discards_queued_work_and_keeps_the_running_handler() throws Exception {
        // Arrange: keep one handler running with two deliveries queued behind it.
        var entered = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch release = release();
        assertThat(sut.register(1, request(3), delivery -> {
            entered.add(delivery);
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 20)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(3), false, 30)).isTrue();

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
    void connection_end_retains_running_bytes_until_old_handlers_finish() throws Exception {
        // Arrange: one handler retains the entire byte budget.
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        assertThat(sut.register(1, request(2), _ -> {
            entered.countDown();
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, BYTE_LIMIT)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act & Assert: a new generation cannot reuse the old handler's byte reservation.
        sut.connectionEnded(false, false);
        assertThat(sut.register(2, request(2), _ -> { })).isTrue();
        assertThat(sut.deliver(token(2, 0), List.of(2L), message(2), false, 1)).isFalse();
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);

        // Act & Assert: returning the old handler permits delivery in the new generation.
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        assertThat(sut.deliver(token(2, 0), List.of(2L), message(2), false, 1)).isTrue();
    }

    @Test
    void the_global_delivery_limit_counts_running_and_queued_recipients() throws Exception {
        // Arrange: fill the delivery budget behind one running handler.
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        assertThat(sut.register(1, request(70_000), _ -> {
            entered.countDown();
            waitFor(release);
        })).isTrue();
        for (int i = 0; i < 65_536; i++) {
            assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isTrue();
        }
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act & Assert: the global limit applies even though the lane has spare capacity.
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isFalse();
        assertThat(sut.retainedDeliveries()).isEqualTo(65_536);

        // Act
        sut.close(null);

        // Assert: queued deliveries are abandoned; the executing handler keeps its reservation.
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(1);
        assertThat(sut.abandonedDeliveries()).isEqualTo(65_535);
    }

    @Test
    void stopped_handlers_keep_the_lane_limit_across_reconnections() throws Exception {
        // Arrange: every lane has a running handler.
        var entered = new CountDownLatch(256);
        CountDownLatch release = release();
        for (long id = 1; id <= 256; id++) {
            assertThat(sut.register(id, request(1), _ -> {
                entered.countDown();
                waitFor(release);
            })).isTrue();
            assertThat(sut.deliver(token(1, 0), List.of(id), message(1), false, 1)).isTrue();
        }
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act & Assert: closing does not release thread reservations for running handlers.
        sut.close(null);
        assertThat(sut.retainedLanes()).isEqualTo(256);
        assertThat(sut.register(257, request(1), _ -> { })).isFalse();

        // Act & Assert: returning the handlers permits a new lane.
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedLanes()).isZero());
        assertThat(sut.register(257, request(1), _ -> { })).isTrue();
    }

    @Test
    void interrupted_handler_errors_preserve_the_cause_and_discard_queued_work_on_connection_end()
            throws Exception {
        // Arrange: one handler will leave its interrupt flag set and throw with a delivery queued.
        var entered = new CountDownLatch(1);
        var cause = new AssertionError("application failure");
        var calls = new AtomicInteger();
        CountDownLatch release = release();
        assertThat(sut.register(1, request(2), _ -> {
            calls.incrementAndGet();
            entered.countDown();
            waitFor(release);
            Thread.currentThread().interrupt();
            throw cause;
        })).isTrue();
        assertThat(sut.deliver(token(7, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(sut.deliver(token(7, 0), List.of(1L), message(2), false, 20)).isTrue();

        // Act
        release.countDown();

        // Assert
        Failure failure = requireNonNull(failures.poll(5, SECONDS));
        assertThat(failure.token().connection()).isEqualTo(7);
        assertThat(failure.cause()).isSameAs(cause);
        sut.connectionEnded(false, false);
        assertThat(sut.retainedLanes()).isEqualTo(1);
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.abandonedDeliveries()).isEqualTo(1);
        assertThat(sut.discardedDeliveries()).isEqualTo(1);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void a_failure_after_connection_end_keeps_its_origin_and_resumes_the_lane() throws Exception {
        // Arrange: the handler outlives the connection that received its QoS 0 delivery.
        var mailbox = new Mailbox(1, 10, 1, 10, 1);
        mailbox.activate(7);
        DeliveryLanes sut = lanes(BYTE_LIMIT, () -> { }, (token, cause) -> {
            failures.add(new Failure(token, cause));
            return mailbox.terminate(new Mailbox.Event(
                    token.connection(), () -> { }, Mailbox.Kind.CONTROL, 0
            ));
        });
        var workers = new LinkedBlockingQueue<Thread>();
        CountDownLatch release = release();
        var cause = new IllegalStateException("old handler failed");
        var first = new AtomicBoolean(true);
        assertThat(sut.register(1, request(1), _ -> {
            workers.add(Thread.currentThread());
            if (first.getAndSet(false)) {
                waitFor(release);
                throw cause;
            }
        })).isTrue();
        assertThat(sut.deliver(token(7, 0), List.of(1L), message(1), false, 10)).isTrue();
        Thread worker = requireNonNull(workers.poll(5, SECONDS));
        mailbox.discard(7);
        assertThat(sut.retainedLanes()).isEqualTo(1);

        // Act: the failure arrives after admission ends but before connection cleanup.
        release.countDown();
        await().until(() -> worker.getState() == Thread.State.WAITING);
        sut.connectionEnded(false, false);

        // Assert: the callback retains the receipt's origin without retaining its resources.
        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(token(7, 0), cause));
        assertThat(sut.retainedLanes()).isEqualTo(1);
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(ready).isEmpty();

        // Act & Assert: the same worker takes new input after the next connection starts.
        mailbox.activate(8);
        assertThat(sut.deliver(token(8, 0), List.of(1L), message(2), false, 10)).isTrue();
        assertThat(workers.poll(5, SECONDS)).isSameAs(worker);
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isZero());
    }

    @Test
    void failure_uses_the_receiving_connection_after_the_acknowledgement_is_released()
            throws Exception {
        // Arrange: a lane itself has no connection; its second delivery fails after PUBACK.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var cause = new IllegalStateException("failed after PUBACK");
        CountDownLatch release = release();
        assertThat(sut.register(1, request(AckMode.MANUAL, 2), delivery -> {
            received.add(delivery);
            if (delivery.message().qos() == Qos.AT_LEAST_ONCE) {
                waitFor(release);
                throw cause;
            }
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isTrue();
        assertThat(received.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(token(2, 10), List.of(1L), qosOneMessage(), false, 1)).isTrue();
        requireNonNull(received.poll(5, SECONDS)).ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        sut.release(10);
        sut.forget(10);
        assertThat(sut.acknowledgingDeliveries()).isZero();

        // Act
        release.countDown();

        // Assert: the failure still names the second delivery's origin and original cause.
        assertThat(failures.poll(5, SECONDS))
                .isEqualTo(new Failure(token(2, 10), cause));
        assertThat(sut.retainedLanes()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isZero();
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
            assertThat(sut.register(1, request(1), _ -> {
                throw cause;
            })).isTrue();

            // Act
            assertThat(sut.deliver(token(7, 0), List.of(1L), message(1), false, 10)).isTrue();

            // Assert: the original cause is reported once and all reservations are released.
            Failure failure = requireNonNull(failures.poll(5, SECONDS));
            assertThat(failure.token().connection()).isEqualTo(7);
            assertThat(failure.cause()).isSameAs(cause);
            assertThat(sut.retainedLanes()).isEqualTo(1);
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
        assertThat(sut.register(1, request(AckMode.MANUAL, 1), delivery -> {
            workers.add(Thread.currentThread());
            received.add(delivery);
        })).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), true, 10)).isTrue();
        MqttDelivery delivery = requireNonNull(received.poll(5, SECONDS));
        Thread worker = requireNonNull(workers.poll(5, SECONDS));
        await().until(() -> worker.getState() == Thread.State.WAITING);

        // Assert: normal return neither acknowledges nor releases MANUAL delivery capacity.
        assertThat(delivery.dup()).isTrue();
        assertThat(ready).isEmpty();
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 10)).isFalse();

        // Act: repeated acknowledgements represent one completed recipient condition.
        delivery.ack();
        delivery.ack();

        // Assert: ordered acknowledgement release is a separate boundary.
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
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
        assertThat(sut.register(1, request(AckMode.ON_RECEIVE, 2), delivery -> {
            acknowledgementsAtEntry.add(ready.size());
            received.add(delivery);
            delivery.ack();
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(received.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 20)).isTrue();

        // Assert: the queued message is not yet acknowledged, and explicit ack is a no-op.
        assertThat(acknowledgementsAtEntry.poll(5, SECONDS)).isEqualTo(1);
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(ready).isEmpty();
        sut.release(10);
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        assertThat(sut.retainedBytes()).isEqualTo(30);

        // Act: returning the first handler lets ON_RECEIVE reach the second invocation.
        release.countDown();

        // Assert
        assertThat(received.poll(5, SECONDS)).isNotNull();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(11L);
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
        assertThat(sut.register(1, request(AckMode.ON_RECEIVE, 1), delivery -> {
            received.add(delivery);
            waitFor(receiveRelease);
        })).isTrue();
        assertThat(sut.register(2, request(AckMode.AFTER_HANDLER, 1), delivery -> {
            received.add(delivery);
            delivery.ack();
            waitFor(handlerRelease);
        })).isTrue();
        var manual = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(3, request(AckMode.MANUAL, 1), manual::add)).isTrue();
        MqttMessage message = qosOneMessage();
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L, 3L), message, false, 10)).isTrue();
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
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
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
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 2), _ -> { })).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 20)).isTrue();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(ready.poll(5, SECONDS)).isEqualTo(11L);

        // Act & Assert
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        assertThat(sut.retainedBytes()).isEqualTo(30);
        assertThat(sut.deliver(token(1, 12), List.of(1L), qosOneMessage(), false, 1)).isFalse();
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
        assertThat(sut.register(1, request(AckMode.MANUAL, 2), delivery -> {
            received.add(delivery);
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery delivery = requireNonNull(received.poll(5, SECONDS));
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 20)).isTrue();

        // Act: successful removal abandons only the work not yet handed to the application.
        sut.remove(1);
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedLanes()).isZero());

        // Assert: the delivered view still requires its explicit ack after handler return.
        assertThat(ready.poll(5, SECONDS)).isEqualTo(11L);
        assertThat(ready).isEmpty();
        assertThat(received).isEmpty();
        assertThat(sut.abandonedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        delivery.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
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
        assertThat(sut.register(1, request(AckMode.MANUAL, 1), delivery -> {
            workers.add(Thread.currentThread());
            completed.add(delivery);
        })).isTrue();
        assertThat(sut.register(2, request(AckMode.MANUAL, 1), delivery -> {
            running.add(delivery);
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery finished = requireNonNull(completed.poll(5, SECONDS));
        Thread worker = requireNonNull(workers.poll(5, SECONDS));
        await().until(() -> worker.getState() == Thread.State.WAITING);
        assertThat(sut.deliver(token(1, 11), List.of(2L), qosOneMessage(), false, 20)).isTrue();
        MqttDelivery executing = requireNonNull(running.poll(5, SECONDS));

        // Act & Assert: ending the connection releases only locally completed reservations.
        sut.close(null);
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(20);
        assertThat(sut.register(3, request(AckMode.MANUAL, 1), completed::add)).isTrue();
        assertThat(sut.deliver(token(2, 12), List.of(3L), qosOneMessage(), false, 30)).isTrue();
        MqttDelivery fresh = requireNonNull(completed.poll(5, SECONDS));
        finished.ack();
        executing.ack();
        assertThat(ready).isEmpty();
        fresh.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(12L);
        sut.release(12);
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
    }

    @Test
    void absent_recipients_do_not_leave_an_ack_obligation() throws Exception {
        // Act & Assert: zero admitted recipients still reports readiness for this QoS 1 packet.
        assertThat(sut.deliver(token(1, 10), List.of(99L), qosOneMessage(), false, 10)).isTrue();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void a_paused_lane_keeps_qos_one_for_a_refused_removal_and_drops_qos_zero() throws Exception {
        // Arrange: the subscription is awaiting removal.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 2), received::add)).isTrue();
        sut.pause(1);

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isTrue();

        // Assert: the QoS 1 delivery keeps its acknowledgement obligation.
        assertThat(ready).isEmpty();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);

        // Act: a refused removal resumes the lane.
        sut.resume(1);

        // Assert: the held delivery reaches the handler before it is acknowledged.
        MqttDelivery delivery = requireNonNull(received.poll(5, SECONDS));
        assertThat(delivery.message().qos()).isEqualTo(Qos.AT_LEAST_ONCE);
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(received).isEmpty();
    }

    @Test
    void removal_abandons_qos_one_held_by_a_paused_lane_without_running_it() throws Exception {
        // Arrange
        var received = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 1), received::add)).isTrue();
        sut.pause(1);
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Act: UNSUBACK removes the lane.
        sut.remove(1);

        // Assert: the abandoned delivery releases its condition, and the worker never ran it.
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
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
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 1), delivery -> {
            delivery.ack();
            throw cause;
        })).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Assert: failure notification retires the connection; it is never a successful ack.
        assertThat(failures.poll(5, SECONDS))
                .isEqualTo(new Failure(token(1, 10), cause));
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
        assertThat(sut.register(2, request(AckMode.AFTER_HANDLER, 2), delivery -> {
            if (delivery.message().qos() == Qos.AT_LEAST_ONCE) {
                waitFor(otherRelease);
            }
            completed.add(delivery);
        })).isTrue();
        assertThat(sut.register(1, request(AckMode.MANUAL, 1), delivery -> {
            delivery.ack();
            throw cause;
        })).isTrue();

        // Act: the other recipient finishes only after the failure.
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        assertThat(failures.poll(5, SECONDS))
                .isEqualTo(new Failure(token(1, 10), cause));
        otherRelease.countDown();
        assertThat(sut.deliver(token(1, 0), List.of(2L), message(1), false, 1)).isTrue();

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
        assertThat(sut.register(1, request(AckMode.ON_RECEIVE, 2), delivery -> {
            entered.add(delivery);
            waitFor(failRelease);
            throw cause;
        })).isTrue();
        var completed = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch otherRelease = release();
        assertThat(sut.register(2, request(AckMode.AFTER_HANDLER, 2), delivery -> {
            if (delivery.message().qos() == Qos.AT_LEAST_ONCE) {
                waitFor(otherRelease);
            }
            completed.add(delivery);
        })).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isNotNull();
        assertThat(sut.deliver(token(1, 11), List.of(1L, 2L), qosOneMessage(), false, 20)).isTrue();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);

        // Act: the first lane fails, then the other recipient finishes the shared delivery.
        failRelease.countDown();
        assertThat(failures.poll(5, SECONDS))
                .isEqualTo(new Failure(token(1, 10), cause));
        otherRelease.countDown();
        assertThat(sut.deliver(token(1, 0), List.of(2L), message(1), false, 1)).isTrue();

        // Assert: the later QoS 0 handler runs only after the shared delivery's completion.
        assertThat(completed.poll(5, SECONDS)).isNotNull();
        assertThat(completed.poll(5, SECONDS)).isNotNull();
        assertThat(ready).isEmpty();
    }

    @Test
    void qos_zero_manual_delivery_does_not_wait_for_ack() throws Exception {
        // Arrange
        var received = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, request(AckMode.MANUAL, 1), received::add)).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
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
        var wakes = new LinkedBlockingQueue<Boolean>();
        DeliveryLanes sut = lanes(BYTE_LIMIT, () -> wakes.add(true));
        CountDownLatch release = release();
        var entered = new CountDownLatch(1);
        assertThat(sut.register(1, request(AckMode.ON_RECEIVE, 321), _ -> {
            entered.countDown();
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(token(1, 1), List.of(1L), qosOneMessage(), false, 1)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(wakes.poll(5, SECONDS)).isTrue();
        assertThat(sut.takeReady()).containsExactly(1L);
        sut.release(1);
        for (long token = 2; token <= 321; token++) {
            assertThat(sut.deliver(token(1, token), List.of(1L), qosOneMessage(), false, 1))
                    .isTrue();
        }

        // Act
        sut.remove(1);

        // Assert: reservations wait for send admission while a single wake covers the whole batch.
        assertThat(wakes).containsExactly(true);
        assertThat(sut.takeReady()).isEqualTo(LongStream.rangeClosed(2, 321).boxed().toList());
        assertThat(sut.takeReady()).isEmpty();
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
        var wakes = new LinkedBlockingQueue<Boolean>();
        DeliveryLanes sut = lanes(BYTE_LIMIT, () -> wakes.add(true));
        var received = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, request(AckMode.MANUAL, 2), received::add)).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 1)).isTrue();
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 1)).isTrue();
        MqttDelivery first = requireNonNull(received.poll(5, SECONDS));
        MqttDelivery second = requireNonNull(received.poll(5, SECONDS));

        // Act
        first.ack();
        assertThat(wakes.poll(5, SECONDS)).isTrue();
        assertThat(sut.takeReady()).containsExactly(10L);
        second.ack();
        first.ack();
        second.ack();

        // Assert
        assertThat(wakes).containsExactly(true);
        assertThat(sut.takeReady()).containsExactly(11L);
    }

    @Test
    void close_drops_pending_ready_tokens_and_the_next_snapshot_takes_new_work() {
        // Arrange: an empty recipient set records only a token, without retaining payload bytes.
        var wakes = new LinkedBlockingQueue<Boolean>();
        DeliveryLanes sut = lanes(1, () -> wakes.add(true));
        for (long token = 1; token <= 4096; token++) {
            assertThat(sut.deliver(token(1, token), List.of(), qosOneMessage(), false, 10))
                    .isTrue();
        }
        wakes.clear();
        assertThat(sut.retainedBytes()).isZero();

        // Act
        sut.close(null);

        // Assert
        assertThat(wakes).isEmpty();
        assertThat(sut.takeReady()).isEmpty();

        // Act
        assertThat(sut.deliver(token(2, 4097), List.of(), qosOneMessage(), false, 10)).isTrue();

        // Assert: every wake takes current work; the core rejects tokens outside its receive table.
        assertThat(sut.takeReady()).containsExactly(4097L);
        assertThat(sut.takeReady()).isEmpty();
        assertThat(sut.retainedBytes()).isZero();
    }

    private record Failure(DeliveryToken token, Throwable cause) {
    }

    private DeliveryToken token(long connection, long acknowledgement) {
        return new DeliveryToken(SESSION, connection, acknowledgement);
    }

    private static Subscribe request(AckMode ackMode, int capacity) {
        return Subscribe.of(new TopicFilter("#"), Qos.AT_LEAST_ONCE)
                .withAckMode(ackMode)
                .withBufferCapacity(capacity);
    }

    private static MqttMessage qosOneMessage() {
        return MqttMessage.of(new Topic("a"), new byte[] {1}, Qos.AT_LEAST_ONCE, false);
    }

    private DeliveryLanes lanes(long byteLimit) {
        var owner = new AtomicReference<DeliveryLanes>();
        DeliveryLanes manager = lanes(byteLimit, () ->
                ready.addAll(requireNonNull(owner.get()).takeReady()));
        owner.set(manager);
        return manager;
    }

    private DeliveryLanes lanes(long byteLimit, Runnable onReady) {
        return lanes(byteLimit, onReady, (token, cause) -> failures.add(new Failure(token, cause)));
    }

    private DeliveryLanes lanes(
            long byteLimit,
            Runnable onReady,
            BiPredicate<DeliveryToken, Throwable> onFailure
    ) {
        var manager = new DeliveryLanes(
                "lane-test",
                byteLimit,
                onFailure,
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
        return MqttMessage.of(new Topic("a"), new byte[] {(byte) value}, Qos.AT_MOST_ONCE, false);
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

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }
}
