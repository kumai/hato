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
import java.util.concurrent.RejectedExecutionException;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.FailurePolicy;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.DeliveryLanes.Dropped;

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

    // Arrivals whose connection a refused hand-off ended as overloaded.
    private final LinkedBlockingQueue<DeliveryToken> overloads = new LinkedBlockingQueue<>();

    private final LinkedBlockingQueue<Long> ready = new LinkedBlockingQueue<>();

    private final LinkedBlockingQueue<Long> terminated = new LinkedBlockingQueue<>();

    private final List<DeliveryLanes> managers = new ArrayList<>();

    private final List<CountDownLatch> releases = new ArrayList<>();

    private final DeliveryLanes sut = lanes(BYTE_LIMIT);

    @AfterEach
    void close() {
        // The client closes hand-off before its lanes, which also ends the hand-off worker.
        managers.forEach(DeliveryLanes::closeHandOffs);
        managers.forEach(manager -> manager.close(null));
        releases.forEach(CountDownLatch::countDown);
        await().untilAsserted(() -> managers.forEach(manager -> {
            assertThat(manager.retainedLanes()).isZero();
            assertThat(manager.retainedDeliveries()).isZero();
            assertThat(manager.retainedBytes()).isZero();
        }));
        assertThat(failures).isEmpty();
        assertThat(overloads).isEmpty();
    }

    @Test
    void connection_end_discards_queued_pull_work_and_retires_handed_off_acknowledgements()
            throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(AckMode.MANUAL, 2));
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery handedOff = received(receiver.receive());
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 20)).isTrue();

        // Act
        sut.connectionEnded(false, false);

        // Assert
        assertThat(sut.dropped(Dropped.SUBSCRIPTION_STOPPED)).isZero();
        assertThat(sut.dropped(Dropped.CONNECTION_ENDED)).isEqualTo(1);
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
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(1));
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();

        // Act
        sut.connectionEnded(false, false);

        // Assert
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(sut.dropped(Dropped.SUBSCRIPTION_STOPPED)).isZero();
        assertThat(sut.dropped(Dropped.CONNECTION_ENDED)).isEqualTo(1);
        assertThat(sut.retainedLanes()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void a_continuing_session_keeps_handed_manual_conditions_but_discards_queued_receipts()
            throws Exception {
        DeliveryLanes.Receiver receiver = registerPull(
                sut,
                1,
                request(AckMode.MANUAL, 2)
        );
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery original = received(receiver.receive());
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, 20)).isTrue();

        assertThat(sut.connectionEnded(true, false)).containsExactlyEntriesOf(Map.of(10L, false));
        assertThat(sut.dropped(Dropped.CONNECTION_ENDED)).isEqualTo(1);
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
        DeliveryLanes.Receiver completed = registerPull(sut, 1, request(1));
        DeliveryLanes.Receiver queued = registerPull(sut, 2, request(1));
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        assertThat(received(completed.receive()).dup()).isFalse();

        assertThat(sut.connectionEnded(true, false)).containsExactlyEntriesOf(Map.of(10L, false));
        assertThat(sut.retainedBytes()).isZero();
        DeliveryLanes.Receiver added = registerPull(sut, 3, request(1));
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
    void each_delivered_copy_reports_the_attempt_of_the_connection_that_delivered_it()
            throws Exception {
        // Arrange: the first copy arrives on the second connection of attempt 1, as when that
        // attempt replaced a broker session. One receiver takes it, and the other's copy is
        // discarded when the connection ends, so the continued session keeps the receipt.
        var firstCopy = new DeliveryToken(SESSION, 2, 1, 10);
        var retransmission = new DeliveryToken(SESSION, 3, 2, 10);
        DeliveryLanes.Receiver taken = registerPull(sut, 1, request(1));
        DeliveryLanes.Receiver discarded = registerPull(sut, 2, request(1));
        assertThat(sut.deliver(firstCopy, List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery first = received(taken.receive());
        assertThat(sut.connectionEnded(true, false)).containsKey(10L);

        // Act: the retransmission on the connection of attempt 2 joins that receipt.
        assertThat(sut.deliver(retransmission, List.of(1L, 2L), qosOneMessage(), true, 10))
                .isTrue();

        // Assert
        MqttDelivery second = received(discarded.receive());
        assertThat(first.connectionAttempt()).isEqualTo(1);
        assertThat(second.connectionAttempt()).isEqualTo(2);
        assertThat(second.dup()).isTrue();
    }

    @Test
    void a_duplicate_of_queued_work_does_not_reserve_or_queue_another_view() throws Exception {
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(1));
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
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(1));
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
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 2), delivery -> {
            received.add(delivery);
            if (first.getAndSet(false)) {
                throw cause;
            }
        })).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(received.poll(5, SECONDS)).isNotNull();
        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(token(1, 10), cause));
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 10)).isTrue();

        // Act: that failure no longer ends a connection once its session is discarded.
        sut.sessionDiscarded();

        // Assert
        assertThat(requireNonNull(received.poll(5, SECONDS)).message()).isEqualTo(message(2));
    }

    @Test
    void a_discarded_stop_releases_its_failed_delivery_and_resumes_the_lane() throws Exception {
        // Arrange: a handed-off handler fails after its connection ended, while a reconnect
        // waits, and the accepted failure pauses the lane.
        DeliveryLanes sut = lanes(BYTE_LIMIT, () -> { }, (token, cause) -> {
            failures.add(new Failure(token, cause));
            return true;
        });
        var started = new CountDownLatch(1);
        CountDownLatch release = release();
        var handled = new LinkedBlockingQueue<MqttDelivery>();
        var cause = new IllegalStateException("handler failed while waiting");
        var first = new AtomicBoolean(true);
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 1), delivery -> {
            if (first.getAndSet(false)) {
                started.countDown();
                waitFor(release);
                throw cause;
            }
            handled.add(delivery);
        })).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(started.await(5, SECONDS)).isTrue();
        assertThat(sut.connectionEnded(true, false)).containsExactlyEntriesOf(Map.of(10L, false));
        release.countDown();
        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(token(1, 10), cause));
        assertThat(sut.retainedDeliveries()).isEqualTo(1);

        // Act: withdrawing the request discards the stop, and no connection end follows.
        sut.stopDiscarded();

        // Assert: the failed delivery no longer holds the lane's only slot.
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.deliver(token(2, 11), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(handled.poll(5, SECONDS)).isNotNull();
    }

    @Test
    void stopping_invalidates_unready_handed_views_and_keeps_completed_recipient_conditions()
            throws Exception {
        DeliveryLanes.Receiver manual = registerPull(
                sut,
                1,
                request(AckMode.MANUAL, 1)
        );
        DeliveryLanes.Receiver completed = registerPull(sut, 2, request(1));
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
        DeliveryLanes.Receiver completed = registerPull(sut, 2, request(1));
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
    void a_failure_after_a_manual_ack_keeps_the_acknowledgement() throws Exception {
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
        DeliveryLanes.Receiver completed = registerPull(sut, 2, request(1));
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        assertThat(received.poll(5, SECONDS)).isNotNull();
        received(completed.receive());
        assertThat(wakes.poll(5, SECONDS)).isTrue();
        assertThat(sut.isReady(10)).isTrue();

        failRelease.countDown();

        // The acknowledged delivery holds back nothing, so its failure is skipped and the
        // acknowledgement stays in force.
        await().until(() -> sut.reportedFailures() == 1);
        assertThat(failures).isEmpty();
        assertThat(sut.takeReady()).containsExactly(10L);
        assertThat(sut.isReady(10)).isTrue();
        assertThat(sut.connectionEnded(true, true)).containsExactlyEntriesOf(Map.of(10L, true));
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
        DeliveryLanes.Receiver completed = registerPull(sut, 2, request(1));
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
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(1));
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
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(AckMode.MANUAL, 1));
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
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(1));
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
        DeliveryLanes.Receiver first = registerPull(sut, 1, request(8));
        DeliveryLanes.Receiver second = registerPull(sut, 2, request(8));
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
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(1));

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
            boolean _ = Thread.interrupted();
        }
        assertThat(received(receiver.receive(Duration.ofSeconds(Long.MAX_VALUE))).message())
                .isEqualTo(message(1));
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
    }

    @Test
    void a_timed_wait_reports_normal_end_without_waiting_for_its_deadline() throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(1));
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
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(256));
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
    void a_pull_lane_awaiting_removal_drops_new_deliveries_without_holding_back_acknowledgement()
            throws Exception {
        // Arrange: the pull subscription is awaiting removal.
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(2));
        sut.pause(1);

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isTrue();

        // Assert: neither is received, and the QoS 1 delivery is ready for acknowledgement.
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.retainedBytes()).isZero();
        sut.remove(1);
        assertThat(receiver.receive()).isEqualTo(new ReceiveResult.Ended());
        assertThat(sut.retainedLanes()).isZero();
    }

    @Test
    void a_retransmission_is_not_handed_to_a_lane_awaiting_removal() throws Exception {
        // Arrange: of two overlapping lanes, only the first took the delivery before the
        // connection ended; then the second lane awaits removal.
        DeliveryLanes.Receiver taken = registerPull(sut, 1, request(AckMode.MANUAL, 2));
        DeliveryLanes.Receiver removing = registerPull(sut, 2, request(2));
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10))
                .isTrue();
        MqttDelivery original = received(taken.receive());
        assertThat(sut.connectionEnded(true, false)).containsExactlyEntriesOf(Map.of(10L, false));
        sut.pause(2);

        // Act: the continued session retransmits the reception.
        assertThat(sut.deliver(token(2, 10), List.of(1L, 2L), qosOneMessage(), true, 10)).isTrue();

        // Assert: only the first lane's condition remains.
        assertThat(removing.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        assertThat(ready).isEmpty();
        original.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
    }

    @Test
    void a_receiver_ended_before_admission_stays_ended_without_a_lane() throws Exception {
        // Arrange: the handle ends before the actor admits its routing lane.
        DeliveryLanes.Receiver receiver = sut.receiver(1, request(1));
        receiver.end(null);

        // Act
        assertThat(sut.register(receiver)).isTrue();

        // Assert: no lane slot remains, and a later failure cannot change the ending.
        assertThat(sut.retainedLanes()).isZero();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        sut.close(new IllegalStateException("internal failure"));
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
        assertThat(terminated).containsExactly(1L);
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
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(3), false, 30)).isTrue();

        // Assert: the second handler waits for the first, with both reservations retained, and
        // the third QoS 0 delivery found no slot and was dropped.
        assertThat(virtual.get()).isTrue();
        assertThat(entered).isEmpty();
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        assertThat(sut.retainedBytes()).isEqualTo(30);
        assertThat(sut.dropped(1, Dropped.SUBSCRIPTION_FULL)).isEqualTo(1);

        // Act: returning the first handler lets the queued delivery run.
        release.countDown();

        // Assert: the waiting delivery runs, and the dropped one never does.
        MqttDelivery received = requireNonNull(entered.poll(5, SECONDS));
        assertThat(received.message()).isEqualTo(second);
        assertThat(received.dup()).isFalse();
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isZero());
        assertThat(sut.retainedBytes()).isZero();
        assertThat(entered).isEmpty();
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
        assertThat(sut.dropped(Dropped.FAILED)).isZero();
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
        assertThat(sut.dropped(Dropped.FAILED)).isZero();
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
        assertThat(sut.dropped(Dropped.SUBSCRIPTION_STOPPED)).isZero();
    }

    @Test
    void a_qos_1_fan_out_reserves_all_recipients_or_none() throws Exception {
        // Arrange: the first lane is full and the second lane is empty.
        var entered = new LinkedBlockingQueue<Long>();
        CountDownLatch release = release();
        assertThat(sut.register(1, request(1), _ -> {
            entered.add(1L);
            waitFor(release);
        })).isTrue();
        assertThat(sut.register(2, request(1), _ -> entered.add(2L))).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(1), false, 10)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(1L);

        // Act & Assert: discovering the full lane second does not reserve the empty one.
        assertThat(sut.deliver(token(1, 11), List.of(2L, 1L), qosOneMessage(2), false, 20))
                .isFalse();
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(entered).isEmpty();

        // Act & Assert: refusing the fan-out leaves the empty lane's capacity available.
        assertThat(sut.deliver(token(1, 12), List.of(2L), qosOneMessage(3), false, 30)).isTrue();
        assertThat(entered.poll(5, SECONDS)).isEqualTo(2L);
    }

    @Test
    void a_qos_0_fan_out_skips_only_its_full_lane() throws Exception {
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

        // Act
        assertThat(sut.deliver(token(1, 0), List.of(2L, 1L), message(2), false, 20)).isTrue();

        // Assert: only the full lane misses the publication; the other lane runs it.
        assertThat(entered.poll(5, SECONDS)).isEqualTo(2L);
        assertThat(sut.dropped(1, Dropped.SUBSCRIPTION_FULL)).isEqualTo(1);
        assertThat(sut.dropped(2, Dropped.SUBSCRIPTION_FULL)).isZero();
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isZero());
        assertThat(entered).isEmpty();
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

        // Act & Assert: returning only one handler keeps the shared byte reservation, so a QoS 0
        // delivery that needs one more byte is dropped.
        firstRelease.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedDeliveries()).isEqualTo(1));
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 1)).isTrue();
        assertThat(sut.dropped(1, Dropped.CLIENT_FULL)).isEqualTo(1);

        // Act & Assert: the last handler returns the shared capacity.
        secondRelease.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 1)).isTrue();
        assertThat(sut.dropped(Dropped.CLIENT_FULL)).isEqualTo(1);
    }

    @Test
    void a_configured_large_packet_fits_but_a_single_packet_above_budget_does_not() {
        // Arrange
        DeliveryLanes sut = lanes(BYTE_LIMIT + 1);
        CountDownLatch release = release();
        assertThat(sut.register(1, request(2), _ -> waitFor(release))).isTrue();

        // Act & Assert: rejection preserves capacity for a packet at the configured limit.
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, BYTE_LIMIT + 2))
                .isFalse();
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(), false, BYTE_LIMIT + 1))
                .isTrue();
    }

    @Test
    void pausing_stops_admission_but_keeps_queued_work() throws Exception {
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
        assertThat(entered).isEmpty();
        assertThat(sut.dropped(Dropped.SUBSCRIPTION_STOPPED)).isZero();
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
        assertThat(sut.dropped(Dropped.SUBSCRIPTION_STOPPED)).isEqualTo(2);

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

        // Act & Assert: a new generation cannot reuse the old handler's byte reservation, so its
        // QoS 0 delivery is dropped.
        sut.connectionEnded(false, false);
        assertThat(sut.register(2, request(2), _ -> { })).isTrue();
        assertThat(sut.deliver(token(2, 0), List.of(2L), message(2), false, 1)).isTrue();
        assertThat(sut.dropped(2, Dropped.CLIENT_FULL)).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(BYTE_LIMIT);

        // Act & Assert: returning the old handler permits delivery in the new generation.
        release.countDown();
        await().untilAsserted(() -> assertThat(sut.retainedBytes()).isZero());
        assertThat(sut.deliver(token(2, 0), List.of(2L), message(2), false, 1)).isTrue();
        assertThat(sut.dropped(2, Dropped.CLIENT_FULL)).isEqualTo(1);
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

        // Act & Assert: the global limit applies even though the lane has spare capacity. A QoS 0
        // delivery is dropped, and a QoS 1 delivery behind the QoS 0 backlog is refused, which
        // ends the connection.
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isTrue();
        assertThat(sut.dropped(1, Dropped.CLIENT_FULL)).isEqualTo(1);
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 1)).isFalse();
        assertThat(sut.retainedDeliveries()).isEqualTo(65_536);

        // Act
        sut.close(null);

        // Assert: queued deliveries are abandoned; the executing handler keeps its reservation.
        assertThat(sut.retainedDeliveries()).isEqualTo(1);
        assertThat(sut.retainedBytes()).isEqualTo(1);
        assertThat(sut.dropped(Dropped.SUBSCRIPTION_STOPPED)).isEqualTo(65_535);
    }

    @Test
    void a_qos_0_publication_reaches_no_lane_when_the_client_has_room_for_only_some()
            throws Exception {
        // Arrange: all but one of the client's delivery budget waits behind a running handler.
        var entered = new CountDownLatch(1);
        var handled = new LinkedBlockingQueue<MqttMessage>();
        CountDownLatch release = release();
        assertThat(sut.register(1, request(70_000), _ -> {
            entered.countDown();
            waitFor(release);
        })).isTrue();
        assertThat(sut.register(2, request(1), delivery -> handled.add(delivery.message())))
                .isTrue();
        for (int i = 0; i < 65_535; i++) {
            assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isTrue();
        }
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 0), List.of(1L, 2L), message(2), false, 1)).isTrue();

        // Assert: the publication reaches neither lane rather than the one that would fit, and
        // a later one for the second lane alone fits.
        assertThat(sut.retainedDeliveries()).isEqualTo(65_535);
        assertThat(sut.dropped(1, Dropped.CLIENT_FULL)).isEqualTo(1);
        assertThat(sut.dropped(2, Dropped.CLIENT_FULL)).isEqualTo(1);
        assertThat(sut.deliver(token(1, 0), List.of(2L), message(3), false, 1)).isTrue();
        assertThat(handled.poll(5, SECONDS)).isEqualTo(message(3));
        assertThat(handled).isEmpty();
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
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 2), _ -> {
            calls.incrementAndGet();
            entered.countDown();
            waitFor(release);
            Thread.currentThread().interrupt();
            throw cause;
        })).isTrue();
        assertThat(sut.deliver(token(7, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(sut.deliver(token(7, 11), List.of(1L), qosOneMessage(), false, 20)).isTrue();

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
        assertThat(sut.dropped(Dropped.FAILED)).isEqualTo(1);
        assertThat(sut.dropped(Dropped.CONNECTION_ENDED)).isEqualTo(1);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void a_failure_after_connection_end_keeps_its_origin_and_resumes_the_lane() throws Exception {
        // Arrange: the handler outlives the connection that received its QoS 1 delivery.
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
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 1), _ -> {
            workers.add(Thread.currentThread());
            if (first.getAndSet(false)) {
                waitFor(release);
                throw cause;
            }
        })).isTrue();
        assertThat(sut.deliver(token(7, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        Thread worker = requireNonNull(workers.poll(5, SECONDS));
        mailbox.discard(7);
        assertThat(sut.retainedLanes()).isEqualTo(1);

        // Act: the failure arrives after admission ends but before connection cleanup.
        release.countDown();
        await().until(() -> sut.reportedFailures() == 1);
        sut.connectionEnded(false, false);

        // Assert: the callback retains the receipt's origin without retaining its resources.
        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(token(7, 10), cause));
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
    void a_failure_after_its_acknowledgement_was_released_is_skipped() throws Exception {
        // Arrange: the second delivery fails after its PUBACK was queued and its receipt retired.
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

        // Assert: nothing is left to protect, so the failure stops nothing and the lane goes on.
        await().until(() -> sut.reportedFailures() == 1);
        assertThat(failures).isEmpty();
        assertThat(sut.dropped(Dropped.FAILED)).isEqualTo(1);
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.deliver(token(2, 0), List.of(1L), message(2), false, 1)).isTrue();
        assertThat(requireNonNull(received.poll(5, SECONDS)).message()).isEqualTo(message(2));
    }

    @Test
    void a_failure_after_its_receipt_was_withdrawn_is_skipped() throws Exception {
        // Arrange: a reused packet identifier withdraws the receipt while its handler runs.
        var received = new LinkedBlockingQueue<MqttMessage>();
        var cause = new IllegalStateException("failed after withdrawal");
        CountDownLatch release = release();
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 2), delivery -> {
            received.add(delivery.message());
            if (delivery.message().equals(qosOneMessage(1))) {
                waitFor(release);
                throw cause;
            }
        })).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(1), false, 1)).isTrue();
        assertThat(received.poll(5, SECONDS)).isEqualTo(qosOneMessage(1));
        sut.release(10);
        sut.forget(10);

        // Act
        release.countDown();

        // Assert: the withdrawn receipt holds back no acknowledgement, so the default policy
        // skips the failure, and the lane goes on.
        await().until(() -> sut.reportedFailures() == 1);
        assertThat(failures).isEmpty();
        assertThat(sut.dropped(Dropped.FAILED)).isEqualTo(1);
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(2), false, 1)).isTrue();
        assertThat(received.poll(5, SECONDS)).isEqualTo(qosOneMessage(2));
        assertThat(ready.poll(5, SECONDS)).isEqualTo(11L);
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
            assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 1), _ -> {
                throw cause;
            })).isTrue();

            // Act
            assertThat(sut.deliver(token(7, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();

            // Assert: the original cause is reported once, and the lane stays registered.
            Failure failure = requireNonNull(failures.poll(5, SECONDS));
            assertThat(failure.token().connection()).isEqualTo(7);
            assertThat(failure.cause()).isSameAs(cause);
            assertThat(sut.retainedLanes()).isEqualTo(1);
            assertThat(failures).isEmpty();
            assertThat(logCalls.get()).isEqualTo(1);
            assertThat(sut.dropped(Dropped.FAILED)).isEqualTo(1);
        } finally {
            jul.removeHandler(handler);
            jul.setLevel(previousLevel);
            jul.setUseParentHandlers(previousParents);
        }
    }

    @Test
    void manual_ack_is_once_only_and_keeps_capacity_until_the_sequencer_releases_it()
            throws Exception {
        // Arrange: the handler returns without acknowledging its delivery, and the lane has
        // updated the acknowledgement condition and capacity for that return.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, request(AckMode.MANUAL, 1), received::add)).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), true, 10)).isTrue();
        MqttDelivery delivery = requireNonNull(received.poll(5, SECONDS));
        await().until(() -> sut.handledDeliveries() == 1);

        // Assert: normal return neither acknowledges nor releases MANUAL delivery capacity.
        assertThat(delivery.dup()).isTrue();
        assertThat(sut.isReady(10)).isFalse();
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
        assertThat(sut.dropped(Dropped.SUBSCRIPTION_STOPPED)).isEqualTo(1);
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
        var running = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch release = release();
        assertThat(sut.register(1, request(AckMode.MANUAL, 1), completed::add)).isTrue();
        assertThat(sut.register(2, request(AckMode.MANUAL, 1), delivery -> {
            running.add(delivery);
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery finished = requireNonNull(completed.poll(5, SECONDS));
        await().until(() -> sut.handledDeliveries() == 1);
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
    void a_push_lane_awaiting_removal_drops_new_deliveries_without_running_them()
            throws Exception {
        // Arrange: the subscription is awaiting removal.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 2), received::add)).isTrue();
        sut.pause(1);

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isTrue();

        // Assert: the QoS 1 delivery is ready for acknowledgement, and the worker runs neither.
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(sut.retainedDeliveries()).isZero();
        sut.remove(1);
        await().untilAsserted(() -> assertThat(sut.retainedLanes()).isZero());
        assertThat(received).isEmpty();
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
    void a_handler_that_acknowledges_and_then_fails_keeps_its_acknowledgement() throws Exception {
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
        await().until(() -> sut.reportedFailures() == 1);
        otherRelease.countDown();

        // Assert: the skipped failure keeps the acknowledgement, so the receipt is ready once the
        // other recipient completes, and nothing stops either lane.
        assertThat(completed.poll(5, SECONDS)).isNotNull();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(failures).isEmpty();
    }

    @Test
    void a_failed_lane_keeps_its_queued_deliveries_unacknowledged() throws Exception {
        // Arrange: the failing lane has a shared delivery queued behind its running handler.
        var cause = new IllegalStateException("handler failed");
        var entered = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch failRelease = release();
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 2), delivery -> {
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

    @Test
    void a_finishing_push_lane_runs_its_accepted_deliveries_then_terminates() throws Exception {
        // Arrange: the first handler runs while a second delivery waits.
        var handled = new LinkedBlockingQueue<MqttMessage>();
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        assertThat(sut.register(1, request(4), delivery -> {
            handled.add(delivery.message());
            entered.countDown();
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 10)).isTrue();

        // Act: removal is confirmed; a later delivery is not admitted.
        sut.finish(1);
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(3), false, 10)).isTrue();
        release.countDown();

        // Assert: the accepted delivery runs, then the dispatcher stops and reports once.
        assertThat(terminated.poll(5, SECONDS)).isEqualTo(1L);
        assertThat(handled).containsExactly(message(1), message(2));
        assertThat(sut.retainedLanes()).isZero();
        assertThat(sut.dropped(Dropped.SUBSCRIPTION_STOPPED)).isZero();
        assertThat(terminated).isEmpty();
    }

    @Test
    void a_finishing_receiver_stops_when_its_last_accepted_delivery_is_received()
            throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(4));
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 10)).isTrue();

        // Act
        receiver.finish();

        // Assert: both deliveries stay receivable; taking the last one ends the lane.
        assertThat(received(receiver.receive(Duration.ZERO)).message()).isEqualTo(message(1));
        assertThat(terminated).isEmpty();
        assertThat(received(receiver.receive(Duration.ZERO)).message()).isEqualTo(message(2));
        assertThat(terminated.poll(5, SECONDS)).isEqualTo(1L);
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
        assertThat(sut.retainedLanes()).isZero();
    }

    @Test
    void a_finishing_receiver_without_deliveries_stops_at_once() {
        // Arrange
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(4));

        // Act
        receiver.finish();

        // Assert
        assertThat(terminated).containsExactly(1L);
        assertThat(sut.retainedLanes()).isZero();
    }

    @Test
    void a_receiver_finished_before_admission_ends_at_once() throws Exception {
        // Arrange
        DeliveryLanes.Receiver receiver = sut.receiver(1, request(1));

        // Act
        receiver.finish();

        // Assert: it holds no delivery to wait for, and admission takes no lane slot.
        assertThat(terminated).containsExactly(1L);
        assertThat(sut.register(receiver)).isTrue();
        assertThat(sut.retainedLanes()).isZero();
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
    }

    @Test
    void a_connection_end_stops_a_finishing_receiver_by_discarding_its_queue() throws Exception {
        // Arrange: a receiver finishes with one accepted delivery left.
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(4));
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        receiver.finish();

        // Act
        sut.connectionEnded(false, false);

        // Assert: nothing remains to receive, so the lane stops without another receive call.
        assertThat(terminated).containsExactly(1L);
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
        assertThat(sut.dropped(Dropped.CONNECTION_ENDED)).isEqualTo(1);
    }

    @Test
    void closed_hand_off_lets_no_lane_take_a_queued_delivery() throws Exception {
        // Arrange: a push handler runs with one delivery queued, and a receiver has one waiting.
        var handled = new LinkedBlockingQueue<MqttMessage>();
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        assertThat(sut.register(1, request(4), delivery -> {
            handled.add(delivery.message());
            entered.countDown();
            waitFor(release);
        })).isTrue();
        DeliveryLanes.Receiver receiver = registerPull(sut, 2, request(4));
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L, 2L), message(2), false, 10)).isTrue();

        // Act
        sut.closeHandOffs();
        release.countDown();

        // Assert: the dispatcher exits after its running handler, receiving ends, and the queued
        // deliveries wait for close() to discard them.
        assertThat(terminated.poll(5, SECONDS)).isEqualTo(1L);
        assertThat(receiver.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
        assertThat(handled).containsExactly(message(1));
        assertThat(sut.retainedDeliveries()).isEqualTo(2);
        sut.close(null);
        assertThat(sut.retainedDeliveries()).isZero();
        assertThat(terminated.poll(5, SECONDS)).isEqualTo(2L);
    }

    @Test
    void a_stopped_receiver_reports_termination_once() {
        // Arrange
        DeliveryLanes.Receiver receiver = registerPull(sut, 1, request(4));

        // Act
        receiver.end(null);
        receiver.end(null);
        receiver.finish();
        sut.close(null);

        // Assert
        assertThat(terminated).containsExactly(1L);
    }

    @Test
    void a_refusing_executor_skips_each_waiting_delivery_that_holds_back_no_acknowledgement()
            throws Exception {
        // Arrange: three QoS 0 deliveries wait behind one accepted task, and the executor
        // refuses every later request.
        var executor = new ManualExecutor();
        var handled = new LinkedBlockingQueue<Byte>();
        var execution = new HandlerExecution.OnExecutor(executor);
        assertThat(sut.register(1, request(3), execution, delivery -> {
            handled.add(delivery.message().payload()[0]);
        })).isTrue();
        for (int value = 1; value <= 3; value++) {
            assertThat(sut.deliver(token(1, 0), List.of(1L), message(value), false, 10)).isTrue();
        }
        await().until(() -> sut.placedDeliveries() == 3);
        executor.refuse(new RejectedExecutionException("Executor is saturated"));

        // Act
        executor.runNext();

        // Assert: the task ran the first delivery, and each refused request skipped the next
        // one, so no delivery waits for a task, every slot has returned, and the lane goes on.
        assertThat(handled).containsExactly((byte) 1);
        assertThat(sut.dropped(Dropped.FAILED)).isEqualTo(2);
        assertThat(executor.executions()).isEqualTo(3);
        assertThat(executor.held()).isZero();
        assertThat(sut.retainedDeliveries()).isZero();
    }

    @Test
    void after_a_connection_ends_new_deliveries_wait_for_a_handler_still_running_on_the_executor()
            throws Exception {
        // Arrange: the first connection's handler still runs on the executor's thread when that
        // connection ends.
        var executor = new ManualExecutor();
        var handled = new LinkedBlockingQueue<Byte>();
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        var execution = new HandlerExecution.OnExecutor(executor);
        assertThat(sut.register(1, request(2), execution, delivery -> {
            byte value = delivery.message().payload()[0];
            handled.add(value);
            if (value == 1) {
                entered.countDown();
                waitFor(release);
            }
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        Thread.ofVirtual().start(executor::runNext);
        assertThat(entered.await(5, SECONDS)).isTrue();
        sut.connectionEnded(false, false);

        // Act: the next connection delivers while that handler runs.
        assertThat(sut.deliver(token(2, 0), List.of(1L), message(2), false, 10)).isTrue();
        await().until(() -> sut.placedDeliveries() == 2);

        // Assert: no second task starts beside the running one, which asks for the new
        // delivery once its handler returns.
        assertThat(executor.held()).isZero();
        release.countDown();
        await().until(() -> executor.held() == 1);
        executor.runNext();
        assertThat(handled).containsExactly((byte) 1, (byte) 2);
    }

    @Test
    void a_confirmed_removal_ends_an_executor_lane_after_its_waiting_deliveries_run()
            throws Exception {
        // Arrange: two deliveries wait for the executor when removal is confirmed.
        var executor = new ManualExecutor();
        var handled = new LinkedBlockingQueue<Byte>();
        var execution = new HandlerExecution.OnExecutor(executor);
        assertThat(sut.register(1, request(2), execution, delivery -> {
            handled.add(delivery.message().payload()[0]);
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 10)).isTrue();
        await().until(() -> sut.placedDeliveries() == 2);

        // Act
        sut.finish(1);

        // Assert: the lane lasts until the executor has run both, and then ends.
        executor.runNext();
        assertThat(terminated).isEmpty();
        executor.runNext();
        assertThat(terminated.poll(5, SECONDS)).isEqualTo(1L);
        assertThat(handled).containsExactly((byte) 1, (byte) 2);
        assertThat(sut.retainedLanes()).isZero();
    }

    @Test
    void a_failure_that_is_only_logged_does_not_lift_the_stop_of_another_lanes_failure()
            throws Exception {
        // Arrange: the first failure stops the subscription, and later ones are only logged.
        // Lane b's first handler waits while lane a's fails; b's second delivery waits behind it.
        var reported = new LinkedBlockingQueue<Throwable>();
        var first = new AtomicBoolean(true);
        DeliveryLanes sut = lanes(BYTE_LIMIT, () -> { }, (_, cause) -> {
            reported.add(cause);
            return first.getAndSet(false);
        });
        var a = new ManualExecutor();
        var b = new ManualExecutor();
        var handled = new LinkedBlockingQueue<Byte>();
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        var stopping = new IllegalStateException("lane a failed");
        var logged = new IllegalStateException("lane b failed");
        var execution = new HandlerExecution.Lanes(message -> message.payload()[0] == 1 ? a : b);
        assertThat(sut.register(1, request(AckMode.AFTER_HANDLER, 3), execution, delivery -> {
            byte value = delivery.message().payload()[0];
            handled.add(value);
            if (value == 1) {
                throw stopping;
            }
            if (value == 2) {
                entered.countDown();
                waitFor(release);
                throw logged;
            }
        })).isTrue();
        for (int value = 1; value <= 3; value++) {
            DeliveryToken token = token(1, 10 + value);
            assertThat(sut.deliver(token, List.of(1L), qosOneMessage(value), false, 10)).isTrue();
        }
        await().until(() -> sut.placedDeliveries() == 3);
        Thread laneB = Thread.ofVirtual().start(b::runNext);
        assertThat(entered.await(5, SECONDS)).isTrue();
        a.runNext();

        // Act: lane b's handler fails after lane a's failure has stopped the subscription.
        release.countDown();
        assertThat(laneB.join(Duration.ofSeconds(5))).isTrue();

        // Assert: lane b's task asks for nothing more, so its waiting delivery stays held.
        assertThat(reported).containsExactly(stopping, logged);
        assertThat(b.held()).isZero();
        assertThat(handled).containsExactly((byte) 2, (byte) 1);
    }

    @ParameterizedTest(name = "selector fails: {0}")
    @ValueSource(booleans = {false, true})
    void a_lane_chosen_for_a_delivery_that_the_connection_end_discarded_is_dropped(boolean fails)
            throws Exception {
        // Arrange: the selector is still choosing a lane for the first delivery when its
        // connection ends; it then returns a lane or fails.
        var lane = new ManualExecutor();
        var selected = new LinkedBlockingQueue<Byte>();
        var handled = new LinkedBlockingQueue<Byte>();
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        var execution = new HandlerExecution.Lanes(message -> {
            byte value = message.payload()[0];
            selected.add(value);
            if (value == 1) {
                entered.countDown();
                waitFor(release);
                if (fails) {
                    throw new IllegalStateException("No lane after the connection ended");
                }
            }
            return lane;
        });
        assertThat(sut.register(1, request(2), execution, delivery -> {
            handled.add(delivery.message().payload()[0]);
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        sut.connectionEnded(false, false);

        // Act: the selector answers after the connection's end, and the next connection
        // delivers.
        release.countDown();
        assertThat(sut.deliver(token(2, 0), List.of(1L), message(2), false, 10)).isTrue();
        lane.runNext();

        // Assert: the discarded delivery got no lane and no failure; the next one ran.
        assertThat(selected).containsExactly((byte) 1, (byte) 2);
        assertThat(handled).containsExactly((byte) 2);
        assertThat(sut.dropped(Dropped.CONNECTION_ENDED)).isEqualTo(1);
        assertThat(sut.reportedFailures()).isZero();
    }

    @Test
    void a_skipped_failure_settles_only_its_own_recipient_of_a_shared_delivery()
            throws Exception {
        // Arrange: a skipping lane fails for a delivery that another lane still runs.
        var cause = new IllegalStateException("handler failed");
        var completed = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch otherRelease = release();
        Subscribe skipping = request(AckMode.AFTER_HANDLER, 1)
                .withFailurePolicy(FailurePolicy.SKIP);
        assertThat(sut.register(1, skipping, _ -> {
            throw cause;
        })).isTrue();
        assertThat(sut.register(2, request(AckMode.AFTER_HANDLER, 1), delivery -> {
            waitFor(otherRelease);
            completed.add(delivery);
        })).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10)).isTrue();
        await().until(() -> sut.reportedFailures() == 1);

        // Assert: the skipped recipient's condition is met, but the receipt waits for the
        // other recipient, whose handler still runs, and is acknowledged once it returns.
        assertThat(sut.isReady(10)).isFalse();
        otherRelease.countDown();
        assertThat(completed.poll(5, SECONDS)).isNotNull();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(failures).isEmpty();
    }

    @Test
    void unprocessed_deliveries_are_counted_per_subscription_and_reason() throws Exception {
        // Arrange: lane 1 skips two failed deliveries, and lane 2 has one waiting behind a
        // running handler when the connection ends.
        CountDownLatch release = release();
        var entered = new CountDownLatch(1);
        assertThat(sut.register(1, request(2), _ -> {
            throw new IllegalStateException("handler failed");
        })).isTrue();
        assertThat(sut.register(2, request(2), _ -> {
            entered.countDown();
            waitFor(release);
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(2L), message(3), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(2L), message(4), false, 10)).isTrue();
        await().until(() -> sut.reportedFailures() == 2);

        // Act
        sut.connectionEnded(false, false);

        // Assert
        assertThat(sut.dropped(1, Dropped.FAILED)).isEqualTo(2);
        assertThat(sut.dropped(1, Dropped.CONNECTION_ENDED)).isZero();
        assertThat(sut.dropped(2, Dropped.FAILED)).isZero();
        assertThat(sut.dropped(2, Dropped.CONNECTION_ENDED)).isEqualTo(1);
        assertThat(sut.dropped(Dropped.FAILED)).isEqualTo(2);
        assertThat(sut.dropped(Dropped.CONNECTION_ENDED)).isEqualTo(1);
    }

    @Test
    void the_hand_off_worker_offers_the_oldest_delivery_across_lanes_first() throws Exception {
        // Arrange: lane 1's acceptor holds the worker on its first delivery while more arrive on
        // both lanes, one of them matching both.
        var offered = new LinkedBlockingQueue<String>();
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        assertThat(sut.registerHandOff(1, request(8), delivery -> {
            offered.add("1:" + delivery.message().payload()[0]);
            entered.countDown();
            waitFor(release);
            return true;
        })).isTrue();
        assertThat(sut.registerHandOff(2, request(8), delivery ->
                offered.add("2:" + delivery.message().payload()[0]))).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(2L), message(2), false, 1)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(3), false, 1)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(2L), message(4), false, 1)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L, 2L), message(5), false, 1)).isTrue();

        // Act
        release.countDown();

        // Assert: one receive order across the lanes, with a shared message offered in turn.
        await().until(() -> offered.size() == 6);
        assertThat(offered).containsExactly("1:1", "2:2", "1:3", "2:4", "1:5", "2:5");
    }

    @Test
    void accepting_a_delivery_meets_its_on_receive_condition_and_returns_its_capacity()
            throws Exception {
        // Arrange: the application keeps what it accepts, and a push lane also holds the
        // message, so its payload stays counted.
        var mailbox = new LinkedBlockingQueue<MqttDelivery>();
        CountDownLatch release = release();
        assertThat(sut.registerHandOff(1, request(1), mailbox::offer)).isTrue();
        assertThat(sut.register(2, request(AckMode.ON_RECEIVE, 1), _ -> waitFor(release)))
                .isTrue();

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(1), false, 10))
                .isTrue();

        // Assert: the accepted delivery holds no slot of its lane, so a later one fits, while
        // the push lane still holds the payload.
        assertThat(mailbox.poll(5, SECONDS)).isNotNull();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        await().until(() -> sut.retainedDeliveries() == 1);
        assertThat(sut.retainedBytes()).isEqualTo(10);
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(2), false, 10)).isTrue();
        assertThat(mailbox.poll(5, SECONDS)).isNotNull();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(11L);
    }

    @Test
    void accepting_a_manual_delivery_returns_its_capacity_but_waits_for_its_ack()
            throws Exception {
        // Arrange: the application keeps what it accepts and acknowledges it later.
        var mailbox = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.registerHandOff(1, request(AckMode.MANUAL, 1), mailbox::offer)).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(1), false, 10)).isTrue();
        MqttDelivery accepted = requireNonNull(mailbox.poll(5, SECONDS));

        // Assert: the one slot is free again for a later delivery, while the acknowledgement
        // waits for ack().
        await().until(() -> sut.retainedDeliveries() == 0);
        assertThat(sut.retainedBytes()).isZero();
        assertThat(sut.isReady(10)).isFalse();
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(2), false, 10)).isTrue();
        assertThat(mailbox.poll(5, SECONDS)).isNotNull();
        accepted.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(sut.isReady(11)).isFalse();
    }

    @Test
    void an_ack_made_while_the_acceptor_runs_counts_once_the_delivery_is_accepted()
            throws Exception {
        // Arrange: the delivery is acknowledged before the acceptor returns.
        assertThat(sut.registerHandOff(1, request(AckMode.MANUAL, 1), delivery -> {
            delivery.ack();
            return true;
        })).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Assert
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        await().until(() -> sut.retainedDeliveries() == 0);
    }

    @Test
    void an_ack_made_before_a_refusal_or_a_failure_has_no_effect() throws Exception {
        // Arrange: lane 1 refuses and lane 2 throws, each after acknowledging.
        var cause = new IllegalStateException("acceptor failed");
        assertThat(sut.registerHandOff(1, request(AckMode.MANUAL, 1), delivery -> {
            delivery.ack();
            return false;
        })).isTrue();
        assertThat(sut.registerHandOff(2, request(AckMode.MANUAL, 1), delivery -> {
            delivery.ack();
            throw cause;
        })).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(1), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 11), List.of(2L), qosOneMessage(2), false, 10)).isTrue();

        // Assert: neither acknowledgement condition is met.
        assertThat(overloads.poll(5, SECONDS)).isEqualTo(token(1, 10));
        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(token(1, 11), cause));
        assertThat(sut.isReady(10)).isFalse();
        assertThat(sut.isReady(11)).isFalse();
        assertThat(ready).isEmpty();
    }

    @Test
    void a_stop_voids_the_ack_of_an_accepted_manual_delivery_and_hands_it_off_again()
            throws Exception {
        // Arrange: the application holds an accepted MANUAL delivery when a stop ends the
        // connection of a continued session.
        var mailbox = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.registerHandOff(1, request(AckMode.MANUAL, 2), mailbox::offer)).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(), false, 10)).isTrue();
        MqttDelivery stale = requireNonNull(mailbox.poll(5, SECONDS));
        await().until(() -> sut.retainedDeliveries() == 0);

        // Act: the stop leaves the receipt not handed off, the application acknowledges late,
        // and the broker retransmits as a new reception.
        assertThat(sut.connectionEnded(true, true)).isEmpty();
        stale.ack();
        assertThat(sut.deliver(token(2, 11), List.of(1L), qosOneMessage(), true, 10)).isTrue();

        // Assert: the late ack completes nothing, and only the retransmission's own ack counts.
        MqttDelivery again = requireNonNull(mailbox.poll(5, SECONDS));
        assertThat(again.dup()).isTrue();
        assertThat(ready).isEmpty();
        again.ack();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(11L);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 256})
    void every_key_lands_within_the_range_and_equal_keys_share_a_stripe(int stripes) {
        for (Object key : List.of("devices/1", "", Integer.MIN_VALUE, -1, Long.MAX_VALUE)) {
            assertThat(DeliveryLanes.stripeOf(key, stripes)).isBetween(0, stripes - 1);
        }
        // An equal but distinct key lands on the same stripe.
        assertThat(DeliveryLanes.stripeOf(new String("devices/1"), stripes))
                .isEqualTo(DeliveryLanes.stripeOf("devices/1", stripes));
    }

    @Test
    void keys_in_steps_of_the_stripe_count_still_spread_over_every_stripe() {
        // Arrange: identifiers that are all multiples of the stripe count.
        var used = new ArrayList<Integer>();

        // Act
        for (int id = 0; id < 8 * 64; id += 8) {
            used.add(DeliveryLanes.stripeOf(id, 8));
        }

        // Assert
        assertThat(used).containsOnly(0, 1, 2, 3, 4, 5, 6, 7);
    }

    @Test
    void a_refused_qos_0_delivery_is_dropped_for_its_lane_only() throws Exception {
        // Arrange: lane 1 refuses every delivery, and lane 2 accepts.
        var offers = new AtomicInteger();
        var accepted = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.registerHandOff(1, request(2), _ -> {
            offers.incrementAndGet();
            return false;
        })).isTrue();
        assertThat(sut.registerHandOff(2, request(2), accepted::offer)).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 0), List.of(1L, 2L), message(1), false, 10)).isTrue();

        // Assert: the other lane receives it, the refusing acceptor is not asked again, and no
        // connection ends.
        assertThat(accepted.poll(5, SECONDS)).isNotNull();
        assertThat(sut.dropped(1, Dropped.REFUSED)).isEqualTo(1);
        assertThat(sut.dropped(2, Dropped.REFUSED)).isZero();
        assertThat(offers).hasValue(1);
    }

    @Test
    void a_refused_qos_1_delivery_overloads_its_connection_and_is_not_offered_again()
            throws Exception {
        // Arrange
        var offers = new AtomicInteger();
        var arrival = new DeliveryToken(SESSION, 3, 2, 10);
        assertThat(sut.registerHandOff(1, request(AckMode.ON_RECEIVE, 2), _ -> {
            offers.incrementAndGet();
            return false;
        })).isTrue();

        // Act
        assertThat(sut.deliver(arrival, List.of(1L), qosOneMessage(), false, 10)).isTrue();

        // Assert: the connection that delivered it ends as overloaded. Its acknowledgement is
        // not met, and it counts as not handed off, so a continued session hands it off again.
        assertThat(overloads.poll(5, SECONDS)).isEqualTo(arrival);
        assertThat(sut.dropped(1, Dropped.REFUSED)).isEqualTo(1);
        assertThat(offers).hasValue(1);
        assertThat(sut.isReady(10)).isFalse();
        assertThat(sut.receiveState()).isEmpty();
        assertThat(sut.retainedDeliveries()).isZero();
    }

    @Test
    void a_qos_1_retransmission_reaches_only_the_lane_that_refused_its_first_copy()
            throws Exception {
        // Arrange: lane 1 accepts and lane 2 refuses the first copy, which overloads the
        // connection, and the session continues.
        var first = new LinkedBlockingQueue<MqttDelivery>();
        var second = new LinkedBlockingQueue<MqttDelivery>();
        var refuse = new AtomicBoolean(true);
        assertThat(sut.registerHandOff(1, request(AckMode.ON_RECEIVE, 2), first::offer)).isTrue();
        assertThat(sut.registerHandOff(2, request(AckMode.ON_RECEIVE, 2), delivery ->
                !refuse.getAndSet(false) && second.offer(delivery))).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10))
                .isTrue();
        assertThat(overloads.poll(5, SECONDS)).isEqualTo(token(1, 10));
        assertThat(first.poll(5, SECONDS)).isNotNull();
        assertThat(sut.connectionEnded(true, false)).containsExactlyEntriesOf(Map.of(10L, false));

        // Act
        assertThat(sut.deliver(token(2, 10), List.of(1L, 2L), qosOneMessage(), true, 10))
                .isTrue();

        // Assert: the accepted copy is neither taken back nor offered again.
        assertThat(requireNonNull(second.poll(5, SECONDS)).dup()).isTrue();
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(first).isEmpty();
    }

    @Test
    void a_refused_retransmission_overloads_the_connection_that_delivered_it() throws Exception {
        // Arrange: lane 1 accepts the first copy, which keeps the receipt across a continued
        // session, and lane 2 refuses every copy.
        var first = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.registerHandOff(1, request(AckMode.ON_RECEIVE, 2), first::offer)).isTrue();
        assertThat(sut.registerHandOff(2, request(AckMode.ON_RECEIVE, 2), _ -> false)).isTrue();
        assertThat(sut.deliver(token(1, 10), List.of(1L, 2L), qosOneMessage(), false, 10))
                .isTrue();
        assertThat(overloads.poll(5, SECONDS)).isEqualTo(token(1, 10));
        assertThat(first.poll(5, SECONDS)).isNotNull();
        assertThat(sut.connectionEnded(true, false)).containsKey(10L);

        // Act: the retransmission on connection 2 joins that receipt and is refused again.
        assertThat(sut.deliver(token(2, 10), List.of(1L, 2L), qosOneMessage(), true, 10))
                .isTrue();

        // Assert: the overload ends connection 2, not the receipt's first connection.
        assertThat(overloads.poll(5, SECONDS)).isEqualTo(token(2, 10));
        assertThat(sut.dropped(2, Dropped.REFUSED)).isEqualTo(2);
    }

    @Test
    void an_acceptor_failure_on_a_qos_1_delivery_stops_only_its_lane() throws Exception {
        // Arrange: lane 1's acceptor throws under the default policy, and lane 2 accepts.
        var cause = new IllegalStateException("acceptor failed");
        var offers = new AtomicInteger();
        var accepted = new LinkedBlockingQueue<MqttDelivery>();
        assertThat(sut.registerHandOff(1, request(AckMode.ON_RECEIVE, 2), _ -> {
            offers.incrementAndGet();
            throw cause;
        })).isTrue();
        assertThat(sut.registerHandOff(2, request(2), accepted::offer)).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(1), false, 10)).isTrue();
        assertThat(failures.poll(5, SECONDS)).isEqualTo(new Failure(token(1, 10), cause));
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(2), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(2L), message(3), false, 10)).isTrue();

        // Assert: nothing accepted the delivery, so its failure held back the acknowledgement
        // and stopped lane 1, while lane 2 goes on.
        assertThat(accepted.poll(5, SECONDS)).isNotNull();
        assertThat(offers).hasValue(1);
        assertThat(sut.isReady(10)).isFalse();
    }

    @Test
    void an_acceptor_failure_is_skipped_under_skip_and_acknowledges_the_delivery()
            throws Exception {
        // Arrange: the acceptor of a skipping lane throws for the first delivery.
        var offered = new LinkedBlockingQueue<Byte>();
        Subscribe skipping = request(AckMode.ON_RECEIVE, 2).withFailurePolicy(FailurePolicy.SKIP);
        assertThat(sut.registerHandOff(1, skipping, delivery -> {
            offered.add(delivery.message().payload()[0]);
            if (delivery.message().payload()[0] == 1) {
                throw new IllegalStateException("acceptor failed");
            }
            return true;
        })).isTrue();

        // Act
        assertThat(sut.deliver(token(1, 10), List.of(1L), qosOneMessage(1), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 11), List.of(1L), qosOneMessage(2), false, 10)).isTrue();

        // Assert: the failed delivery is acknowledged unprocessed, and the next one is offered.
        assertThat(ready.poll(5, SECONDS)).isEqualTo(10L);
        assertThat(ready.poll(5, SECONDS)).isEqualTo(11L);
        assertThat(offered).containsExactly((byte) 1, (byte) 2);
        assertThat(sut.dropped(1, Dropped.FAILED)).isEqualTo(1);
    }

    @Test
    void a_stopped_hand_off_lane_ends_once_its_running_acceptor_returns() throws Exception {
        // Arrange: the acceptor runs when the lane stops, and another delivery waits.
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        var accepted = new AtomicBoolean();
        assertThat(sut.registerHandOff(1, request(2), _ -> {
            entered.countDown();
            waitFor(release);
            accepted.set(true);
            return true;
        })).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 10)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(2), false, 10)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act
        sut.remove(1);

        // Assert: the waiting delivery is discarded at once, but the lane keeps its slot and
        // ends only after the acceptor returns, which accepts the offered delivery.
        assertThat(sut.dropped(Dropped.SUBSCRIPTION_STOPPED)).isEqualTo(1);
        assertThat(terminated).isEmpty();
        assertThat(sut.retainedLanes()).isEqualTo(1);
        release.countDown();
        assertThat(terminated.poll(5, SECONDS)).isEqualTo(1L);
        assertThat(accepted).isTrue();
        assertThat(sut.retainedLanes()).isZero();
    }

    @Test
    void a_confirmed_removal_hands_off_the_accepted_deliveries_and_then_ends_the_lane()
            throws Exception {
        // Arrange: two deliveries wait while the acceptor holds the first.
        var offered = new LinkedBlockingQueue<Byte>();
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        assertThat(sut.registerHandOff(1, request(4), delivery -> {
            offered.add(delivery.message().payload()[0]);
            entered.countDown();
            waitFor(release);
            return true;
        })).isTrue();
        for (int value = 1; value <= 3; value++) {
            assertThat(sut.deliver(token(1, 0), List.of(1L), message(value), false, 1)).isTrue();
        }
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act: the removal is confirmed, and a later delivery finds the lane closed to it.
        sut.finish(1);
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(4), false, 1)).isTrue();
        release.countDown();

        // Assert
        assertThat(terminated.poll(5, SECONDS)).isEqualTo(1L);
        assertThat(offered).containsExactly((byte) 1, (byte) 2, (byte) 3);
    }

    @Test
    void a_confirmed_removal_ends_a_hand_off_lane_with_nothing_waiting_at_once() {
        // Arrange
        assertThat(sut.registerHandOff(1, request(1), _ -> true)).isTrue();

        // Act
        sut.finish(1);

        // Assert
        assertThat(terminated).containsExactly(1L);
        assertThat(sut.retainedLanes()).isZero();
    }

    @Test
    void a_connection_end_discards_the_deliveries_waiting_for_hand_off() throws Exception {
        // Arrange: two deliveries wait while the acceptor holds the first.
        var offered = new LinkedBlockingQueue<Byte>();
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        assertThat(sut.registerHandOff(1, request(4), delivery -> {
            offered.add(delivery.message().payload()[0]);
            entered.countDown();
            waitFor(release);
            return true;
        })).isTrue();
        for (int value = 1; value <= 3; value++) {
            assertThat(sut.deliver(token(1, 0), List.of(1L), message(value), false, 1)).isTrue();
        }
        assertThat(entered.await(5, SECONDS)).isTrue();

        // Act
        sut.connectionEnded(false, false);
        release.countDown();

        // Assert
        await().until(() -> sut.retainedDeliveries() == 0);
        assertThat(offered).containsExactly((byte) 1);
        assertThat(sut.dropped(1, Dropped.CONNECTION_ENDED)).isEqualTo(2);
    }

    @Test
    void one_hand_off_worker_serves_every_lane_until_the_last_one_ends() throws Exception {
        // Arrange: lane 1's acceptor holds the worker while a delivery for lane 2 arrives.
        var workers = new LinkedBlockingQueue<Thread>();
        var entered = new CountDownLatch(1);
        CountDownLatch release = release();
        assertThat(sut.registerHandOff(1, request(1), _ -> {
            workers.add(Thread.currentThread());
            entered.countDown();
            waitFor(release);
            return true;
        })).isTrue();
        assertThat(sut.registerHandOff(2, request(1), _ -> workers.add(Thread.currentThread())))
                .isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(1L), message(1), false, 1)).isTrue();
        assertThat(entered.await(5, SECONDS)).isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(2L), message(2), false, 1)).isTrue();

        // Act
        release.countDown();

        // Assert: lane 2's delivery waited for the same internal thread.
        Thread worker = requireNonNull(workers.poll(5, SECONDS));
        assertThat(workers.poll(5, SECONDS)).isSameAs(worker);
        assertThat(worker.isVirtual()).isTrue();
        assertThat(worker.getName()).isEqualTo("hato-lane-test-hand-off");

        // Act & Assert: the worker ends with the last hand-off lane, and the next hand-off lane
        // starts another, which ends when hand-off closes.
        sut.remove(1);
        sut.remove(2);
        assertThat(worker.join(Duration.ofSeconds(5))).isTrue();
        assertThat(sut.registerHandOff(3, request(1), _ -> workers.add(Thread.currentThread())))
                .isTrue();
        assertThat(sut.deliver(token(1, 0), List.of(3L), message(3), false, 1)).isTrue();
        Thread next = requireNonNull(workers.poll(5, SECONDS));
        assertThat(next).isNotSameAs(worker);
        sut.closeHandOffs();
        assertThat(next.join(Duration.ofSeconds(5))).isTrue();
    }

    private record Failure(DeliveryToken token, Throwable cause) {
    }

    /// A token whose connection was established by an attempt of the same number.
    private DeliveryToken token(long connection, long acknowledgement) {
        return new DeliveryToken(SESSION, connection, connection, acknowledgement);
    }

    private static Subscribe request(AckMode ackMode, int capacity) {
        return Subscribe.of(new TopicFilter("#"), Qos.AT_LEAST_ONCE)
                .withAckMode(ackMode)
                .withBufferCapacity(capacity);
    }

    private static MqttMessage qosOneMessage() {
        return qosOneMessage(1);
    }

    private static MqttMessage qosOneMessage(int value) {
        return MqttMessage.of(new Topic("a"), new byte[] {(byte) value}, Qos.AT_LEAST_ONCE, false);
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
                overloads::add,
                onReady,
                terminated::add
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

    private static DeliveryLanes.Receiver registerPull(
            DeliveryLanes lanes,
            long id,
            Subscribe request
    ) {
        DeliveryLanes.Receiver receiver = lanes.receiver(id, request);
        assertThat(lanes.register(receiver)).isTrue();
        return receiver;
    }
}
