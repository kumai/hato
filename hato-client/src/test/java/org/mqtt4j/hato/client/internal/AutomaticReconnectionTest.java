package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.spi.TimerScheduler;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// The shell's automatic reconnection on manual time. The backoff always picks its ceiling, so
/// consecutive delays are 1, 2, and then 4 seconds.
@Timeout(30)
class AutomaticReconnectionTest {

    private static final Duration FIRST_DELAY = Duration.ofSeconds(1);

    private static final Duration MAX_DELAY = Duration.ofSeconds(4);

    private final ManualTimerScheduler scheduler = new ManualTimerScheduler();

    private final LinkedBlockingQueue<ScriptTransport> transports = new LinkedBlockingQueue<>();

    private final Recorder listener = new Recorder();

    // Thrown once by the next transport creation.
    private volatile @Nullable RuntimeException factoryFailure;

    @Test
    void a_lost_connection_reconnects_after_its_announced_delay_with_the_next_attempt_number()
            throws Exception {
        // Arrange: the clock records each delay that the client schedules.
        var delays = new CopyOnWriteArrayList<Duration>();
        var clock = new TimerScheduler() {
            @Override
            public long nanoTime() {
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                delays.add(delay);
                return scheduler.schedule(delay, task);
            }
        };
        try (MqttClient sut = client(listener, true, false, Duration.ofSeconds(60), clock)) {
            ScriptTransport first = connect(sut);
            var cause = new IOException("peer reset");

            // Act
            first.receiver().onClosed(cause);

            // Assert: the announced delay is the scheduled one, and nothing starts before it.
            assertThat(listener.next()).isEqualTo("connected " + ConnAck.of(false, 1));
            assertThat(listener.next()).isEqualTo(
                    "disconnected " + Disconnection.of(cause, false, 1, FIRST_DELAY)
            );
            assertThat(delays.getLast()).isEqualTo(FIRST_DELAY);
            scheduler.advance(FIRST_DELAY.minusNanos(1));
            assertThat(transports).isEmpty();
            scheduler.advance(Duration.ofNanos(1));
            ScriptTransport second = nextTransport();
            second.accept();
            assertThat(listener.next()).isEqualTo("connected " + ConnAck.of(false, 2));
            assertThat(sut.whenConnected().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
        }
    }

    @Test
    void failed_automatic_attempts_back_off_and_a_final_refusal_stops_reconnecting()
            throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            lose(connect(sut));
            listener.next();
            listener.next();

            // Act & Assert: refusals that a retry can overcome are followed by one that it
            // cannot. Each failure announces the delay it reserved, and the last announces none.
            refuseNext(Duration.ofSeconds(1), ReasonCode.SERVER_UNAVAILABLE);
            assertFailure(listener.next(), 2, ReasonCode.SERVER_UNAVAILABLE, Duration.ofSeconds(2));
            refuseNext(Duration.ofSeconds(2), ReasonCode.NOT_AUTHORIZED);
            assertFailure(listener.next(), 3, ReasonCode.NOT_AUTHORIZED, MAX_DELAY);
            refuseNext(Duration.ofSeconds(4), ReasonCode.CLIENT_IDENTIFIER_NOT_VALID);
            assertFailure(listener.next(), 4, ReasonCode.CLIENT_IDENTIFIER_NOT_VALID, null);
            assertThat(scheduler.pendingTaskCount()).isZero();
            assertThat(sut.whenConnected()).isNotDone();

            // Act & Assert: an explicit request reconnects, and no other notification came first.
            CompletableFuture<ConnAck> restored = sut.connect();
            nextTransport().accept();
            assertThat(restored.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 5));
            assertThat(listener.next()).isEqualTo("connected " + ConnAck.of(false, 5));
        }
    }

    @Test
    void a_requests_first_failure_ends_it_by_default() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> connection = sut.connect();

            // Act
            nextTransport().receive(new Packet.ConnAck(
                    false,
                    ReasonCode.SERVER_UNAVAILABLE,
                    EMPTY
            ));

            // Assert
            Throwable failure = failureOf(connection);
            assertThat(listener.next())
                    .isEqualTo("failed " + Disconnection.of(failure, false, 1));
            assertThat(scheduler.pendingTaskCount()).isZero();
        }
    }

    @Test
    void a_requests_first_failure_is_retried_when_the_policy_retries_initial_failures()
            throws Exception {
        // Arrange
        try (MqttClient sut = client(listener, true, true)) {
            CompletableFuture<ConnAck> connection = sut.connect();

            // Act
            nextTransport().receive(new Packet.ConnAck(
                    false,
                    ReasonCode.SERVER_UNAVAILABLE,
                    EMPTY
            ));

            // Assert
            assertFailure(listener.next(), 1, ReasonCode.SERVER_UNAVAILABLE, FIRST_DELAY);
            assertThat(connection).isNotDone();
            scheduler.advance(FIRST_DELAY);
            nextTransport().accept();
            assertThat(connection.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
        }
    }

    @Test
    void connect_while_waiting_attempts_at_once_without_shortening_later_delays()
            throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            lose(connect(sut));
            listener.next();
            listener.next();

            // Act
            CompletableFuture<ConnAck> hastened = sut.connect();
            nextTransport().receive(new Packet.ConnAck(
                    false,
                    ReasonCode.SERVER_UNAVAILABLE,
                    EMPTY
            ));

            // Assert: the request keeps waiting, and the doubled delay follows.
            assertFailure(
                    listener.next(),
                    2,
                    ReasonCode.SERVER_UNAVAILABLE,
                    Duration.ofSeconds(2)
            );
            assertThat(hastened).isNotDone();
            scheduler.advance(Duration.ofSeconds(2));
            nextTransport().accept();
            assertThat(hastened.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 3));
        }
    }

    @Test
    void disconnect_while_waiting_completes_at_once_and_fails_held_publications()
            throws Exception {
        // Arrange: a persistent session holds an unacknowledged QoS 1 publication.
        try (MqttClient sut = client(listener, false, false)) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<PublishResult> held = sut.publish(message(Qos.AT_LEAST_ONCE));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            lose(transport);
            listener.next();
            listener.next();
            assertThat(held).isNotDone();

            // Act
            sut.disconnect().get(5, SECONDS);

            // Assert
            assertThat(failureOf(held)).isInstanceOf(MqttConnectionLostException.class);
            assertThat(scheduler.pendingTaskCount()).isZero();
        }
    }

    @Test
    void a_disconnect_from_the_ending_callback_stops_the_reconnection() throws Exception {
        // Arrange
        var stopped = new CountDownLatch(1);
        var holder = new MqttClient[1];
        var stopping = new Recorder() {
            @Override
            public void onDisconnected(Disconnection disconnection) {
                super.onDisconnected(disconnection);
                holder[0].disconnect().thenRun(stopped::countDown);
            }
        };
        try (MqttClient sut = client(stopping, true, false)) {
            holder[0] = sut;
            ScriptTransport transport = connect(sut);

            // Act
            lose(transport);

            // Assert
            assertThat(stopped.await(5, SECONDS)).isTrue();
            assertThat(scheduler.pendingTaskCount()).isZero();
        }
    }

    @Test
    @SuppressWarnings("try") // Closing during the wait is the behavior under test.
    void close_stops_a_wait_and_fails_its_observations() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            lose(connect(sut));
            listener.next();
            listener.next();
            CompletableFuture<ConnAck> observation = sut.whenConnected();

            // Act
            sut.close();

            // Assert
            assertThat(failureOf(observation)).isInstanceOf(IllegalStateException.class);
            assertThat(failureOf(sut.whenConnected())).isInstanceOf(IllegalStateException.class);
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isZero());
        }
    }

    @Test
    @SuppressWarnings("try") // Closing during the wait is the behavior under test.
    void a_terminated_client_calls_no_application_code_when_its_late_timer_fires()
            throws Exception {
        // Arrange: the clock counts its calls, and its cancellation of the waiting reconnection
        // comes too late, so that timer can still fire after termination.
        var calls = new AtomicInteger();
        var late = new AtomicReference<@Nullable Runnable>();
        var clock = new TimerScheduler() {
            @Override
            public long nanoTime() {
                calls.incrementAndGet();
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                calls.incrementAndGet();
                Cancellable cancellation = scheduler.schedule(delay, task);
                if (!delay.equals(FIRST_DELAY)) {
                    return cancellation;
                }
                return () -> {
                    calls.incrementAndGet();
                    cancellation.cancel();
                    late.set(task);
                };
            }
        };
        try (MqttClient sut = client(listener, true, false, Duration.ofSeconds(60), clock)) {
            lose(connect(sut));
            listener.next();
            listener.next();

            // Act: closing ends the wait without a notification, and the client terminates.
            sut.close();
            sut.terminated().get(5, SECONDS);
            int before = calls.get();
            requireNonNull(late.get()).run();

            // Assert: the late timer starts no attempt, and the client calls nothing more.
            assertThat(transports).isEmpty();
            assertThat(calls).hasValue(before);
            assertThat(listener.notifications).isEmpty();
        }
    }

    @Test
    void disconnect_during_an_automatic_attempt_disconnects_once_it_connects() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport attempt = automaticAttempt(sut);
            CompletableFuture<ConnAck> observation = sut.whenConnected();

            // Act
            CompletableFuture<Void> disconnected = sut.disconnect();
            attempt.accept();

            // Assert: the attempt connects only to write DISCONNECT, and nothing reconnects.
            assertThat(attempt.sent.poll(5, SECONDS)).isInstanceOf(Packet.Disconnect.class);
            assertThat(sut.whenConnected()).isNotDone();
            disconnected.get(5, SECONDS);
            assertThat(observation).isNotDone();
            assertThat(listener.next()).isEqualTo("connected " + ConnAck.of(false, 2));
            assertThat(listener.next())
                    .isEqualTo("disconnected " + Disconnection.of(null, true, 2));
            assertThat(scheduler.pendingTaskCount()).isZero();
        }
    }

    @Test
    void disconnect_during_an_automatic_attempt_completes_when_the_attempt_is_refused()
            throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport attempt = automaticAttempt(sut);

            // Act
            CompletableFuture<Void> disconnected = sut.disconnect();
            attempt.receive(new Packet.ConnAck(false, ReasonCode.SERVER_UNAVAILABLE, EMPTY));

            // Assert: the refusal ends the request that the disconnect withdrew.
            disconnected.get(5, SECONDS);
            Disconnection failure = listener.ending(listener.next());
            assertThat(failure.connectionAttempt()).isEqualTo(2);
            assertThat(failure.userRequested()).isTrue();
            assertThat(failure.reconnectDelay()).isEmpty();
            assertThat(scheduler.pendingTaskCount()).isZero();
        }
    }

    @Test
    void publications_fail_at_once_while_reconnecting_but_registrations_are_kept()
            throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            lose(connect(sut));
            listener.next();
            listener.next();

            // Act
            CompletableFuture<PublishResult> waiting = sut.publish(message(Qos.AT_LEAST_ONCE));
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("later"), Qos.AT_LEAST_ONCE)
            );
            scheduler.advance(FIRST_DELAY);
            ScriptTransport attempt = nextTransport();
            CompletableFuture<PublishResult> attempting = sut.publish(message(Qos.AT_LEAST_ONCE));
            attempt.accept();

            // Assert
            assertThat(failureOf(waiting)).isInstanceOf(MqttNotConnectedException.class);
            assertThat(failureOf(attempting)).isInstanceOf(MqttNotConnectedException.class);
            Packet.Subscribe packet = assertInstanceOf(
                    Packet.Subscribe.class,
                    attempt.sent.poll(5, SECONDS)
            );
            attempt.receive(new Packet.SubAck(
                    packet.packetId(),
                    List.of(ReasonCode.GRANTED_QOS_1),
                    EMPTY
            ));
            assertThat(subscription.established().get(5, SECONDS)).isEqualTo(Qos.AT_LEAST_ONCE);
        }
    }

    @Test
    void a_scheduled_reconnection_fails_qos_zero_and_holds_persistent_qos_one_until_the_puback()
            throws Exception {
        // Arrange: a QoS 0 write and a QoS 1 acknowledgement are both outstanding.
        try (MqttClient sut = client(listener, false, false)) {
            ScriptTransport first = connect(sut);
            CompletableFuture<PublishResult> acknowledged = sut.publish(message(Qos.AT_LEAST_ONCE));
            Packet.Publish original = assertInstanceOf(
                    Packet.Publish.class,
                    first.sent.poll(5, SECONDS)
            );
            first.completeSends = false;
            CompletableFuture<PublishResult> unwritten = sut.publish(message(Qos.AT_MOST_ONCE));
            assertThat(first.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act
            lose(first);

            // Assert: QoS 0 belongs to the connection; QoS 1 waits for the next one.
            assertThat(failureOf(unwritten)).isInstanceOf(MqttConnectionLostException.class);
            listener.next();
            listener.next();
            assertThat(acknowledged).isNotDone();
            scheduler.advance(FIRST_DELAY);
            ScriptTransport second = nextTransport();
            second.receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
            Packet.Publish replay = assertInstanceOf(
                    Packet.Publish.class,
                    second.sent.poll(5, SECONDS)
            );
            assertThat(replay.packetId()).isEqualTo(original.packetId());
            assertThat(replay.dup()).isTrue();
            second.receive(new Packet.PubAck(replay.packetId(), ReasonCode.SUCCESS, EMPTY));
            assertThat(acknowledged.get(5, SECONDS)).isEqualTo(PublishResult.of());
        }
    }

    @Test
    void a_nonpersistent_session_fails_its_publications_when_the_connection_ends()
            throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport first = connect(sut);
            CompletableFuture<PublishResult> publication = sut.publish(message(Qos.EXACTLY_ONCE));
            assertThat(first.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act
            lose(first);

            // Assert
            assertThat(failureOf(publication)).isInstanceOf(MqttConnectionLostException.class);
        }
    }

    @Test
    void a_reconnection_without_session_present_fails_the_held_publications() throws Exception {
        // Arrange: a persistent session holds a QoS 2 publication across the reconnection.
        try (MqttClient sut = client(listener, false, false)) {
            ScriptTransport first = connect(sut);
            CompletableFuture<PublishResult> publication = sut.publish(message(Qos.EXACTLY_ONCE));
            assertThat(first.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            lose(first);
            listener.next();
            listener.next();
            assertThat(publication).isNotDone();

            // Act: the broker reports no session.
            scheduler.advance(FIRST_DELAY);
            nextTransport().accept();

            // Assert
            assertThat(failureOf(publication)).isInstanceOf(MqttConnectionLostException.class);
        }
    }

    @Test
    void a_handler_failure_while_waiting_stops_reconnecting_and_reports_the_cancelled_attempt()
            throws Exception {
        // Arrange: a persistent session's handler is still running when its connection ends.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var failure = new IllegalStateException("handler failed while waiting");
        try (MqttClient sut = client(listener, false, false)) {
            ScriptTransport first = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("events"), Qos.AT_LEAST_ONCE),
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                        throw failure;
                    }
            );
            establish(first, subscription);
            first.receive(new Packet.Publish(
                    new Topic("events"),
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false,
                    false,
                    7,
                    EMPTY
            ));
            assertThat(entered.await(5, SECONDS)).isTrue();
            CompletableFuture<PublishResult> held = sut.publish(message(Qos.AT_LEAST_ONCE));
            assertThat(first.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            lose(first);
            listener.next();
            assertThat(listener.next()).contains("reconnectDelay=" + FIRST_DELAY);

            // Act
            release.countDown();

            // Assert: the stop reports the reserved attempt, and nothing reconnects.
            assertThat(listener.next())
                    .isEqualTo("failed " + Disconnection.of(failure, false, 2));
            assertThat(failureOf(held)).isInstanceOf(MqttConnectionLostException.class);
            assertThat(scheduler.pendingTaskCount()).isZero();

            // Act & Assert: an explicit connection restores the request with the next number.
            CompletableFuture<ConnAck> restored = sut.connect();
            nextTransport().receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
            assertThat(restored.get(5, SECONDS)).isEqualTo(ConnAck.of(true, 3));
            assertThat(listener.next()).isEqualTo("connected " + ConnAck.of(true, 3));
        }
    }

    @Test
    void a_handler_failure_during_an_automatic_attempt_ends_it_without_another()
            throws Exception {
        // Arrange: a persistent session's handler is still running when its connection ends,
        // and it fails while the automatic attempt waits for CONNACK.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var failure = new IllegalStateException("handler failed during the attempt");
        try (MqttClient sut = client(listener, false, false)) {
            ScriptTransport first = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("events"), Qos.AT_LEAST_ONCE),
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                        throw failure;
                    }
            );
            establish(first, subscription);
            first.receive(new Packet.Publish(
                    new Topic("events"),
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false,
                    false,
                    7,
                    EMPTY
            ));
            assertThat(entered.await(5, SECONDS)).isTrue();
            lose(first);
            listener.next();
            listener.next();
            scheduler.advance(FIRST_DELAY);
            ScriptTransport attempt = nextTransport();

            // Act
            release.countDown();

            // Assert: the stop ends the attempt, closes its transport, and schedules nothing.
            assertThat(listener.next())
                    .isEqualTo("failed " + Disconnection.of(failure, false, 2));
            await().untilAsserted(() -> assertThat(attempt.closed).isTrue());
            assertThat(scheduler.pendingTaskCount()).isZero();
            assertThat(sut.whenConnected()).isNotDone();

            // Act & Assert: an explicit request reconnects, and no other notification came first.
            CompletableFuture<ConnAck> restored = sut.connect();
            nextTransport().receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
            assertThat(restored.get(5, SECONDS)).isEqualTo(ConnAck.of(true, 3));
            assertThat(listener.next()).isEqualTo("connected " + ConnAck.of(true, 3));
        }
    }

    @Test
    void connection_observations_complete_on_the_next_connection_without_starting_one()
            throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> first = sut.whenConnected();
            CompletableFuture<ConnAck> cancelled = sut.whenConnected();
            cancelled.cancel(false);
            // Without an attempt, a disconnect completes at once.
            sut.disconnect().get(5, SECONDS);
            assertThat(transports).isEmpty();

            // Act
            ScriptTransport transport = connect(sut);

            // Assert: completion admits operations on the announced connection.
            ConnAck ack = first.get(5, SECONDS);
            assertThat(ack).isEqualTo(ConnAck.of(false, 1));
            assertThat(sut.whenConnected().getNow(null)).isSameAs(ack);
            lose(transport);
            listener.next();
            listener.next();
            CompletableFuture<ConnAck> next = sut.whenConnected();
            CompletableFuture<PublishResult> published = next.thenCompose(_ ->
                    sut.publish(message(Qos.AT_MOST_ONCE)));
            assertThat(next).isNotDone();
            scheduler.advance(FIRST_DELAY);
            nextTransport().accept();
            assertThat(next.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
            assertThat(published.get(5, SECONDS)).isEqualTo(PublishResult.of());
            await().untilAsserted(() -> assertThat(((ClientActor) sut).usage().waiters()).isZero());
        }
    }

    @Test
    void an_internal_failure_fails_pending_connection_observations_with_its_cause()
            throws Exception {
        // Arrange: the actor reads a clock that fails with an error, an internal failure.
        var failure = new AssertionError("clock broken");
        var broken = new AtomicBoolean();
        var clock = new TimerScheduler() {
            @Override
            public long nanoTime() {
                if (broken.get()) {
                    throw failure;
                }
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                return scheduler.schedule(delay, task);
            }
        };
        try (MqttClient sut = client(listener, true, false, Duration.ofSeconds(60), clock)) {
            lose(connect(sut));
            listener.next();
            listener.next();
            CompletableFuture<ConnAck> observation = sut.whenConnected();
            broken.set(true);

            // Act: the reconnection timer makes the actor read the clock.
            scheduler.advance(FIRST_DELAY);

            // Assert: the listener learns that the reserved attempt will not run.
            assertThat(failureOf(observation))
                    .isInstanceOf(MqttConnectionLostException.class)
                    .hasCause(failure);
            assertThat(failureOf(sut.whenConnected()))
                    .isInstanceOf(MqttConnectionLostException.class)
                    .hasCause(failure);
            Disconnection ending = listener.ending(listener.next());
            assertThat(ending.connectionAttempt()).isEqualTo(2);
            assertThat(ending.userRequested()).isFalse();
            assertThat(ending.reconnectDelay()).isEmpty();
            assertThat(ending.cause()).get()
                    .isInstanceOfSatisfying(MqttConnectException.class, cause ->
                            assertThat(cause).hasCause(failure));
        }
    }

    @Test
    void pending_connection_observations_share_the_waiter_limit() throws Exception {
        // Arrange
        try (MqttClient client = client()) {
            var sut = (ClientActor) client;
            var observations = new ArrayList<CompletableFuture<ConnAck>>();
            for (int i = 0; i < 1024; i++) {
                observations.add(sut.whenConnected());
            }

            // Act
            CompletableFuture<ConnAck> refused = sut.whenConnected();

            // Assert: a full limit refuses only the observation.
            assertThat(refused).isDone();
            assertThat(failureOf(refused)).isInstanceOf(MqttCapacityException.class);
            assertThat(sut.usage().waiters()).isEqualTo(1024);
            connect(sut);
            for (CompletableFuture<ConnAck> observation : observations) {
                assertThat(observation.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            }
            await().untilAsserted(() -> assertThat(sut.usage().waiters()).isZero());
            assertThat(sut.whenConnected().getNow(null)).isEqualTo(ConnAck.of(false, 1));
        }
    }

    @Test
    void a_blocked_listener_delays_no_automatic_attempt() throws Exception {
        // Arrange: the listener blocks in the first disconnection callback.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var blocking = new Recorder() {
            @Override
            public void onDisconnected(Disconnection disconnection) {
                super.onDisconnected(disconnection);
                entered.countDown();
                waitFor(release);
            }
        };
        try (MqttClient sut = client(blocking, true, false)) {
            lose(connect(sut));
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            scheduler.advance(FIRST_DELAY);

            // Assert
            nextTransport().accept();
            assertThat(sut.whenConnected().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
        } finally {
            release.countDown();
        }
    }

    @Test
    void late_events_of_an_earlier_connection_cannot_end_the_next_attempt() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport first = connect(sut);
            lose(first);
            listener.next();
            listener.next();
            scheduler.advance(FIRST_DELAY);
            ScriptTransport second = nextTransport();

            // Act: the ended transport keeps reporting input and failures.
            first.receive(new Packet.ConnAck(false, ReasonCode.NOT_AUTHORIZED, EMPTY));
            first.receiver().onClosed(new IOException("late failure"));
            second.accept();

            // Assert
            assertThat(listener.next()).isEqualTo("connected " + ConnAck.of(false, 2));
            assertThat(sut.publish(message(Qos.AT_MOST_ONCE)).get(5, SECONDS))
                    .isEqualTo(PublishResult.of());
        }
    }

    @Test
    void an_unanswered_publication_times_out_reconnects_and_completes_on_the_next_connection()
            throws Exception {
        // Arrange: a persistent session waits five seconds for PUBACK.
        try (MqttClient sut = client(listener, false, false, Duration.ofSeconds(5))) {
            ScriptTransport first = connect(sut);
            CompletableFuture<PublishResult> publication = sut.publish(message(Qos.AT_LEAST_ONCE));
            Packet.Publish original = assertInstanceOf(
                    Packet.Publish.class,
                    first.sent.poll(5, SECONDS)
            );
            // Wait until the response deadline joins the stable-connection timer.
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(2));

            // Act
            scheduler.advance(Duration.ofSeconds(5));

            // Assert
            listener.next();
            String ending = listener.next();
            assertThat(ending).contains("TimeoutException: PUBACK deadline expired")
                    .contains("reconnectDelay=" + FIRST_DELAY);
            assertThat(publication).isNotDone();
            scheduler.advance(FIRST_DELAY);
            ScriptTransport second = nextTransport();
            second.receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
            Packet.Publish replay = assertInstanceOf(
                    Packet.Publish.class,
                    second.sent.poll(5, SECONDS)
            );
            assertThat(replay.packetId()).isEqualTo(original.packetId());
            second.receive(new Packet.PubAck(replay.packetId(), ReasonCode.SUCCESS, EMPTY));
            assertThat(publication.get(5, SECONDS)).isEqualTo(PublishResult.of());
        }
    }

    @Test
    void registrations_and_removals_continue_across_an_automatic_reconnection() throws Exception {
        // Arrange: one SUBSCRIBE and one UNSUBSCRIBE await their answers when the connection ends.
        try (MqttClient sut = client(listener, false, false)) {
            ScriptTransport first = connect(sut);
            Subscription removed = sut.subscribe(
                    Subscribe.of(new TopicFilter("removed"), Qos.AT_LEAST_ONCE)
            );
            establish(first, removed);
            Subscription pending = sut.subscribe(
                    Subscribe.of(new TopicFilter("pending"), Qos.AT_LEAST_ONCE)
            );
            assertThat(first.sent.poll(5, SECONDS)).isInstanceOf(Packet.Subscribe.class);
            CompletableFuture<Void> removal = removed.unsubscribe();
            assertThat(first.sent.poll(5, SECONDS)).isInstanceOf(Packet.Unsubscribe.class);

            // Act
            lose(first);
            listener.next();
            listener.next();
            scheduler.advance(FIRST_DELAY);
            ScriptTransport second = nextTransport();
            second.receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));

            // Assert: the continued session receives both requests again.
            var resent = new ArrayList<Packet>();
            resent.add(requireNonNull(second.sent.poll(5, SECONDS)));
            resent.add(requireNonNull(second.sent.poll(5, SECONDS)));
            Packet.Subscribe subscribe = resent.stream()
                    .filter(Packet.Subscribe.class::isInstance)
                    .map(Packet.Subscribe.class::cast)
                    .findFirst()
                    .orElseThrow();
            Packet.Unsubscribe unsubscribe = resent.stream()
                    .filter(Packet.Unsubscribe.class::isInstance)
                    .map(Packet.Unsubscribe.class::cast)
                    .findFirst()
                    .orElseThrow();
            second.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.GRANTED_QOS_1),
                    EMPTY
            ));
            second.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            assertThat(pending.established().get(5, SECONDS)).isEqualTo(Qos.AT_LEAST_ONCE);
            removal.get(5, SECONDS);
        }
    }

    @Test
    void overload_reconnects_and_connections_that_end_quickly_keep_growing_the_delay()
            throws Exception {
        // Arrange: a blocked handler with room for one QoS 1 delivery.
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription slow = sut.subscribe(
                    Subscribe.of(new TopicFilter("burst"), Qos.AT_LEAST_ONCE).withBufferCapacity(1),
                    _ -> waitFor(release)
            );
            establish(transport, slow);
            listener.next();

            // Act: the second delivery overloads the subscription.
            for (int packetId = 1; packetId <= 2; packetId++) {
                transport.receive(new Packet.Publish(
                        new Topic("burst"),
                        new byte[] {1},
                        Qos.AT_LEAST_ONCE,
                        false,
                        false,
                        packetId,
                        EMPTY
                ));
            }

            // Assert: overload reconnects, and each quick ending doubles the delay.
            assertThat(listener.next()).contains("MqttCapacityException")
                    .contains("reconnectDelay=" + FIRST_DELAY);
            for (Duration delay : List.of(FIRST_DELAY, Duration.ofSeconds(2))) {
                scheduler.advance(delay);
                transport = nextTransport();
                transport.accept();
                listener.next();
                lose(transport);
                assertThat(listener.next()).contains("reconnectDelay=" + delay.multipliedBy(2));
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_handler_failure_while_connected_stops_without_reconnecting() throws Exception {
        // Arrange
        var failure = new IllegalStateException("handler failed");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("events"), Qos.AT_LEAST_ONCE),
                    _ -> {
                        throw failure;
                    }
            );
            establish(transport, subscription);
            listener.next();

            // Act
            transport.receive(new Packet.Publish(
                    new Topic("events"),
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false,
                    false,
                    1,
                    EMPTY
            ));

            // Assert: the stop announces no reconnection, and an explicit connect resumes.
            assertThat(listener.next())
                    .isEqualTo("disconnected " + Disconnection.of(failure, false, 1));
            assertThat(scheduler.pendingTaskCount()).isZero();
            CompletableFuture<ConnAck> restored = sut.connect();
            nextTransport().accept();
            assertThat(restored.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
            assertThat(listener.next()).isEqualTo("connected " + ConnAck.of(false, 2));
        }
    }

    @Test
    void a_reconnection_due_at_once_survives_a_control_queue_that_ended_the_connection()
            throws Exception {
        // Arrange: the scheduler runs a due task inside its own registration, and the next
        // delay is zero, so the reconnection fires while the ended connection's events still
        // fill the control queue.
        var random = new ReconnectBackoffTest.MaximumRandom();
        random.zero = true;
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var holding = new AtomicBoolean();
        var inline = new TimerScheduler() {
            @Override
            public long nanoTime() {
                if (Thread.currentThread().getName().endsWith("-actor") && holding.get()) {
                    holding.set(false);
                    held.countDown();
                    waitFor(release);
                }
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                if (delay.isZero()) {
                    task.run();
                    return () -> { };
                }
                return scheduler.schedule(delay, task);
            }
        };
        try (MqttClient sut = client(
                listener,
                true,
                false,
                Duration.ofSeconds(60),
                inline,
                new ReconnectBackoff(FIRST_DELAY, MAX_DELAY, random)
        )) {
            ScriptTransport first = connect(sut);
            holding.set(true);
            first.receive(new Packet.PingResp());
            assertThat(held.await(5, SECONDS)).isTrue();

            // Act: control events overflow while the actor is held.
            for (int i = 0; i < 300; i++) {
                first.receiver().onConnected();
            }
            release.countDown();

            // Assert: the overload ends the connection, and the reconnection still starts.
            listener.next();
            assertThat(listener.next()).contains("Control mailbox capacity")
                    .contains("reconnectDelay=PT0S");
            nextTransport().accept();
            assertThat(listener.next()).isEqualTo("connected " + ConnAck.of(false, 2));
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_late_callback_of_a_cancelled_reconnection_timer_cannot_hide_the_next_one()
            throws Exception {
        // Arrange: cancelling the waiting timer comes too late for its callback, which runs
        // just after the next timer fires and before the actor takes their shared wake.
        var second = Duration.ofSeconds(2);
        var late = new AtomicReference<@Nullable Runnable>();
        var clock = new TimerScheduler() {
            @Override
            public long nanoTime() {
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                if (delay.equals(second)) {
                    task.run();
                    requireNonNull(late.get()).run();
                    return () -> { };
                }
                Cancellable cancellation = scheduler.schedule(delay, task);
                if (!delay.equals(FIRST_DELAY)) {
                    return cancellation;
                }
                return () -> {
                    cancellation.cancel();
                    late.set(task);
                };
            }
        };
        try (MqttClient sut = client(listener, true, false, Duration.ofSeconds(60), clock)) {
            lose(connect(sut));
            listener.next();
            listener.next();

            // Act: hasten the reconnection, whose refused attempt reserves the next timer.
            CompletableFuture<ConnAck> hastened = sut.connect();
            nextTransport().receive(new Packet.ConnAck(
                    false,
                    ReasonCode.SERVER_UNAVAILABLE,
                    EMPTY
            ));

            // Assert: the next timer's attempt still starts.
            assertFailure(listener.next(), 2, ReasonCode.SERVER_UNAVAILABLE, second);
            nextTransport().accept();
            assertThat(hastened.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 3));
        }
    }

    @Test
    void a_wake_of_a_replaced_reconnection_timer_starts_no_early_attempt() throws Exception {
        // Arrange: the waiting timer fires while the actor is held in connect(), and the hastened
        // attempt fails at once, reserving the next timer before the actor takes the wake.
        var hold = new AtomicBoolean();
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var clock = new TimerScheduler() {
            @Override
            public long nanoTime() {
                if (Thread.currentThread().getName().endsWith("-actor") && hold.getAndSet(false)) {
                    held.countDown();
                    waitFor(release);
                }
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                return scheduler.schedule(delay, task);
            }
        };
        try (MqttClient sut = client(listener, true, false, Duration.ofSeconds(60), clock)) {
            lose(connect(sut));
            listener.next();
            listener.next();
            hold.set(true);
            CompletableFuture<ConnAck> hastened = sut.connect();
            assertThat(held.await(5, SECONDS)).isTrue();
            scheduler.advance(FIRST_DELAY);
            factoryFailure = new IllegalStateException("transport creation failed");

            // Act
            release.countDown();

            // Assert: the failure reserves the next delay, and the wake starts nothing; without
            // an attempt, a disconnect completes at once.
            Disconnection failure = listener.ending(listener.next());
            assertThat(failure.connectionAttempt()).isEqualTo(2);
            assertThat(failure.reconnectDelay()).contains(Duration.ofSeconds(2));
            sut.disconnect().get(5, SECONDS);
            assertThat(transports).isEmpty();
            assertThat(failureOf(hastened)).isInstanceOf(MqttConnectException.class);
        } finally {
            release.countDown();
        }
    }

    @Test
    void the_builder_reconnects_by_default_after_a_sixty_second_response_deadline()
            throws Exception {
        // Arrange
        try (MqttClient sut = builder().build()) {
            ScriptTransport transport = connect(sut);
            listener.next();
            sut.publish(message(Qos.AT_LEAST_ONCE));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            // Wait until the response deadline joins the stable-connection timer.
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(2));

            // Act & Assert: the deadline counts from the write. A QoS 0 publication passes the
            // actor after any expired timer, so it fails if the deadline came earlier.
            scheduler.advance(Duration.ofSeconds(60).minusNanos(1));
            assertThat(sut.publish(message(Qos.AT_MOST_ONCE)).get(5, SECONDS))
                    .isEqualTo(PublishResult.of());
            scheduler.advance(Duration.ofNanos(1));
            Disconnection ending = listener.ending(listener.next());
            assertThat(ending.cause()).get()
                    .isInstanceOf(TimeoutException.class)
                    .hasFieldOrPropertyWithValue("message", "PUBACK deadline expired");
            // The default policy's first delay is at most one second.
            Duration delay = ending.reconnectDelay().orElseThrow();
            assertThat(delay).isLessThanOrEqualTo(Duration.ofSeconds(1));
            assertThat(transport.closed).isTrue();
        }
    }

    @Test
    void the_default_policy_gives_up_a_first_refusal() throws Exception {
        // Arrange
        try (MqttClient sut = builder().build()) {
            CompletableFuture<ConnAck> connection = sut.connect();

            // Act
            nextTransport().receive(new Packet.ConnAck(
                    false,
                    ReasonCode.SERVER_UNAVAILABLE,
                    EMPTY
            ));

            // Assert
            assertThat(failureOf(connection)).isInstanceOf(MqttConnectException.class);
            assertFailure(listener.next(), 1, ReasonCode.SERVER_UNAVAILABLE, null);
            assertThat(scheduler.pendingTaskCount()).isZero();
        }
    }

    @Test
    void a_builder_policy_with_initial_retry_retries_a_first_refusal_within_its_initial_delay()
            throws Exception {
        // Arrange: the initial delay is a millionth of the maximum.
        ReconnectPolicy policy = ReconnectPolicy
                .exponentialBackoff(Duration.ofMillis(1), Duration.ofSeconds(1000))
                .withInitialRetry();
        try (MqttClient sut = builder().reconnectPolicy(policy).build()) {
            CompletableFuture<ConnAck> connection = sut.connect();

            // Act
            nextTransport().receive(new Packet.ConnAck(
                    false,
                    ReasonCode.SERVER_UNAVAILABLE,
                    EMPTY
            ));

            // Assert
            Disconnection failure = listener.ending(listener.next());
            Duration delay = failure.reconnectDelay().orElseThrow();
            assertThat(delay).isLessThanOrEqualTo(Duration.ofMillis(1));
            assertThat(connection).isNotDone();
            scheduler.advance(Duration.ofMillis(1));
            nextTransport().accept();
            assertThat(connection.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
        }
    }

    // A builder on manual time and scripted transports that keeps the default policy.
    private MqttClientBuilder builder() {
        return MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .keepAlive(Duration.ZERO)
                .timerScheduler(scheduler)
                .connectionListener(listener)
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                });
    }

    private MqttClient client() {
        return client(listener, true, false);
    }

    private MqttClient client(
            ConnectionListener notifications,
            boolean cleanStart,
            boolean initialRetry
    ) {
        return client(notifications, cleanStart, initialRetry, Duration.ofSeconds(60));
    }

    private MqttClient client(
            ConnectionListener notifications,
            boolean cleanStart,
            boolean initialRetry,
            Duration responseTimeout
    ) {
        return client(notifications, cleanStart, initialRetry, responseTimeout, scheduler);
    }

    private MqttClient client(
            ConnectionListener notifications,
            boolean cleanStart,
            boolean initialRetry,
            Duration responseTimeout,
            TimerScheduler clock
    ) {
        return client(
                notifications,
                cleanStart,
                initialRetry,
                responseTimeout,
                clock,
                new ReconnectBackoff(
                        FIRST_DELAY,
                        MAX_DELAY,
                        new ReconnectBackoffTest.MaximumRandom()
                )
        );
    }

    private MqttClient client(
            ConnectionListener notifications,
            boolean cleanStart,
            boolean initialRetry,
            Duration responseTimeout,
            TimerScheduler clock,
            ReconnectBackoff backoff
    ) {
        return new ClientActor(
                URI.create("mqtt://localhost:1883"),
                "reconnection-test",
                Duration.ZERO,
                Duration.ofSeconds(30),
                16 * 1024 * 1024,
                clock,
                notifications,
                _ -> {
                    RuntimeException failure = factoryFailure;
                    if (failure != null) {
                        factoryFailure = null;
                        throw failure;
                    }
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                },
                PacketCodec.of(MqttVersion.V3_1_1),
                MqttVersion.V3_1_1,
                cleanStart ? Duration.ZERO : Duration.ofHours(1),
                null,
                null,
                backoff,
                initialRetry ? RetryRules.DEFAULT.withInitialRetry() : RetryRules.DEFAULT,
                responseTimeout,
                DeliveryOptionsAccess::execution
        );
    }

    private ScriptTransport connect(MqttClient sut) throws Exception {
        CompletableFuture<ConnAck> connection = sut.connect();
        ScriptTransport transport = nextTransport();
        transport.accept();
        connection.get(5, SECONDS);
        return transport;
    }

    private ScriptTransport nextTransport() throws Exception {
        ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
        assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Connect.class);
        return transport;
    }

    private static void lose(ScriptTransport transport) {
        transport.receiver().onClosed(new IOException("connection lost"));
    }

    // Loses the first connection and returns the transport of the automatic attempt after it.
    private ScriptTransport automaticAttempt(MqttClient sut) throws Exception {
        lose(connect(sut));
        listener.next();
        listener.next();
        scheduler.advance(FIRST_DELAY);
        return nextTransport();
    }

    private void refuseNext(Duration delay, ReasonCode reason) throws Exception {
        scheduler.advance(delay);
        nextTransport().receive(new Packet.ConnAck(false, reason, EMPTY));
    }

    private void assertFailure(
            String notification,
            long attempt,
            ReasonCode reason,
            @Nullable Duration delay
    ) {
        Disconnection failure = listener.ending(notification);
        assertThat(failure.connectionAttempt()).isEqualTo(attempt);
        assertThat(failure.userRequested()).isFalse();
        assertThat(failure.reconnectDelay()).isEqualTo(Optional.ofNullable(delay));
        assertThat(failure.cause()).get()
                .isInstanceOfSatisfying(MqttConnectException.class, refusal ->
                        assertThat(refusal.reasonCode()).contains(reason));
    }

    private static void establish(ScriptTransport transport, Subscription subscription)
            throws Exception {
        Packet.Subscribe packet = assertInstanceOf(
                Packet.Subscribe.class,
                transport.sent.poll(5, SECONDS)
        );
        transport.receive(new Packet.SubAck(
                packet.packetId(),
                List.of(ReasonCode.of(packet.subscriptions().getFirst().qos().level())),
                EMPTY
        ));
        subscription.established().get(5, SECONDS);
    }

    private static MqttMessage message(Qos qos) {
        return MqttMessage.of(new Topic("messages"), new byte[] {1}, qos, false);
    }

    private static Throwable failureOf(CompletableFuture<?> operation) throws Exception {
        return requireNonNull(operation.handle((_, failure) -> failure).get(5, SECONDS));
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            if (!latch.await(30, SECONDS)) {
                throw new AssertionError("Test failed to release the callback");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    /// Records every notification in order as its kind and value. Endings are also kept as
    /// values, so a test can inspect their causes and delays.
    private static class Recorder implements ConnectionListener {

        private final LinkedBlockingQueue<String> notifications = new LinkedBlockingQueue<>();

        private final Map<String, Disconnection> endings = new ConcurrentHashMap<>();

        @Override
        public void onConnected(ConnAck connAck) {
            notifications.add("connected " + connAck);
        }

        @Override
        public void onDisconnected(Disconnection disconnection) {
            String notification = "disconnected " + disconnection;
            endings.put(notification, disconnection);
            notifications.add(notification);
        }

        @Override
        public void onConnectFailed(Disconnection failure) {
            String notification = "failed " + failure;
            endings.put(notification, failure);
            notifications.add(notification);
        }

        Disconnection ending(String notification) {
            return requireNonNull(endings.get(notification), notification);
        }

        String next() throws InterruptedException {
            String notification = notifications.poll(5, SECONDS);
            if (notification == null) {
                throw new AssertionError(new TimeoutException("No notification arrived"));
            }
            return notification;
        }
    }
}
