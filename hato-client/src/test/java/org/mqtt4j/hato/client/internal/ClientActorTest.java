package org.mqtt4j.hato.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.DeliveryOptionsAccess;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.FailurePolicy;
import org.mqtt4j.hato.MqttCapacityException;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttConnectException;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttCredentials;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttNotConnectedException;
import org.mqtt4j.hato.MqttProtocolException;
import org.mqtt4j.hato.MqttSubscribeException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.hato.client.internal.common.protocol.Properties;
import org.mqtt4j.hato.spi.TimerScheduler;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.spi.Transport.SendResult.CLOSED;

class ClientActorTest {

    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    private final ManualTimerScheduler scheduler = new ManualTimerScheduler();
    private final LinkedBlockingQueue<Disconnection> ends = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<ScriptTransport> transports = new LinkedBlockingQueue<>();

    private volatile Runnable beforeActorTime = () -> { };

    @Test
    void joining_connection_requests_start_no_new_attempt_or_notification() throws Exception {
        // Arrange: the listener records connections as well as their endings.
        var connections = new LinkedBlockingQueue<ConnAck>();
        var listener = new ConnectionListener() {
            @Override
            public void onConnected(ConnAck ack) {
                connections.add(ack);
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                ends.add(disconnection);
            }
        };
        try (MqttClient client = client(listener)) {
            var sut = (ClientActor) client;
            CompletableFuture<ConnAck> first = sut.connect();
            ScriptTransport transport = nextTransport();
            CompletableFuture<ConnAck> joined = sut.connect();

            // Act
            transport.accept();

            // Assert: joining an attempt and observing its established result start no new I/O.
            assertThat(first.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            assertThat(joined.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            for (int i = 0; i < 64; i++) {
                assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            }
            assertThat(transports).isEmpty();
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            // The ending follows every earlier notification, so one connection was announced.
            assertThat(connections).containsExactly(ConnAck.of(false, 1));
            await().untilAsserted(() -> assertThat(sut.usage().notifications()).isZero());
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void a_clock_failure_while_observing_an_established_connection_stops_its_request()
            throws Exception {
        // Arrange
        var failure = new IllegalStateException("Clock unavailable");
        var reject = new AtomicBoolean();
        var clock = new TimerScheduler() {
            private volatile @Nullable Thread actor;

            @Override
            public long nanoTime() {
                // The first read starts connect on the actor. A write completion read by the
                // sender must not take the failure meant for the next connect.
                if (actor == null) {
                    actor = Thread.currentThread();
                }
                if (actor == Thread.currentThread() && reject.getAndSet(false)) {
                    throw failure;
                }
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                return scheduler.schedule(delay, task);
            }
        };
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .timerScheduler(clock)
                .connectionListener(ends::add)
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                })
                .build()) {
            ScriptTransport transport = connect(sut);

            // Act
            reject.set(true);

            // Assert: failure is not confined to the additional connect future.
            assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .hasRootCause(failure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(transport.closed).isTrue();
            assertThat(scheduler.pendingTaskCount()).isZero();
            assertThat(transports).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalid_credentials_fail_one_attempt_and_allow_another(boolean returnsNull)
            throws Exception {
        // Arrange
        var calls = new AtomicInteger();
        var supplierFailure = new IllegalStateException("credentials unavailable");
        try (MqttClient sut = clientWithCredentials(() -> {
            assertThat(Thread.currentThread().isVirtual()).isTrue();
            assertThat(Thread.currentThread().getName()).endsWith("-credentials");
            if (calls.incrementAndGet() == 1) {
                if (returnsNull) {
                    return null;
                }
                throw supplierFailure;
            }
            return MqttCredentials.of("fresh");
        })) {
            // Act & Assert
            CompletableFuture<ConnAck> first = sut.connect();
            assertThatThrownBy(() -> first.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .hasRootCauseInstanceOf(
                            returnsNull ? NullPointerException.class : IllegalStateException.class
                    );
            assertThat(transports).isEmpty();

            CompletableFuture<ConnAck> second = sut.connect();
            ScriptTransport current = requireNonNull(transports.poll(5, SECONDS));
            assertThat(current.sent.poll(5, SECONDS))
                    .isInstanceOfSatisfying(
                            Packet.Connect.class,
                            packet -> assertThat(packet.username()).isEqualTo("fresh")
                    );
            current.accept();
            assertThat(second.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
            assertThat(calls).hasValue(2);
        }
    }

    @Test
    void an_expired_supplier_cannot_overlap_or_supply_the_next_attempt() throws Exception {
        // Arrange: the first supplier deliberately ignores cancellation.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        try (MqttClient sut = clientWithCredentials(() -> {
            int running = active.incrementAndGet();
            maximum.accumulateAndGet(running, Math::max);
            try {
                if (calls.incrementAndGet() == 1) {
                    entered.countDown();
                    awaitIgnoringInterrupts(release);
                    return MqttCredentials.of("expired");
                }
                assertThat(Thread.currentThread().isInterrupted()).isFalse();
                return MqttCredentials.of("current");
            } finally {
                active.decrementAndGet();
            }
        })) {
            CompletableFuture<ConnAck> first = sut.connect();
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(transports).isEmpty();
            await().pollInterval(Duration.ofMillis(1))
                    .until(() -> scheduler.pendingTaskCount() == 1);

            // Act: two attempts expire while the first application call still holds its slot.
            scheduler.advance(Duration.ofSeconds(30));
            assertThatThrownBy(() -> first.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .hasRootCauseInstanceOf(TimeoutException.class);
            CompletableFuture<ConnAck> second = sut.connect();
            await().pollInterval(Duration.ofMillis(1))
                    .until(() -> scheduler.pendingTaskCount() == 1);
            scheduler.advance(Duration.ofSeconds(30));
            assertThatThrownBy(() -> second.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class);
            CompletableFuture<ConnAck> third = sut.connect();
            await().pollInterval(Duration.ofMillis(1))
                    .until(() -> scheduler.pendingTaskCount() == 1);
            assertThat(calls).hasValue(1);
            assertThat(transports).isEmpty();
            release.countDown();

            // Assert
            ScriptTransport current = requireNonNull(transports.poll(5, SECONDS));
            assertThat(current.sent.poll(5, SECONDS))
                    .isInstanceOfSatisfying(
                            Packet.Connect.class,
                            packet -> assertThat(packet.username()).isEqualTo("current")
                    );
            current.accept();
            assertThat(third.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 3));
            assertThat(transports).isEmpty();
            assertThat(calls).hasValue(2);
            assertThat(maximum).hasValue(1);
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Closing while the supplier runs is the behavior under test.
    void close_finishes_without_waiting_for_a_blocked_credentials_supplier() throws Exception {
        // Arrange
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var returned = new CountDownLatch(1);
        var actor = new AtomicReference<@Nullable Thread>();
        var timer = new TimerScheduler() {
            @Override
            public long nanoTime() {
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                // The actor schedules the connection deadline itself.
                actor.compareAndSet(null, Thread.currentThread());
                return scheduler.schedule(delay, task);
            }
        };
        try (MqttClient sut = clientWithCredentials(() -> {
            entered.countDown();
            awaitIgnoringInterrupts(release);
            returned.countDown();
            return MqttCredentials.of("late");
        }, timer)) {
            CompletableFuture<ConnAck> connection = sut.connect();
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(transports).isEmpty();

            // Act
            sut.close();

            // Assert: the actor finishes its whole shutdown while the supplier is still blocked.
            assertThatThrownBy(() -> connection.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class);
            assertThat(requireNonNull(actor.get()).join(Duration.ofSeconds(5))).isTrue();
            assertThat(returned.getCount()).isEqualTo(1);
            release.countDown();
            assertThat(returned.await(5, SECONDS)).isTrue();
            assertThat(transports).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void credentials_finish_before_transport_opens_with_the_remaining_deadline() throws Exception {
        // Arrange
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = clientWithCredentials(() -> {
            entered.countDown();
            waitFor(release);
            return MqttCredentials.of("ready");
        })) {
            CompletableFuture<ConnAck> connection = sut.connect();
            assertThat(entered.await(5, SECONDS)).isTrue();
            assertThat(scheduler.pendingTaskCount()).isEqualTo(1);
            assertThat(transports).isEmpty();

            // Act
            scheduler.advance(Duration.ofSeconds(20));
            release.countDown();
            ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Connect.class);

            // Assert: waiting for CONNACK still ends at the original total deadline.
            assertThat(transport.startTimeout).isEqualTo(Duration.ofSeconds(10));
            scheduler.advance(Duration.ofSeconds(10));
            assertThatThrownBy(() -> connection.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .hasRootCauseInstanceOf(TimeoutException.class);
            await().until(() -> transport.closed);
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {30, 31})
    void expired_credentials_cannot_open_transport_before_the_timer_callback(long elapsed)
            throws Exception {
        // Arrange: time advances, but the scheduler has not delivered its callback yet.
        var now = new AtomicLong();
        var scheduled = new AtomicInteger();
        var delayed = new TimerScheduler() {
            @Override
            public long nanoTime() {
                return now.get();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                scheduled.incrementAndGet();
                return () -> { };
            }
        };
        try (MqttClient sut = clientWithCredentials(() -> {
            assertThat(scheduled).hasValue(1);
            now.set(Duration.ofSeconds(elapsed).toNanos());
            return MqttCredentials.of("expired");
        }, delayed)) {
            // Act & Assert
            assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .hasRootCauseInstanceOf(TimeoutException.class);
            assertThat(transports).isEmpty();
        }
    }

    private MqttClient clientWithCredentials(Supplier<MqttCredentials> supplier) {
        return clientWithCredentials(supplier, scheduler);
    }

    private MqttClient clientWithCredentials(
            Supplier<MqttCredentials> supplier,
            TimerScheduler timer
    ) {
        return MqttClient.builder()
                .clientId("credentials-test")
                .server(URI.create("mqtt://localhost:1883"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionTimeout(Duration.ofSeconds(30))
                .timerScheduler(timer)
                .credentials(supplier)
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                })
                .build();
    }

    /// Waits through interruption like a supplier that ignores cancellation, then returns with
    /// the interrupt status set, as a supplier restoring it would.
    private static void awaitIgnoringInterrupts(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (!latch.await(30, SECONDS)) {
                        throw new AssertionError("Test failed to release the supplier");
                    }
                    return;
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void a_registration_burst_leaves_connection_and_publication_admission_available()
            throws Exception {
        // Arrange: hold the established actor while registrations accumulate.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            holdActor(transport, entered, release);
            for (int i = 0; i < 256; i++) {
                sut.subscribe(Subscribe.of(new TopicFilter("burst/" + i), Qos.AT_MOST_ONCE));
            }

            // Act
            CompletableFuture<ConnAck> connected = sut.connect();
            CompletableFuture<PublishResult> published = sut.publish(message(new byte[] {1}));
            CompletableFuture<Void> disconnected = sut.disconnect();

            // Assert: admission is independent; the core still decides each operation's result.
            assertThat(connected).isNotDone();
            assertThat(published).isNotDone();
            assertThat(disconnected).isNotDone();
            release.countDown();
            assertThat(connected.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            published.get(5, SECONDS);
            disconnected.get(5, SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test
    void disconnect_during_connection_defers_subscription_establishment_until_the_next_connection()
            throws Exception {
        // Arrange: the registration waits for the connection already in progress.
        try (MqttClient sut = client()) {
            Subscription subscription = sut.subscribe(request());
            CompletableFuture<Qos> establishing = subscription.established();
            CompletableFuture<ConnAck> connecting = sut.connect();
            ScriptTransport transport = nextTransport();

            // Act: withdraw the request, then let the outstanding CONNECT finish gracefully.
            CompletableFuture<Void> disconnected = sut.disconnect();
            transport.accept();
            disconnected.get(5, SECONDS);

            // Assert: the cancelled attempt cannot establish its registered subscriptions.
            assertThatThrownBy(() -> connecting.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class);
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Disconnect.class);
            assertThat(transport.sent).isEmpty();
            assertThat(establishing).isNotDone();
            assertThatThrownBy(subscription::grantedQos).isInstanceOf(IllegalStateException.class);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));

            ScriptTransport current = connect(sut);
            var subscribe = (Packet.Subscribe) requireNonNull(current.sent.poll(5, SECONDS));
            current.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));

            // Assert: the durable registration establishes on the next connection instead.
            assertThat(establishing.get(5, SECONDS)).isEqualTo(Qos.AT_MOST_ONCE);
            assertThat(subscription.established().get(5, SECONDS)).isEqualTo(Qos.AT_MOST_ONCE);
            assertThat(subscription.grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void a_waiting_pull_receiver_survives_disconnect_and_receives_after_resubscribe()
            throws Exception {
        // Arrange: the application is waiting on the original subscription before disconnect.
        var receiver = new AtomicReference<Thread>();
        try (MqttClient sut = client(); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            ScriptTransport old = connect(sut);
            Subscription subscription = subscribePull(sut, old,
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE));
            Future<ReceiveResult> waiting = workers.submit(() -> {
                receiver.set(Thread.currentThread());
                return subscription.receive();
            });
            try {
                await().until(() -> receiver.get() != null
                        && requireNonNull(receiver.get()).getState() == Thread.State.WAITING);

                // Act: the clean session ends but the subscription and its waiting caller stay.
                sut.disconnect().get(5, SECONDS);
                assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
                assertThat(subscription.receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.TimedOut());
                assertThat(waiting).isNotDone();
                ScriptTransport current = connect(sut);
                var subscribe = (Packet.Subscribe) requireNonNull(current.sent.poll(5, SECONDS));
                assertThat(subscribe.subscriptions().getFirst().filter())
                        .isEqualTo(subscription.filter());
                current.receive(new Packet.SubAck(subscribe.packetId(),
                        List.of(ReasonCode.SUCCESS), EMPTY));
                current.receive(publication(new byte[] {9}));

                // Assert: the same handle observes the new grant and receives the new delivery.
                assertThat(received(waiting.get(5, SECONDS)).message().payload())
                        .containsExactly((byte) 9);
                assertThat(subscription.grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);
                assertThat(ends).isEmpty();
            } finally {
                waiting.cancel(true);
            }
        }
    }

    @ParameterizedTest(name = "old handler fails: {0}")
    @ValueSource(booleans = {false, true})
    void one_subscription_keeps_serial_delivery_across_reconnect(boolean oldHandlerFails)
            throws Exception {
        // Arrange: a handler outlives its connection while its subscription stays registered.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        var oldFailure = new IllegalStateException("old handler failed");
        var currentFailure = new IllegalStateException("current handler failed");
        try (MqttClient sut = client()) {
            ScriptTransport old = connect(sut);
            Subscription subscription = sut.subscribe(qosOneRequest(), delivery -> {
                maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
                try {
                    byte payload = delivery.message().payload()[0];
                    if (payload == 1) {
                        entered.countDown();
                        waitFor(release);
                        if (oldHandlerFails) {
                            throw oldFailure;
                        }
                    } else if (payload == 3) {
                        throw currentFailure;
                    } else {
                        delivered.add(delivery);
                    }
                } finally {
                    active.decrementAndGet();
                }
            });
            establish(old, subscription);
            old.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            ScriptTransport current = connect(sut);
            acknowledgeSubscription(current, subscription);

            // Act: current input reaches the actor while the old invocation still holds its lane.
            current.receive(publication(new byte[] {2}));
            sut.publish(message(new byte[] {0})).get(5, SECONDS);
            assertThat(current.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            assertThat(delivered).isEmpty();
            release.countDown();

            // Assert: success and failure of the old invocation both allow the next delivery.
            assertThat(requireNonNull(delivered.poll(5, SECONDS)).message().payload())
                    .containsExactly((byte) 2);
            assertThat(maximum).hasValue(1);
            assertThat(ends).isEmpty();

            // Act & Assert: a failure of a QoS 1 delivery from this connection still stops it.
            current.receive(qosOnePublication("messages", 3, false));
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(currentFailure, false, 2));
        } finally {
            release.countDown();
        }
    }

    @Test
    void closing_a_subscription_from_the_end_listener_prevents_its_resubscription()
            throws Exception {
        // Arrange: the listener disposes the same public handle that existed before disconnect.
        var handle = new AtomicReference<Subscription>();
        var removing = new AtomicReference<CompletableFuture<Void>>();
        try (MqttClient sut = client(end -> {
            Subscription subscription = requireNonNull(handle.get());
            subscription.close();
            removing.set(subscription.unsubscribe());
            ends.add(end);
        })) {
            ScriptTransport old = connect(sut);
            handle.set(subscribePull(sut, old, request()));

            // Act
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            CompletableFuture<Void> pending = requireNonNull(removing.get());
            pending.get(5, SECONDS);
            ScriptTransport current = connect(sut);

            // Assert: a fresh clean session needs neither recreation nor remote removal.
            assertThat(current.sent).isEmpty();
            assertThat(requireNonNull(handle.get()).receive()).isEqualTo(new ReceiveResult.Ended());
            Subscription replacement = subscribePull(sut, current, request());
            requireNonNull(handle.get()).unsubscribe().get(5, SECONDS);
            current.receive(publication(new byte[] {9}));
            assertThat(received(replacement.receive(Duration.ofSeconds(5))).message().payload())
                    .containsExactly((byte) 9);
        }
    }

    @Test
    void closing_a_push_subscription_waits_for_send_capacity_without_resuming_delivery()
            throws Exception {
        // Arrange: the handler is running, and a large stalled write fills regular send capacity.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var worker = new AtomicReference<Thread>();
        var handled = new AtomicInteger();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), _ -> {
                worker.set(Thread.currentThread());
                handled.incrementAndGet();
                entered.countDown();
                waitFor(release);
            });
            establish(transport, subscription);
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            transport.completeSends = false;
            CompletableFuture<PublishResult> stalled = sut.publish(MqttMessage.of(
                    new Topic("a"), new byte[16 * 1024 * 1024 - 8], Qos.AT_MOST_ONCE, false
            ));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act
            subscription.close();
            CompletableFuture<Void> removing = subscription.unsubscribe();
            assertThat(removing).isNotDone();
            transport.receive(publication(new byte[] {2}));
            release.countDown();

            // Assert: the old worker exits instead of resuming its queue or another wait.
            assertThat(requireNonNull(worker.get()).join(Duration.ofSeconds(5))).isTrue();
            assertThat(handled).hasValue(1);
            transport.completeSends = true;
            transport.completeSend();
            stalled.get(5, SECONDS);
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            removing.get(5, SECONDS);
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    @Timeout(10)
    void abnormal_connection_end_discards_pull_buffers_and_preserves_the_subscription()
            throws Exception {
        // Arrange
        var failure = new IOException("Peer reset the connection");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());
            transport.receive(publication(new byte[] {7}));
            // An outgoing command confirms the incoming publication has reached the actor.
            sut.publish(message(new byte[0])).get(5, SECONDS);
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act
            transport.receiver().onClosed(failure);
            assertThat(ends.poll(5, SECONDS)).isNotNull();

            // Assert: connection loss clears buffered input without ending this receiver.
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            ScriptTransport replacement = connect(sut);
            acknowledgeSubscription(replacement, subscription);
            replacement.receive(publication(new byte[] {9}));
            assertThat(received(subscription.receive(Duration.ofSeconds(5))).message().payload())
                    .containsExactly((byte) 9);
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void closing_a_pull_subscription_wakes_all_receivers_before_unsuback() throws Exception {
        // Arrange
        try (MqttClient sut = client(); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());
            var waiting = ConcurrentHashMap.<Thread>newKeySet();
            var jobs = new ArrayList<Future<Boolean>>();
            try {
                for (int i = 0; i < 12; i++) {
                    jobs.add(workers.submit(() -> {
                        waiting.add(Thread.currentThread());
                        return subscription.receive() instanceof ReceiveResult.Ended;
                    }));
                }
                await().until(() -> waiting.size() == 12 && waiting.stream()
                        .allMatch(worker -> worker.getState() == Thread.State.WAITING));

                // Act
                subscription.close();

                // Assert: close cannot leave some consumers waiting for broker acknowledgement.
                for (var job : jobs) {
                    assertThat(job.get(5, SECONDS)).isTrue();
                }
                assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Unsubscribe.class);
                assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
            } finally {
                jobs.forEach(job -> job.cancel(true));
            }
        }
    }

    @Test
    void pull_buffer_overflow_reports_the_cause_without_ending_receivers()
            throws Exception {
        // Arrange: an unreceived QoS 1 delivery fills the subscription's only slot.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(
                    sut, transport, qosOneRequest().withBufferCapacity(1)
            );
            transport.receive(qosOnePublication("messages", 1, false));

            // Act
            transport.receive(qosOnePublication("messages", 2, false));

            // Assert: overload goes to the listener while the receiver can resume later.
            Throwable cause = requireNonNull(ends.poll(5, SECONDS)).cause().orElseThrow();
            assertThat(cause).isInstanceOf(MqttCapacityException.class);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
        }
    }

    @Test
    void a_full_qos_0_pull_subscription_drops_new_messages_and_keeps_receiving()
            throws Exception {
        // Arrange: an unreceived delivery fills the subscription's only slot.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(
                    sut, transport, request().withBufferCapacity(1)
            );
            transport.receive(publication(new byte[] {1}));

            // Act
            transport.receive(publication(new byte[] {2}));
            // This command follows the publication through the mailbox, so it completes after
            // routing.
            sut.publish(message(new byte[0])).get(5, SECONDS);

            // Assert: the message that found the subscription full was dropped, and the
            // connection goes on: the waiting message is received, and a later one fits again.
            assertThat(received(subscription.receive(Duration.ofSeconds(5))).message().payload())
                    .containsExactly(1);
            transport.receive(publication(new byte[] {3}));
            assertThat(received(subscription.receive(Duration.ofSeconds(5))).message().payload())
                    .containsExactly(3);
            assertThat(ends).isEmpty();
        }
    }

    @Test
    @Timeout(10)
    void normal_disconnect_discards_pull_buffers_without_ending_receiving() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());
            transport.receive(publication(new byte[] {7}));
            // An outgoing command confirms the incoming publication has reached the actor.
            sut.publish(message(new byte[0])).get(5, SECONDS);
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act
            sut.disconnect().get(5, SECONDS);

            // Assert
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            subscription.close();
            assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
        }
    }

    @Test
    void all_closed_registrations_are_retained_within_capacity_and_removed_when_writes_resume()
            throws Exception {
        // Arrange: establish the full registration budget without starting any delivery workers.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            var subscriptions = new ArrayList<Subscription>();
            for (int i = 0; i < 256; i++) {
                subscriptions.add(subscribePull(sut, transport,
                        Subscribe.of(new TopicFilter("slot/" + i), Qos.AT_MOST_ONCE)));
            }
            transport.completeSends = false;
            CompletableFuture<PublishResult> stalled = sut.publish(MqttMessage.of(
                    new Topic("a"), new byte[16 * 1024 * 1024 - 8], Qos.AT_MOST_ONCE, false
            ));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act: all local lanes end, while broker removals wait for send capacity.
            subscriptions.forEach(Subscription::close);
            var processed = new CountDownLatch(1);
            beforeActorTime = processed::countDown;
            transport.receive(new Packet.PingResp());
            assertThat(processed.await(5, SECONDS)).isTrue();
            beforeActorTime = () -> { };
            transport.completeSends = true;
            transport.completeSend();
            stalled.get(5, SECONDS);

            // Assert: new registrations do not accumulate behind discarded delivery lanes.
            assertThatThrownBy(() -> sut.subscribe(request()).established().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class)
                    .hasRootCauseMessage("Subscription capacity");
            var removed = ConcurrentHashMap.<TopicFilter>newKeySet();
            for (int i = 0; i < 256; i++) {
                var unsubscribe = (Packet.Unsubscribe) requireNonNull(
                        transport.sent.poll(5, SECONDS)
                );
                assertThat(removed.add(unsubscribe.filters().getFirst())).isTrue();
                transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            }
            // This observer also provides an actor barrier after the last UNSUBACK.
            subscriptions.getLast().unsubscribe().get(5, SECONDS);
            assertThat(removed).hasSize(256);
            assertThat(subscribePull(sut, transport, request())).isNotNull();
            assertThat(transport.sent).isEmpty();
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void bulk_close_proceeds_even_when_every_public_operation_slot_is_occupied() throws Exception {
        // Arrange: leave 64 outgoing QoS 1 publications waiting for PUBACK.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            var subscriptions = new ArrayList<Subscription>();
            for (int i = 0; i < 100; i++) {
                subscriptions.add(subscribePull(sut, transport,
                        Subscribe.of(new TopicFilter("slot/" + i), Qos.AT_MOST_ONCE)));
            }
            var publishing = new ArrayList<CompletableFuture<PublishResult>>();
            var publications = new ArrayList<Packet.Publish>();
            for (int i = 0; i < 64; i++) {
                publishing.add(sut.publish(MqttMessage.of(new Topic("out"), new byte[0],
                        Qos.AT_LEAST_ONCE, false)));
                publications.add((Packet.Publish) requireNonNull(transport.sent.poll(5, SECONDS)));
            }

            // Act: repeated close and observations must not cancel or duplicate cleanup.
            subscriptions.forEach(Subscription::close);
            subscriptions.forEach(Subscription::close);
            CompletableFuture<Void> removing = subscriptions.getLast().unsubscribe();
            assertThat(removing).isNotCompletedExceptionally();

            // Assert: all removals progress while all 64 public operations remain pending.
            var removed = ConcurrentHashMap.<TopicFilter>newKeySet();
            for (int i = 0; i < 100; i++) {
                var packet = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
                assertThat(removed.add(packet.filters().getFirst())).isTrue();
                transport.receive(new Packet.UnsubAck(packet.packetId(), List.of(), EMPTY));
            }
            removing.get(5, SECONDS);
            assertThat(publishing).allSatisfy(future -> assertThat(future).isNotDone());
            for (Packet.Publish packet : publications) {
                transport.receive(new Packet.PubAck(packet.packetId(), ReasonCode.SUCCESS, EMPTY));
            }
            CompletableFuture.allOf(publishing.toArray(CompletableFuture[]::new)).get(5, SECONDS);
            for (Subscription subscription : subscriptions) {
                assertThat(subscribePull(sut, transport,
                        Subscribe.of(subscription.filter(), Qos.AT_MOST_ONCE))).isNotNull();
            }
            assertThat(transport.sent).isEmpty();
            assertThat(ends).isEmpty();
        }
    }

    @Test
    @Timeout(10)
    void close_preserves_an_explicit_removal_waiting_for_send_capacity() throws Exception {
        // Arrange: stop the actor before it queues removal behind exhausted send capacity.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());
            transport.completeSends = false;
            CompletableFuture<PublishResult> stalled = sut.publish(MqttMessage.of(
                    new Topic("a"), new byte[16 * 1024 * 1024 - 8], Qos.AT_MOST_ONCE, false
            ));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            holdActor(transport, entered, release);

            // Act
            CompletableFuture<Void> explicit = subscription.unsubscribe();
            subscription.close();
            release.countDown();
            assertThat(explicit).isNotDone();
            transport.completeSends = true;
            transport.completeSend();

            // Assert: the retained removal is sent without another close or unsubscribe.
            stalled.get(5, SECONDS);
            var packet = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            CompletableFuture<Void> cleanup = subscription.unsubscribe();
            transport.receive(new Packet.UnsubAck(packet.packetId(), List.of(), EMPTY));
            cleanup.get(5, SECONDS);
            explicit.get(5, SECONDS);
            assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
            assertThat(transport.sent).isEmpty();
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void closing_another_subscription_keeps_an_observed_removal_pending_until_its_unsuback()
            throws Exception {
        // Arrange: the first removal is sent before the second close wakes the actor again.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription first = subscribePull(sut, transport,
                    Subscribe.of(new TopicFilter("first"), Qos.AT_MOST_ONCE));
            Subscription second = subscribePull(sut, transport,
                    Subscribe.of(new TopicFilter("second"), Qos.AT_MOST_ONCE));
            first.close();
            var firstRemoval = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            CompletableFuture<Void> observer = first.unsubscribe();

            // Act
            second.close();

            // Assert: the observer completes with its own UNSUBACK, and nothing is sent again.
            var secondRemoval = (Packet.Unsubscribe) requireNonNull(
                    transport.sent.poll(5, SECONDS)
            );
            assertThat(secondRemoval.filters()).containsExactly(new TopicFilter("second"));
            assertThat(observer).isNotDone();
            transport.receive(new Packet.UnsubAck(firstRemoval.packetId(), List.of(), EMPTY));
            observer.get(5, SECONDS);
            assertThat(transport.sent).isEmpty();
            assertThat(ends).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void connection_loss_confirms_background_removal_from_a_clean_session(
            boolean beforeStart
    ) throws Exception {
        // Arrange: optionally hold the close wake until termination takes its priority slot.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());
            if (beforeStart) {
                holdActor(transport, entered, release);
            }
            subscription.close();
            if (!beforeStart) {
                assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Unsubscribe.class);
            }
            CompletableFuture<Void> observer = subscription.unsubscribe();

            // Act: codec failure uses the terminal slot, ahead of queued API work.
            transport.receiver().onBytes(new byte[] {(byte) 0xff, 0});
            release.countDown();

            // Assert
            assertThat(ends.poll(5, SECONDS)).isNotNull();
            observer.get(5, SECONDS);
            // The connection failure came after close(), so it does not replace the normal end.
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
            ScriptTransport current = connect(sut);
            subscription.close();
            subscription.unsubscribe().get(5, SECONDS);
            observer.get(5, SECONDS);
            assertThat(subscribePull(sut, current, request())).isNotNull();
            assertThat(current.sent).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void offline_observers_of_a_closed_subscription_leave_capacity_for_reconnect()
            throws Exception {
        // Arrange: local disposal persists while there is no connection to observe removal on.
        try (MqttClient sut = client()) {
            ScriptTransport old = connect(sut);
            Subscription subscription = subscribePull(sut, old, request());
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            await().until(() -> ((ClientActor) sut).usage().operations() == 0);
            subscription.close();
            assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
            var observers = new ArrayList<CompletableFuture<Void>>();

            // Act: repeated observations of the ended clean session complete locally.
            for (int i = 0; i < 64; i++) {
                observers.add(subscription.unsubscribe());
            }
            CompletableFuture<ConnAck> reconnecting = sut.connect();

            // Assert: observing an offline removal cannot prevent its clean-session resolution.
            assertThat(reconnecting).isNotCompletedExceptionally();
            for (CompletableFuture<Void> observer : observers) {
                observer.get(5, SECONDS);
            }
            ScriptTransport current = nextTransport();
            current.accept();
            reconnecting.get(5, SECONDS);
            subscription.unsubscribe().get(5, SECONDS);
            assertThat(current.sent).isEmpty();
            assertThat(subscribePull(sut, current, request())).isNotNull();
        }
    }

    @ParameterizedTest(name = "closed subscriptions: {0}")
    @ValueSource(ints = {1, 64})
    void connection_loss_releases_every_close_observer_but_preserves_local_disposal(
            int registrations
    ) throws Exception {
        // Arrange: background removals have multiple independently observed results.
        var failure = new IOException("connection ended before removal was confirmed");
        try (MqttClient sut = client()) {
            ScriptTransport old = connect(sut);
            var subscriptions = new ArrayList<Subscription>();
            for (int i = 0; i < registrations; i++) {
                subscriptions.add(subscribePull(sut, old,
                        Subscribe.of(new TopicFilter("closed/" + i), Qos.AT_MOST_ONCE)));
            }
            subscriptions.forEach(Subscription::close);
            // More removals than the send window can wait without ever reaching the transport.
            assertThat(old.sent.poll(5, SECONDS)).isInstanceOf(Packet.Unsubscribe.class);
            await().until(() -> ((ClientActor) sut).usage().waiters() == 0);
            var observers = new ArrayList<CompletableFuture<Void>>();
            for (int i = 0; i < 64; i++) {
                observers.add(subscriptions.get(i % registrations).unsubscribe());
            }
            assertThat(((ClientActor) sut).usage().waiters()).isEqualTo(64);
            assertThat(((ClientActor) sut).usage().operations()).isZero();
            assertThat(observers).allSatisfy(observer -> assertThat(observer).isNotDone());

            // Act: FIFO closure follows admission of every observer.
            old.receiver().onClosed(failure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));

            // Assert: clean-session termination confirms removal and returns observation slots.
            for (CompletableFuture<Void> observer : observers) {
                observer.get(5, SECONDS);
            }
            await().until(() -> ((ClientActor) sut).usage().waiters() == 0);
            ScriptTransport current = connect(sut);
            for (Subscription subscription : subscriptions) {
                assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
                subscription.unsubscribe().get(5, SECONDS);
            }
            assertThat(current.sent).isEmpty();
            assertThat(subscribePull(sut, current, Subscribe.of(
                    subscriptions.getFirst().filter(), Qos.AT_MOST_ONCE))).isNotNull();
            observers.getFirst().get(5, SECONDS);
        }
    }

    @Test
    void completed_pull_deliveries_still_obey_the_separate_acknowledgement_limit()
            throws Exception {
        // Arrange: the first receipt keeps every later PUBACK pending, while pull payloads drain.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            subscribePull(sut, transport, Subscribe.of(new TopicFilter("head"), Qos.AT_LEAST_ONCE)
                    .withAckMode(AckMode.MANUAL));
            Subscription tail = subscribePull(sut, transport,
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE)
                            .withBufferCapacity(1));
            transport.receive(new Packet.Publish(new Topic("head"), new byte[] {1},
                    Qos.AT_LEAST_ONCE, false, false, 1, EMPTY));

            // Act: local capacity is reusable for all remaining receipt slots.
            for (int id = 2; id <= 4096; id++) {
                transport.receive(new Packet.Publish(new Topic("messages"), new byte[] {1},
                        Qos.AT_LEAST_ONCE, false, false, id, EMPTY));
                assertThat(tail.receive(Duration.ofSeconds(5)))
                        .isInstanceOf(ReceiveResult.Received.class);
            }
            assertThat(transport.sent).isEmpty();
            assertThat(ends).isEmpty();
            transport.receive(new Packet.Publish(new Topic("messages"), new byte[] {1},
                    Qos.AT_LEAST_ONCE, false, false, 4097, EMPTY));

            // Assert: it is the bounded wire receipt metadata, not the one-slot buffer, that fills.
            assertThat(requireNonNull(ends.poll(5, SECONDS)).cause().orElseThrow())
                    .isInstanceOf(MqttCapacityException.class)
                    .hasMessage("Acknowledgement capacity");
        }
    }

    @ParameterizedTest
    @EnumSource(value = AckMode.class, names = {"ON_RECEIVE", "MANUAL"})
    @Timeout(30)
    void retained_receptions_keep_their_limit_without_holding_completed_pull_capacity(AckMode mode)
            throws Exception {
        // Arrange: half the receive table is occupied before reconnect, with local capacity one.
        try (MqttClient sut = client(false)) {
            ScriptTransport old = connect(sut);
            Subscription head = subscribePull(
                    sut,
                    old,
                    Subscribe.of(new TopicFilter("head"), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.MANUAL)
                            .withBufferCapacity(1)
            );
            Subscription tail = subscribePull(
                    sut,
                    old,
                    Subscribe.of(new TopicFilter("tail"), Qos.AT_LEAST_ONCE)
                            .withAckMode(mode)
                            .withBufferCapacity(1)
            );
            old.receive(qosOnePublication("head", 1, false));
            received(head.receive(Duration.ofSeconds(5)));
            for (int id = 2; id <= 2048; id++) {
                old.receive(qosOnePublication("tail", id, false));
                received(tail.receive(Duration.ofSeconds(5))).ack();
            }
            old.receiver().onClosed(new IOException("connection lost"));
            assertThat(ends.poll(5, SECONDS)).isNotNull();

            // Act: all replays join, then the remaining receive slots accept new local handoffs.
            ScriptTransport current = connect(sut, true);
            current.receive(qosOnePublication("head", 1, true));
            for (int id = 2; id <= 2048; id++) {
                current.receive(qosOnePublication("tail", id, true));
            }
            sut.publish(message(new byte[] {0})).get(5, SECONDS);
            assertThat(current.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            assertThat(tail.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            for (int id = 2049; id <= 4096; id++) {
                current.receive(qosOnePublication("tail", id, false));
                received(tail.receive(Duration.ofSeconds(5))).ack();
            }
            assertThat(current.sent).isEmpty();
            assertThat(ends).isEmpty();
            current.receive(qosOnePublication("tail", 4097, false));

            // Assert: retained wire metadata, rather than either one-slot pull buffer, is full.
            assertThat(requireNonNull(ends.poll(5, SECONDS)).cause().orElseThrow())
                    .isInstanceOf(MqttCapacityException.class)
                    .hasMessage("Acknowledgement capacity");
        }
    }

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_LEAST_ONCE", "EXACTLY_ONCE"})
    void a_manual_push_delivery_keeps_its_capacity_when_the_session_continues(Qos qos)
            throws Exception {
        // Arrange: the handler returns, but its manual acknowledgement is still outstanding.
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client(false)) {
            ScriptTransport old = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), qos)
                            .withAckMode(AckMode.MANUAL)
                            .withBufferCapacity(1),
                    delivered::add
            );
            establish(old, subscription);
            old.receive(acknowledgedPublication("messages", 7, qos, false));
            assertThat(delivered.poll(5, SECONDS)).isNotNull();
            old.receiver().onClosed(new IOException("connection lost"));
            assertThat(ends.poll(5, SECONDS)).isNotNull();

            // Act: joining the replay consumes no new slot; a different reception still needs one.
            ScriptTransport current = connect(sut, true);
            current.receive(acknowledgedPublication("messages", 7, qos, true));
            sut.publish(message(new byte[] {0})).get(5, SECONDS);
            assertThat(current.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            assertThat(delivered).isEmpty();
            current.receive(acknowledgedPublication("messages", 8, qos, false));

            // Assert
            assertThat(requireNonNull(ends.poll(5, SECONDS)).cause().orElseThrow())
                    .isInstanceOf(MqttCapacityException.class)
                    .hasMessage("Delivery capacity");
            assertThat(current.sent).isEmpty();
        }
    }

    @Test
    void pull_subscriptions_reject_handler_settings_before_sending_subscribe() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);

            // Act & Assert
            assertThatThrownBy(() -> sut.subscribe(request().withAckMode(AckMode.AFTER_HANDLER)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> sut.subscribe(
                    request().withFailurePolicy(FailurePolicy.SKIP)
            )).isInstanceOf(IllegalArgumentException.class);
            assertThat(transport.sent).isEmpty();
            assertThat(ends).isEmpty();
        }
    }

    @Test
    @Timeout(10)
    void handler_subscriptions_reject_receive() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribe(sut, transport);

            // Act & Assert
            assertThatThrownBy(subscription::receive)
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> subscription.receive(Duration.ZERO))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void formatting_a_failed_assertion_on_an_empty_pull_subscription_does_not_wait()
            throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());

            // Act & Assert
            assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
                    assertThatThrownBy(() -> assertThat(subscription).isNull())
                            .isInstanceOf(AssertionError.class));
            transport.receive(publication(new byte[] {7}));
            MqttDelivery delivery = received(subscription.receive(Duration.ofSeconds(5)));
            assertThat(delivery.message().payload()).containsExactly((byte) 7);
        }
    }

    @Test
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void interrupted_receiving_does_not_end_the_subscription()
            throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());

            // Act & Assert
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(subscription::receive)
                        .isInstanceOf(InterruptedException.class);
            } finally {
                boolean _ = Thread.interrupted();
            }
            transport.receive(publication(new byte[] {7}));
            MqttDelivery delivery = received(subscription.receive(Duration.ofSeconds(5)));
            assertThat(delivery.message().payload())
                    .containsExactly((byte) 7);
            assertThat(ends).isEmpty();
        }
    }

    @ParameterizedTest(name = "transport connected: {0}")
    @CsvSource({
            "false, Transport connection deadline expired",
            "true, CONNACK deadline expired"
    })
    void the_total_connect_deadline_reports_the_phase_that_expired(
            boolean connected,
            String message
    ) throws Exception {
        // Arrange
        var transport = new ScriptTransport();
        transport.connectOnStart = false;
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionTimeout(Duration.ofSeconds(30))
                .timerScheduler(scheduler)
                .connectionListener(ends::add)
                .transportFactory(_ -> transport)
                .build()) {
            CompletableFuture<ConnAck> future = sut.connect();
            await().until(() -> transport.listener != null);
            assertThat(scheduler.pendingTaskCount()).isEqualTo(1);

            // Act: establishing the transport partway through must not restart the deadline.
            scheduler.advance(Duration.ofSeconds(20));
            if (connected) {
                transport.receiver().onConnected();
                assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Connect.class);
            }
            assertThat(future).isNotDone();
            scheduler.advance(Duration.ofSeconds(10));

            // Assert
            assertThatThrownBy(() -> future.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .hasRootCauseInstanceOf(TimeoutException.class)
                    .hasRootCauseMessage(message);
            assertThat(transport.closed).isTrue();
            assertThat(transport.sent).isEmpty();
            assertThat(ends).isEmpty();
            assertThat(scheduler.pendingTaskCount()).isZero();
        }
    }

    @ParameterizedTest(name = "failure in {0}")
    @EnumSource(SpiCall.class)
    void spi_failures_preserve_the_cause_and_allow_another_attempt(SpiCall call)
            throws Exception {
        // Arrange: fail more attempts than the listener queue holds before succeeding.
        var failure = new RejectedExecutionException("SPI rejected this attempt");
        var remaining = new AtomicInteger(65);
        Runnable reject = () -> {
            if (remaining.getAndDecrement() > 0) {
                throw failure;
            }
        };
        var created = new ArrayList<ScriptTransport>();
        var timers = new TimerScheduler() {
            @Override
            public long nanoTime() {
                if (call == SpiCall.CLOCK) {
                    reject.run();
                }
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                if (call == SpiCall.SCHEDULE) {
                    reject.run();
                }
                return scheduler.schedule(delay, task);
            }
        };
        try (MqttClient client = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .timerScheduler(timers)
                .connectionListener(ends::add)
                .transportFactory(_ -> {
                    if (call == SpiCall.CREATE) {
                        reject.run();
                    }
                    var transport = new ScriptTransport();
                    if (call == SpiCall.START) {
                        transport.onStart = reject;
                    } else if (call == SpiCall.SEND) {
                        transport.beforeSend = reject;
                    }
                    created.add(transport);
                    transports.add(transport);
                    return transport;
                })
                .build()) {
            var sut = (ClientActor) client;

            // Act & Assert
            for (int i = 0; i < 65; i++) {
                assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                        .hasCauseInstanceOf(MqttConnectException.class)
                        .hasRootCause(failure);
                assertThat(scheduler.pendingTaskCount()).isZero();
            }
            assertThat(created).allSatisfy(transport -> assertThat(transport.closed).isTrue());
            assertThat(ends).isEmpty();
            transports.clear();

            // Act: the same client can establish a new connection after the SPI recovers.
            CompletableFuture<ConnAck> connection = sut.connect();
            nextTransport().accept();

            // Assert: a clock failure precedes every attempt, so it numbers none.
            long attempt = (call == SpiCall.CLOCK) ? 1 : 66;
            assertThat(connection.get(5, SECONDS)).isEqualTo(ConnAck.of(false, attempt));
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, attempt));
            await().pollInterval(Duration.ofMillis(1))
                    .untilAsserted(() -> assertThat(sut.usage().notifications()).isZero());
        }
    }

    @Test
    void cancellation_and_logging_failures_do_not_stop_connection_completion_or_cleanup()
            throws Exception {
        // Arrange: cancellation removes its timer and then fails, and every diagnostic of the
        // actor fails too, including the connection and disconnection records.
        var cancellationFailure = new IllegalStateException("Cancellation failed");
        var loggingFailure = new IllegalStateException("Logging backend failed");
        var logCalls = new AtomicInteger();
        var timers = new TimerScheduler() {
            @Override
            public long nanoTime() {
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                Cancellable scheduled = scheduler.schedule(delay, task);
                return () -> {
                    scheduled.cancel();
                    throw cancellationFailure;
                };
            }
        };
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
        var logger = Logger.getLogger(ClientActor.class.getName());
        Level previousLevel = logger.getLevel();
        boolean previousParents = logger.getUseParentHandlers();
        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);
        logger.addHandler(handler);
        try (MqttClient client = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .timerScheduler(timers)
                .connectionListener(ends::add)
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                })
                .build()) {
            var sut = (ClientActor) client;

            // Act: a refused attempt preserves its own failure.
            CompletableFuture<ConnAck> refused = sut.connect();
            nextTransport().receive(new Packet.ConnAck(
                    false,
                    ReasonCode.SERVER_UNAVAILABLE,
                    EMPTY
            ));

            // Assert
            assertThatThrownBy(() -> refused.get(5, SECONDS)).cause().isInstanceOfSatisfying(
                    MqttConnectException.class,
                    failure -> assertThat(failure.reasonCode())
                            .contains(ReasonCode.SERVER_UNAVAILABLE)
            );
            await().pollInterval(Duration.ofMillis(1))
                    .untilAsserted(() -> assertThat(sut.usage().notifications()).isZero());
            assertThat(ends).isEmpty();

            // Act: successful establishment and peer closure also complete normally.
            ScriptTransport transport = connect(sut);
            CompletableFuture<PublishResult> pending = sut.publish(MqttMessage.of(
                    new Topic("pending"),
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false
            ));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            var peerFailure = new IOException("Peer reset");
            transport.receiver().onClosed(peerFailure);

            // Assert
            assertThatThrownBy(() -> pending.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(peerFailure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(peerFailure, false, 2));
            await().pollInterval(Duration.ofMillis(1))
                    .untilAsserted(() -> assertThat(sut.usage().notifications()).isZero());

            // Act & Assert: a later explicit disconnect retains its successful result too.
            connect(sut);
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 3));
            await().pollInterval(Duration.ofMillis(1))
                    .untilAsserted(() -> assertThat(sut.usage().notifications()).isZero());
            assertThat(scheduler.pendingTaskCount()).isZero();
            assertThat(logCalls.get()).isPositive();
        } finally {
            logger.removeHandler(handler);
            logger.setLevel(previousLevel);
            logger.setUseParentHandlers(previousParents);
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close races with commands waiting behind start().
    void close_rejects_unstarted_operations_but_fails_a_started_connect_as_establishment_failure()
            throws Exception {
        // Arrange: hold the actor inside the first connection's SPI start call.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var transport = new ScriptTransport();
        transport.onStart = () -> {
            entered.countDown();
            waitFor(release);
        };
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .timerScheduler(scheduler)
                .transportFactory(_ -> transport)
                .build()) {
            CompletableFuture<ConnAck> started = sut.connect();
            assertThat(entered.await(5, SECONDS)).isTrue();
            CompletableFuture<ConnAck> queuedConnect = sut.connect();
            CompletableFuture<Void> queuedDisconnect = sut.disconnect();

            // Act
            sut.close();
            release.countDown();

            // Assert
            assertThatThrownBy(() -> started.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class);
            assertThatThrownBy(() -> queuedConnect.get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> queuedDisconnect.get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(transport.closed).isTrue();
            assertThat(transport.sent).isEmpty();
            assertThat(scheduler.pendingTaskCount()).isZero();
        } finally {
            release.countDown();
        }
    }

    @Test
    void ping_deadline_rejection_ends_only_the_connection_without_sending_an_unbounded_ping()
            throws Exception {
        // Arrange
        var failure = new RejectedExecutionException("PING deadline rejected");
        var reject = new AtomicBoolean();
        TimerScheduler timers = new TimerScheduler() {
            @Override
            public long nanoTime() {
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                if (reject.getAndSet(false)) {
                    throw failure;
                }
                return scheduler.schedule(delay, task);
            }
        };
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .keepAlive(Duration.ofSeconds(10))
                .timerScheduler(timers)
                .connectionListener(ends::add)
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                })
                .build()) {
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport transport = nextTransport();
            transport.accept();
            connection.get(5, SECONDS);
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
            reject.set(true);

            // Act
            scheduler.advance(Duration.ofSeconds(10));

            // Assert: schedule failure unwinds the core command before it can send PINGREQ.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(transport.sent).isEmpty();
            assertThat(transport.closed).isTrue();
            assertThat(scheduler.pendingTaskCount()).isZero();
            CompletableFuture<ConnAck> next = sut.connect();
            nextTransport().accept();
            assertThat(next.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
        }
    }

    @Test
    void a_send_exception_fails_disconnect_with_its_cause_and_allows_reconnection()
            throws Exception {
        // Arrange
        var failure = new IllegalStateException("SPI send failed");
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> connected = sut.connect();
            ScriptTransport transport = nextTransport();
            transport.accept();
            connected.get(5, SECONDS);
            transport.beforeSend = () -> {
                throw failure;
            };

            // Act & Assert
            assertThatThrownBy(() -> sut.disconnect().get(5, SECONDS)).hasRootCause(failure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, true, 1));
            assertThat(transport.closed).isTrue();
            CompletableFuture<ConnAck> next = sut.connect();
            nextTransport().accept();
            assertThat(next.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void unsolicited_pingresp_does_not_disconnect_an_established_client() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport transport = nextTransport();
            transport.accept();
            connection.get(5, SECONDS);

            // Act: the following API command observes both preceding PINGRESP packets.
            transport.receiver().onBytes(HexFormat.of().parseHex("d000d000"));
            sut.disconnect().get(5, SECONDS);

            // Assert
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void unsolicited_pingresp_warnings_name_each_client_once() throws Exception {
        // Arrange: capture the JUL backend of System.Logger, as applications commonly use it.
        var jul = Logger.getLogger(ClientActor.class.getName());
        boolean previousParents = jul.getUseParentHandlers();
        var warnings = new LinkedBlockingQueue<String>();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.WARNING) {
                    warnings.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        jul.addHandler(handler);
        jul.setUseParentHandlers(false);
        try {
            for (String clientId : List.of("first", "second")) {
                try (MqttClient sut = client(clientId, ends::add, 16 * 1024 * 1024)) {
                    CompletableFuture<ConnAck> connection = sut.connect();
                    ScriptTransport transport = nextTransport();
                    transport.accept();
                    connection.get(5, SECONDS);

                    // Act: the following API command observes both preceding PINGRESP packets.
                    transport.receiver().onBytes(HexFormat.of().parseHex("d000d000"));
                    sut.disconnect().get(5, SECONDS);
                }
            }

            // Assert: each client identifies itself once, however often the broker repeats.
            assertThat(warnings).containsExactly(
                    "Ignoring PINGRESP without an outstanding PINGREQ"
                            + " for first at mqtt://localhost:1883",
                    "Ignoring PINGRESP without an outstanding PINGREQ"
                            + " for second at mqtt://localhost:1883"
            );
        } finally {
            jul.removeHandler(handler);
            jul.setUseParentHandlers(previousParents);
        }
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("invalidPackets")
    void connect_packet_failures_expose_a_public_cause_without_a_broker_refusal_code(
            String hex,
            ReasonCode reasonCode
    ) throws Exception {
        // Arrange
        try (MqttClient sut = client(ends::add, 16)) {
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport transport = nextTransport();

            // Act
            transport.receiver().onBytes(HexFormat.of().parseHex(hex));

            // Assert
            assertThatThrownBy(() -> connection.get(5, SECONDS)).cause().isInstanceOfSatisfying(
                    MqttConnectException.class,
                    failure -> {
                        assertThat(failure.reasonCode()).isEmpty();
                        assertProtocolFailure(failure.getCause(), reasonCode);
                    }
            );
            assertThat(ends).isEmpty();
            assertThat(transport.closed).isTrue();
        }
    }

    @Test
    void replacing_a_broker_session_announces_one_connection_and_closes_the_replaced_ones()
            throws Exception {
        // Arrange
        var notifications = new LinkedBlockingQueue<Object>();
        try (MqttClient sut = client(
                "actor-test",
                recording(notifications),
                16 * 1024 * 1024,
                codec,
                false
        )) {
            CompletableFuture<ConnAck> connection = sut.connect();
            CompletableFuture<ConnAck> observed = sut.whenConnected();
            var disconnect = new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY);
            ScriptTransport first = nextTransport();
            first.receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
            assertThat(first.sent.poll(5, SECONDS)).isEqualTo(disconnect);
            ScriptTransport discarding = nextTransport();
            discarding.receive(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));
            assertThat(discarding.sent.poll(5, SECONDS)).isEqualTo(disconnect);
            ScriptTransport last = nextTransport();
            // Only the CONNACK that establishes a connection completes observations.
            assertThat(observed).isNotDone();

            // Act: late callbacks of the replaced transports, then a write on the last one.
            last.receive(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));
            ConnAck connAck = connection.get(5, SECONDS);
            first.receiver().onClosed(new IOException("late end of the first connection"));
            discarding.receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
            discarding.receiver().onClosed(new IOException("late end of the discarding one"));
            sut.publish(message(new byte[] {1})).get(5, SECONDS);
            Packet published = last.sent.poll(5, SECONDS);

            // Assert
            assertThat(connAck).isEqualTo(ConnAck.of(false, 1));
            assertThat(observed.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            assertThat(first.closed).isTrue();
            assertThat(discarding.closed).isTrue();
            assertThat(last.closed).isFalse();
            assertThat(published).isInstanceOf(Packet.Publish.class);
            assertThat(notifications.poll(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            assertThat(notifications).isEmpty();
            assertThat(transports).isEmpty();
            // Replacing the connections without ending them leaves nothing held.
            await().untilAsserted(() -> {
                ClientActor.Usage usage = ((ClientActor) sut).usage();
                assertThat(usage.operations()).isZero();
                assertThat(usage.mailboxEvents()).isZero();
                assertThat(usage.sends()).isZero();
                assertThat(usage.notifications()).isZero();
            });
        }
    }

    @Test
    void a_transport_that_cannot_start_for_the_next_connection_fails_the_attempt_once()
            throws Exception {
        // Arrange: the transport for the discarding connection throws when started.
        var notifications = new LinkedBlockingQueue<Object>();
        var startFailure = new IllegalStateException("cannot start");
        var created = new AtomicInteger();
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId("actor-test")
                .cleanStart(false)
                .sessionExpiry(Duration.ofSeconds(30))
                .timerScheduler(scheduler)
                .connectionListener(recording(notifications))
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    if (created.incrementAndGet() == 2) {
                        transport.onStart = () -> {
                            throw startFailure;
                        };
                    }
                    transports.add(transport);
                    return transport;
                })
                .build()) {
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport first = nextTransport();

            // Act
            first.receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));

            // Assert
            assertThatThrownBy(() -> connection.get(5, SECONDS)).cause()
                    .isInstanceOf(MqttConnectException.class)
                    .hasCause(startFailure);
            assertThat(notifications.poll(5, SECONDS)).isInstanceOfSatisfying(
                    Disconnection.class,
                    failure -> assertThat(failure.cause()).hasValueSatisfying(cause ->
                            assertThat(cause).hasCause(startFailure)
                    )
            );
            assertThat(first.closed).isTrue();
            assertThat(created).hasValue(2);
            await().until(() -> ((ClientActor) sut).usage().operations() == 0);
            assertThat(notifications).isEmpty();
        }
    }

    @Test
    void a_session_refusal_survives_a_priority_packet_failure_during_disconnect()
            throws Exception {
        // Arrange: keep the refused session's DISCONNECT write outstanding.
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId("actor-test")
                .cleanStart(false)
                .sessionExpiry(Duration.ofSeconds(30))
                .timerScheduler(scheduler)
                .connectionListener(ends::add)
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                })
                .build()) {
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport transport = discardBrokerSession(nextTransport());
            transport.completeSends = false;
            transport.receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY));
            assertThat(connection).isNotDone();

            // Act: decoding fails through the priority termination slot.
            transport.receiver().onBytes(HexFormat.of().parseHex("d100"));

            // Assert: cleanup failure supplements the original refusal.
            assertThatThrownBy(() -> connection.get(5, SECONDS)).cause().isInstanceOfSatisfying(
                    MqttConnectException.class,
                    failure -> {
                        assertThat(failure)
                                .hasMessage("Broker session present after discarding it")
                                .hasNoCause();
                        assertThat(failure.reasonCode()).isEmpty();
                        assertThat(failure.getSuppressed()).singleElement().satisfies(
                                secondary -> assertProtocolFailure(
                                        secondary,
                                        ReasonCode.MALFORMED_PACKET
                                )
                        );
                    }
            );
            assertThat(ends).isEmpty();
        }
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("invalidPackets")
    void received_packet_failures_notify_the_listener_with_a_public_reason(
            String hex,
            ReasonCode reasonCode
    ) throws Exception {
        // Arrange
        try (MqttClient sut = client(ends::add, 16)) {
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport transport = nextTransport();
            transport.accept();
            connection.get(5, SECONDS);

            // Act
            transport.receiver().onBytes(HexFormat.of().parseHex(hex));

            // Assert
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            assertProtocolFailure(end.cause().orElseThrow(), reasonCode);
            assertThat(end.userRequested()).isFalse();
            assertThat(transport.closed).isTrue();
        }
    }

    @Test
    void closure_queued_before_connect_send_keeps_its_original_cause() throws Exception {
        // Arrange: both callbacks arrive before the actor handles the connected event.
        var failure = new EOFException("Peer closed before CONNECT");
        var transport = new ScriptTransport();
        transport.startFailure = failure;
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .timerScheduler(scheduler)
                .transportFactory(_ -> transport)
                .build()) {
            // Act & Assert
            assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .hasRootCause(failure);
        }
    }

    @Test
    void a_closed_send_waits_for_the_terminal_callback_to_report_its_cause() throws Exception {
        // Arrange
        var failure = new IOException("Connection reset during DISCONNECT");
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport transport = nextTransport();
            transport.accept();
            connection.get(5, SECONDS);
            transport.sendResult = CLOSED;

            // Act: reject the send while transport cleanup has not yet finished.
            CompletableFuture<Void> disconnection = sut.disconnect();

            // Assert
            assertThatThrownBy(() -> disconnection.get(100, MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(ends).isEmpty();

            // Act: cleanup completes and releases the original termination cause.
            transport.receiver().onClosed(failure);

            // Assert
            assertThatThrownBy(() -> disconnection.get(5, SECONDS)).hasRootCause(failure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, true, 1));
            assertThat(transport.closed).isTrue();
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void replaying_a_full_window_of_releases_still_sends_acknowledgements_and_pings()
            throws Exception {
        // Arrange: a persistent session keeps a full window of QoS 2 exchanges awaiting PUBCOMP.
        try (MqttClient sut = client(false)) {
            ScriptTransport first = connect(sut);
            for (int value = 0; value < 64; value++) {
                sut.publish(MqttMessage.of(
                        new Topic("window"),
                        new byte[] {(byte) value},
                        Qos.EXACTLY_ONCE,
                        false
                ));
            }
            var published = new ArrayList<Integer>();
            for (int i = 0; i < 64; i++) {
                var publication = (Packet.Publish) requireNonNull(first.sent.poll(5, SECONDS));
                published.add(publication.packetId());
            }
            // PUBRECs arrive in reverse PUBLISH order, so the replayed releases show PUBREC order.
            List<Integer> released = published.reversed();
            for (int packetId : released) {
                first.receive(new Packet.PubRec(packetId, ReasonCode.SUCCESS, EMPTY));
            }
            for (int i = 0; i < 64; i++) {
                assertThat(first.sent.poll(5, SECONDS)).isInstanceOf(Packet.PubRel.class);
            }
            first.receiver().onClosed(new IOException("lost before PUBCOMP"));
            assertThat(ends.poll(5, SECONDS)).isNotNull();
            CompletableFuture<ConnAck> reconnection = sut.connect();
            ScriptTransport second = nextTransport();
            // Settle CONNECT's write notification, so the reserved queue holds only the replay.
            await().untilAsserted(() -> assertThat(((ClientActor) sut).usage().sends()).isZero());
            // Writes now wait for the test, so the replay fills the reserved queue.
            second.completeSends = false;
            second.receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
            reconnection.get(5, SECONDS);

            // Act: PUBACKs for incoming publications and a keep-alive PINGREQ need the same queue.
            for (int id = 1; id <= 20; id++) {
                second.receive(qosOnePublication("incoming", id, false));
            }
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
            scheduler.advance(Duration.ofSeconds(10));
            // No write has completed yet: the replay holds the 14 acknowledgement slots of the
            // reserved queue (16 less the 2 kept for control packets), so the PINGREQ must fit
            // in those 2.
            await().until(() -> ((ClientActor) sut).usage().sends() == 14 + 1 || !ends.isEmpty());
            assertThat(ends).isEmpty();
            var written = new ArrayList<Packet>();
            while (written.size() < 64 + 20 + 1) {
                Packet packet = requireNonNull(second.sent.poll(5, SECONDS));
                written.add(packet);
                second.completeSend();
                if (packet instanceof Packet.PingReq) {
                    second.receive(new Packet.PingResp());
                }
            }

            // Assert: everything is written in its order, and the connection continues.
            assertThat(written).filteredOn(Packet.PubRel.class::isInstance)
                    .map(packet -> ((Packet.PubRel) packet).packetId())
                    .containsExactlyElementsOf(released);
            assertThat(written).filteredOn(Packet.PubAck.class::isInstance)
                    .map(packet -> ((Packet.PubAck) packet).packetId())
                    .containsExactlyElementsOf(IntStream.rangeClosed(1, 20).boxed().toList());
            assertThat(written).filteredOn(Packet.PingReq.class::isInstance).hasSize(1);
            assertThat(ends).isEmpty();
            assertThat(second.closed).isFalse();
        }
    }

    @Test
    void disconnect_waits_for_an_outstanding_write_before_starting_its_own() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport transport = nextTransport();
            transport.accept();
            connection.get(5, SECONDS);
            transport.completeSends = false;
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
            scheduler.advance(Duration.ofSeconds(10));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.PingReq.class);

            // Act
            CompletableFuture<Void> disconnection = sut.disconnect();

            // Assert: DISCONNECT is queued until the active PINGREQ finishes writing.
            assertThatThrownBy(() -> disconnection.get(100, MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(transport.sent).isEmpty();
            transport.completeSend();
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Disconnect.class);
            transport.completeSend();
            disconnection.get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            assertThat(transport.closed).isTrue();
        }
    }

    @Test
    void a_blocked_connect_continuation_cannot_stop_disconnect_or_notifications() throws Exception {
        // Arrange
        var release = new CountDownLatch(1);
        var entered = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> connection = sut.connect();
            CompletableFuture<Void> continuation = connection.thenRun(() -> {
                entered.countDown();
                waitFor(release);
            });
            ScriptTransport transport = nextTransport();

            // Act: complete connect while its continuation remains blocked.
            transport.accept();
            assertThat(entered.await(5, SECONDS)).isTrue();
            sut.disconnect().get(5, SECONDS);

            // Assert
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            assertThat(continuation).isNotDone();
            assertThat(transport.closed).isTrue();
        } finally {
            release.countDown();
        }
    }

    @Test
    void cancelling_the_returned_future_does_not_cancel_connect() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> future = sut.connect();

            // Act
            future.cancel(false);
            ScriptTransport transport = nextTransport();
            transport.accept();

            // Assert: cancellation leaves the connection and its notifications intact.
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            assertThat(future).isCancelled();
        }
    }

    @Test
    void fragmented_connack_and_refusal_followed_by_eof_keep_wire_order() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> future = sut.connect();
            ScriptTransport transport = nextTransport();
            byte[] refusal = codec.encode(new Packet.ConnAck(
                    false,
                    ReasonCode.NOT_AUTHORIZED,
                    EMPTY
            ));

            // Act
            for (byte part : refusal) {
                transport.receiver().onBytes(new byte[] {part});
            }
            transport.receiver().onClosed(new IOException("EOF"));

            // Assert
            assertThatThrownBy(() -> future.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .satisfies(failure -> {
                        var cause = (MqttConnectException) requireNonNull(failure.getCause());
                        assertThat(cause.reasonCode()).contains(ReasonCode.NOT_AUTHORIZED);
                    });
            assertThat(ends).isEmpty();
            assertThat(transport.closed).isTrue();
        }
    }

    @Test
    void ping_timeout_and_late_old_transport_callbacks_cannot_end_the_next_connection()
            throws Exception {
        // Arrange: establish the first connection.
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> first = sut.connect();
            ScriptTransport old = nextTransport();
            old.accept();
            first.get(5, SECONDS);
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));

            // Act: let the ping and its response deadline elapse.
            scheduler.advance(Duration.ofSeconds(10));
            assertThat(old.sent.poll(5, SECONDS)).isInstanceOf(Packet.PingReq.class);
            scheduler.advance(Duration.ofSeconds(10));
            assertThat(ends.poll(5, SECONDS)).isNotNull();
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isZero());

            // Act: reconnect, then deliver callbacks belonging to the old connection.
            CompletableFuture<ConnAck> second = sut.connect();
            ScriptTransport current = nextTransport();
            old.receiver().onClosed(new IOException("late close"));
            old.receiver().onBytes(new byte[] {(byte) 0xd0, 0});
            current.accept();

            // Assert
            assertThat(second.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 2));
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 2));
        }
    }

    @Test
    void inline_timer_callbacks_are_enqueued_instead_of_reentering_the_core() throws Exception {
        // Arrange: a scheduler that invokes the callback before returning its handle.
        TimerScheduler inline = new TimerScheduler() {
            @Override
            public long nanoTime() {
                return 0;
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                task.run();
                return () -> { };
            }
        };
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .timerScheduler(inline)
                .transportFactory(_ -> new ScriptTransport())
                .build()) {
            // Act & Assert
            assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class);
        }
    }

    @Test
    void api_calls_and_shutdown_do_not_wait_for_the_application_monitor() throws Exception {
        // Arrange
        try (MqttClient sut = client();
                var caller = Executors.newVirtualThreadPerTaskExecutor()) {
            synchronized (sut) {
                // Act: call each API from another thread while holding the client monitor.
                CompletableFuture<ConnAck> connection = caller.submit(sut::connect).get(5, SECONDS);
                ScriptTransport transport = nextTransport();
                transport.accept();
                connection.get(5, SECONDS);
                CompletableFuture<Void> disconnection = caller.submit(sut::disconnect)
                        .get(5, SECONDS);
                disconnection.get(5, SECONDS);
                caller.submit(sut::close).get(5, SECONDS);

                // Assert
                assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
                assertThat(transport.closed).isTrue();
                assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                        .hasCauseInstanceOf(IllegalStateException.class);
            }
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void close_completes_pending_connect_while_the_application_holds_the_client_monitor()
            throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport transport = nextTransport();
            synchronized (sut) {
                // Act
                sut.close();

                // Assert: completion and transport cleanup must not need this monitor.
                assertThatThrownBy(() -> connection.get(5, SECONDS))
                        .hasCauseInstanceOf(MqttConnectException.class);
                assertThat(transport.closed).isTrue();
                assertThat(ends).isEmpty();
            }
        }
    }

    @Test
    void a_fatal_send_error_closes_io_fails_operations_and_notifies_once() throws Exception {
        // Arrange
        var failure = new AssertionError("Transport send failed fatally");
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> connected = sut.connect();
            ScriptTransport transport = nextTransport();
            transport.accept();
            connected.get(5, SECONDS);
            transport.beforeSend = () -> {
                throw failure;
            };

            synchronized (sut) {
                // Act & Assert: abnormal shutdown must also ignore the application monitor.
                assertThatThrownBy(() -> sut.disconnect().get(5, SECONDS)).hasRootCause(failure);
                assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, true, 1));
                assertThat(transport.closed).isTrue();
                assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                        .hasCauseInstanceOf(IllegalStateException.class);
                assertThat(ends).isEmpty();
            }
        }
    }

    @Test
    void a_logging_failure_cannot_skip_cleanup_or_replace_the_actor_failure() throws Exception {
        // Arrange: only the diagnostic for the actor's fatal failure throws from the backend.
        var failure = new AssertionError("Actor failed");
        var loggingFailure = new AssertionError("Logging backend failed");
        var logCalls = new AtomicInteger();
        var jul = Logger.getLogger(ClientActor.class.getName());
        Level previousLevel = jul.getLevel();
        boolean previousParents = jul.getUseParentHandlers();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.SEVERE
                        && record.getMessage().startsWith("Client failed for ")) {
                    logCalls.incrementAndGet();
                    throw loggingFailure;
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        jul.addHandler(handler);
        jul.setLevel(Level.ALL);
        jul.setUseParentHandlers(false);
        var sender = new CompletableFuture<Thread>();
        var encoder = new ScriptCodec();
        encoder.beforePublish = () -> sender.complete(Thread.currentThread());
        try (MqttClient sut = client("failed-logging", ends::add, 16 * 1024 * 1024, encoder)) {
            ScriptTransport transport = connect(sut);
            await().untilAsserted(() -> assertThat(transport.pendingTicket).isZero());
            transport.completeSends = false;
            CompletableFuture<PublishResult> pending = sut.publish(message(new byte[] {1}));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            beforeActorTime = () -> {
                throw failure;
            };

            // Act
            transport.receive(new Packet.PingResp());

            // Assert: cleanup preserves the first cause and stops the idle sender too.
            assertThatThrownBy(() -> pending.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(failure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(transport.closed).isTrue();
            assertThat(sender.get(5, SECONDS).join(Duration.ofSeconds(5))).isTrue();
            assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(logCalls.get()).isEqualTo(1);
            assertThat(ends).isEmpty();
        } finally {
            jul.removeHandler(handler);
            jul.setLevel(previousLevel);
            jul.setUseParentHandlers(previousParents);
        }
    }

    @Test
    void actor_failure_is_visible_to_subscription_calls_before_cleanup() throws Exception {
        // Arrange: the diagnostic pauses shutdown after the actor has recorded its failure.
        var failure = new AssertionError("Actor failed");
        var cleanupStarted = new CompletableFuture<Thread>();
        var releaseCleanup = new CountDownLatch(1);
        var jul = Logger.getLogger(ClientActor.class.getName());
        Level previousLevel = jul.getLevel();
        boolean previousParents = jul.getUseParentHandlers();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getThrown() == failure) {
                    cleanupStarted.complete(Thread.currentThread());
                    waitFor(releaseCleanup);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        jul.addHandler(handler);
        jul.setLevel(Level.ALL);
        jul.setUseParentHandlers(false);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription observed = sut.subscribe(request());
            Subscription closed = sut.subscribe(
                    Subscribe.of(new TopicFilter("closed"), Qos.AT_MOST_ONCE)
            );
            CompletableFuture<Qos> waiting = observed.established();
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Subscribe.class);
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Subscribe.class);
            beforeActorTime = () -> {
                throw failure;
            };
            transport.receive(new Packet.PingResp());
            Thread actor = cleanupStarted.get(5, SECONDS);

            // Act & Assert: callers see the permanent cause before shutdown reaches the core.
            try {
                assertThat(waiting).isNotDone();
                assertInternalFailureBeforeCleanup(observed, closed, failure);
            } finally {
                releaseCleanup.countDown();
            }
            assertThat(actor.join(Duration.ofSeconds(5))).isTrue();
            assertInternalFailureAfterCleanup(observed, waiting, failure);
        } finally {
            releaseCleanup.countDown();
            jul.removeHandler(handler);
            jul.setLevel(previousLevel);
            jul.setUseParentHandlers(previousParents);
        }
    }

    @Test
    void blocked_continuations_hold_their_slots_but_disconnect_keeps_its_own()
            throws Exception {
        // Arrange
        var release = new CountDownLatch(1);
        var entered = new AtomicInteger();
        var continuations = new ArrayList<CompletableFuture<Void>>();
        try (MqttClient sut = client()) {
            // Act: retain the connect slot and every operation slot with blocked continuations.
            // The previous operation's completion thread may still be returning its slot, so
            // retry refusals.
            for (int i = 0; i < 65; i++) {
                CompletableFuture<ConnAck> connection = await()
                        .pollInterval(Duration.ofMillis(1))
                        .until(sut::connect, attempt -> !attempt.isCompletedExceptionally());
                continuations.add(connection.thenRun(() -> {
                    entered.incrementAndGet();
                    waitFor(release);
                }));
                ScriptTransport transport = nextTransport();
                transport.accept();
                int expected = i + 1;
                await().pollInterval(Duration.ofMillis(1)).untilAtomic(entered, is(expected));
                if (i < 64) {
                    await().pollInterval(Duration.ofMillis(1))
                            .until(sut::disconnect, attempt -> !attempt.isCompletedExceptionally())
                            .get(5, SECONDS);
                    requireNonNull(ends.poll(5, SECONDS));
                }
            }

            // Assert: saturation refuses connect and publish, but not disconnect.
            CompletableFuture<ConnAck> refusedConnect = sut.connect();
            assertThat(refusedConnect).isCompletedExceptionally();
            assertThatThrownBy(refusedConnect::join)
                    .hasCauseInstanceOf(MqttCapacityException.class);
            CompletableFuture<PublishResult> refusedPublish = sut.publish(message(new byte[0]));
            assertThatThrownBy(refusedPublish::join)
                    .hasCauseInstanceOf(MqttCapacityException.class);
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 65));
            release.countDown();
            CompletableFuture.allOf(continuations.toArray(CompletableFuture[]::new))
                    .get(5, SECONDS);
            await().untilAsserted(() -> assertThat(sut.connect().isCompletedExceptionally())
                    .isFalse());
        } finally {
            release.countDown();
        }
    }

    @Test
    void connect_and_disconnect_are_admitted_behind_publications_that_fill_the_command_queue()
            throws Exception {
        // Arrange: the actor is held while 64 publications wait for it with every shared slot.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            holdActor(transport, entered, release);
            var publications = new ArrayList<CompletableFuture<PublishResult>>();
            for (int i = 0; i < 64; i++) {
                publications.add(sut.publish(message(new byte[0])));
            }

            // Act
            CompletableFuture<ConnAck> connected = sut.connect();
            CompletableFuture<Void> disconnected = sut.disconnect();
            release.countDown();

            // Assert: both wait behind the publications instead of failing on capacity.
            assertThat(connected.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            disconnected.get(5, SECONDS);
            for (CompletableFuture<PublishResult> publication : publications) {
                publication.get(5, SECONDS);
            }
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_slow_throwing_listener_delays_no_attempt_and_loses_only_its_oldest_notifications()
            throws Exception {
        // Arrange: the first disconnection notification blocks, and every callback throws.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var notifications = new LinkedBlockingQueue<String>();
        var listener = new ConnectionListener() {
            @Override
            public void onConnected(ConnAck ack) {
                notifications.add("connected " + ack.attempt());
                throw new IllegalStateException("application listener failure");
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                notifications.add("disconnected " + disconnection.attempt());
                if (entered.getCount() > 0) {
                    entered.countDown();
                    waitFor(release);
                }
                throw new IllegalStateException("application listener failure");
            }
        };
        try (MqttClient client = client(listener)) {
            var sut = (ClientActor) client;
            connect(sut);
            sut.disconnect().get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act: 39 more connections queue 78 notifications behind the blocked callback.
            for (int attempt = 2; attempt <= 40; attempt++) {
                CompletableFuture<ConnAck> connection = sut.connect();
                nextTransport().accept();
                assertThat(connection.get(5, SECONDS).attempt()).isEqualTo(attempt);
                sut.disconnect().get(5, SECONDS);
            }

            // Assert: the queue keeps the newest 64 behind the running callback.
            assertThat(sut.usage().notifications()).isEqualTo(65);
            assertThat(sut.droppedNotifications()).isEqualTo(14);
            release.countDown();
            var expected = new ArrayList<>(List.of("connected 1", "disconnected 1"));
            for (int attempt = 9; attempt <= 40; attempt++) {
                expected.add("connected " + attempt);
                expected.add("disconnected " + attempt);
            }
            var delivered = new ArrayList<String>();
            for (int i = 0; i < expected.size(); i++) {
                delivered.add(notifications.poll(5, SECONDS));
            }
            assertThat(delivered).containsExactlyElementsOf(expected);
            await().pollInterval(Duration.ofMillis(1))
                    .untilAsserted(() -> assertThat(sut.usage().notifications()).isZero());
            assertThat(notifications).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void refused_connections_are_reported_by_attempt_and_leave_no_capacity_behind()
            throws Exception {
        // Arrange
        var failures = new LinkedBlockingQueue<Disconnection>();
        var listener = new ConnectionListener() {
            @Override
            public void onDisconnected(Disconnection disconnection) {
                ends.add(disconnection);
            }

            @Override
            public void onConnectFailed(Disconnection failure) {
                failures.add(failure);
            }
        };
        try (MqttClient client = client(listener)) {
            var sut = (ClientActor) client;

            // Act & Assert: more refusals than the listener queue holds.
            for (int attempt = 1; attempt <= 80; attempt++) {
                CompletableFuture<ConnAck> connection = sut.connect();
                ScriptTransport transport = nextTransport();
                transport.receive(new Packet.ConnAck(false, ReasonCode.SERVER_UNAVAILABLE, EMPTY));
                Throwable refusal = connection.handle((_, failure) -> failure).get(5, SECONDS);
                assertThat(refusal).isInstanceOf(MqttConnectException.class);
                assertThat(failures.poll(5, SECONDS))
                        .isEqualTo(Disconnection.of(refusal, false, attempt));
            }
            assertThat(ends).isEmpty();

            // Act: failure did not obstruct the next attempt.
            connect(sut);
            sut.disconnect().get(5, SECONDS);

            // Assert
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 81));
            await().pollInterval(Duration.ofMillis(1))
                    .untilAsserted(() -> assertThat(sut.usage().notifications()).isZero());
            assertThat(sut.droppedNotifications()).isZero();
        }
    }

    @Test
    void refused_resubscriptions_release_their_registrations_before_a_slow_listener_returns()
            throws Exception {
        // Arrange: the listener blocks in its first refusal callback.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var failures = new LinkedBlockingQueue<Subscription>();
        var listener = new ConnectionListener() {
            @Override
            public void onDisconnected(Disconnection disconnection) {
                ends.add(disconnection);
            }

            @Override
            public void onResubscribeFailed(Subscription handle, MqttSubscribeException failure) {
                failures.add(handle);
                entered.countDown();
                waitFor(release);
                throw new IllegalStateException("Listener failed");
            }
        };
        try (MqttClient client = client(listener)) {
            var sut = (ClientActor) client;
            ScriptTransport first = connect(sut);
            var subscriptions = new ArrayList<Subscription>();
            for (int i = 0; i < 256; i++) {
                subscriptions.add(subscribePull(
                        sut,
                        first,
                        Subscribe.of(new TopicFilter("bounded/" + i), Qos.AT_MOST_ONCE)
                ));
            }
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            ScriptTransport current = connect(sut);

            // Act: the broker refuses every re-creation, all but the first while the first
            // refusal callback blocks.
            for (int i = 0; i < 256; i++) {
                var packet = (Packet.Subscribe) requireNonNull(current.sent.poll(5, SECONDS));
                current.receive(new Packet.SubAck(
                        packet.packetId(),
                        List.of(ReasonCode.UNSPECIFIED_ERROR),
                        EMPTY
                ));
                if (i == 0) {
                    assertThat(entered.await(5, SECONDS)).isTrue();
                }
            }

            // Assert: the refused registrations are gone, so a new one is sent at once.
            assertThat(subscribePull(sut, current, request())).isNotNull();
            assertThat(sut.usage().notifications()).isEqualTo(65);
            assertThat(sut.droppedNotifications()).isEqualTo(191);
            release.countDown();
            var delivered = new ArrayList<Subscription>();
            for (int i = 0; i < 65; i++) {
                delivered.add(failures.poll(5, SECONDS));
            }
            var expected = new ArrayList<>(List.of(subscriptions.getFirst()));
            expected.addAll(subscriptions.subList(192, 256));
            assertThat(delivered).containsExactlyElementsOf(expected);
            assertThat(failures).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void closing_a_subscription_before_its_recreation_is_refused_reports_no_refusal()
            throws Exception {
        // Arrange: re-creation of a pull subscription awaits the broker's answer.
        var refused = new LinkedBlockingQueue<Subscription>();
        var listener = new ConnectionListener() {
            @Override
            public void onDisconnected(Disconnection disconnection) {
                ends.add(disconnection);
            }

            @Override
            public void onResubscribeFailed(Subscription handle, MqttSubscribeException failure) {
                refused.add(handle);
            }
        };
        try (MqttClient client = client(listener)) {
            var sut = (ClientActor) client;
            ScriptTransport first = connect(sut);
            Subscription subscription = subscribePull(sut, first, request());
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            ScriptTransport current = connect(sut);
            var packet = (Packet.Subscribe) requireNonNull(current.sent.poll(5, SECONDS));

            // Act: the application closes the handle before the broker refuses re-creation.
            subscription.close();
            current.receive(new Packet.SubAck(
                    packet.packetId(),
                    List.of(ReasonCode.UNSPECIFIED_ERROR),
                    EMPTY
            ));

            // Assert: the closure stays the first ending. The disconnection notification follows
            // any refusal notification in the listener's order.
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 2));
            assertThat(refused).isEmpty();
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
        }
    }

    @Test
    void continued_subscriptions_are_established_before_the_connection_is_announced()
            throws Exception {
        // Arrange: a persistent session keeps an established subscription across reconnection.
        var continuing = new AtomicReference<@Nullable Subscription>();
        var announced = new LinkedBlockingQueue<Boolean>();
        var listener = new ConnectionListener() {
            @Override
            public void onConnected(ConnAck ack) {
                Subscription subscription = continuing.get();
                if (subscription != null) {
                    announced.add(subscription.established().isDone());
                }
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                ends.add(disconnection);
            }
        };
        try (MqttClient sut = client("actor-test", listener, 16 * 1024 * 1024, codec, false)) {
            ScriptTransport first = connect(sut);
            Subscription subscription = subscribe(sut, first);
            first.receiver().onClosed(new IOException("connection lost"));
            assertThat(ends.poll(5, SECONDS)).isNotNull();
            continuing.set(subscription);

            // Act
            CompletableFuture<Boolean> completed = sut.connect()
                    .thenApply(_ -> subscription.established().isDone());
            nextTransport().receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));

            // Assert: both connect() and onConnected find the subscription established.
            assertThat(announced.poll(5, SECONDS)).isTrue();
            assertThat(completed.get(5, SECONDS)).isTrue();
        }
    }

    @Test
    void a_publication_from_on_connected_is_admitted_before_the_connect_future_completes()
            throws Exception {
        // Arrange: the actor waits inside CONNECT-timer cancellation, which the core reaches
        // after announcing the connection and before completing the connect future.
        var holdCancellation = new AtomicBoolean();
        var publication = new CompletableFuture<CompletableFuture<PublishResult>>();
        var client = new AtomicReference<MqttClient>();
        var timers = new TimerScheduler() {
            @Override
            public long nanoTime() {
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                Cancellable scheduled = scheduler.schedule(delay, task);
                return () -> {
                    scheduled.cancel();
                    if (holdCancellation.getAndSet(false)) {
                        publication.orTimeout(5, SECONDS).join();
                    }
                };
            }
        };
        var listener = new ConnectionListener() {
            @Override
            public void onConnected(ConnAck ack) {
                publication.complete(requireNonNull(client.get()).publish(message(new byte[] {1})));
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                ends.add(disconnection);
            }
        };
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .timerScheduler(timers)
                .connectionListener(listener)
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                })
                .build()) {
            client.set(sut);
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport transport = nextTransport();
            holdCancellation.set(true);

            // Act
            transport.accept();

            // Assert
            assertThat(publication.get(5, SECONDS)).succeedsWithin(Duration.ofSeconds(5));
            assertThat(connection.get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
        }
    }

    @Test
    void a_registration_refused_by_the_lanes_leaves_its_filter_available()
            throws Exception {
        // Arrange: an unsubscribed push lane stays retained while its handler runs, so 256 lanes
        // are held while only 255 subscriptions remain registered.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient client = client()) {
            var sut = (ClientActor) client;
            ScriptTransport transport = connect(sut);
            for (int i = 0; i < 255; i++) {
                subscribePull(
                        sut,
                        transport,
                        Subscribe.of(new TopicFilter("pull/" + i), Qos.AT_MOST_ONCE)
                );
            }
            Subscription push = sut.subscribe(
                    Subscribe.of(new TopicFilter("push"), Qos.AT_MOST_ONCE),
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                    }
            );
            establish(transport, push);
            transport.receive(new Packet.Publish(
                    new Topic("push"),
                    new byte[0],
                    Qos.AT_MOST_ONCE,
                    false,
                    false,
                    0,
                    EMPTY
            ));
            assertThat(entered.await(5, SECONDS)).isTrue();
            CompletableFuture<Void> unsubscribing = push.unsubscribe();
            var removal = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY));
            unsubscribing.get(5, SECONDS);
            assertThat(sut.usage().lanes()).isEqualTo(256);

            // Act: the core has room; only the lanes refuse.
            Subscription refused = sut.subscribe(request());
            CompletableFuture<Qos> refusedEstablished = refused.established();

            // Assert
            assertThatThrownBy(() -> refusedEstablished.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class)
                    .hasRootCauseMessage("Subscription capacity");
            assertThat(transport.sent).isEmpty();
            release.countDown();
            await().pollInterval(Duration.ofMillis(1))
                    .untilAsserted(() -> assertThat(sut.usage().lanes()).isEqualTo(255));
            assertThat(subscribePull(sut, transport, request())).isNotNull();
        } finally {
            release.countDown();
        }
    }

    @Test
    void disconnected_publication_fails_while_subscription_registration_is_accepted() {
        // Arrange: no connection has been requested.
        try (MqttClient sut = client()) {
            // Act & Assert
            assertThat(sut.publish(message(new byte[0])))
                    .isCompletedExceptionally();
            assertThatThrownBy(() -> sut.publish(message(new byte[0])).join())
                    .hasCauseInstanceOf(MqttNotConnectedException.class);
            Subscription subscription = sut.subscribe(request(), _ -> { });
            assertThat(subscription.established()).isNotDone();
        }
    }

    @Test
    void qos_zero_publish_completes_only_after_writing_and_preserves_terminal_failure()
            throws Exception {
        // Arrange: writes remain pending until explicitly completed.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            transport.completeSends = false;

            // Act & Assert: accepting a write leaves publication incomplete until it finishes.
            CompletableFuture<PublishResult> publication = sut.publish(message(new byte[] {1}));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            assertThat(publication).isNotDone();
            transport.completeSend();
            assertThat(publication.get(5, SECONDS)).isEqualTo(PublishResult.of());

            // Act: fail the next pending write.
            CompletableFuture<PublishResult> failed = sut.publish(message(new byte[] {2}));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            var cause = new IOException("write failed");
            transport.receiver().onClosed(cause);

            // Assert
            assertThatThrownBy(() -> failed.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(cause);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(cause, false, 1));
        }
    }

    @Test
    void qos_one_puback_can_complete_before_the_write_notification() throws Exception {
        // Arrange: hold the transport's completion while allowing inbound PUBACK.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            transport.completeSends = false;
            var message = MqttMessage.of(
                    new Topic("messages"), new byte[] {1}, Qos.AT_LEAST_ONCE, false
            );

            // Act: a fast peer responds before the sender posts write completion.
            CompletableFuture<PublishResult> publication = sut.publish(message);
            var packet = (Packet.Publish) requireNonNull(transport.sent.poll(5, SECONDS));
            assertThat(packet.qos()).isEqualTo(Qos.AT_LEAST_ONCE);
            assertThat(packet.packetId()).isNotZero();
            assertThat(publication).isNotDone();
            transport.receive(new Packet.PubAck(packet.packetId(), ReasonCode.SUCCESS, EMPTY));

            // Assert: PUBACK alone completes the operation; the late write stays harmless.
            assertThat(publication.get(5, SECONDS)).isEqualTo(PublishResult.of());
            transport.completeSend();
            transport.completeSends = true;
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
        }
    }

    @Test
    void qos_two_publish_waits_for_pubcomp_and_can_complete_before_the_pubrel_write_notification()
            throws Exception {
        // Arrange: the peer may respond before the transport reports a completed write.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            transport.completeSends = false;
            MqttMessage message = message(new byte[] {1}).withQos(Qos.EXACTLY_ONCE);

            // Act: PUBREC advances the exchange without completing the publication.
            CompletableFuture<PublishResult> publication = sut.publish(message);
            var packet = (Packet.Publish) requireNonNull(transport.sent.poll(5, SECONDS));
            assertThat(packet.qos()).isEqualTo(Qos.EXACTLY_ONCE);
            assertThat(packet.packetId()).isNotZero();
            transport.receive(new Packet.PubRec(packet.packetId(), ReasonCode.SUCCESS, EMPTY));
            transport.completeSend();

            // Assert: observing PUBREL establishes that PUBREC was handled.
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.PubRel(packet.packetId(), ReasonCode.SUCCESS, EMPTY));
            assertThat(publication).isNotDone();

            // Act & Assert: PUBCOMP completes before the delayed PUBREL write notification.
            transport.receive(new Packet.PubComp(packet.packetId(), ReasonCode.SUCCESS, EMPTY));
            assertThat(publication.get(5, SECONDS)).isEqualTo(PublishResult.of());
            transport.completeSend();
            transport.completeSends = true;
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
        }
    }

    @Test
    void qos_two_pull_subscriptions_acknowledge_after_manual_completion() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(
                    sut,
                    transport,
                    Subscribe.of(new TopicFilter("messages"), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.MANUAL)
            );
            assertThat(subscription.grantedQos()).isEqualTo(Qos.EXACTLY_ONCE);
            transport.receive(acknowledgedPublication("messages", 7, Qos.EXACTLY_ONCE, false));
            MqttDelivery delivery = received(subscription.receive(Duration.ofSeconds(5)));
            assertThat(delivery.message().qos()).isEqualTo(Qos.EXACTLY_ONCE);
            assertThat(transport.sent).isEmpty();

            // Act
            delivery.ack();

            // Assert
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.PubRec(7, ReasonCode.SUCCESS, EMPTY));
            transport.receive(new Packet.PubRel(7, ReasonCode.SUCCESS, EMPTY));
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.PubComp(7, ReasonCode.SUCCESS, EMPTY));
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void a_pubrel_can_arrive_before_the_pubrec_send_returns_or_reports_write_completion()
            throws Exception {
        // Arrange: a peer answers after seeing PUBREC, before either transport notification.
        var answered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(
                    sut,
                    transport,
                    Subscribe.of(new TopicFilter("messages"), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.MANUAL)
            );
            transport.completeSends = false;
            transport.afterSend = packet -> {
                if (packet instanceof Packet.PubRec) {
                    transport.receive(new Packet.PubRel(7, ReasonCode.SUCCESS, EMPTY));
                    answered.countDown();
                    waitFor(release);
                }
            };
            transport.receive(acknowledgedPublication("messages", 7, Qos.EXACTLY_ONCE, false));
            MqttDelivery delivery = received(subscription.receive(Duration.ofSeconds(5)));

            // Act: PUBREL reaches the actor before its PUBREC write completion event.
            delivery.ack();
            assertThat(answered.await(5, SECONDS)).isTrue();
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.PubRec(7, ReasonCode.SUCCESS, EMPTY));
            // Observe the queued response before returning ACCEPTED or posting onSent.
            await().pollInterval(Duration.ofMillis(1)).untilAsserted(() ->
                    assertThat(((ClientActor) sut).usage().sends()).isEqualTo(2));
            transport.completeSends = true;
            transport.completeSend();
            release.countDown();

            // Assert
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.PubComp(7, ReasonCode.SUCCESS, EMPTY));
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_handler_can_publish_qos_two_while_its_delivery_holds_the_receive_acknowledgements()
            throws Exception {
        // Arrange: AFTER_HANDLER holds the receive sequence until its publication completes.
        var completed = new CompletableFuture<PublishResult>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.EXACTLY_ONCE),
                    _ -> completed.complete(sut.publish(
                            message(new byte[] {2}).withQos(Qos.EXACTLY_ONCE)
                    ).join())
            );
            establish(transport, subscription);

            // Act: this receipt cannot be acknowledged while the handler waits for PUBCOMP.
            transport.receive(acknowledgedPublication("messages", 7, Qos.EXACTLY_ONCE, false));
            var outgoing = (Packet.Publish) requireNonNull(transport.sent.poll(5, SECONDS));
            assertThat(outgoing.qos()).isEqualTo(Qos.EXACTLY_ONCE);
            transport.receive(new Packet.PubRec(outgoing.packetId(), ReasonCode.SUCCESS, EMPTY));

            // Assert: the independent send sequence advances before the receive head is ready.
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.PubRel(outgoing.packetId(), ReasonCode.SUCCESS, EMPTY));
            assertThat(completed).isNotDone();
            transport.receive(new Packet.PubComp(outgoing.packetId(), ReasonCode.SUCCESS, EMPTY));
            assertThat(completed.get(5, SECONDS)).isEqualTo(PublishResult.of());
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.PubRec(7, ReasonCode.SUCCESS, EMPTY));
            transport.receive(new Packet.PubRel(7, ReasonCode.SUCCESS, EMPTY));
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.PubComp(7, ReasonCode.SUCCESS, EMPTY));
            assertThat(ends).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_LEAST_ONCE", "EXACTLY_ONCE"})
    void acknowledgements_made_while_the_actor_is_busy_share_one_mailbox_wake(Qos qos)
            throws Exception {
        // Arrange: more MANUAL deliveries than the 256-event control mailbox can hold.
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), qos)
                            .withAckMode(AckMode.MANUAL)
                            .withBufferCapacity(512),
                    delivered::add
            );
            establish(transport, subscription);
            var deliveries = new ArrayList<MqttDelivery>();
            for (int id = 1; id <= 300; id++) {
                transport.receive(acknowledgedPublication("messages", id, qos, false));
                deliveries.add(requireNonNull(delivered.poll(5, SECONDS)));
            }
            holdActor(transport, entered, release);

            // Act: every acknowledgement completes its condition before the actor resumes.
            deliveries.forEach(MqttDelivery::ack);
            release.countDown();

            // Assert
            for (int id = 1; id <= 300; id++) {
                assertThat(transport.sent.poll(5, SECONDS))
                        .isEqualTo(acknowledgement(qos, id));
            }
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_pubrec_burst_while_the_actor_is_busy_is_answered_without_flooding_the_mailbox()
            throws Exception {
        // Arrange: more unknown PUBRECs than the 256-event control mailbox can hold.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            var burst = new ByteArrayOutputStream();
            for (int id = 1; id <= 1_000; id++) {
                burst.writeBytes(codec.encode(new Packet.PubRec(id, ReasonCode.SUCCESS, EMPTY)));
            }
            holdActor(transport, entered, release);
            transport.receiver().onBytes(burst.toByteArray());

            // Act: each PUBREL write would report its completion behind the queued PUBRECs.
            release.countDown();

            // Assert
            for (int id = 1; id <= 1_000; id++) {
                assertThat(transport.sent.poll(5, SECONDS))
                        .isEqualTo(new Packet.PubRel(id, ReasonCode.SUCCESS, EMPTY));
            }
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest(name = "{0}, {1}")
    @MethodSource("acknowledgedQosModes")
    void acknowledged_bursts_wait_for_send_capacity_without_losing_the_tail(Qos qos, AckMode mode)
            throws Exception {
        // Arrange: the first write holds the transport, with room for 100 local deliveries.
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), qos)
                            .withAckMode(mode)
                            .withBufferCapacity(128),
                    delivery -> {
                        delivered.add(delivery);
                        delivery.ack();
                    }
            );
            establish(transport, subscription);
            transport.completeSends = false;
            var blocked = sut.publish(message(new byte[] {0}));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act
            for (int id = 1; id <= 100; id++) {
                transport.receive(acknowledgedPublication("messages", id, qos, false));
            }
            for (int i = 0; i < 100; i++) {
                assertThat(delivered.poll(5, SECONDS)).isNotNull();
            }
            transport.completeSends = true;
            transport.completeSend();
            blocked.get(5, SECONDS);

            // Assert: completing the held write suffices; no new incoming packet wakes the pump.
            for (int id = 1; id <= 100; id++) {
                assertThat(transport.sent.poll(5, SECONDS))
                        .isEqualTo(acknowledgement(qos, id));
            }
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
        }
    }

    @ParameterizedTest(name = "{0}, reused id has DUP={1}")
    @CsvSource({
            "AT_LEAST_ONCE, false", "AT_LEAST_ONCE, true",
            "EXACTLY_ONCE, false", "EXACTLY_ONCE, true"
    })
    void id_reuse_after_inline_receive_completion_precedes_send_buffer_release(Qos qos, boolean dup)
            throws Exception {
        // Arrange: the completion callback runs, but send() has not returned its buffer yet.
        var deliveries = new LinkedBlockingQueue<MqttDelivery>();
        var releaseSender = new CountDownLatch(1);
        var sentFirstAck = new AtomicBoolean();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), qos)
                            .withAckMode(AckMode.MANUAL),
                    deliveries::add
            );
            establish(transport, subscription);
            transport.afterSend = packet -> {
                if ((packet instanceof Packet.PubAck || packet instanceof Packet.PubComp)
                        && sentFirstAck.compareAndSet(false, true)) {
                    transport.receive(acknowledgedPublication("messages", 7, qos, dup));
                    waitFor(releaseSender);
                }
            };
            transport.receive(acknowledgedPublication("messages", 7, qos, false));
            MqttDelivery first = requireNonNull(deliveries.poll(5, SECONDS));

            // Act
            first.ack();
            if (qos == Qos.EXACTLY_ONCE) {
                assertThat(transport.sent.poll(5, SECONDS))
                        .isEqualTo(new Packet.PubRec(7, ReasonCode.SUCCESS, EMPTY));
                transport.receive(new Packet.PubRel(7, ReasonCode.SUCCESS, EMPTY));
            }
            MqttDelivery reused = requireNonNull(deliveries.poll(5, SECONDS));

            // Assert: identical payload and DUP do not cause a legal reuse to be discarded.
            assertThat(reused).isNotSameAs(first);
            assertThat(reused.dup()).isEqualTo(dup);
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(qos == Qos.AT_LEAST_ONCE
                            ? new Packet.PubAck(7, ReasonCode.SUCCESS, EMPTY)
                            : new Packet.PubComp(7, ReasonCode.SUCCESS, EMPTY));

            // Act: old handles cannot acknowledge the new reception.
            first.ack();
            releaseSender.countDown();
            sut.publish(message(new byte[] {2})).get(5, SECONDS);

            // Assert: an outgoing write is a barrier after the old acknowledgement.
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            reused.ack();
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(acknowledgement(qos, 7));
            assertThat(ends).isEmpty();
        } finally {
            releaseSender.countDown();
        }
    }

    @ParameterizedTest
    @EnumSource(ApplicationCompletion.class)
    void changing_publication_futures_neither_stops_writes_nor_releases_slots_before_puback(
            ApplicationCompletion completion
    ) throws Exception {
        // Arrange: the transport holds the first write, so later publications stay unwritten.
        var packets = new ArrayList<Packet.Publish>();
        var publications = new ArrayList<CompletableFuture<PublishResult>>();
        var message = MqttMessage.of(
                new Topic("messages"), new byte[] {1}, Qos.AT_LEAST_ONCE, false
        );
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            transport.completeSends = false;
            // connect() used its own slot, so every shared slot is free.
            for (int i = 0; i < 64; i++) {
                publications.add(sut.publish(message));
            }
            packets.add((Packet.Publish) requireNonNull(transport.sent.poll(5, SECONDS)));

            // Act: an application changes its observable futures, not the wire operations.
            for (CompletableFuture<PublishResult> publication : publications) {
                assertThat(completion.apply.test(publication)).isTrue();
            }
            transport.completeSends = true;
            transport.completeSend();

            // Assert: every publication is still written, and none releases its operation slot
            // before PUBACK.
            for (int i = 1; i < 64; i++) {
                packets.add((Packet.Publish) requireNonNull(transport.sent.poll(5, SECONDS)));
            }
            assertThatThrownBy(() -> sut.publish(message).join())
                    .hasCauseInstanceOf(MqttCapacityException.class);
            assertThat(transport.sent).isEmpty();
            for (Packet.Publish packet : packets) {
                transport.receive(new Packet.PubAck(packet.packetId(), ReasonCode.SUCCESS, EMPTY));
            }

            // Act & Assert: acknowledgements restore admission despite the changed futures.
            await().untilAsserted(() -> assertThat(sut.publish(message))
                    .isNotCompletedExceptionally());
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void a_completed_large_publish_releases_capacity_before_its_continuation_republishes()
            throws Exception {
        // Arrange: the first inline completion still leaves its buffers on the sender's stack.
        var completedInline = new CountDownLatch(1);
        var releaseSender = new CountDownLatch(1);
        var holdFirst = new AtomicBoolean(true);
        MqttMessage large = message(new byte[9 * 1024 * 1024]);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            transport.afterSend = packet -> {
                if (packet instanceof Packet.Publish && holdFirst.compareAndSet(true, false)) {
                    completedInline.countDown();
                    waitFor(releaseSender);
                }
            };

            // Act: completion immediately attempts another publication exceeding half the budget.
            CompletableFuture<PublishResult> first = sut.publish(large);
            CompletableFuture<PublishResult> second = first.thenCompose(_ -> sut.publish(large));
            assertThat(completedInline.await(5, SECONDS)).isTrue();

            // Assert: completion cannot admit the continuation before the retained buffers return.
            assertThatThrownBy(() -> second.get(100, MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);

            // Act
            releaseSender.countDown();

            // Assert: no unrelated send-capacity refusal reaches the immediately following call.
            assertThat(first.get(5, SECONDS)).isEqualTo(PublishResult.of());
            assertThat(second.get(5, SECONDS)).isEqualTo(PublishResult.of());
            assertThat(transport.sent).hasSize(2).allSatisfy(packet -> {
                assertThat(packet).isInstanceOf(Packet.Publish.class);
            });
            assertThat(ends).isEmpty();
        } finally {
            releaseSender.countDown();
        }
    }

    @Test
    void an_inline_close_after_write_completion_preserves_the_successful_publish()
            throws Exception {
        // Arrange: both callbacks occur inline before Transport.send returns its accepted result.
        var failure = new IOException("Peer closed after the write");
        var closedInline = new CountDownLatch(1);
        var releaseSender = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            transport.afterSend = packet -> {
                if (packet instanceof Packet.Publish) {
                    transport.receiver().onClosed(failure);
                    closedInline.countDown();
                    waitFor(releaseSender);
                }
            };

            // Act
            CompletableFuture<PublishResult> publication = sut.publish(message(new byte[] {1}));
            assertThat(closedInline.await(5, SECONDS)).isTrue();

            // Assert: terminal notification waits for the preceding write to release its buffers.
            assertThat(ends.poll(100, MILLISECONDS)).isNull();
            assertThat(publication).isNotDone();

            // Act
            releaseSender.countDown();

            // Assert
            assertThat(publication.get(5, SECONDS)).isEqualTo(PublishResult.of());
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(transport.closed).isTrue();
            ScriptTransport replacement = connect(sut);
            assertThat(replacement).isNotSameAs(transport);
        } finally {
            releaseSender.countDown();
        }
    }

    @Test
    void a_later_send_exception_preserves_an_earlier_write_completion() throws Exception {
        // Arrange: queue both publications before holding the actor ahead of their results.
        var failure = new RejectedExecutionException("Second send failed");
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var sending = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            await().untilAsserted(() -> assertThat(transport.pendingTicket).isZero());
            transport.completeSends = false;
            CompletableFuture<PublishResult> first = sut.publish(message(new byte[] {1}));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            CompletableFuture<PublishResult> second = sut.publish(message(new byte[] {2}));
            holdActor(transport, actorHeld, releaseActor);
            transport.beforeSend = () -> {
                sending.countDown();
                throw failure;
            };

            // Act: notify the successful write, then let the next send fail before resuming.
            transport.completeSend();
            assertThat(sending.await(5, SECONDS)).isTrue();
            // The posted end proves the sender has reported the failure, not merely entered send.
            await().until(() -> ((ClientActor) sut).postedConnectionEnds() == 1);
            releaseActor.countDown();

            // Assert: FIFO notification preserves success and only the later operation fails.
            assertThat(first.get(5, SECONDS)).isEqualTo(PublishResult.of());
            assertThatThrownBy(() -> second.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(failure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(transport.closed).isTrue();
            assertThat(transport.sent).isEmpty();

            // Act & Assert: a transport exception ends the connection, keeping the client usable.
            ScriptTransport replacement = connect(sut);
            assertThat(replacement).isNotSameAs(transport);
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 2));
        } finally {
            releaseActor.countDown();
        }
    }

    @Test
    void delivery_starts_before_suback_and_stops_before_unsuback() throws Exception {
        // Arrange
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), received::add);
            CompletableFuture<Qos> subscribing = subscription.established();
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));

            // Act & Assert: delivery starts before the broker acknowledges the subscription.
            transport.receive(publication(new byte[] {1}));
            assertThat(received.poll(5, SECONDS)).isNotNull();
            assertThat(subscribing).isNotDone();
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            subscribing.get(5, SECONDS);
            assertThat(subscription.grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);

            // Act & Assert: new delivery stops before the broker acknowledges removal.
            CompletableFuture<Void> unsubscribing = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(publication(new byte[] {2}));
            assertThatThrownBy(() -> sut.subscribe(request(), received::add)
                    .established().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            unsubscribing.get(5, SECONDS);
            assertThat(received).isEmpty();
            CompletableFuture<Void> repeated = subscription.unsubscribe();
            assertThat(repeated).isNotSameAs(unsubscribing);
            repeated.get(5, SECONDS);

            // Act & Assert: the filter becomes available again after acknowledged removal.
            Subscription replacement = sut.subscribe(request(), received::add);
            establish(transport, replacement);
        }
    }

    @Test
    void unsubscribe_stops_delivery_of_a_publication_queued_before_the_call()
            throws Exception {
        // Arrange: the handler holds the only delivery slot of its subscription. The messages are
        // QoS 1, so a full lane would end the connection rather than drop them.
        var handlerEntered = new CountDownLatch(1);
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE)
                            .withBufferCapacity(1),
                    _ -> {
                        handlerEntered.countDown();
                        waitFor(releaseHandler);
                    }
            );
            establish(transport, subscription);
            transport.receive(qosOnePublication("messages", 1, false));
            assertThat(handlerEntered.await(5, SECONDS)).isTrue();
            holdActor(transport, actorHeld, releaseActor);
            transport.receive(qosOnePublication("messages", 2, false));

            // Act: the call precedes the actor's handling of the queued publication.
            CompletableFuture<Void> unsubscribing = subscription.unsubscribe();
            releaseActor.countDown();

            // Assert: admitting that publication would have overflowed the one-slot lane.
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            unsubscribing.get(5, SECONDS);
            assertThat(ends).isEmpty();
        } finally {
            releaseActor.countDown();
            releaseHandler.countDown();
        }
    }

    @Test
    void a_full_regular_byte_budget_delays_subscribe_and_unsubscribe_without_losing_them()
            throws Exception {
        // Arrange: one pending write fills the normal send budget.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), received::add);
            CompletableFuture<Qos> subscribing = subscription.established();
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            subscribing.get(5, SECONDS);
            transport.completeSends = false;
            // One-byte topic + length prefix + five-byte fixed header fills 16 MiB exactly.
            CompletableFuture<PublishResult> publication = sut.publish(MqttMessage.of(
                    new Topic("a"),
                    new byte[16 * 1024 * 1024 - 8],
                    Qos.AT_MOST_ONCE,
                    false
            ));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act: both requests remain accepted while send capacity is exhausted.
            CompletableFuture<Void> removing = subscription.unsubscribe();
            Subscription other = sut.subscribe(
                    Subscribe.of(new TopicFilter("other"), Qos.AT_MOST_ONCE),
                    _ -> { }
            );
            CompletableFuture<Qos> establishing = other.established();
            transport.receive(publication(new byte[] {3}));
            assertThat(removing).isNotDone();
            assertThat(establishing).isNotDone();

            // Assert: returning send capacity sends both requests without another API call.
            transport.completeSends = true;
            transport.completeSend();
            publication.get(5, SECONDS);
            for (int i = 0; i < 2; i++) {
                Packet packet = requireNonNull(transport.sent.poll(5, SECONDS));
                switch (packet) {
                    case Packet.Subscribe(var id, _, _) -> transport.receive(
                            new Packet.SubAck(id, List.of(ReasonCode.SUCCESS), EMPTY)
                    );
                    case Packet.Unsubscribe(var id, _, _) -> transport.receive(
                            new Packet.UnsubAck(id, List.of(), EMPTY)
                    );
                    default -> throw new AssertionError("Unexpected packet: " + packet);
                }
            }
            establishing.get(5, SECONDS);
            removing.get(5, SECONDS);
            assertThat(received).isEmpty();
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void a_full_observation_budget_does_not_cancel_a_new_removal() throws Exception {
        // Arrange: another subscription's pending removal and its observers hold every waiter slot.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), received::add);
            CompletableFuture<Qos> subscribing = subscription.established();
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            subscribing.get(5, SECONDS);
            Subscription other = sut.subscribe(
                    Subscribe.of(new TopicFilter("other"), Qos.AT_MOST_ONCE),
                    _ -> { }
            );
            establish(transport, other);
            await().until(() -> ((ClientActor) sut).usage().waiters() == 0);
            var observers = new ArrayList<CompletableFuture<Void>>();
            for (int i = 0; i < 1024; i++) {
                observers.add(other.unsubscribe());
            }
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Unsubscribe.class);
            assertThat(observers).allSatisfy(observer -> assertThat(observer).isNotDone());

            // Act
            assertThatThrownBy(() -> subscription.unsubscribe().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);
            transport.receive(publication(new byte[] {9}));

            // Assert: refusal limits observation only; the second removal still stops delivery.
            var removal = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            assertThat(removal.filters()).containsExactly(subscription.filter());
            transport.receive(new Packet.UnsubAck(removal.packetId(), List.of(), EMPTY));
            sut.publish(message(new byte[0])).get(5, SECONDS);
            assertThat(received).isEmpty();
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void a_blocked_handler_keeps_its_delivery_slot_and_overflow_notifies_the_listener()
            throws Exception {
        // Arrange: a subscription with one delivery slot uses a blocking handler.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE)
                            .withBufferCapacity(1),
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                    }
            );
            establish(transport, subscription);

            // Act: a running handler still counts toward the subscription's delivery limit.
            transport.receive(qosOnePublication("messages", 1, false));
            assertThat(entered.await(5, SECONDS)).isTrue();
            transport.receive(qosOnePublication("messages", 2, false));
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));

            // Assert
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class);
            assertThat(transport.closed).isTrue();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_full_qos_0_subscription_drops_new_messages_and_keeps_the_connection()
            throws Exception {
        // Arrange: a blocking handler holds the subscription's only delivery slot.
        var handled = new LinkedBlockingQueue<Byte>();
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    request().withBufferCapacity(1),
                    delivery -> {
                        handled.add(delivery.message().payload()[0]);
                        waitFor(release);
                    }
            );
            establish(transport, subscription);
            transport.receive(publication(new byte[] {1}));
            assertThat(handled.poll(5, SECONDS)).isEqualTo((byte) 1);

            // Act: two messages arrive while the subscription is full.
            transport.receive(publication(new byte[] {2}));
            transport.receive(publication(new byte[] {3}));
            // This command follows the publications through the mailbox, so it completes after
            // routing.
            sut.publish(message(new byte[0])).get(5, SECONDS);
            release.countDown();
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 0);
            transport.receive(publication(new byte[] {4}));

            // Assert: only the messages that found the subscription full were dropped, and the
            // connection goes on.
            assertThat(handled.poll(5, SECONDS)).isEqualTo((byte) 4);
            assertThat(handled).isEmpty();
            assertThat(ends).isEmpty();
            assertThat(transport.closed).isFalse();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_qos_1_publication_overflowing_a_qos_0_subscription_ends_the_connection()
            throws Exception {
        // Arrange: the subscription asked for QoS 0, but a broker can send QoS 1 when another
        // subscription of the client also matches. A blocking handler holds the only slot.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    request().withBufferCapacity(1),
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                    }
            );
            establish(transport, subscription);
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            transport.receive(qosOnePublication("messages", 2, false));

            // Assert: overflow follows the QoS of the received publication, not the requested one.
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class);
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_LEAST_ONCE", "EXACTLY_ONCE"})
    void a_handler_that_acknowledges_before_failing_keeps_its_acknowledgement(Qos qos)
            throws Exception {
        // Arrange: the handler acknowledges its MANUAL delivery and then throws.
        var failure = new IllegalStateException("handler failed after ack");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), qos)
                            .withAckMode(AckMode.MANUAL),
                    delivery -> {
                        delivery.ack();
                        throw failure;
                    }
            );
            establish(transport, subscription);

            // Act
            transport.receive(acknowledgedPublication("messages", 7, qos, false));

            // Assert: the delivery held back nothing, so its failure is skipped; the
            // acknowledgement goes out and the connection stays.
            assertThat(transport.sent.poll(5, SECONDS)).isEqualTo(acknowledgement(qos, 7));
            await().until(() -> ((ClientActor) sut).reportedHandlerFailures() == 1);
            assertThat(ends).isEmpty();
            assertThat(transport.closed).isFalse();
        }
    }

    @ParameterizedTest(name = "subscriptions: {0}")
    @ValueSource(ints = {1, 256})
    void handler_failure_overtakes_queued_input_and_commands(int subscriptions) throws Exception {
        // Arrange: a QoS 1 publication reaches every lane before input and a command wait in the
        // actor.
        var delivered = new CountDownLatch(subscriptions);
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var received = new AtomicInteger();
        var encoded = new AtomicInteger();
        var failure = new IllegalStateException("handler failed before queued work");
        var encoder = new ScriptCodec();
        try (MqttClient sut = client("handler-priority", ends::add, 16 * 1024 * 1024, encoder)) {
            ScriptTransport transport = connect(sut);
            var registered = new ArrayList<Subscription>();
            for (int i = 0; i < subscriptions; i++) {
                boolean failing = i == 0;
                var request = Subscribe.of(
                        new TopicFilter("a/".repeat(i) + "#"),
                        Qos.AT_LEAST_ONCE
                );
                Subscription subscription = sut.subscribe(request, _ -> {
                    delivered.countDown();
                    waitFor(releaseHandler);
                    if (failing) {
                        throw failure;
                    }
                });
                establish(transport, subscription);
                registered.add(subscription);
            }
            var incoming = new Packet.Publish(new Topic("a/".repeat(subscriptions) + "message"),
                    new byte[] {1}, Qos.AT_LEAST_ONCE, false, false, 1, EMPTY);
            transport.receive(incoming);
            assertThat(delivered.await(5, SECONDS)).isTrue();
            holdActor(transport, actorHeld, releaseActor);
            // The held clock call keeps its original callback. Later calls count input handling.
            beforeActorTime = received::incrementAndGet;
            encoder.beforeEncode = packet -> {
                if (packet instanceof Packet.Unsubscribe) {
                    encoded.incrementAndGet();
                }
            };
            for (int i = 0; i < 32; i++) {
                transport.receive(incoming);
            }
            CompletableFuture<Void> queued = registered.getLast().unsubscribe();

            // Act: the failure report reaches the held actor behind the queued input and command.
            releaseHandler.countDown();
            await().until(() -> ((ClientActor) sut).reportedHandlerFailures() == 1);
            releaseActor.countDown();

            // Assert: termination discards input and confirms clean-session removal
            // without sending the queued request.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            queued.get(5, SECONDS);
            assertThat(List.of(received.get(), encoded.get())).containsExactly(0, 0);
            assertThat(transport.closed).isTrue();
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @ParameterizedTest(name = "handler failure first: {0}")
    @ValueSource(booleans = {false, true})
    void a_handler_stop_precedes_transport_failure_regardless_of_arrival_order(boolean handlerFirst)
            throws Exception {
        // Arrange: both failures arrive while the actor cannot consume either notification.
        var entered = new CountDownLatch(1);
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var failure = new IllegalStateException("handler failed");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(qosOneRequest(), _ -> {
                entered.countDown();
                waitFor(releaseHandler);
                throw failure;
            });
            establish(transport, subscription);
            transport.receive(qosOnePublication("messages", 1, false));
            assertThat(entered.await(5, SECONDS)).isTrue();
            holdActor(transport, actorHeld, releaseActor);

            // Act: invalid input uses the termination slot before or after the failure report.
            if (!handlerFirst) {
                transport.receiver().onBytes(new byte[] {(byte) 0xff, 0});
            }
            releaseHandler.countDown();
            await().until(() -> ((ClientActor) sut).reportedHandlerFailures() == 1);
            if (handlerFirst) {
                transport.receiver().onBytes(new byte[] {(byte) 0xff, 0});
            }
            releaseActor.countDown();

            // Assert: a handler stop takes precedence over an already pending connection ending.
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            assertThat(end).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(transport.closed).isTrue();
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @Test
    void old_handler_failure_cannot_hide_the_current_transport_failure() throws Exception {
        // Arrange: the old handler is still running after a new connection is established.
        var entered = new CountDownLatch(1);
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var failure = new IllegalStateException("old handler failed");
        try (MqttClient sut = client()) {
            ScriptTransport old = connect(sut);
            Subscription subscription = sut.subscribe(request(), _ -> {
                entered.countDown();
                waitFor(releaseHandler);
                throw failure;
            });
            establish(old, subscription);
            old.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            ScriptTransport current = connect(sut);
            var resubscribe = (Packet.Subscribe) requireNonNull(current.sent.poll(5, SECONDS));
            current.receive(new Packet.SubAck(resubscribe.packetId(),
                    List.of(ReasonCode.SUCCESS), EMPTY));
            holdActor(current, actorHeld, releaseActor);

            // Act: the stale failure is handled first, but as a skip it cannot reserve the current
            // termination.
            releaseHandler.countDown();
            await().until(() -> ((ClientActor) sut).reportedHandlerFailures() == 1);
            current.receiver().onBytes(new byte[] {(byte) 0xff, 0});
            releaseActor.countDown();

            // Assert: the valid current cause still ends the connection.
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            assertProtocolFailure(end.cause().orElseThrow(), ReasonCode.MALFORMED_PACKET);
            assertThat(end.userRequested()).isFalse();
            assertThat(current.closed).isTrue();
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @ParameterizedTest(name = "{0}, session present: {1}, handler failure first: {2}")
    @CsvSource({
            "AT_LEAST_ONCE, true, true", "AT_LEAST_ONCE, true, false",
            "AT_LEAST_ONCE, false, true", "AT_LEAST_ONCE, false, false",
            "EXACTLY_ONCE, true, true", "EXACTLY_ONCE, true, false",
            "EXACTLY_ONCE, false, true", "EXACTLY_ONCE, false, false"
    })
    void a_previous_connections_handler_failure_is_prioritized_only_for_the_retained_session(
            Qos qos,
            boolean sessionPresent,
            boolean handlerFirst
    ) throws Exception {
        // Arrange: the old handler remains active while a new connection's actor is held.
        var entered = new CountDownLatch(1);
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var handledInput = new AtomicInteger();
        var encodedCommands = new AtomicInteger();
        var failure = new IllegalStateException("retained handler failed");
        var encoder = new ScriptCodec();
        try (
                MqttClient sut = client(
                        "session-priority",
                        ends::add,
                        16 * 1024 * 1024,
                        encoder,
                        false
                )
        ) {
            ScriptTransport old = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), qos)
                            .withAckMode(AckMode.AFTER_HANDLER),
                    _ -> {
                        entered.countDown();
                        waitFor(releaseHandler);
                        throw failure;
                    }
            );
            establish(old, subscription);
            old.receive(acknowledgedPublication("messages", 7, qos, false));
            assertThat(entered.await(5, SECONDS)).isTrue();
            old.receiver().onClosed(new IOException("connection lost"));
            assertThat(ends.poll(5, SECONDS)).isNotNull();
            ScriptTransport current = connect(sut, sessionPresent);
            if (sessionPresent) {
                current.receive(acknowledgedPublication("messages", 7, qos, true));
                // A round trip settles the retransmission, so the actor is held at the PINGRESP.
                sut.publish(message(new byte[0])).get(5, SECONDS);
                assertThat(current.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            } else {
                acknowledgeSubscription(current, subscription);
            }
            holdActor(current, actorHeld, releaseActor);
            beforeActorTime = handledInput::incrementAndGet;
            encoder.beforeEncode = packet -> {
                if (packet instanceof Packet.Unsubscribe) {
                    encodedCommands.incrementAndGet();
                }
            };
            for (int i = 0; i < 32; i++) {
                current.receive(publication(new byte[] {1}));
            }
            CompletableFuture<Void> queued = subscription.unsubscribe();

            // Act: invalid input reaches the held actor before or after the failure is handled. The
            // failure is reported for the retained session and skipped once its session was
            // discarded.
            if (!handlerFirst) {
                current.receiver().onBytes(new byte[] {(byte) 0xff, 0});
            }
            releaseHandler.countDown();
            await().until(() -> ((ClientActor) sut).reportedHandlerFailures() == 1);
            if (handlerFirst) {
                current.receiver().onBytes(new byte[] {(byte) 0xff, 0});
            }
            releaseActor.countDown();

            // Assert: valid termination overtakes every queued packet and application command.
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            if (sessionPresent) {
                assertThat(end).isEqualTo(Disconnection.of(failure, false, 2));
            } else {
                assertProtocolFailure(end.cause().orElseThrow(), ReasonCode.MALFORMED_PACKET);
            }
            assertThat(queued).isNotDone();
            assertThat(List.of(handledInput.get(), encodedCommands.get())).containsExactly(0, 0);
            assertThat(current.sent).isEmpty();
            assertThat(current.closed).isTrue();
            assertThat(ends).isEmpty();
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @ParameterizedTest(name = "session present: {0}")
    @ValueSource(booleans = {false, true})
    void connack_keeps_a_stop_only_for_its_current_session_when_io_also_fails(
            boolean sessionPresent
    ) throws Exception {
        // Arrange: CONNACK is taken, but its clock read precedes the session identity decision.
        var entered = new CountDownLatch(1);
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var failure = new IllegalStateException("handler failed during CONNACK");
        try (MqttClient sut = client(false)) {
            ScriptTransport old = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE),
                    _ -> {
                        entered.countDown();
                        waitFor(releaseHandler);
                        throw failure;
                    }
            );
            establish(old, subscription);
            old.receive(qosOnePublication("messages", 7, false));
            assertThat(entered.await(5, SECONDS)).isTrue();
            old.receiver().onClosed(new IOException("connection lost"));
            assertThat(ends.poll(5, SECONDS)).isNotNull();
            CompletableFuture<ConnAck> connected = sut.connect();
            ScriptTransport current = nextTransport();
            // Settle CONNECT's write notification before holding CONNACK's clock read.
            await().untilAsserted(() -> assertThat(((ClientActor) sut).usage().sends()).isZero());
            // Empty input takes the sender's monitor after budget release, so the completion
            // is posted before the following FIFO barrier without admitting another packet.
            current.receiver().onBytes(new byte[0]);
            assertThatThrownBy(() -> sut.subscribe(request()).established().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            beforeActorTime = () -> {
                actorHeld.countDown();
                waitFor(releaseActor);
            };
            current.receive(new Packet.ConnAck(sessionPresent, ReasonCode.SUCCESS, EMPTY));
            assertThat(actorHeld.await(5, SECONDS)).isTrue();

            // Act: I/O failure follows an admitted handler failure before CONNACK resumes.
            releaseHandler.countDown();
            await().until(() -> ((ClientActor) sut).reportedHandlerFailures() == 1);
            current.receiver().onBytes(new byte[] {(byte) 0xff, 0});
            releaseActor.countDown();

            // Assert: SP=0 withdraws only the old handler cause, preserving the waiting I/O cause.
            assertThat(connected.get(5, SECONDS)).isEqualTo(ConnAck.of(sessionPresent, 2));
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            if (sessionPresent) {
                assertThat(end).isEqualTo(Disconnection.of(failure, false, 2));
            } else {
                assertProtocolFailure(end.cause().orElseThrow(), ReasonCode.MALFORMED_PACKET);
                assertThat(end.userRequested()).isFalse();
            }
            assertThat(current.closed).isTrue();
            assertThat(current.sent).noneMatch(Packet.PubAck.class::isInstance);
            assertThat(ends).isEmpty();
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @Test
    void a_publication_in_progress_cannot_turn_an_accepted_handler_failure_into_overload()
            throws Exception {
        // Arrange: the failing QoS 1 delivery keeps the lane's only slot, and the actor is held
        // just before the next publication for that lane reaches the session.
        var entered = new CountDownLatch(1);
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var failure = new IllegalStateException("handler failed with a full lane");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE)
                            .withBufferCapacity(1),
                    _ -> {
                        entered.countDown();
                        waitFor(releaseHandler);
                        throw failure;
                    }
            );
            establish(transport, subscription);
            transport.receive(new Packet.Publish(new Topic("messages"), new byte[] {1},
                    Qos.AT_LEAST_ONCE, false, false, 7, EMPTY));
            assertThat(entered.await(5, SECONDS)).isTrue();
            beforeActorTime = () -> {
                actorHeld.countDown();
                waitFor(releaseActor);
            };
            transport.receive(new Packet.Publish(new Topic("messages"), new byte[] {2},
                    Qos.AT_LEAST_ONCE, false, false, 8, EMPTY));
            assertThat(actorHeld.await(5, SECONDS)).isTrue();

            // Act: the failure is accepted while that publication is still in progress.
            releaseHandler.countDown();
            await().until(() -> ((ClientActor) sut).reportedHandlerFailures() == 1);
            releaseActor.countDown();

            // Assert: the full lane neither replaces the accepted cause nor releases a PUBACK.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(transport.sent).isEmpty();
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Shutdown starts inside the block while the actor is held.
    void a_handler_failure_while_the_client_closes_does_not_run_the_next_delivery()
            throws Exception {
        // Arrange: a second delivery waits behind the failing handler, and the held actor
        // cannot yet remove the lane during shutdown.
        var workers = new LinkedBlockingQueue<Thread>();
        var handled = new LinkedBlockingQueue<Byte>();
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var failure = new IllegalStateException("handler failed during shutdown");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(qosOneRequest(), delivery -> {
                byte payload = delivery.message().payload()[0];
                handled.add(payload);
                if (payload == 1) {
                    workers.add(Thread.currentThread());
                    waitFor(releaseHandler);
                    throw failure;
                }
            });
            establish(transport, subscription);
            transport.receive(qosOnePublication("messages", 1, false));
            Thread worker = requireNonNull(workers.poll(5, SECONDS));
            transport.receive(qosOnePublication("messages", 2, false));
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 2);
            holdActor(transport, actorHeld, releaseActor);

            // Act: shutdown is requested before the handler fails.
            sut.close();
            releaseHandler.countDown();
            await().until(() -> ((ClientActor) sut).reportedHandlerFailures() == 1);
            releaseActor.countDown();

            // Assert: the failed lane stays paused until shutdown stops its worker.
            await().until(() -> !worker.isAlive());
            assertThat(handled).containsExactly((byte) 1);
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @Test
    void received_packet_size_counts_toward_the_delivery_byte_budget() throws Exception {
        // Arrange: three QoS 1 deliveries fit the subscription's slots but not the 32 MiB budget.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE)
                            .withBufferCapacity(3),
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                    }
            );
            establish(transport, subscription);
            byte[] payload = new byte[11 * 1024 * 1024];
            transport.receive(qosOnePublication(1, payload));
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            transport.receive(qosOnePublication(2, payload));
            transport.receive(qosOnePublication(3, payload));

            // Assert
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class);
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_full_incoming_mailbox_closes_the_connection_as_overload() throws Exception {
        // Arrange: hold the actor so received packets stay in the mailbox.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            await().untilAsserted(() -> assertThat(transport.pendingTicket).isZero());
            transport.completeSends = false;
            CompletableFuture<PublishResult> held = sut.publish(message(new byte[0]));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            holdActor(transport, entered, release);
            byte[] payload = new byte[11 * 1024 * 1024];

            // Act: the third packet exceeds the 32 MiB incoming byte budget.
            transport.receive(publication(payload));
            transport.receive(publication(payload));
            transport.receive(publication(payload));
            release.countDown();

            // Assert
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            assertThat(end.cause().orElseThrow()).isInstanceOf(MqttCapacityException.class);
            assertThat(held).failsWithin(Duration.ofSeconds(5));
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_burst_of_small_publications_waits_for_a_busy_actor_without_overload() throws Exception {
        // Arrange: a pull subscription has room for the whole burst, and the actor is busy.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(
                    sut, transport, request().withBufferCapacity(65_536)
            );
            holdActor(transport, entered, release);
            byte[] packet = codec.encode(publication(new byte[] {1}));
            var burst = new ByteArrayOutputStream();
            for (int i = 0; i < 65_536; i++) {
                burst.writeBytes(packet);
            }

            // Act: one chunk queues as many packets as the incoming mailbox counts.
            transport.receiver().onBytes(burst.toByteArray());
            release.countDown();
            // This command follows the burst through the mailbox, so it completes after routing.
            sut.publish(message(new byte[0])).get(5, SECONDS);

            // Assert: the subscription holds the whole burst within the client-wide bound.
            assertThat(((ClientActor) sut).usage().deliveries()).isEqualTo(65_536);
            for (int i = 0; i < 65_536; i++) {
                assertThat(subscription.receive(Duration.ofSeconds(5)))
                        .isInstanceOf(ReceiveResult.Received.class);
            }
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void keep_alive_overtakes_a_receive_backlog_for_256_subscriptions() throws Exception {
        // Arrange: each of 65,536 queued publications is matched against 256 filters.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var enteredAgain = new CountDownLatch(1);
        var releaseAgain = new CountDownLatch(1);
        var queuedAtPing = new LinkedBlockingQueue<Integer>();
        var encoder = new ScriptCodec();
        try (MqttClient sut = client("keep-alive", ends::add, 16 * 1024 * 1024, encoder)) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            for (int i = 0; i < 256; i++) {
                Subscription subscription = sut.subscribe(
                        Subscribe.of(new TopicFilter("backlog/" + i), Qos.AT_MOST_ONCE),
                        _ -> { }
                );
                establish(transport, subscription);
            }
            // The actor encodes each packet it queues, so this records what still waits then.
            encoder.beforeEncode = packet -> {
                if (packet instanceof Packet.PingReq) {
                    queuedAtPing.add(actor.usage().mailboxEvents());
                }
            };
            var burst = new ByteArrayOutputStream();
            for (int i = 0; i < 65_536; i++) {
                burst.writeBytes(codec.encode(new Packet.Publish(new Topic("backlog/" + i % 256),
                        new byte[1], Qos.AT_MOST_ONCE, false, false, 0, EMPTY)));
            }
            holdActor(transport, entered, release);
            transport.receiver().onBytes(burst.toByteArray());

            // Act: the keep-alive interval elapses with the whole backlog queued.
            scheduler.advance(Duration.ofSeconds(10));
            release.countDown();

            // Assert: the PINGREQ is queued before the actor takes any of the publications.
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.PingReq.class);
            assertThat(queuedAtPing.poll(5, SECONDS)).isGreaterThanOrEqualTo(65_536);

            // Act: hold the actor inside the backlog while PINGRESP arrives and the next
            // interval, with the PINGRESP deadline, elapses.
            beforeActorTime = () -> {
                enteredAgain.countDown();
                waitFor(releaseAgain);
            };
            assertThat(enteredAgain.await(5, SECONDS)).isTrue();
            transport.receive(new Packet.PingResp());
            scheduler.advance(Duration.ofSeconds(10));
            releaseAgain.countDown();

            // Assert: PINGRESP also goes first, so the deadline passes and the next PINGREQ
            // follows while most publications still wait.
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.PingReq.class);
            assertThat(queuedAtPing.poll(5, SECONDS)).isGreaterThan(1_000);
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> actor.usage().mailboxEvents() == 0);
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
            releaseAgain.countDown();
        }
    }

    @Test
    void connection_end_frees_io_timers_and_mailbox_while_application_code_still_runs()
            throws Exception {
        // Arrange: application code holds a continuation, an unacknowledged MANUAL delivery,
        // a handler waiting for its own QoS 1 publication, and the connection listener.
        var listening = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var publishFailure = new CompletableFuture<Throwable>();
        try (MqttClient sut = client(end -> {
            ends.add(end);
            listening.countDown();
            waitFor(release);
        })) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            Subscription manual = subscribePull(sut, transport,
                    Subscribe.of(new TopicFilter("manual"), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.MANUAL));
            transport.receive(new Packet.Publish(new Topic("manual"), new byte[] {1},
                    Qos.AT_LEAST_ONCE, false, false, 1, EMPTY));
            assertThat(manual.receive(Duration.ofSeconds(5)))
                    .isInstanceOf(ReceiveResult.Received.class);
            Subscription subscription = sut.subscribe(
                    request().withBufferCapacity(2),
                    _ -> {
                        try {
                            sut.publish(MqttMessage.of(new Topic("replies"), new byte[] {2},
                                    Qos.AT_LEAST_ONCE, false)).join();
                        } catch (CompletionException failure) {
                            publishFailure.complete(failure.getCause());
                        }
                        waitFor(release);
                    }
            );
            establish(transport, subscription);
            // Attach the continuation before the write completes, so it blocks a completion
            // thread rather than this one. The transport is slow to close, so it keeps owning
            // a stalled write after the connection ends.
            transport.completeSends = false;
            transport.deferCloseReport = true;
            CompletableFuture<Void> continuation = sut.publish(message(new byte[] {3}))
                    .thenRun(() -> waitFor(release));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            transport.completeSend();

            // Act: the handler waits for PUBACK behind a stalled write, another publication
            // queues behind that write, and a burst of 1,000 overflows the handler's queue. The
            // burst is QoS 1, which overflow cannot drop as it drops QoS 0.
            transport.receive(publication(new byte[] {4}));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOfSatisfying(
                    Packet.Publish.class,
                    reply -> assertThat(reply.topic()).isEqualTo(new Topic("replies"))
            );
            CompletableFuture<PublishResult> queued = sut.publish(message(new byte[] {7}));
            var burst = new ByteArrayOutputStream();
            for (int i = 0; i < 1000; i++) {
                burst.writeBytes(codec.encode(qosOnePublication(10 + i, new byte[] {5})));
            }
            transport.receiver().onBytes(burst.toByteArray());
            assertThat(listening.await(5, SECONDS)).isTrue();

            // Assert: the overload fails both publications and discards the queued write.
            // Timers and the mailbox are free; the transport still owns the stalled write.
            assertThat(publishFailure.get(5, SECONDS))
                    .isInstanceOf(MqttConnectionLostException.class)
                    .hasCauseInstanceOf(MqttCapacityException.class);
            assertThatThrownBy(() -> queued.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class);
            assertThat(scheduler.pendingTaskCount()).isZero();
            assertThat(actor.usage().sends()).isEqualTo(1);

            // Act: the transport's final callback releases the stalled write.
            transport.finishClose();

            // Assert: both subscriptions remain; only the running handler retains a delivery.
            var held = new ClientActor.Usage(1, 1, 0, 0, 0, 0, 2, 1, 13, 0);
            await().untilAsserted(() -> assertThat(actor.usage()).isEqualTo(held));

            // Act
            release.countDown();

            // Assert
            continuation.get(5, SECONDS);
            var retained = new ClientActor.Usage(0, 0, 0, 0, 0, 0, 2, 0, 0, 0);
            await().untilAsserted(() -> assertThat(actor.usage()).isEqualTo(retained));
        } finally {
            release.countDown();
        }
    }

    @Test
    void publish_continuations_retain_operation_capacity_until_they_return() throws Exception {
        // Arrange
        var entered = new AtomicInteger();
        var release = new CountDownLatch(1);
        var continuations = new ArrayList<CompletableFuture<Void>>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            transport.completeSends = false;

            // Act: hold every operation slot in a blocked completion callback.
            for (int i = 0; i < 64; i++) {
                CompletableFuture<PublishResult> publication = sut.publish(message(new byte[0]));
                continuations.add(publication.thenRun(() -> {
                    entered.incrementAndGet();
                    waitFor(release);
                }));
                assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
                transport.completeSend();
                int expected = i + 1;
                await().pollInterval(Duration.ofMillis(1)).untilAtomic(entered, is(expected));
            }

            // Assert: no new operation is admitted while the callbacks retain all slots.
            CompletableFuture<PublishResult> refused = sut.publish(message(new byte[0]));
            assertThat(refused).isCompletedExceptionally();
            assertThatThrownBy(refused::join).hasCauseInstanceOf(MqttCapacityException.class);
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
            CompletableFuture.allOf(continuations.toArray(CompletableFuture[]::new))
                    .get(5, SECONDS);
        }
    }

    @Test
    void outgoing_mailbox_saturation_cannot_consume_incoming_or_control_capacity()
            throws Exception {
        // Arrange: hold the actor so queued publications fill the 32 MiB command budget.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), delivered::add);
            CompletableFuture<Qos> subscribing = subscription.established();
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            holdActor(transport, entered, release);
            var publications = new ArrayList<CompletableFuture<PublishResult>>();
            MqttMessage message = message(new byte[1024 * 1024]);
            for (int i = 0; i < 40; i++) {
                publications.add(sut.publish(message));
            }

            // Act: receive responses and a new delivery while the command budget is full.
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            transport.receive(publication(new byte[] {42}));
            transport.receive(new Packet.PingResp());
            release.countDown();

            // Assert: excess publications fail individually, and the connection remains usable.
            subscribing.get(5, SECONDS);
            assertThat(delivered.poll(5, SECONDS)).isNotNull();
            int refused = 0;
            for (CompletableFuture<PublishResult> publication : publications) {
                Throwable failure = publication.handle((_, cause) -> cause).get(5, SECONDS);
                if (failure != null) {
                    assertThat(failure).isInstanceOf(MqttCapacityException.class);
                    refused++;
                }
            }
            assertThat(refused).isGreaterThanOrEqualTo(7);
            sut.publish(message(new byte[0])).get(5, SECONDS);
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest
    @EnumSource(ApplicationCompletion.class)
    void an_application_completing_one_unsubscribe_call_does_not_affect_later_calls(
            ApplicationCompletion completion
    ) throws Exception {
        // Arrange: the broker has not acknowledged removal.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribe(sut, transport);
            CompletableFuture<Void> first = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));

            // Act
            assertThat(completion.apply.test(first)).isTrue();
            CompletableFuture<Void> second = subscription.unsubscribe();

            // Act & Assert: the next call still waits for the original wire exchange.
            assertThat(second).isNotSameAs(first).isNotDone();
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            second.get(5, SECONDS);
            subscription.unsubscribe().get(5, SECONDS);
            assertThat(transport.sent).isEmpty();
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void overwriting_a_completed_unsubscribe_future_does_not_affect_later_calls() throws Exception {
        // Arrange: removal has already succeeded.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribe(sut, transport);
            CompletableFuture<Void> removing = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            removing.get(5, SECONDS);
            CompletableFuture<Void> completed = subscription.unsubscribe();

            // Act
            completed.obtrudeException(new IllegalStateException("application overwrite"));
            CompletableFuture<Void> repeated = subscription.unsubscribe();

            // Assert
            assertThat(repeated).isNotSameAs(completed);
            repeated.get(5, SECONDS);
            assertThat(transport.sent).isEmpty();
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void an_old_subscription_can_be_disposed_without_removing_its_replacement() throws Exception {
        // Arrange: graceful disconnect ends the clean session but retains the subscription.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport oldTransport = connect(sut);
            Subscription oldSubscription = subscribe(sut, oldTransport);
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));

            // Act: dispose the old handle while disconnected and while reconnecting.
            oldSubscription.close();
            CompletableFuture<Void> removing = oldSubscription.unsubscribe();
            removing.get(5, SECONDS);
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport current = nextTransport();
            oldSubscription.close();
            CompletableFuture<Void> observer = oldSubscription.unsubscribe();
            observer.get(5, SECONDS);

            // Assert: the new connection sends no UNSUBSCRIBE for the old session.
            assertThat(current.sent).isEmpty();

            // Arrange: the new session establishes the same filter with another handler.
            current.accept();
            connection.get(5, SECONDS);
            Subscription subscription = sut.subscribe(request(), received::add);
            establish(current, subscription);

            // Act
            CompletableFuture<Void> first = oldSubscription.unsubscribe();
            CompletableFuture<Void> second = oldSubscription.unsubscribe();
            oldSubscription.close();
            current.receive(publication(new byte[] {7}));

            // Assert: repeated disposal has independent results and leaves new delivery active.
            first.get(5, SECONDS);
            second.get(5, SECONDS);
            assertThat(second).isNotSameAs(first);
            assertThat(requireNonNull(received.poll(5, SECONDS)).message())
                    .isEqualTo(message(new byte[] {7}));
            assertThat(current.sent).isEmpty();
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void connection_loss_confirms_explicit_removal_from_a_clean_session_for_every_observer()
            throws Exception {
        // Arrange: two calls observe one removal that the broker has not acknowledged.
        var failure = new IOException("Peer reset during unsubscribe");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());
            CompletableFuture<Void> first = subscription.unsubscribe();
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Unsubscribe.class);
            CompletableFuture<Void> observer = subscription.unsubscribe();

            // Act
            transport.receiver().onClosed(failure);

            // Assert: losing the clean session confirms removal for both independent callers.
            first.get(5, SECONDS);
            observer.get(5, SECONDS);
            assertThat(observer).isNotSameAs(first);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());

            // Act & Assert: reconnect does not recreate the removed subscription.
            ScriptTransport current = connect(sut);
            assertThat(current.sent).isEmpty();
            assertThat(subscribePull(sut, current, request())).isNotNull();
        }
    }

    @Test
    void an_additional_call_queued_behind_the_acknowledgement_completes_with_the_removal()
            throws Exception {
        // Arrange: hold the actor after the removal was sent.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribe(sut, transport);
            CompletableFuture<Void> first = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            holdActor(transport, entered, release);

            // Act: UNSUBACK precedes the additional call in the actor's FIFO.
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            CompletableFuture<Void> second = subscription.unsubscribe();
            release.countDown();

            // Assert
            first.get(5, SECONDS);
            second.get(5, SECONDS);
            assertThat(transport.sent).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void additional_removal_observations_queued_behind_disconnect_complete_with_the_clean_session()
            throws Exception {
        // Arrange: hold the actor so that disconnect is handled before the removal.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribe(sut, transport);
            holdActor(transport, entered, release);

            // Act
            CompletableFuture<Void> disconnecting = sut.disconnect();
            CompletableFuture<Void> first = subscription.unsubscribe();
            CompletableFuture<Void> second = subscription.unsubscribe();
            release.countDown();

            // Assert
            first.get(5, SECONDS);
            second.get(5, SECONDS);
            disconnecting.get(5, SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void disposal_succeeds_as_soon_as_client_close_is_requested() throws Exception {
        // Arrange: leave removal pending and hold the actor before it can clean up.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribe(sut, transport);
            CompletableFuture<Void> removing = subscription.unsubscribe();
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Unsubscribe.class);
            await().untilAsserted(() -> assertThat(transport.pendingTicket).isZero());
            transport.completeSends = false;
            CompletableFuture<PublishResult> publication = sut.publish(message(new byte[0]));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            holdActor(transport, entered, release);

            // Act
            sut.close();
            CompletableFuture<Void> disposed = subscription.unsubscribe();
            subscription.close();

            // Assert: later disposal succeeds without waiting for actor cleanup.
            assertThat(disposed).isDone();
            disposed.get(5, SECONDS);
            assertThat(removing).isNotDone();
            assertThat(transport.sent).isEmpty();

            // Act
            release.countDown();

            // Assert: closing the nonpersistent session confirms the pending removal, the
            // unacknowledged publication fails, and no additional removal was sent.
            removing.get(5, SECONDS);
            assertThatThrownBy(() -> publication.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            subscription.unsubscribe().get(5, SECONDS);
            assertThat(transport.sent).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Closing with a removal pending is the behavior under test.
    void closing_a_persistent_client_fails_a_pending_removal_the_broker_may_still_hold()
            throws Exception {
        // Arrange: the UNSUBACK has not arrived when the client closes.
        try (MqttClient sut = client(false)) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribe(sut, transport);
            CompletableFuture<Void> removing = subscription.unsubscribe();
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Unsubscribe.class);

            // Act
            sut.close();

            // Assert
            assertThatThrownBy(() -> removing.get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    @SuppressWarnings("try") // Closing with a removal pending is the behavior under test.
    void closing_a_nonpersistent_client_confirms_removals_that_never_reached_the_core()
            throws Exception {
        // Arrange: hold the actor in a connection attempt, so neither the registration nor its
        // removal reaches the core before the client closes.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        beforeActorTime = () -> {
            entered.countDown();
            waitFor(release);
        };
        try (MqttClient sut = client()) {
            CompletableFuture<ConnAck> connection = sut.connect();
            assertThat(entered.await(5, SECONDS)).isTrue();
            Subscription subscription = sut.subscribe(request());
            CompletableFuture<Void> removing = subscription.unsubscribe();

            // Act
            sut.close();
            release.countDown();

            // Assert: closing the nonpersistent session confirms the removal.
            removing.get(5, SECONDS);
            assertThatThrownBy(() -> connection.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class);
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_refusal_after_unsubscribe_ends_the_subscription_as_removed() throws Exception {
        // Arrange: the SUBSCRIBE still awaits its SUBACK when removal is requested.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request());
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            CompletableFuture<Void> removing = subscription.unsubscribe();

            // Act
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.UNSPECIFIED_ERROR),
                    EMPTY
            ));

            // Assert: the requested removal decides the ending; the refusal only confirms it.
            removing.get(5, SECONDS);
            assertThat(subscription.receive(Duration.ofSeconds(5)))
                    .isEqualTo(new ReceiveResult.Ended());
            assertThatThrownBy(() -> subscription.established().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(transport.sent).noneMatch(Packet.Unsubscribe.class::isInstance);
        }
    }

    @Test
    void a_refusal_after_unsubscribe_leaves_the_accepted_delivery_receivable() throws Exception {
        // Arrange: a delivery arrives before SUBACK, and removal is requested while it waits.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request());
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(publication(new byte[] {1}));
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 1);
            CompletableFuture<Void> removing = subscription.unsubscribe();

            // Act
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.UNSPECIFIED_ERROR),
                    EMPTY
            ));

            // Assert: the refusal confirms removal without discarding the accepted delivery.
            removing.get(5, SECONDS);
            assertThat(received(subscription.receive(Duration.ZERO)).message())
                    .isEqualTo(message(new byte[] {1}));
            subscription.terminated().get(5, SECONDS);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
        }
    }

    @Test
    void a_recreation_refused_after_unsubscribe_leaves_the_accepted_delivery_receivable()
            throws Exception {
        // Arrange: re-creation awaits its SUBACK when a delivery arrives and removal is requested.
        var refused = new LinkedBlockingQueue<Subscription>();
        var listener = new ConnectionListener() {
            @Override
            public void onDisconnected(Disconnection disconnection) {
                ends.add(disconnection);
            }

            @Override
            public void onResubscribeFailed(Subscription handle, MqttSubscribeException failure) {
                refused.add(handle);
            }
        };
        try (MqttClient client = client(listener)) {
            var sut = (ClientActor) client;
            ScriptTransport first = connect(sut);
            Subscription subscription = subscribePull(sut, first, request());
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            ScriptTransport current = connect(sut);
            var packet = (Packet.Subscribe) requireNonNull(current.sent.poll(5, SECONDS));
            current.receive(publication(new byte[] {1}));
            await().until(() -> sut.usage().deliveries() == 1);
            CompletableFuture<Void> removing = subscription.unsubscribe();

            // Act
            current.receive(new Packet.SubAck(
                    packet.packetId(),
                    List.of(ReasonCode.UNSPECIFIED_ERROR),
                    EMPTY
            ));

            // Assert: the requested removal decides the ending and keeps the accepted delivery.
            removing.get(5, SECONDS);
            assertThat(received(subscription.receive(Duration.ZERO)).message())
                    .isEqualTo(message(new byte[] {1}));
            subscription.terminated().get(5, SECONDS);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
            assertThat(refused).isEmpty();
        }
    }

    @Test
    @SuppressWarnings("try") // Closing while the refusal's DISCONNECT waits is under test.
    void closing_during_a_session_refusal_ends_subscriptions_as_an_ordinary_close()
            throws Exception {
        // Arrange: a persistent client finds the broker's session again after discarding it,
        // and keeps the refusing DISCONNECT write outstanding.
        try (MqttClient sut = client(false)) {
            Subscription subscription = sut.subscribe(request());
            CompletableFuture<Qos> establishing = subscription.established();
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport transport = discardBrokerSession(nextTransport());
            transport.completeSends = false;
            transport.receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY));

            // Act
            sut.close();

            // Assert
            assertThatThrownBy(() -> establishing.get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(subscription.receive(Duration.ofSeconds(5)))
                    .isEqualTo(new ReceiveResult.Ended());
            assertThatThrownBy(() -> connection.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class);
        }
    }

    @Test
    @SuppressWarnings("try") // Shutdown starts inside the block while the actor is held.
    void unsubscribing_during_shutdown_fixes_the_ending_before_a_later_internal_failure()
            throws Exception {
        // Arrange: hold the actor in an event that fails once released, then close.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request());
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Subscribe.class);
            beforeActorTime = () -> {
                entered.countDown();
                waitFor(release);
                throw new AssertionError("Actor failed");
            };
            transport.receive(new Packet.PingResp());
            assertThat(entered.await(5, SECONDS)).isTrue();
            sut.close();

            // Act
            subscription.unsubscribe().get(5, SECONDS);
            release.countDown();

            // Assert: both observations keep the ordinary ending decided first.
            assertThat(subscription.receive(Duration.ofSeconds(5)))
                    .isEqualTo(new ReceiveResult.Ended());
            assertThatThrownBy(() -> subscription.established().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
        } finally {
            release.countDown();
        }
    }

    @Test
    void an_established_subscription_is_observed_without_a_waiter_slot() throws Exception {
        // Arrange: pending observations of another subscription fill every waiter slot.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription established = subscribe(sut, transport);
            Subscription pending = sut.subscribe(
                    Subscribe.of(new TopicFilter("pending"), Qos.AT_MOST_ONCE),
                    _ -> { }
            );
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Subscribe.class);
            var observers = new ArrayList<CompletableFuture<Qos>>();
            for (int i = 0; i < 1024; i++) {
                observers.add(pending.established());
            }
            assertThat(pending.established()).isCompletedExceptionally();

            // Act
            CompletableFuture<Qos> observed = established.established();

            // Assert: an already established result needs no slot and survives local closure.
            assertThat(observed).isCompletedWithValue(Qos.AT_MOST_ONCE);
            assertThat(((ClientActor) sut).usage().waiters()).isEqualTo(1024);
            established.close();
            assertThat(established.grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);
            assertThat(observers).allMatch(observer -> !observer.isDone());
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void disposal_after_client_close_is_requested_discards_queued_deliveries() throws Exception {
        // Arrange: one delivery runs, another waits in its lane, and the actor is held.
        var handlers = new LinkedBlockingQueue<Thread>();
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), delivery -> {
                delivered.add(delivery);
                handlers.add(Thread.currentThread());
                waitFor(releaseHandler);
            });
            establish(transport, subscription);
            transport.receive(publication(new byte[] {1}));
            Thread handler = requireNonNull(handlers.poll(5, SECONDS));
            transport.receive(publication(new byte[] {2}));
            holdActor(transport, actorHeld, releaseActor);

            // Act
            sut.close();
            subscription.unsubscribe().get(5, SECONDS);
            releaseHandler.countDown();

            // Assert: the lane ends after its running handler instead of the queued delivery.
            assertThat(handler.join(Duration.ofSeconds(5))).isTrue();
            assertThat(delivered).singleElement()
                    .satisfies(delivery -> assertThat(delivery.message())
                            .isEqualTo(message(new byte[] {1})));
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_close_hands_off_no_queued_delivery_after_it_returns() throws Exception {
        // Arrange: one delivery runs and another waits in its lane. The held actor cannot run
        // the shutdown cleanup that stops the lane.
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        var entered = new CountDownLatch(1);
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), delivery -> {
                delivered.add(delivery);
                entered.countDown();
                waitFor(releaseHandler);
            });
            establish(transport, subscription);
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            transport.receive(publication(new byte[] {2}));
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 2);
            holdActor(transport, actorHeld, releaseActor);

            // Act
            sut.close();
            releaseHandler.countDown();

            // Assert: the dispatcher exits after the running handler without taking the queued
            // delivery.
            await().until(() -> ((ClientActor) sut).usage().lanes() == 0);
            assertThat(delivered).singleElement()
                    .satisfies(delivery -> assertThat(delivery.message())
                            .isEqualTo(message(new byte[] {1})));
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_close_ends_receiving_without_handing_off_a_queued_delivery() throws Exception {
        // Arrange: a delivery waits in a pull subscription, and the actor is held.
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());
            transport.receive(publication(new byte[] {1}));
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 1);
            holdActor(transport, actorHeld, releaseActor);

            // Act
            sut.close();

            // Assert: receiving ends at once, and the delivery waits for the actor to discard it.
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
            assertThat(((ClientActor) sut).usage().deliveries()).isEqualTo(1);
        } finally {
            releaseActor.countDown();
        }
    }

    @Test
    void subscription_termination_waits_for_the_running_handler() throws Exception {
        // Arrange: the subscription closes while its handler runs.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), _ -> {
                entered.countDown();
                waitFor(release);
            });
            establish(transport, subscription);
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            subscription.close();
            CompletableFuture<Void> terminated = subscription.terminated();
            CompletableFuture<Void> cancelled = subscription.terminated();
            cancelled.cancel(false);
            assertThat(terminated).isNotDone();

            // Act
            release.countDown();

            // Assert: each call observes on its own; a later call finds termination complete.
            terminated.get(5, SECONDS);
            assertThat(terminated).isNotSameAs(cancelled);
            assertThat(subscription.terminated()).isCompleted();
        } finally {
            release.countDown();
        }
    }

    @Test
    void unsubscribe_runs_the_deliveries_it_accepted_after_removal_is_confirmed()
            throws Exception {
        // Arrange: one delivery runs and another waits when removal is requested.
        var handled = new LinkedBlockingQueue<Byte>();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), delivery -> {
                handled.add(delivery.message().payload()[0]);
                entered.countDown();
                waitFor(release);
            });
            establish(transport, subscription);
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            transport.receive(publication(new byte[] {2}));
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 2);

            // Act: a later publication arrives, and UNSUBACK confirms removal while the first
            // handler still runs.
            CompletableFuture<Void> removing = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(publication(new byte[] {3}));
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            removing.get(5, SECONDS);
            CompletableFuture<Void> terminated = subscription.terminated();
            assertThat(terminated).isNotDone();
            release.countDown();

            // Assert: the accepted delivery still runs; the later one never reaches the handler.
            terminated.get(5, SECONDS);
            assertThat(handled).containsExactly((byte) 1, (byte) 2);
        } finally {
            release.countDown();
        }
    }

    @Test
    void unsubscribe_leaves_accepted_deliveries_receivable_after_removal_is_confirmed()
            throws Exception {
        // Arrange: two deliveries wait in a pull subscription when removal is requested.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());
            transport.receive(publication(new byte[] {1}));
            transport.receive(publication(new byte[] {2}));
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 2);

            // Act: removal is confirmed before either delivery is received.
            CompletableFuture<Void> removing = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            removing.get(5, SECONDS);
            CompletableFuture<Void> terminated = subscription.terminated();

            // Assert: both stay receivable; receiving the last one ends the subscription.
            assertThat(received(subscription.receive(Duration.ZERO)).message())
                    .isEqualTo(message(new byte[] {1}));
            assertThat(terminated).isNotDone();
            assertThat(received(subscription.receive(Duration.ZERO)).message())
                    .isEqualTo(message(new byte[] {2}));
            terminated.get(5, SECONDS);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
        }
    }

    @Test
    void close_discards_the_deliveries_an_unsubscribe_accepted() throws Exception {
        // Arrange: removal is confirmed while one delivery runs and another waits.
        var handled = new LinkedBlockingQueue<Byte>();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), delivery -> {
                handled.add(delivery.message().payload()[0]);
                entered.countDown();
                waitFor(release);
            });
            establish(transport, subscription);
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            transport.receive(publication(new byte[] {2}));
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 2);
            CompletableFuture<Void> removing = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            removing.get(5, SECONDS);

            // Act
            subscription.close();
            release.countDown();

            // Assert: only the delivery handed off before the close runs.
            subscription.terminated().get(5, SECONDS);
            assertThat(handled).containsExactly((byte) 1);
            assertThat(((ClientActor) sut).usage().deliveries()).isZero();
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void subscriptions_terminate_when_the_client_closes() throws Exception {
        // Arrange: push and pull subscriptions are established and idle.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription push = subscribe(sut, transport);
            Subscription pull = subscribePull(
                    sut,
                    transport,
                    Subscribe.of(new TopicFilter("pull"), Qos.AT_MOST_ONCE)
            );
            CompletableFuture<Void> pushTerminated = push.terminated();
            CompletableFuture<Void> pullTerminated = pull.terminated();

            // Act
            sut.close();

            // Assert
            pushTerminated.get(5, SECONDS);
            pullTerminated.get(5, SECONDS);
        }
    }

    @Test
    void termination_observations_share_the_client_wide_waiter_limit() throws Exception {
        // Arrange: pending establishment observations fill every waiter slot.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), _ -> { });
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Subscribe.class);
            for (int i = 0; i < 1024; i++) {
                subscription.established();
            }

            // Act
            CompletableFuture<Void> refused = subscription.terminated();

            // Assert: the refusal is immediate, and termination itself still completes later,
            // once closing has failed the establishment observations and returned their slots.
            assertThatThrownBy(refused::join).hasCauseInstanceOf(MqttCapacityException.class);
            subscription.close();
            await().until(() -> ((ClientActor) sut).usage().waiters() == 0);
            subscription.terminated().get(5, SECONDS);
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_waits_for_a_running_handler() throws Exception {
        // Arrange: a handler still runs when the client closes.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), _ -> {
                entered.countDown();
                waitFor(release);
            });
            establish(transport, subscription);
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            CompletableFuture<Void> terminated = sut.terminated();

            // Act
            sut.close();

            // Assert: everything else ends, and the handler alone keeps termination pending.
            awaitTerminationPending(actor, ClientTermination.Pending.LANES);
            assertThat(terminated).isNotDone();
            release.countDown();
            terminated.get(5, SECONDS);
            assertThat(actor.terminationPending()).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_waits_for_the_disconnection_notice_that_closing_produces()
            throws Exception {
        // Arrange: the listener blocks in the notice of the connection that closing ends.
        var notices = new LinkedBlockingQueue<String>();
        var release = new CountDownLatch(1);
        try (MqttClient sut = client(blockingListener(notices, release))) {
            var actor = (ClientActor) sut;
            connect(sut);
            CompletableFuture<Void> terminated = sut.terminated();

            // Act
            sut.close();

            // Assert
            assertThat(notices.poll(5, SECONDS)).isEqualTo("disconnected");
            awaitTerminationPending(actor, ClientTermination.Pending.LISTENER);
            assertThat(terminated).isNotDone();
            release.countDown();
            terminated.get(5, SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_waits_for_the_attempt_failure_notice_that_closing_produces()
            throws Exception {
        // Arrange: the listener blocks in the notice of the attempt that closing ends.
        var notices = new LinkedBlockingQueue<String>();
        var release = new CountDownLatch(1);
        try (MqttClient sut = client(blockingListener(notices, release))) {
            var actor = (ClientActor) sut;
            CompletableFuture<ConnAck> connection = sut.connect();
            nextTransport();
            CompletableFuture<Void> terminated = sut.terminated();

            // Act
            sut.close();

            // Assert
            assertThatThrownBy(() -> connection.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class);
            assertThat(notices.poll(5, SECONDS)).isEqualTo("connect failed");
            awaitTerminationPending(actor, ClientTermination.Pending.LISTENER);
            assertThat(terminated).isNotDone();
            release.countDown();
            terminated.get(5, SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_waits_for_a_supplier_that_ignores_interruption() throws Exception {
        // Arrange: the supplier is still evaluating when the client closes.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = clientWithCredentials(() -> {
            entered.countDown();
            awaitIgnoringInterrupts(release);
            return MqttCredentials.of("late");
        })) {
            var actor = (ClientActor) sut;
            CompletableFuture<ConnAck> connection = sut.connect();
            assertThat(entered.await(5, SECONDS)).isTrue();
            CompletableFuture<Void> terminated = sut.terminated();

            // Act: closing interrupts the supplier, which keeps waiting.
            sut.close();

            // Assert
            assertThatThrownBy(() -> connection.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class);
            awaitTerminationPending(actor, ClientTermination.Pending.CREDENTIALS);
            assertThat(terminated).isNotDone();
            release.countDown();
            terminated.get(5, SECONDS);
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest(name = "operation returns last: {0}")
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_waits_for_continuations_of_the_futures_it_completes(
            boolean operationLast
    ) throws Exception {
        // Arrange: a connection attempt and a connection observation each carry a continuation
        // that blocks once closing completes them.
        var releaseOperation = new CountDownLatch(1);
        var releaseObservation = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            CompletableFuture<ConnAck> operation = sut.connect()
                    .whenComplete((_, _) -> waitFor(releaseOperation));
            nextTransport();
            CompletableFuture<ConnAck> observation = sut.whenConnected()
                    .whenComplete((_, _) -> waitFor(releaseObservation));
            CompletableFuture<Void> terminated = sut.terminated();
            CountDownLatch first = operationLast ? releaseObservation : releaseOperation;
            CompletableFuture<ConnAck> firstCompleted = operationLast ? observation : operation;
            CountDownLatch last = operationLast ? releaseOperation : releaseObservation;

            // Act
            sut.close();

            // Assert: either continuation alone keeps termination pending, and whichever returns
            // last completes it.
            awaitTerminationPending(actor, ClientTermination.Pending.COMPLETIONS);
            first.countDown();
            assertThatThrownBy(() -> firstCompleted.get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class);
            assertThat(actor.terminationPending())
                    .containsExactly(ClientTermination.Pending.COMPLETIONS);
            assertThat(terminated).isNotDone();
            last.countDown();
            terminated.get(5, SECONDS);
        } finally {
            releaseOperation.countDown();
            releaseObservation.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_waits_for_the_transport_to_finish_closing() throws Exception {
        // Arrange: the connection's transport is slow to close.
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            transport.deferCloseReport = true;
            CompletableFuture<Void> terminated = sut.terminated();

            // Act
            sut.close();

            // Assert
            awaitTerminationPending(actor, ClientTermination.Pending.TRANSPORTS);
            assertThat(terminated).isNotDone();
            transport.finishClose();
            terminated.get(5, SECONDS);
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_observations_are_independent_and_share_the_waiter_limit()
            throws Exception {
        // Arrange: pending establishment observations fill every waiter slot.
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), _ -> { });
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Subscribe.class);
            for (int i = 0; i < 1024; i++) {
                subscription.established();
            }

            // Act
            CompletableFuture<Void> refused = sut.terminated();

            // Assert: the refusal is immediate. Observations made before close are independent,
            // and once the client has terminated a later call finds it complete.
            assertThatThrownBy(refused::join).hasCauseInstanceOf(MqttCapacityException.class);
            subscription.close();
            await().until(() -> actor.usage().waiters() == 0);
            CompletableFuture<Void> first = sut.terminated();
            CompletableFuture<Void> cancelled = sut.terminated();
            cancelled.cancel(false);
            sut.close();
            first.get(5, SECONDS);
            assertThat(first).isNotSameAs(cancelled);
            assertThat(sut.terminated()).isCompleted();
            await().until(() -> actor.usage().waiters() == 0);
        }
    }

    @Test
    void an_executor_runs_each_handler_in_receive_order_with_one_task_at_a_time()
            throws Exception {
        // Arrange: three QoS 1 publications wait for an executor that runs tasks only when told.
        var executor = new ManualExecutor();
        var handled = new LinkedBlockingQueue<Byte>();
        var threads = new LinkedBlockingQueue<Thread>();
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOn(sut, transport, qosOneRequest(), executor, delivery -> {
                handled.add(delivery.message().payload()[0]);
                threads.add(Thread.currentThread());
            });
            for (int packetId = 1; packetId <= 3; packetId++) {
                transport.receive(qosOnePublication("messages", packetId, false));
            }
            await().until(() -> actor.placedDeliveries() == 3);
            assertThat(transport.sent).isEmpty();

            // Act: run what the executor holds, one task at a time.
            var held = new ArrayList<Integer>();
            var acknowledged = new ArrayList<Packet>();
            for (int i = 0; i < 3; i++) {
                executor.runNext();
                held.add(executor.held());
                acknowledged.add(requireNonNull(transport.sent.poll(5, SECONDS)));
            }

            // Assert: each task ran one handler on the executor's thread and asked again while
            // deliveries waited; each PUBACK followed its handler, and no task is left.
            assertThat(held).containsExactly(1, 1, 0);
            assertThat(handled).containsExactly((byte) 1, (byte) 2, (byte) 3);
            assertThat(threads).containsOnly(Thread.currentThread());
            assertThat(acknowledged).containsExactly(
                    acknowledgement(Qos.AT_LEAST_ONCE, 1),
                    acknowledgement(Qos.AT_LEAST_ONCE, 2),
                    acknowledgement(Qos.AT_LEAST_ONCE, 3)
            );
        }
    }

    @Test
    void an_executor_that_throws_fails_the_delivery_its_task_would_have_run() throws Exception {
        // Arrange: the executor throws an exception that is not a rejection.
        var executor = new ManualExecutor();
        var failure = new IllegalStateException("Executor is unavailable");
        executor.refuse(failure);
        var handled = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            subscribeOn(sut, transport, qosOneRequest(), executor, handled::add);

            // Act
            transport.receive(qosOnePublication("messages", 1, false));

            // Assert: as with a failing handler, the failure ends the connection and the message
            // is not acknowledged.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(transport.closed).isTrue();
            assertThat(transport.sent).isEmpty();
            assertThat(handled).isEmpty();
        }
    }

    @Test
    void a_synchronous_executor_keeps_the_order_without_deepening_the_stack() throws Exception {
        // Arrange: deliveries wait behind one held task; the executor then runs each later task
        // on the thread that submits it, as a caller-runs policy does when its pool is busy.
        var executor = new ManualExecutor();
        var handled = new LinkedBlockingQueue<Byte>();
        var depths = new LinkedBlockingQueue<Long>();
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOn(sut, transport, qosOneRequest(), executor, delivery -> {
                handled.add(delivery.message().payload()[0]);
                depths.add(StackWalker.getInstance().walk(Stream::count));
            });
            for (int packetId = 1; packetId <= 50; packetId++) {
                transport.receive(qosOnePublication("messages", packetId, false));
            }
            await().until(() -> actor.placedDeliveries() == 50);
            executor.runDirectly();

            // Act
            executor.runNext();

            // Assert: the one task ran every delivery in order at one stack depth, asking the
            // executor again for each, and each slot returned with its PUBACK.
            assertThat(handled).containsExactlyElementsOf(
                    IntStream.rangeClosed(1, 50).mapToObj(value -> (byte) value).toList()
            );
            assertThat(Set.copyOf(depths)).hasSize(1);
            assertThat(executor.executions()).isEqualTo(50);
            for (int packetId = 1; packetId <= 50; packetId++) {
                assertThat(transport.sent.poll(5, SECONDS))
                        .isEqualTo(acknowledgement(Qos.AT_LEAST_ONCE, packetId));
            }
            await().until(() -> actor.usage().deliveries() == 0);
        }
    }

    @Test
    void closing_a_subscription_returns_the_slots_of_deliveries_whose_task_never_runs()
            throws Exception {
        // Arrange: three QoS 1 deliveries wait for a task that the executor hands back unrun.
        var executor = new ManualExecutor();
        var handled = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribeOn(
                    sut,
                    transport,
                    qosOneRequest(),
                    executor,
                    handled::add
            );
            for (int packetId = 1; packetId <= 3; packetId++) {
                transport.receive(qosOnePublication("messages", packetId, false));
            }
            await().until(() -> actor.placedDeliveries() == 3);
            List<Runnable> unrun = executor.shutdownNow();

            // Act
            subscription.close();

            // Assert: termination does not wait for the task. The discarded deliveries return
            // their slots and are acknowledged, and the task does nothing when run late.
            subscription.terminated().get(5, SECONDS);
            var sent = new ArrayList<Packet>();
            for (int i = 0; i < 4; i++) {
                sent.add(requireNonNull(transport.sent.poll(5, SECONDS)));
            }
            assertThat(sent).filteredOn(Packet.PubAck.class::isInstance).containsExactly(
                    acknowledgement(Qos.AT_LEAST_ONCE, 1),
                    acknowledgement(Qos.AT_LEAST_ONCE, 2),
                    acknowledgement(Qos.AT_LEAST_ONCE, 3)
            );
            assertThat(sent).filteredOn(Packet.Unsubscribe.class::isInstance).hasSize(1);
            await().until(() -> actor.usage().deliveries() == 0);
            assertThat(unrun).hasSize(1);
            unrun.getFirst().run();
            assertThat(handled).isEmpty();
            assertThat(executor.executions()).isEqualTo(1);
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void a_task_held_when_the_client_closes_does_nothing_and_termination_does_not_wait_for_it()
            throws Exception {
        // Arrange: a delivery waits for a task that the executor still holds.
        var executor = new ManualExecutor();
        var handled = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            subscribeOn(sut, transport, request(), executor, handled::add);
            transport.receive(publication(new byte[] {1}));
            await().until(() -> executor.held() == 1);

            // Act
            sut.close();

            // Assert: the client terminates first; the task run afterwards calls no handler and
            // asks the executor for nothing.
            sut.terminated().get(5, SECONDS);
            executor.runNext();
            assertThat(handled).isEmpty();
            assertThat(executor.executions()).isEqualTo(1);
        }
    }

    @Test
    void a_handler_failure_on_an_executor_holds_the_rest_of_the_subscription() throws Exception {
        // Arrange: two QoS 1 deliveries wait, and the first handler fails.
        var executor = new ManualExecutor();
        var failure = new IllegalStateException("handler failed");
        var handled = new LinkedBlockingQueue<Byte>();
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOn(sut, transport, qosOneRequest(), executor, delivery -> {
                handled.add(delivery.message().payload()[0]);
                throw failure;
            });
            transport.receive(qosOnePublication("messages", 1, false));
            transport.receive(qosOnePublication("messages", 2, false));
            await().until(() -> actor.placedDeliveries() == 2);

            // Act
            executor.runNext();

            // Assert: the task asks for nothing more, the failure ends the connection, and the
            // waiting delivery reaches neither the handler nor an acknowledgement.
            assertThat(executor.held()).isZero();
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(handled).containsExactly((byte) 1);
            assertThat(transport.sent).isEmpty();
            assertThat(executor.executions()).isEqualTo(1);
        }
    }

    @Test
    void on_receive_acknowledges_a_delivery_while_its_handler_runs_on_the_executor()
            throws Exception {
        // Arrange: a QoS 1 delivery waits for its task, and its handler waits once started.
        var executor = new ManualExecutor();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOn(
                    sut,
                    transport,
                    qosOneRequest().withAckMode(AckMode.ON_RECEIVE),
                    executor,
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                    }
            );
            transport.receive(qosOnePublication("messages", 1, false));
            await().until(() -> actor.placedDeliveries() == 1);
            assertThat(transport.sent).isEmpty();

            // Act
            Thread.ofVirtual().start(executor::runNext);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Assert: waiting for the executor acknowledged nothing; starting the handler did.
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(acknowledgement(Qos.AT_LEAST_ONCE, 1));
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_manual_delivery_handled_on_an_executor_is_acknowledged_by_the_application()
            throws Exception {
        // Arrange: the handler keeps its QoS 1 delivery without acknowledging it.
        var executor = new ManualExecutor();
        var kept = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOn(
                    sut,
                    transport,
                    qosOneRequest().withAckMode(AckMode.MANUAL),
                    executor,
                    kept::add
            );
            transport.receive(qosOnePublication("messages", 1, false));
            await().until(() -> actor.placedDeliveries() == 1);
            executor.runNext();
            MqttDelivery delivery = requireNonNull(kept.poll(5, SECONDS));
            assertThat(transport.sent).isEmpty();

            // Act
            delivery.ack();

            // Assert
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(acknowledgement(Qos.AT_LEAST_ONCE, 1));
        }
    }

    @Test
    void shutdown_work_that_waits_for_the_executor_runs_once_the_subscription_terminates()
            throws Exception {
        // Arrange: the subscription closes while its handler runs on a single-thread executor,
        // and the shutdown work waits for that executor.
        ExecutorService lane = Executors.newSingleThreadExecutor();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribeOn(sut, transport, request(), lane, _ -> {
                entered.countDown();
                waitFor(release);
            });
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            CompletableFuture<Void> stopped = subscription.terminated()
                    .thenRun(() -> CompletableFuture.runAsync(() -> { }, lane).join());
            subscription.close();

            // Act: the handler returns, which ends the subscription on the executor's thread.
            release.countDown();

            // Assert: the work completes, so the termination did not hold the executor.
            stopped.get(5, SECONDS);
        } finally {
            release.countDown();
            lane.shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_waits_for_a_handler_running_on_an_executor() throws Exception {
        // Arrange: a handler still runs on the executor when the client closes.
        var executor = new ManualExecutor();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOn(sut, transport, request(), executor, _ -> {
                entered.countDown();
                waitFor(release);
            });
            transport.receive(publication(new byte[] {1}));
            Thread.ofVirtual().start(executor::runNext);
            assertThat(entered.await(5, SECONDS)).isTrue();
            CompletableFuture<Void> terminated = sut.terminated();

            // Act
            sut.close();

            // Assert: everything else ends, and the handler alone keeps termination pending.
            awaitTerminationPending(actor, ClientTermination.Pending.LANES);
            assertThat(terminated).isNotDone();
            release.countDown();
            terminated.get(5, SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_waits_for_an_executor_taking_the_first_task() throws Exception {
        // Arrange: the executor keeps the subscription's first request inside execute().
        var executor = new ManualExecutor();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        executor.onExecute = () -> {
            entered.countDown();
            waitFor(release);
        };
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOn(sut, transport, request(), executor, _ -> { });
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            CompletableFuture<Void> terminated = sut.terminated();

            // Act
            sut.close();

            // Assert: everything else ends, and the executor call alone keeps termination
            // pending.
            awaitTerminationPending(actor, ClientTermination.Pending.LANES);
            assertThat(terminated).isNotDone();
            release.countDown();
            terminated.get(5, SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_waits_for_an_executor_that_a_running_task_asks_again()
            throws Exception {
        // Arrange: a task runs the first of two deliveries, and the executor keeps it inside
        // execute() when it asks again for the second.
        var executor = new ManualExecutor();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOn(sut, transport, request(), executor, _ -> { });
            transport.receive(publication(new byte[] {1}));
            transport.receive(publication(new byte[] {2}));
            await().until(() -> actor.placedDeliveries() == 2);
            executor.onExecute = () -> {
                entered.countDown();
                waitFor(release);
            };
            Thread.ofVirtual().start(executor::runNext);
            assertThat(entered.await(5, SECONDS)).isTrue();
            CompletableFuture<Void> terminated = sut.terminated();

            // Act
            sut.close();

            // Assert: everything else ends, and the executor call alone keeps termination
            // pending.
            awaitTerminationPending(actor, ClientTermination.Pending.LANES);
            assertThat(terminated).isNotDone();
            release.countDown();
            terminated.get(5, SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test
    void lanes_keep_each_lane_in_receive_order_and_let_lanes_run_independently()
            throws Exception {
        // Arrange: odd payloads go to lane a and even ones to lane b; four deliveries wait.
        var a = new ManualExecutor();
        var b = new ManualExecutor();
        var selected = new LinkedBlockingQueue<Byte>();
        var handled = new LinkedBlockingQueue<Byte>();
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOnLanes(
                    sut,
                    transport,
                    request(),
                    message -> {
                        selected.add(message.payload()[0]);
                        return message.payload()[0] % 2 == 1 ? a : b;
                    },
                    delivery -> handled.add(delivery.message().payload()[0])
            );
            for (byte value = 1; value <= 4; value++) {
                transport.receive(publication(new byte[] {value}));
            }
            await().until(() -> actor.placedDeliveries() == 4);
            assertThat(a.held()).isEqualTo(1);
            assertThat(b.held()).isEqualTo(1);

            // Act: lane b runs to the end before lane a starts.
            b.runNext();
            b.runNext();
            a.runNext();
            a.runNext();

            // Assert: the selector saw each delivery once, in receive order. Each lane kept its
            // own order with one task at a time, independently of the other lane.
            assertThat(selected).containsExactly((byte) 1, (byte) 2, (byte) 3, (byte) 4);
            assertThat(handled).containsExactly((byte) 2, (byte) 4, (byte) 1, (byte) 3);
            assertThat(a.executions()).isEqualTo(2);
            assertThat(b.executions()).isEqualTo(2);
            assertThat(a.held()).isZero();
            assertThat(b.held()).isZero();
        }
    }

    @Test
    void a_lane_selector_that_throws_fails_its_delivery_as_a_handler_failure() throws Exception {
        // Arrange
        var lane = new ManualExecutor();
        var failure = new IllegalStateException("No lane for this message");
        var handled = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            subscribeOnLanes(
                    sut,
                    transport,
                    qosOneRequest(),
                    _ -> {
                        throw failure;
                    },
                    handled::add
            );

            // Act
            transport.receive(qosOnePublication("messages", 1, false));

            // Assert: as with a failing handler, the failure ends the connection and the message
            // is not acknowledged.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(transport.sent).isEmpty();
            assertThat(handled).isEmpty();
            assertThat(lane.executions()).isZero();
        }
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the selector's non-null contract.
    void a_lane_selector_that_returns_null_fails_its_delivery_as_a_handler_failure()
            throws Exception {
        // Arrange
        var handled = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            subscribeOnLanes(sut, transport, qosOneRequest(), _ -> null, handled::add);

            // Act
            transport.receive(qosOnePublication("messages", 1, false));

            // Assert: the null result fails the delivery as an exception would.
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            assertThat(end.cause().orElseThrow()).isInstanceOf(NullPointerException.class);
            assertThat(transport.sent).isEmpty();
            assertThat(handled).isEmpty();
        }
    }

    @Test
    void a_handler_failure_on_one_lane_holds_the_other_lanes() throws Exception {
        // Arrange: QoS 1 deliveries 1 and 2 wait on lanes a and b; the handler fails for 1.
        var a = new ManualExecutor();
        var b = new ManualExecutor();
        var failure = new IllegalStateException("handler failed");
        var handled = new LinkedBlockingQueue<Byte>();
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOnLanes(
                    sut,
                    transport,
                    qosOneRequest(),
                    message -> message.payload()[0] == 1 ? a : b,
                    delivery -> {
                        handled.add(delivery.message().payload()[0]);
                        if (delivery.message().payload()[0] == 1) {
                            throw failure;
                        }
                    }
            );
            transport.receive(qosOnePublication("messages", 1, false));
            transport.receive(qosOnePublication("messages", 2, false));
            await().until(() -> actor.placedDeliveries() == 2);

            // Act: lane a fails first, and lane b's task runs afterwards.
            a.runNext();
            b.runNext();

            // Assert: the failure ends the connection, and lane b runs nothing.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(handled).containsExactly((byte) 1);
            assertThat(transport.sent).isEmpty();
        }
    }

    @Test
    void unsubscribe_runs_what_each_lane_accepted_before_the_subscription_terminates()
            throws Exception {
        // Arrange: deliveries 1 and 3 wait on lane a and 2 on lane b when removal is confirmed.
        var a = new ManualExecutor();
        var b = new ManualExecutor();
        var handled = new LinkedBlockingQueue<Byte>();
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribeOnLanes(
                    sut,
                    transport,
                    request(),
                    message -> message.payload()[0] % 2 == 1 ? a : b,
                    delivery -> handled.add(delivery.message().payload()[0])
            );
            for (byte value = 1; value <= 3; value++) {
                transport.receive(publication(new byte[] {value}));
            }
            await().until(() -> actor.placedDeliveries() == 3);
            CompletableFuture<Void> removing = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            removing.get(5, SECONDS);
            CompletableFuture<Void> terminated = subscription.terminated();

            // Act & Assert: each lane runs what it accepted, in order, and the subscription
            // terminates only after the last of them.
            a.runNext();
            b.runNext();
            assertThat(terminated).isNotDone();
            a.runNext();
            terminated.get(5, SECONDS);
            assertThat(handled).containsExactly((byte) 1, (byte) 2, (byte) 3);
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource also closes on failure.
    void client_termination_waits_for_a_lane_selector_still_choosing() throws Exception {
        // Arrange: the selector keeps the dispatcher while it chooses a lane.
        var lane = new ManualExecutor();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOnLanes(
                    sut,
                    transport,
                    request(),
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                        return lane;
                    },
                    _ -> { }
            );
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            CompletableFuture<Void> terminated = sut.terminated();

            // Act
            sut.close();

            // Assert: everything else ends, and the selector alone keeps termination pending.
            // Its answer, given after the close, starts nothing on the executor.
            awaitTerminationPending(actor, ClientTermination.Pending.LANES);
            assertThat(terminated).isNotDone();
            release.countDown();
            terminated.get(5, SECONDS);
            assertThat(lane.executions()).isZero();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_qos_0_handler_failure_is_skipped_and_the_subscription_continues() throws Exception {
        // Arrange: the default policy, and a handler that fails for the first delivery.
        var failure = new IllegalStateException("handler failed");
        var handled = new LinkedBlockingQueue<Byte>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), delivery -> {
                handled.add(delivery.message().payload()[0]);
                if (delivery.message().payload()[0] == 1) {
                    throw failure;
                }
            });
            establish(transport, subscription);

            // Act
            transport.receive(publication(new byte[] {1}));
            transport.receive(publication(new byte[] {2}));

            // Assert: no acknowledgement is held back for the failure to protect, so the
            // subscription and the connection go on.
            await().until(() -> handled.size() == 2);
            assertThat(handled).containsExactly((byte) 1, (byte) 2);
            assertThat(((ClientActor) sut).reportedHandlerFailures()).isEqualTo(1);
            assertThat(ends).isEmpty();
            assertThat(transport.closed).isFalse();
        }
    }

    @Test
    void an_on_receive_handler_failure_keeps_its_acknowledgement() throws Exception {
        // Arrange: ON_RECEIVE permits the acknowledgement before the failing handler starts.
        var failure = new IllegalStateException("handler failed");
        var handled = new LinkedBlockingQueue<Byte>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    qosOneRequest().withAckMode(AckMode.ON_RECEIVE),
                    delivery -> {
                        if (delivery.message().payload()[0] == 1) {
                            throw failure;
                        }
                        handled.add(delivery.message().payload()[0]);
                    }
            );
            establish(transport, subscription);

            // Act
            transport.receive(qosOnePublication("messages", 1, false));
            transport.receive(qosOnePublication("messages", 2, false));

            // Assert: the acknowledgement stands, and the next delivery runs on the same
            // connection.
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(acknowledgement(Qos.AT_LEAST_ONCE, 1));
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(acknowledgement(Qos.AT_LEAST_ONCE, 2));
            assertThat(handled.poll(5, SECONDS)).isEqualTo((byte) 2);
            assertThat(ends).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_LEAST_ONCE", "EXACTLY_ONCE"})
    void skip_acknowledges_a_failed_delivery_without_processing_and_continues(Qos qos)
            throws Exception {
        // Arrange: a skipping subscription whose handler fails for the first delivery.
        var failure = new IllegalStateException("handler failed");
        var handled = new LinkedBlockingQueue<Byte>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), qos)
                            .withFailurePolicy(FailurePolicy.SKIP),
                    delivery -> {
                        if (delivery.message().payload()[0] == 7) {
                            throw failure;
                        }
                        handled.add(delivery.message().payload()[0]);
                    }
            );
            establish(transport, subscription);

            // Act
            transport.receive(acknowledgedPublication("messages", 7, qos, false));
            transport.receive(acknowledgedPublication("messages", 8, qos, false));

            // Assert: the failed delivery is acknowledged as if handled, in receive order, and
            // the next one runs and is acknowledged on the same connection.
            assertThat(transport.sent.poll(5, SECONDS)).isEqualTo(acknowledgement(qos, 7));
            assertThat(transport.sent.poll(5, SECONDS)).isEqualTo(acknowledgement(qos, 8));
            assertThat(handled).containsExactly((byte) 8);
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void a_refused_task_stops_a_skipping_subscription_instead_of_acknowledging_it()
            throws Exception {
        // Arrange: the subscription skips handler failures, but its executor refuses work.
        var executor = new ManualExecutor();
        var refusal = new RejectedExecutionException("Executor has shut down");
        executor.refuse(refusal);
        var handled = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            subscribeOn(
                    sut,
                    transport,
                    qosOneRequest().withFailurePolicy(FailurePolicy.SKIP),
                    executor,
                    handled::add
            );

            // Act
            transport.receive(qosOnePublication("messages", 1, false));

            // Assert: a refusing executor processes nothing, so the delivery is not
            // acknowledged, and the refusal stops the client as under STOP.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(refusal, false, 1));
            assertThat(transport.sent).isEmpty();
            assertThat(handled).isEmpty();
        }
    }

    @Test
    void skip_acknowledges_a_failed_delivery_on_one_lane_while_the_other_lanes_go_on()
            throws Exception {
        // Arrange: QoS 1 deliveries 1 and 2 wait on lanes a and b of a skipping subscription,
        // and the handler fails for 1.
        var a = new ManualExecutor();
        var b = new ManualExecutor();
        var failure = new IllegalStateException("handler failed");
        var handled = new LinkedBlockingQueue<Byte>();
        try (MqttClient sut = client()) {
            var actor = (ClientActor) sut;
            ScriptTransport transport = connect(sut);
            subscribeOnLanes(
                    sut,
                    transport,
                    qosOneRequest().withFailurePolicy(FailurePolicy.SKIP),
                    message -> message.payload()[0] == 1 ? a : b,
                    delivery -> {
                        if (delivery.message().payload()[0] == 1) {
                            throw failure;
                        }
                        handled.add(delivery.message().payload()[0]);
                    }
            );
            transport.receive(qosOnePublication("messages", 1, false));
            transport.receive(qosOnePublication("messages", 2, false));
            await().until(() -> actor.placedDeliveries() == 2);

            // Act
            a.runNext();
            b.runNext();

            // Assert: the failed delivery is acknowledged unprocessed, and the other lane runs.
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(acknowledgement(Qos.AT_LEAST_ONCE, 1));
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(acknowledgement(Qos.AT_LEAST_ONCE, 2));
            assertThat(handled).containsExactly((byte) 2);
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void close_starts_one_removal_without_waiting_or_retaining_operation_slots()
            throws Exception {
        // Arrange
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), received::add);
            CompletableFuture<Qos> subscribing = subscription.established();
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            subscribing.get(5, SECONDS);

            // Act: every call returns before the broker acknowledges removal.
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                for (int i = 0; i < 64; i++) {
                    subscription.close();
                }
            });

            // Assert: one removal stops delivery, and repeated calls retain no operation slot.
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(publication(new byte[] {1}));
            sut.publish(message(new byte[0])).get(5, SECONDS);
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            sut.subscribe(request(), _ -> { });
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Subscribe.class);
            assertThat(received).isEmpty();
            assertThat(ends).isEmpty();
        }
    }

    @Test
    void a_refused_additional_call_keeps_delivery_stopped_for_the_pending_removal()
            throws Exception {
        // Arrange: hold the actor with an incoming publication queued ahead of the removal.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), received::add);
            establish(transport, subscription);
            holdActor(transport, entered, release);
            transport.receive(publication(new byte[] {1}));
            CompletableFuture<Void> removing = subscription.unsubscribe();

            // Act: additional calls take the remaining observation slots until one is refused.
            CompletableFuture<Void> refused = subscription.unsubscribe();
            for (int i = 0; i < 1024 && !refused.isCompletedExceptionally(); i++) {
                refused = subscription.unsubscribe();
            }
            release.countDown();

            // Assert: the core handles the queued publication while the lane stays paused.
            assertThat(refused).isCompletedExceptionally();
            assertThatThrownBy(refused::join).hasCauseInstanceOf(MqttCapacityException.class);
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            removing.get(5, SECONDS);
            assertThat(received).isEmpty();
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void unsubscribe_observers_have_independent_completion_threads_and_bounded_admission()
            throws Exception {
        // Arrange: repeated calls wait for the same unacknowledged removal.
        var release = new CountDownLatch(1);
        var entered = new AtomicInteger();
        var continuations = new ArrayList<CompletableFuture<Void>>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), _ -> { });
            establish(transport, subscription);
            await().until(() -> ((ClientActor) sut).usage().waiters() == 0);
            CompletableFuture<Void> first = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            var observers = new ArrayList<CompletableFuture<Void>>();
            observers.add(first);
            for (int i = 1; i < 1024; i++) {
                observers.add(subscription.unsubscribe());
            }
            assertThat(observers).allSatisfy(observer -> assertThat(observer).isNotDone());
            for (CompletableFuture<Void> observer : observers) {
                continuations.add(observer.thenRun(() -> {
                    entered.incrementAndGet();
                    waitFor(release);
                }));
            }

            // Act & Assert: no further call is admitted while all observation slots are reserved.
            assertThatThrownBy(() -> subscription.unsubscribe().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);

            // Act: acknowledge removal while every completion callback blocks.
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));

            // Assert: every callback starts independently and retains its own observation slot.
            await().untilAtomic(entered, is(1024));
            sut.publish(message(new byte[0])).get(5, SECONDS);
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            assertThat(((ClientActor) sut).usage().waiters()).isEqualTo(1024);
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
            CompletableFuture.allOf(continuations.toArray(CompletableFuture[]::new))
                    .get(5, SECONDS);
        }
    }

    @Test
    void blocked_publish_encoding_does_not_stop_delivery_or_ping_timeout() throws Exception {
        // Arrange: encoding holds the sender while the actor remains available.
        var encoding = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        var encoder = new ScriptCodec();
        encoder.beforePublish = () -> {
            encoding.countDown();
            waitFor(release);
        };
        try (MqttClient sut = client("encoding-test", ends::add, 16 * 1024 * 1024, encoder)) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(request(), delivered::add);
            establish(transport, subscription);
            CompletableFuture<PublishResult> publication = sut.publish(message(new byte[] {1}));
            assertThat(encoding.await(5, SECONDS)).isTrue();

            // Act & Assert: incoming delivery is independent of the blocked encoder.
            transport.receive(publication(new byte[] {2}));
            assertThat(requireNonNull(delivered.poll(5, SECONDS)).message())
                    .isEqualTo(message(new byte[] {2}));

            // Act: the PINGREQ joins the blocked PUBLISH after its deadline is armed.
            scheduler.advance(Duration.ofSeconds(10));
            await().untilAsserted(() ->
                    assertThat(((ClientActor) sut).usage().sends()).isEqualTo(2)
            );
            scheduler.advance(Duration.ofSeconds(10));

            // Assert
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            Throwable cause = end.cause().orElseThrow();
            assertThat(cause).isInstanceOf(TimeoutException.class)
                    .hasMessage("PINGRESP deadline expired");
            assertThatThrownBy(() -> publication.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(cause);
            assertThat(transport.closed).isTrue();
            assertThat(transport.sent).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void establishment_observers_preserve_fatal_failure_before_and_after_actor_cleanup()
            throws Exception {
        // Arrange: the actor cannot retire the registration when the encoder reports failure.
        var failure = new IllegalStateException("Encoder failed");
        var encoding = new CompletableFuture<Thread>();
        var releaseEncoding = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var encoder = new ScriptCodec();
        encoder.beforePublish = () -> {
            encoding.complete(Thread.currentThread());
            waitFor(releaseEncoding);
            throw failure;
        };
        try (MqttClient sut = client("fatal-establishment", ends::add, 16 * 1024 * 1024, encoder)) {
            ScriptTransport transport = connect(sut);
            Subscription observed = sut.subscribe(request());
            Subscription closed = sut.subscribe(
                    Subscribe.of(new TopicFilter("closed"), Qos.AT_MOST_ONCE)
            );
            CompletableFuture<Qos> waiting = observed.established();
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Subscribe.class);
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Subscribe.class);
            sut.publish(message(new byte[] {1}));
            Thread sender = encoding.get(5, SECONDS);
            holdActor(transport, actorHeld, releaseActor);

            // Act & Assert: fatal notification precedes cleanup and has the same public cause.
            try {
                releaseEncoding.countDown();
                assertThat(sender.join(Duration.ofSeconds(5))).isTrue();
                assertThat(waiting).isNotDone();
                assertInternalFailureBeforeCleanup(observed, closed, failure);
            } finally {
                releaseActor.countDown();
            }
            assertInternalFailureAfterCleanup(observed, waiting, failure);
        } finally {
            releaseEncoding.countDown();
            releaseActor.countDown();
        }
    }

    @Test
    void a_fatal_encoder_failure_bypasses_a_full_mailbox_and_preserves_the_cause()
            throws Exception {
        // Arrange: keep the actor and sender busy independently before overflowing the mailbox.
        var failure = new IllegalArgumentException("Encoder failed");
        var encoding = new CompletableFuture<Thread>();
        var releaseEncoding = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var encoder = new ScriptCodec();
        encoder.beforePublish = () -> {
            encoding.complete(Thread.currentThread());
            waitFor(releaseEncoding);
            throw failure;
        };
        try (MqttClient sut = client("fatal-encoding", ends::add, 16 * 1024 * 1024, encoder)) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<PublishResult> started = sut.publish(message(new byte[] {1}));
            Thread sender = encoding.get(5, SECONDS);
            holdActor(transport, actorHeld, releaseActor);
            CompletableFuture<PublishResult> queued = sut.publish(message(new byte[] {2}));
            for (int i = 0; i < 65_537; i++) {
                transport.receive(new Packet.PingResp());
            }

            // Act: the fatal cause must win over the pending mailbox-capacity failure.
            releaseEncoding.countDown();
            assertThat(sender.join(Duration.ofSeconds(5))).isTrue();
            releaseActor.countDown();

            // Assert
            assertThatThrownBy(() -> started.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(failure);
            assertThatThrownBy(() -> queued.get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCause(failure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            assertThat(transport.closed).isTrue();
            assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(ends).isEmpty();
        } finally {
            releaseEncoding.countDown();
            releaseActor.countDown();
        }
    }

    @Test
    void a_fatal_old_encoder_ends_a_new_connection_attempt() throws Exception {
        // Arrange: the old sender is still encoding when a replacement connection opens.
        var failure = new AssertionError("Old encoder failed");
        var connectionLoss = new IOException("Old connection closed");
        var encoding = new CompletableFuture<Thread>();
        var release = new CountDownLatch(1);
        var encoder = new ScriptCodec();
        encoder.beforePublish = () -> {
            encoding.complete(Thread.currentThread());
            waitFor(release);
            throw failure;
        };
        try (MqttClient sut = client("old-encoding", ends::add, 16 * 1024 * 1024, encoder)) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<PublishResult> publication = sut.publish(message(new byte[] {1}));
            Thread sender = encoding.get(5, SECONDS);
            transport.receiver().onClosed(connectionLoss);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(Disconnection.of(connectionLoss, false, 1));
            CompletableFuture<ConnAck> reconnecting = sut.connect();
            ScriptTransport replacement = requireNonNull(transports.poll(5, SECONDS));

            // Act
            release.countDown();
            assertThat(sender.join(Duration.ofSeconds(5))).isTrue();

            // Assert: previous results keep their cause; the new attempt fails permanently.
            assertThatThrownBy(() -> publication.get(5, SECONDS)).hasRootCause(connectionLoss);
            assertThatThrownBy(() -> reconnecting.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectException.class)
                    .hasRootCause(failure);
            assertThat(replacement.closed).isTrue();
            assertThatThrownBy(() -> sut.connect().get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    private ScriptTransport connect(MqttClient client) throws Exception {
        return connect(client, false);
    }

    private ScriptTransport connect(MqttClient client, boolean sessionPresent) throws Exception {
        CompletableFuture<ConnAck> connection = client.connect();
        ScriptTransport transport = nextTransport();
        transport.receive(new Packet.ConnAck(sessionPresent, ReasonCode.SUCCESS, EMPTY));
        assertThat(connection.get(5, SECONDS).sessionPresent()).isEqualTo(sessionPresent);
        return transport;
    }

    private static Subscription subscribe(MqttClient client, ScriptTransport transport)
            throws Exception {
        Subscription subscription = client.subscribe(request(), _ -> { });
        establish(transport, subscription);
        return subscription;
    }

    private static Subscription subscribePull(
            MqttClient client,
            ScriptTransport transport,
            Subscribe request
    ) throws Exception {
        Subscription subscription = client.subscribe(request);
        establish(transport, subscription);
        return subscription;
    }

    private static Subscription subscribeOn(
            MqttClient client,
            ScriptTransport transport,
            Subscribe request,
            Executor executor,
            Consumer<MqttDelivery> handler
    ) throws Exception {
        Subscription subscription = client.subscribe(
                request,
                DeliveryOptions.serial().withExecutor(executor),
                handler
        );
        establish(transport, subscription);
        return subscription;
    }

    private static Subscription subscribeOnLanes(
            MqttClient client,
            ScriptTransport transport,
            Subscribe request,
            Function<MqttMessage, Executor> selector,
            Consumer<MqttDelivery> handler
    ) throws Exception {
        Subscription subscription = client.subscribe(
                request,
                DeliveryOptions.lanes(selector),
                handler
        );
        establish(transport, subscription);
        return subscription;
    }

    /// Once an internal failure is published, establishment reports it, and a subscription closed
    /// in that window still ends with it.
    private static void assertInternalFailureBeforeCleanup(
            Subscription observed,
            Subscription closed,
            Throwable failure
    ) {
        closed.close();
        assertThatThrownBy(() -> closed.receive(Duration.ZERO))
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(failure);
        assertInternalFailure(closed.established(), failure);
        assertInternalFailure(observed.established(), failure);
    }

    /// After cleanup, the waiting observer, a later observer, and receiving all report the failure.
    private static void assertInternalFailureAfterCleanup(
            Subscription subscription,
            CompletableFuture<Qos> waiting,
            Throwable failure
    ) {
        assertInternalFailure(waiting, failure);
        assertInternalFailure(subscription.established(), failure);
        await().untilAsserted(() -> assertThatThrownBy(() -> subscription.receive(Duration.ZERO))
                .isInstanceOf(MqttConnectionLostException.class)
                .hasCause(failure));
    }

    private static void assertInternalFailure(
            CompletableFuture<Qos> observation,
            Throwable failure
    ) {
        assertThatThrownBy(() -> observation.get(5, SECONDS))
                .hasCauseInstanceOf(MqttConnectionLostException.class)
                .hasRootCause(failure);
    }

    /// Answers the subscription's SUBSCRIBE with its requested QoS and waits for establishment.
    private static void establish(ScriptTransport transport, Subscription subscription)
            throws Exception {
        acknowledgeSubscription(transport, subscription);
        subscription.established().get(5, SECONDS);
    }

    private static void acknowledgeSubscription(
            ScriptTransport transport,
            Subscription subscription
    ) throws Exception {
        var packet = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
        assertThat(packet.subscriptions()).singleElement().satisfies(request ->
                assertThat(request.filter()).isEqualTo(subscription.filter()));
        transport.receive(new Packet.SubAck(packet.packetId(),
                List.of(ReasonCode.of(packet.subscriptions().getFirst().qos().level())), EMPTY));
    }

    private static Subscribe request() {
        return Subscribe.of(new TopicFilter("messages"), Qos.AT_MOST_ONCE);
    }

    private static Subscribe qosOneRequest() {
        return Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE);
    }

    private static MqttMessage message(byte[] bytes) {
        return MqttMessage.of(new Topic("messages"), bytes, Qos.AT_MOST_ONCE, false);
    }

    private static Packet.Publish publication(byte[] bytes) {
        return new Packet.Publish(
                new Topic("messages"),
                bytes,
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
    }

    private static Packet.Publish qosOnePublication(String topic, int packetId, boolean duplicate) {
        return acknowledgedPublication(topic, packetId, Qos.AT_LEAST_ONCE, duplicate);
    }

    private static Packet.Publish qosOnePublication(int packetId, byte[] payload) {
        return new Packet.Publish(
                new Topic("messages"),
                payload,
                Qos.AT_LEAST_ONCE,
                false,
                false,
                packetId,
                EMPTY
        );
    }

    private static Packet.Publish acknowledgedPublication(
            String topic,
            int packetId,
            Qos qos,
            boolean duplicate
    ) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {(byte) packetId},
                qos,
                false,
                duplicate,
                packetId,
                EMPTY
        );
    }

    private static Packet acknowledgement(Qos qos, int packetId) {
        return switch (qos) {
            case AT_MOST_ONCE -> throw new IllegalArgumentException("QoS 0 has no acknowledgement");
            case AT_LEAST_ONCE -> new Packet.PubAck(packetId, ReasonCode.SUCCESS, EMPTY);
            case EXACTLY_ONCE -> new Packet.PubRec(packetId, ReasonCode.SUCCESS, EMPTY);
        };
    }

    private static Stream<Arguments> acknowledgedQosModes() {
        return Stream.of(Qos.AT_LEAST_ONCE, Qos.EXACTLY_ONCE)
                .flatMap(qos -> Stream.of(AckMode.values()).map(mode -> Arguments.of(qos, mode)));
    }

    private static Stream<Arguments> invalidPackets() {
        return Stream.of(
                Arguments.of("30ffffffff", ReasonCode.MALFORMED_PACKET),
                Arguments.of("d100", ReasonCode.MALFORMED_PACKET),
                Arguments.of("307f", ReasonCode.PACKET_TOO_LARGE),
                Arguments.of("c000", ReasonCode.PROTOCOL_ERROR)
        );
    }

    private static void assertProtocolFailure(@Nullable Throwable cause, ReasonCode reasonCode) {
        assertThat(cause).isInstanceOfSatisfying(MqttProtocolException.class, failure -> {
            assertThat(failure.reasonCode()).isEqualTo(reasonCode);
            assertThat(failure.getCause()).isInstanceOfSatisfying(
                    PacketDecodeException.class,
                    original -> {
                        assertThat(original.reasonCode()).isEqualTo(reasonCode);
                        assertThat(failure).hasMessage(original.getMessage());
                    }
            );
        });
    }

    private MqttClient client() {
        return client(ends::add);
    }

    private MqttClient client(boolean cleanStart) {
        return client("actor-test", ends::add, 16 * 1024 * 1024, codec, cleanStart);
    }

    private MqttClient client(ConnectionListener listener) {
        return client(listener, 16 * 1024 * 1024);
    }

    private MqttClient client(ConnectionListener listener, int maxPacketSize) {
        return client("actor-test", listener, maxPacketSize);
    }

    private MqttClient client(String clientId, ConnectionListener listener, int maxPacketSize) {
        return client(clientId, listener, maxPacketSize, codec);
    }

    private MqttClient client(
            String clientId,
            ConnectionListener listener,
            int maxPacketSize,
            PacketCodec packetCodec
    ) {
        return client(clientId, listener, maxPacketSize, packetCodec, true);
    }

    private MqttClient client(
            String clientId,
            ConnectionListener listener,
            int maxPacketSize,
            PacketCodec packetCodec,
            boolean cleanStart
    ) {
        var clock = new TimerScheduler() {
            private volatile @Nullable Thread actor;

            @Override
            public long nanoTime() {
                // The first clock read starts connect on the actor; sender reads are independent.
                if (actor == null) {
                    actor = Thread.currentThread();
                }
                if (actor == Thread.currentThread()) {
                    beforeActorTime.run();
                }
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                return scheduler.schedule(delay, task);
            }
        };
        return new ClientActor(
                URI.create("mqtt://localhost:1883"),
                clientId,
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                maxPacketSize,
                clock,
                listener,
                _ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                },
                packetCodec,
                cleanStart,
                null,
                null,
                ReconnectBackoff.disabled(),
                false,
                Duration.ofSeconds(60),
                DeliveryOptionsAccess::execution
        );
    }

    private void holdActor(
            ScriptTransport transport,
            CountDownLatch entered,
            CountDownLatch release
    ) throws InterruptedException {
        beforeActorTime = () -> {
            entered.countDown();
            waitFor(release);
        };
        // The actor reads the clock for each received packet; the session ignores this PINGRESP.
        transport.receive(new Packet.PingResp());
        assertThat(entered.await(5, SECONDS)).isTrue();
    }

    /// Waits until the client's termination waits for `pending` alone, every other part having
    /// ended.
    private static void awaitTerminationPending(
            ClientActor client,
            ClientTermination.Pending pending
    ) {
        await().until(() -> client.terminationPending().equals(Set.of(pending)));
    }

    /// Blocks in either ending notification until `release`, recording which one arrived.
    private static ConnectionListener blockingListener(
            LinkedBlockingQueue<String> notices,
            CountDownLatch release
    ) {
        return new ConnectionListener() {
            @Override
            public void onDisconnected(Disconnection disconnection) {
                notices.add("disconnected");
                waitFor(release);
            }

            @Override
            public void onConnectFailed(Disconnection failure) {
                notices.add("connect failed");
                waitFor(release);
            }
        };
    }

    private ScriptTransport nextTransport() throws Exception {
        ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
        assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Connect.class);
        return transport;
    }

    /// Records every connection notification in order.
    private static ConnectionListener recording(LinkedBlockingQueue<Object> notifications) {
        return new ConnectionListener() {
            @Override
            public void onConnected(ConnAck connAck) {
                notifications.add(connAck);
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                notifications.add(disconnection);
            }

            @Override
            public void onConnectFailed(Disconnection failure) {
                notifications.add(failure);
            }
        };
    }

    /// Answers the first connection with Session Present and lets the client discard that
    /// broker session. Returns the transport of the connection that starts a new session.
    private ScriptTransport discardBrokerSession(ScriptTransport first) throws Exception {
        var disconnect = new Packet.Disconnect(ReasonCode.SUCCESS, EMPTY);
        first.receive(new Packet.ConnAck(true, ReasonCode.SUCCESS, EMPTY));
        assertThat(first.sent.poll(5, SECONDS)).isEqualTo(disconnect);
        ScriptTransport discarding = nextTransport();
        discarding.receive(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY));
        assertThat(discarding.sent.poll(5, SECONDS)).isEqualTo(disconnect);
        return nextTransport();
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            if (!latch.await(30, SECONDS)) {
                throw new AssertionError("Test failed to release continuation");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    private static final class ScriptCodec implements PacketCodec {

        Runnable beforePublish = () -> { };

        volatile Consumer<Packet> beforeEncode = _ -> { };

        @Override
        public byte[] encode(Packet packet) {
            beforeEncode.accept(packet);
            return codec.encode(packet);
        }

        @Override
        public Packet decode(byte[] bytes) {
            return codec.decode(bytes);
        }

        @Override
        public byte[] encodePublishHeader(
                Topic topic,
                Qos qos,
                boolean retain,
                boolean dup,
                int packetId,
                Properties properties,
                int payloadSize
        ) {
            return codec.encodePublishHeader(
                    topic,
                    qos,
                    retain,
                    dup,
                    packetId,
                    properties,
                    payloadSize
            );
        }

        @Override
        public byte[] encodePublish(byte[] header, byte[] payload) {
            beforePublish.run();
            return codec.encodePublish(header, payload);
        }
    }

    private enum ApplicationCompletion {
        TIME_OUT(future -> future.orTimeout(0, MILLISECONDS)
                .handle((_, failure) -> failure instanceof TimeoutException)
                .join()),
        CANCEL(future -> future.cancel(false)),
        COMPLETE(future -> future.complete(null)),
        FAIL(future -> future.completeExceptionally(new IllegalArgumentException("application")));

        final Predicate<CompletableFuture<?>> apply;

        ApplicationCompletion(Predicate<CompletableFuture<?>> apply) {
            this.apply = apply;
        }
    }

    private enum SpiCall {
        CLOCK,
        SCHEDULE,
        CREATE,
        START,
        SEND
    }

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }
}
