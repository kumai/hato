package org.mqtt4j.mqtj.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
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

import org.mqtt4j.mqtj.AckMode;
import org.mqtt4j.mqtj.ConnAck;
import org.mqtt4j.mqtj.ConnectionListener;
import org.mqtt4j.mqtj.Disconnection;
import org.mqtt4j.mqtj.MqttCapacityException;
import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttConnectException;
import org.mqtt4j.mqtj.MqttConnectionLostException;
import org.mqtt4j.mqtj.MqttDelivery;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.MqttNotConnectedException;
import org.mqtt4j.mqtj.MqttProtocolException;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.PublishResult;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.ReasonCode;
import org.mqtt4j.mqtj.ReceiveResult;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.Subscription;
import org.mqtt4j.mqtj.Topic;
import org.mqtt4j.mqtj.TopicFilter;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketDecodeException;
import org.mqtt4j.mqtj.client.internal.common.protocol.Properties;
import org.mqtt4j.mqtj.spi.TimerScheduler;
import org.mqtt4j.mqtj.spi.Transport;
import org.mqtt4j.mqtj.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.mqtj.spi.Transport.SendResult.CLOSED;

class ClientActorTest {

    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    private final ManualTimerScheduler scheduler = new ManualTimerScheduler();
    private final LinkedBlockingQueue<Disconnection> ends = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<ScriptTransport> transports = new LinkedBlockingQueue<>();

    private volatile Runnable beforeActorTime = () -> { };

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
                assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
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
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), delivery -> {
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
            var subscribe = (Packet.Subscribe) requireNonNull(old.sent.poll(5, SECONDS));
            old.receive(new Packet.SubAck(subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS), EMPTY));
            Subscription subscription = subscribing.get(5, SECONDS);
            old.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
            ScriptTransport current = connect(sut);
            acknowledgeResubscription(current, subscription);

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

            // Act & Assert: a failure of a delivery from this connection still stops it.
            current.receive(publication(new byte[] {3}));
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(currentFailure, false));
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
            CompletableFuture<Void> pending = requireNonNull(removing.get());
            assertThatThrownBy(pending::join).hasCauseInstanceOf(MqttNotConnectedException.class);
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
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), _ -> {
                worker.set(Thread.currentThread());
                handled.incrementAndGet();
                entered.countDown();
                waitFor(release);
            });
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS), EMPTY));
            Subscription subscription = subscribing.get(5, SECONDS);
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            transport.completeSends = false;
            CompletableFuture<PublishResult> stalled = sut.publish(new MqttMessage(
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
            acknowledgeResubscription(replacement, subscription);
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
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(
                    sut, transport, request().withBufferCapacity(1)
            );
            transport.receive(publication(new byte[] {1}));

            // Act
            transport.receive(publication(new byte[] {2}));

            // Assert: overload goes to the listener while the receiver can resume later.
            Throwable cause = requireNonNull(ends.poll(5, SECONDS)).cause();
            assertThat(cause).isInstanceOf(MqttCapacityException.class);
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
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
            CompletableFuture<PublishResult> stalled = sut.publish(new MqttMessage(
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
            assertThatThrownBy(() -> sut.subscribe(request()).get(5, SECONDS))
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
                publishing.add(sut.publish(new MqttMessage(new Topic("out"), new byte[0],
                        Qos.AT_LEAST_ONCE, false)));
                publications.add((Packet.Publish) requireNonNull(transport.sent.poll(5, SECONDS)));
            }

            // Act: repeated close and refused observers must not cancel or duplicate cleanup.
            subscriptions.forEach(Subscription::close);
            subscriptions.forEach(Subscription::close);
            assertThatThrownBy(() -> subscriptions.getLast().unsubscribe().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);

            // Assert: all removals progress while all 64 public operations remain pending.
            var removed = ConcurrentHashMap.<TopicFilter>newKeySet();
            for (int i = 0; i < 100; i++) {
                var packet = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
                assertThat(removed.add(packet.filters().getFirst())).isTrue();
                transport.receive(new Packet.UnsubAck(packet.packetId(), List.of(), EMPTY));
            }
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
    void close_retries_an_explicit_removal_refused_after_local_closure() throws Exception {
        // Arrange: stop the actor before it can refuse the explicit removal for send capacity.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribePull(sut, transport, request());
            transport.completeSends = false;
            CompletableFuture<PublishResult> stalled = sut.publish(new MqttMessage(
                    new Topic("a"), new byte[16 * 1024 * 1024 - 8], Qos.AT_MOST_ONCE, false
            ));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            holdActor(transport, entered, release);

            // Act
            CompletableFuture<Void> explicit = subscription.unsubscribe();
            subscription.close();
            release.countDown();
            assertThatThrownBy(() -> explicit.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);
            transport.completeSends = true;
            transport.completeSend();

            // Assert: the retained removal is sent without another close or unsubscribe.
            stalled.get(5, SECONDS);
            var packet = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            CompletableFuture<Void> cleanup = subscription.unsubscribe();
            transport.receive(new Packet.UnsubAck(packet.packetId(), List.of(), EMPTY));
            cleanup.get(5, SECONDS);
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
    void connection_loss_keeps_background_removal_pending_until_the_next_clean_session(
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
            assertThatThrownBy(() -> observer.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class);
            // The connection failure came after close(), so it does not replace the normal end.
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());
            ScriptTransport current = connect(sut);
            subscription.close();
            subscription.unsubscribe().get(5, SECONDS);
            assertThatThrownBy(observer::join)
                    .hasCauseInstanceOf(MqttConnectionLostException.class);
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
            await().until(() -> ((ClientActor) sut).usage().operations() == 0);
            subscription.close();
            assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
            var observers = new ArrayList<CompletableFuture<Void>>();

            // Act: enough calls to consume the entire operation budget must all refuse locally.
            for (int i = 0; i < 64; i++) {
                observers.add(subscription.unsubscribe());
            }
            CompletableFuture<ConnAck> reconnecting = sut.connect();

            // Assert: observing an offline removal cannot prevent its clean-session resolution.
            assertThat(reconnecting).isNotCompletedExceptionally();
            for (CompletableFuture<Void> observer : observers) {
                assertThatThrownBy(observer::join)
                        .hasCauseInstanceOf(MqttNotConnectedException.class);
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
        // Arrange: background removals share the full budget of public observers.
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
            await().until(() -> ((ClientActor) sut).usage().operations() == 0);
            var observers = new ArrayList<CompletableFuture<Void>>();
            for (int i = 0; i < 64; i++) {
                observers.add(subscriptions.get(i % registrations).unsubscribe());
            }
            assertThat(((ClientActor) sut).usage().operations()).isEqualTo(64);
            assertThat(observers).allSatisfy(observer -> assertThat(observer).isNotDone());

            // Act: FIFO closure follows admission of every observer.
            old.receiver().onClosed(failure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));

            // Assert: public observations end with their connection and return operation slots.
            for (CompletableFuture<Void> observer : observers) {
                assertThatThrownBy(() -> observer.get(5, SECONDS))
                        .hasCauseInstanceOf(MqttConnectionLostException.class)
                        .hasRootCause(failure);
            }
            await().until(() -> ((ClientActor) sut).usage().operations() == 0);
            ScriptTransport current = connect(sut);
            for (Subscription subscription : subscriptions) {
                assertThat(subscription.receive()).isEqualTo(new ReceiveResult.Ended());
                subscription.unsubscribe().get(5, SECONDS);
            }
            assertThat(current.sent).isEmpty();
            assertThat(subscribePull(sut, current, Subscribe.of(
                    subscriptions.getFirst().filter(), Qos.AT_MOST_ONCE))).isNotNull();
            assertThatThrownBy(() -> observers.getFirst().join()).hasRootCause(failure);
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
            assertThat(requireNonNull(ends.poll(5, SECONDS)).cause())
                    .isInstanceOf(MqttCapacityException.class)
                    .hasMessage("Acknowledgement capacity");
        }
    }

    @Test
    void pull_subscriptions_reject_after_handler_before_sending_subscribe() throws Exception {
        // Arrange
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);

            // Act & Assert
            assertThatThrownBy(() -> sut.subscribe(request().withAckMode(AckMode.AFTER_HANDLER))
                    .get(5, SECONDS)).hasCauseInstanceOf(IllegalArgumentException.class);
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
                Thread.interrupted();
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
                .connectionTimeout(Duration.ofSeconds(30))
                .timerScheduler(scheduler)
                .connectionListener(ends::add)
                .transportFactory(_ -> transport)
                .build()) {
            CompletableFuture<ConnAck> future = sut.connect();
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));

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
    void spi_failures_preserve_the_cause_release_notification_slots_and_allow_another_attempt(
            SpiCall call
    ) throws Exception {
        // Arrange: exceed the notification capacity with failed attempts before succeeding.
        var failure = new RejectedExecutionException("SPI rejected this attempt");
        var remaining = new AtomicInteger(65);
        Runnable reject = () -> {
            if (remaining.getAndDecrement() > 0) {
                throw failure;
            }
        };
        var created = new ArrayList<ScriptTransport>();
        TimerScheduler timers = new TimerScheduler() {
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
        try (MqttClient sut = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
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

            // Assert
            assertThat(connection.get(5, SECONDS)).isEqualTo(new ConnAck(false));
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));
            assertThat(transport.sent).isEmpty();
            assertThat(transport.closed).isTrue();
            assertThat(scheduler.pendingTaskCount()).isZero();
            CompletableFuture<ConnAck> next = sut.connect();
            nextTransport().accept();
            assertThat(next.get(5, SECONDS)).isEqualTo(new ConnAck(false));
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, true));
            assertThat(transport.closed).isTrue();
            CompletableFuture<ConnAck> next = sut.connect();
            nextTransport().accept();
            assertThat(next.get(5, SECONDS)).isEqualTo(new ConnAck(false));
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
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
            assertProtocolFailure(end.cause(), reasonCode);
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, true));
            assertThat(transport.closed).isTrue();
            assertThat(ends).isEmpty();
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
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
            assertThat(second.get(5, SECONDS)).isEqualTo(new ConnAck(false));
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
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
                assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
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
                assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, true));
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));
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
    void operation_slots_remain_reserved_until_blocked_continuations_return() throws Exception {
        // Arrange
        var release = new CountDownLatch(1);
        var entered = new AtomicInteger();
        var continuations = new ArrayList<CompletableFuture<Void>>();
        try (MqttClient sut = client()) {
            // Act: retain each operation slot with a blocked continuation. The previous
            // operation's completion thread may still be returning its slot, so retry refusals.
            for (int i = 0; i < 64; i++) {
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
                if (i < 63) {
                    await().pollInterval(Duration.ofMillis(1))
                            .until(sut::disconnect, attempt -> !attempt.isCompletedExceptionally())
                            .get(5, SECONDS);
                    requireNonNull(ends.poll(5, SECONDS));
                }
            }

            // Assert: saturation rejects admission until the continuations return.
            CompletableFuture<Void> refused = sut.disconnect();
            assertThat(refused).isCompletedExceptionally();
            assertThatThrownBy(refused::join).hasCauseInstanceOf(MqttCapacityException.class);
            release.countDown();
            CompletableFuture.allOf(continuations.toArray(CompletableFuture[]::new))
                    .get(5, SECONDS);
            await().untilAsserted(() -> assertThat(sut.disconnect())
                    .succeedsWithin(Duration.ofSeconds(5)));
        } finally {
            release.countDown();
        }
    }

    @Test
    void slow_and_throwing_listeners_are_serial_bounded_and_do_not_stop_connections()
            throws Exception {
        // Arrange
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        try (MqttClient sut = client(_ -> {
            calls.incrementAndGet();
            waitFor(release);
            throw new IllegalStateException("application listener failure");
        })) {
            // Act: accumulate notifications behind the first blocked listener call.
            for (int i = 0; i < 64; i++) {
                CompletableFuture<ConnAck> connection = sut.connect();
                nextTransport().accept();
                connection.get(5, SECONDS);
                sut.disconnect().get(5, SECONDS);
            }

            // Assert: no new transport starts until notification capacity returns.
            assertThatThrownBy(() -> sut.connect().join())
                    .hasCauseInstanceOf(MqttCapacityException.class);
            assertThat(transports).isEmpty();
            assertThat(calls.get()).isEqualTo(1);
            release.countDown();
            await().untilAtomic(calls, is(64));
            CompletableFuture<ConnAck> connection = sut.connect();
            nextTransport().accept();
            assertThat(connection.get(5, SECONDS)).isEqualTo(new ConnAck(false));
        } finally {
            release.countDown();
        }
    }

    @Test
    void disconnected_messaging_operations_fail_before_returning() {
        // Arrange: no connection has been requested.
        try (MqttClient sut = client()) {
            // Act & Assert
            assertThat(sut.publish(message(new byte[0])))
                    .isCompletedExceptionally();
            assertThatThrownBy(() -> sut.publish(message(new byte[0])).join())
                    .hasCauseInstanceOf(MqttNotConnectedException.class);
            assertThat(sut.subscribe(request(), _ -> { })).isCompletedExceptionally();
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
            assertThat(publication.get(5, SECONDS)).isEqualTo(new PublishResult());

            // Act: fail the next pending write.
            CompletableFuture<PublishResult> failed = sut.publish(message(new byte[] {2}));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            var cause = new IOException("write failed");
            transport.receiver().onClosed(cause);

            // Assert
            assertThatThrownBy(() -> failed.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(cause);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(cause, false));
        }
    }

    @Test
    void qos_one_puback_can_complete_before_the_write_notification() throws Exception {
        // Arrange: hold the transport's completion while allowing inbound PUBACK.
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            transport.completeSends = false;
            var message = new MqttMessage(
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
            assertThat(publication.get(5, SECONDS)).isEqualTo(new PublishResult());
            transport.completeSend();
            transport.completeSends = true;
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
        }
    }

    @Test
    void acknowledgements_made_while_the_actor_is_busy_share_one_mailbox_wake() throws Exception {
        // Arrange: more MANUAL deliveries than the 256-event control mailbox can hold.
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.MANUAL)
                            .withBufferCapacity(512),
                    delivered::add
            );
            var request = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    request.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY
            ));
            subscribing.get(5, SECONDS);
            var deliveries = new ArrayList<MqttDelivery>();
            for (int id = 1; id <= 300; id++) {
                transport.receive(new Packet.Publish(new Topic("messages"), new byte[] {1},
                        Qos.AT_LEAST_ONCE, false, false, id, EMPTY));
                deliveries.add(requireNonNull(delivered.poll(5, SECONDS)));
            }
            holdActor(transport, entered, release);

            // Act: every acknowledgement completes its condition before the actor resumes.
            deliveries.forEach(MqttDelivery::ack);
            release.countDown();

            // Assert
            for (int id = 1; id <= 300; id++) {
                assertThat(transport.sent.poll(5, SECONDS))
                        .isEqualTo(new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY));
            }
            assertThat(ends).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest
    @EnumSource(AckMode.class)
    void a_qos_one_burst_waits_for_send_capacity_without_losing_the_tail(AckMode mode)
            throws Exception {
        // Arrange: the first write holds the transport, with room for 100 local deliveries.
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE)
                            .withAckMode(mode)
                            .withBufferCapacity(128),
                    delivery -> {
                        delivered.add(delivery);
                        delivery.ack();
                    }
            );
            var request = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    request.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY
            ));
            subscribing.get(5, SECONDS);
            transport.completeSends = false;
            var blocked = sut.publish(message(new byte[] {0}));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act
            for (int id = 1; id <= 100; id++) {
                transport.receive(new Packet.Publish(new Topic("messages"), new byte[] {1},
                        Qos.AT_LEAST_ONCE, false, false, id, EMPTY));
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
                        .isEqualTo(new Packet.PubAck(id, ReasonCode.SUCCESS, EMPTY));
            }
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
        }
    }

    @ParameterizedTest(name = "reused id has DUP={0}")
    @ValueSource(booleans = {false, true})
    void id_reuse_after_inline_puback_completion_precedes_send_buffer_release(boolean dup)
            throws Exception {
        // Arrange: the completion callback runs, but send() has not returned its buffer yet.
        var deliveries = new LinkedBlockingQueue<MqttDelivery>();
        var releaseSender = new CountDownLatch(1);
        var sentFirstAck = new AtomicBoolean();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            var subscribing = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.MANUAL),
                    deliveries::add
            );
            var request = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    request.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY
            ));
            subscribing.get(5, SECONDS);
            transport.afterSend = packet -> {
                if (packet instanceof Packet.PubAck && sentFirstAck.compareAndSet(false, true)) {
                    transport.receive(new Packet.Publish(new Topic("messages"), new byte[] {1},
                            Qos.AT_LEAST_ONCE, false, dup, 7, EMPTY));
                    waitFor(releaseSender);
                }
            };
            transport.receive(new Packet.Publish(new Topic("messages"), new byte[] {1},
                    Qos.AT_LEAST_ONCE, false, false, 7, EMPTY));
            MqttDelivery first = requireNonNull(deliveries.poll(5, SECONDS));

            // Act
            first.ack();
            MqttDelivery reused = requireNonNull(deliveries.poll(5, SECONDS));

            // Assert: identical payload and DUP do not cause a legal reuse to be discarded.
            assertThat(reused).isNotSameAs(first);
            assertThat(reused.dup()).isEqualTo(dup);
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.PubAck(7, ReasonCode.SUCCESS, EMPTY));

            // Act: old handles cannot acknowledge the new reception.
            first.ack();
            releaseSender.countDown();
            sut.publish(message(new byte[] {2})).get(5, SECONDS);

            // Assert: an outgoing write is a barrier after the old acknowledgement.
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            reused.ack();
            assertThat(transport.sent.poll(5, SECONDS))
                    .isEqualTo(new Packet.PubAck(7, ReasonCode.SUCCESS, EMPTY));
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
        var message = new MqttMessage(
                new Topic("messages"), new byte[] {1}, Qos.AT_LEAST_ONCE, false
        );
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            transport.completeSends = false;
            // The connect completion thread may still be returning its slot. Refused
            // publications can retry because they never reach the wire.
            await().pollInterval(Duration.ofMillis(1)).until(() -> {
                CompletableFuture<PublishResult> publication = sut.publish(message);
                if (publication.isCompletedExceptionally()) {
                    assertThatThrownBy(publication::join)
                            .hasCauseInstanceOf(MqttCapacityException.class);
                } else {
                    publications.add(publication);
                }
                return publications.size() == 64;
            });
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
            assertThat(first.get(5, SECONDS)).isEqualTo(new PublishResult());
            assertThat(second.get(5, SECONDS)).isEqualTo(new PublishResult());
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
            assertThat(publication.get(5, SECONDS)).isEqualTo(new PublishResult());
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));
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
        var sending = new CompletableFuture<Thread>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            await().untilAsserted(() -> assertThat(transport.pendingTicket).isZero());
            transport.completeSends = false;
            CompletableFuture<PublishResult> first = sut.publish(message(new byte[] {1}));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            CompletableFuture<PublishResult> second = sut.publish(message(new byte[] {2}));
            holdActor(transport, actorHeld, releaseActor);
            transport.beforeSend = () -> {
                sending.complete(Thread.currentThread());
                throw failure;
            };

            // Act: notify the successful write, then let the next send fail before resuming.
            transport.completeSend();
            Thread sender = sending.get(5, SECONDS);
            // Waiting again proves the sender has posted the failure, not merely entered send.
            await().untilAsserted(() -> assertThat(sender.getState())
                    .isEqualTo(Thread.State.WAITING));
            releaseActor.countDown();

            // Assert: FIFO notification preserves success and only the later operation fails.
            assertThat(first.get(5, SECONDS)).isEqualTo(new PublishResult());
            assertThatThrownBy(() -> second.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(failure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));
            assertThat(transport.closed).isTrue();
            assertThat(transport.sent).isEmpty();

            // Act & Assert: a transport exception ends the connection, keeping the client usable.
            ScriptTransport replacement = connect(sut);
            assertThat(replacement).isNotSameAs(transport);
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
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
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), received::add);
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
            Subscription subscription = subscribing.get(5, SECONDS);
            assertThat(subscription.grantedQos()).isEqualTo(Qos.AT_MOST_ONCE);

            // Act & Assert: new delivery stops before the broker acknowledges removal.
            CompletableFuture<Void> unsubscribing = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(publication(new byte[] {2}));
            assertThatThrownBy(() -> sut.subscribe(request(), received::add).get(5, SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            unsubscribing.get(5, SECONDS);
            assertThat(received).isEmpty();
            CompletableFuture<Void> repeated = subscription.unsubscribe();
            assertThat(repeated).isNotSameAs(unsubscribing);
            repeated.get(5, SECONDS);

            // Act & Assert: the filter becomes available again after acknowledged removal.
            CompletableFuture<Subscription> replacement = sut.subscribe(request(), received::add);
            var next = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    next.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            replacement.get(5, SECONDS);
        }
    }

    @Test
    void unsubscribe_stops_delivery_of_a_publication_queued_before_the_call()
            throws Exception {
        // Arrange: the handler holds the only delivery slot of its subscription.
        var handlerEntered = new CountDownLatch(1);
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_MOST_ONCE)
                            .withBufferCapacity(1),
                    _ -> {
                        handlerEntered.countDown();
                        waitFor(releaseHandler);
                    }
            );
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            Subscription subscription = subscribing.get(5, SECONDS);
            transport.receive(publication(new byte[] {1}));
            assertThat(handlerEntered.await(5, SECONDS)).isTrue();
            holdActor(transport, actorHeld, releaseActor);
            transport.receive(publication(new byte[] {2}));

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
    void a_full_regular_byte_budget_rolls_back_subscribe_and_unsubscribe_without_disconnect()
            throws Exception {
        // Arrange: one pending write fills the normal send budget.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), received::add);
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            Subscription subscription = subscribing.get(5, SECONDS);
            transport.completeSends = false;
            // One-byte topic + length prefix + five-byte fixed header fills 16 MiB exactly.
            CompletableFuture<PublishResult> publication = sut.publish(new MqttMessage(
                    new Topic("a"),
                    new byte[16 * 1024 * 1024 - 8],
                    Qos.AT_MOST_ONCE,
                    false
            ));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);

            // Act & Assert: failed operations leave the connection and subscription usable.
            assertThatThrownBy(() -> subscription.unsubscribe().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);
            var other = Subscribe.of(new TopicFilter("other"), Qos.AT_MOST_ONCE);
            assertThatThrownBy(() -> sut.subscribe(other, _ -> { }).get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);
            transport.receive(publication(new byte[] {3}));
            assertThat(received.poll(5, SECONDS)).isNotNull();
            assertThat(ends).isEmpty();

            // Act & Assert: returning send capacity permits both operations to be retried.
            transport.completeSend();
            publication.get(5, SECONDS);
            transport.completeSends = true;
            CompletableFuture<Subscription> retry = sut.subscribe(other, _ -> { });
            var retried = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    retried.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            retry.get(5, SECONDS);
            CompletableFuture<Void> removing = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));
            removing.get(5, SECONDS);
        }
    }

    @Test
    void an_unsubscribe_refused_by_operation_capacity_resumes_delivery() throws Exception {
        // Arrange: another subscription's pending removal and its observers hold every slot.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), received::add);
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            Subscription subscription = subscribing.get(5, SECONDS);
            CompletableFuture<Subscription> subscribingOther = sut.subscribe(
                    Subscribe.of(new TopicFilter("other"), Qos.AT_MOST_ONCE),
                    _ -> { }
            );
            var subscribeOther = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribeOther.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            Subscription other = subscribingOther.get(5, SECONDS);
            var observers = new ArrayList<CompletableFuture<Void>>();
            observers.add(other.unsubscribe());
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Unsubscribe.class);
            // Setup completion threads may still be returning their slots.
            await().pollInterval(Duration.ofMillis(1)).until(() -> {
                CompletableFuture<Void> observer = other.unsubscribe();
                if (!observer.isCompletedExceptionally()) {
                    observers.add(observer);
                }
                return observers.size() == 64;
            });

            // Act
            assertThatThrownBy(() -> subscription.unsubscribe().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);
            transport.receive(publication(new byte[] {9}));

            // Assert: the refused removal no longer pauses this subscription.
            assertThat(received.poll(5, SECONDS)).isNotNull();
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
            CompletableFuture<Subscription> subscribing = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_MOST_ONCE)
                            .withBufferCapacity(1),
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                    }
            );
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            subscribing.get(5, SECONDS);

            // Act: a running handler still counts toward the subscription's delivery limit.
            transport.receive(publication(new byte[] {1}));
            assertThat(entered.await(5, SECONDS)).isTrue();
            transport.receive(publication(new byte[] {2}));
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));

            // Assert
            assertThat(end.cause()).isInstanceOf(MqttCapacityException.class);
            assertThat(transport.closed).isTrue();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_queued_handler_failure_is_processed_before_its_acknowledgement() throws Exception {
        // Arrange: both the explicit ack and failure arrive while the actor is held.
        var workers = new LinkedBlockingQueue<Thread>();
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var failure = new IllegalStateException("handler failed after ack");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.MANUAL),
                    delivery -> {
                        workers.add(Thread.currentThread());
                        waitFor(releaseHandler);
                        delivery.ack();
                        throw failure;
                    }
            );
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(), List.of(ReasonCode.GRANTED_QOS_1), EMPTY
            ));
            subscribing.get(5, SECONDS);
            transport.receive(new Packet.Publish(
                    new Topic("messages"), new byte[] {1}, Qos.AT_LEAST_ONCE,
                    false, false, 7, EMPTY
            ));
            Thread worker = requireNonNull(workers.poll(5, SECONDS));
            holdActor(transport, actorHeld, releaseActor);

            // Act: the worker waits after notifying failure and coalescing acknowledgement.
            releaseHandler.countDown();
            await().until(() -> worker.getState() == Thread.State.WAITING);
            releaseActor.countDown();

            // Assert: processing the failure retires the queued ACK and preserves its cause.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));
            assertThat(transport.sent).isEmpty();
            assertThat(transport.closed).isTrue();
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @ParameterizedTest(name = "subscriptions: {0}")
    @ValueSource(ints = {1, 256})
    void handler_failure_overtakes_queued_input_and_commands(int subscriptions) throws Exception {
        // Arrange: a publication reaches every lane before input and a command wait in the actor.
        var workers = new LinkedBlockingQueue<Thread>();
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
                var request = Subscribe.of(new TopicFilter("a/".repeat(i) + "#"), Qos.AT_MOST_ONCE);
                CompletableFuture<Subscription> subscribing = sut.subscribe(request, _ -> {
                    if (failing) {
                        workers.add(Thread.currentThread());
                    }
                    delivered.countDown();
                    waitFor(releaseHandler);
                    if (failing) {
                        throw failure;
                    }
                });
                var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
                transport.receive(new Packet.SubAck(subscribe.packetId(),
                        List.of(ReasonCode.SUCCESS), EMPTY));
                registered.add(subscribing.get(5, SECONDS));
            }
            var incoming = new Packet.Publish(new Topic("a/".repeat(subscriptions) + "message"),
                    new byte[] {1}, Qos.AT_MOST_ONCE, false, false, 0, EMPTY);
            transport.receive(incoming);
            assertThat(delivered.await(5, SECONDS)).isTrue();
            Thread worker = requireNonNull(workers.poll(5, SECONDS));
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

            // Act: the untimed lane wait follows notification, unlike the handler's timed wait.
            releaseHandler.countDown();
            await().until(() -> worker.getState() == Thread.State.WAITING);
            releaseActor.countDown();

            // Assert: termination discards input and rejects the queued command without sending.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));
            assertThatThrownBy(() -> queued.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class);
            assertThat(List.of(received.get(), encoded.get())).containsExactly(0, 0);
            assertThat(transport.closed).isTrue();
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @ParameterizedTest(name = "handler failure first: {0}")
    @ValueSource(booleans = {false, true})
    void handler_and_transport_failures_preserve_the_first_notification(boolean handlerFirst)
            throws Exception {
        // Arrange: both failures arrive while the actor cannot consume either notification.
        var workers = new LinkedBlockingQueue<Thread>();
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var failure = new IllegalStateException("handler failed");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), _ -> {
                workers.add(Thread.currentThread());
                waitFor(releaseHandler);
                throw failure;
            });
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS), EMPTY));
            subscribing.get(5, SECONDS);
            transport.receive(publication(new byte[] {1}));
            Thread worker = requireNonNull(workers.poll(5, SECONDS));
            holdActor(transport, actorHeld, releaseActor);

            // Act: invalid input uses the termination slot; the lane wait follows notification.
            if (!handlerFirst) {
                transport.receiver().onBytes(new byte[] {(byte) 0xff, 0});
            }
            releaseHandler.countDown();
            await().until(() -> worker.getState() == Thread.State.WAITING);
            if (handlerFirst) {
                transport.receiver().onBytes(new byte[] {(byte) 0xff, 0});
            }
            releaseActor.countDown();

            // Assert: the second failure cannot replace the first admitted cause.
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            if (handlerFirst) {
                assertThat(end).isEqualTo(new Disconnection(failure, false));
            } else {
                assertProtocolFailure(end.cause(), ReasonCode.MALFORMED_PACKET);
                assertThat(end.userRequested()).isFalse();
            }
            assertThat(transport.closed).isTrue();
        } finally {
            releaseHandler.countDown();
            releaseActor.countDown();
        }
    }

    @Test
    void old_handler_failure_cannot_hide_the_current_transport_failure() throws Exception {
        // Arrange: the old handler is still running after a new connection is established.
        var workers = new LinkedBlockingQueue<Thread>();
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var failure = new IllegalStateException("old handler failed");
        try (MqttClient sut = client()) {
            ScriptTransport old = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), _ -> {
                workers.add(Thread.currentThread());
                waitFor(releaseHandler);
                throw failure;
            });
            var subscribe = (Packet.Subscribe) requireNonNull(old.sent.poll(5, SECONDS));
            old.receive(new Packet.SubAck(subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS), EMPTY));
            subscribing.get(5, SECONDS);
            old.receive(publication(new byte[] {1}));
            Thread worker = requireNonNull(workers.poll(5, SECONDS));
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
            ScriptTransport current = connect(sut);
            var resubscribe = (Packet.Subscribe) requireNonNull(current.sent.poll(5, SECONDS));
            current.receive(new Packet.SubAck(resubscribe.packetId(),
                    List.of(ReasonCode.SUCCESS), EMPTY));
            holdActor(current, actorHeld, releaseActor);

            // Act: the stale failure arrives first but cannot reserve current termination.
            releaseHandler.countDown();
            await().until(() -> worker.getState() == Thread.State.WAITING);
            current.receiver().onBytes(new byte[] {(byte) 0xff, 0});
            releaseActor.countDown();

            // Assert: the valid current cause still ends the connection.
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            assertProtocolFailure(end.cause(), ReasonCode.MALFORMED_PACKET);
            assertThat(end.userRequested()).isFalse();
            assertThat(current.closed).isTrue();
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
        var workers = new LinkedBlockingQueue<Thread>();
        var releaseHandler = new CountDownLatch(1);
        var actorHeld = new CountDownLatch(1);
        var releaseActor = new CountDownLatch(1);
        var failure = new IllegalStateException("handler failed with a full lane");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_LEAST_ONCE)
                            .withBufferCapacity(1),
                    _ -> {
                        workers.add(Thread.currentThread());
                        waitFor(releaseHandler);
                        throw failure;
                    }
            );
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(subscribe.packetId(),
                    List.of(ReasonCode.GRANTED_QOS_1), EMPTY));
            subscribing.get(5, SECONDS);
            transport.receive(new Packet.Publish(new Topic("messages"), new byte[] {1},
                    Qos.AT_LEAST_ONCE, false, false, 7, EMPTY));
            Thread worker = requireNonNull(workers.poll(5, SECONDS));
            beforeActorTime = () -> {
                actorHeld.countDown();
                waitFor(releaseActor);
            };
            transport.receive(new Packet.Publish(new Topic("messages"), new byte[] {2},
                    Qos.AT_LEAST_ONCE, false, false, 8, EMPTY));
            assertThat(actorHeld.await(5, SECONDS)).isTrue();

            // Act: the failure is accepted while that publication is still in progress.
            releaseHandler.countDown();
            await().until(() -> worker.getState() == Thread.State.WAITING);
            releaseActor.countDown();

            // Assert: the full lane neither replaces the accepted cause nor releases a PUBACK.
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));
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
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), delivery -> {
                byte payload = delivery.message().payload()[0];
                handled.add(payload);
                if (payload == 1) {
                    workers.add(Thread.currentThread());
                    waitFor(releaseHandler);
                    throw failure;
                }
            });
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS), EMPTY));
            subscribing.get(5, SECONDS);
            transport.receive(publication(new byte[] {1}));
            Thread worker = requireNonNull(workers.poll(5, SECONDS));
            transport.receive(publication(new byte[] {2}));
            await().until(() -> ((ClientActor) sut).usage().deliveries() == 2);
            holdActor(transport, actorHeld, releaseActor);

            // Act: shutdown is requested before the handler fails.
            sut.close();
            releaseHandler.countDown();
            await().until(() -> worker.getState() == Thread.State.WAITING);
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
        // Arrange: three deliveries fit the subscription's slots but not the 32 MiB budget.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(
                    Subscribe.of(new TopicFilter("messages"), Qos.AT_MOST_ONCE)
                            .withBufferCapacity(3),
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                    }
            );
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            subscribing.get(5, SECONDS);
            byte[] payload = new byte[11 * 1024 * 1024];
            transport.receive(publication(payload));
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            transport.receive(publication(payload));
            transport.receive(publication(payload));

            // Assert
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            assertThat(end.cause()).isInstanceOf(MqttCapacityException.class);
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
            assertThat(end.cause()).isInstanceOf(MqttCapacityException.class);
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
                CompletableFuture<Subscription> subscribing = sut.subscribe(
                        Subscribe.of(new TopicFilter("backlog/" + i), Qos.AT_MOST_ONCE),
                        _ -> { }
                );
                var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
                transport.receive(new Packet.SubAck(
                        subscribe.packetId(),
                        List.of(ReasonCode.SUCCESS),
                        EMPTY
                ));
                subscribing.get(5, SECONDS);
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
            CompletableFuture<Subscription> subscribing = sut.subscribe(
                    request().withBufferCapacity(2),
                    _ -> {
                        try {
                            sut.publish(new MqttMessage(new Topic("replies"), new byte[] {2},
                                    Qos.AT_LEAST_ONCE, false)).join();
                        } catch (CompletionException failure) {
                            publishFailure.complete(failure.getCause());
                        }
                        waitFor(release);
                    }
            );
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            subscribing.get(5, SECONDS);
            // Attach the continuation before the write completes, so it blocks a completion
            // thread rather than this one.
            transport.completeSends = false;
            CompletableFuture<Void> continuation = sut.publish(message(new byte[] {3}))
                    .thenRun(() -> waitFor(release));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Publish.class);
            transport.completeSend();

            // Act: the handler waits for PUBACK behind a stalled write, another publication
            // queues behind that write, and a burst of 1,000 overflows the handler's queue.
            transport.receive(publication(new byte[] {4}));
            assertThat(transport.sent.poll(5, SECONDS)).isInstanceOfSatisfying(
                    Packet.Publish.class,
                    reply -> assertThat(reply.topic()).isEqualTo(new Topic("replies"))
            );
            CompletableFuture<PublishResult> queued = sut.publish(message(new byte[] {7}));
            byte[] packet = codec.encode(publication(new byte[] {5}));
            var burst = new ByteArrayOutputStream();
            for (int i = 0; i < 1000; i++) {
                burst.writeBytes(packet);
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
            transport.receiver().onClosed(new IOException("Transport closed"));

            // Assert: both subscriptions remain; only the running handler retains a delivery.
            var held = new ClientActor.Usage(1, 1, 0, 0, 0, 0, 2, 1, 13);
            await().untilAsserted(() -> assertThat(actor.usage()).isEqualTo(held));

            // Act
            release.countDown();

            // Assert
            continuation.get(5, SECONDS);
            var retained = new ClientActor.Usage(0, 0, 0, 0, 0, 0, 2, 0, 0);
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
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), delivered::add);
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));

            // Act: dispose the old handle while disconnected and while reconnecting.
            oldSubscription.close();
            CompletableFuture<Void> removing = oldSubscription.unsubscribe();
            assertThatThrownBy(removing::join).hasCauseInstanceOf(MqttNotConnectedException.class);
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport current = nextTransport();
            oldSubscription.close();
            CompletableFuture<Void> observer = oldSubscription.unsubscribe();
            assertThatThrownBy(observer::join).hasCauseInstanceOf(MqttNotConnectedException.class);

            // Assert: the new connection sends no UNSUBSCRIBE for the old session.
            assertThat(current.sent).isEmpty();

            // Arrange: the new session establishes the same filter with another handler.
            current.accept();
            connection.get(5, SECONDS);
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), received::add);
            var subscribe = (Packet.Subscribe) requireNonNull(current.sent.poll(5, SECONDS));
            current.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            subscribing.get(5, SECONDS);

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
    void connection_loss_rolls_back_explicit_removal_and_preserves_its_failed_observers()
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

            // Assert: calls accepted before the loss preserve the unfinished exchange's failure.
            assertThatThrownBy(() -> first.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(failure);
            assertThatThrownBy(() -> observer.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class)
                    .hasRootCause(failure);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));

            // Act: the failed attempt restores the subscription for the next connection.
            ScriptTransport current = connect(sut);
            acknowledgeResubscription(current, subscription);
            current.receive(publication(new byte[] {9}));
            assertThat(received(subscription.receive(Duration.ofSeconds(5))).message().payload())
                    .containsExactly((byte) 9);
            subscription.close();
            CompletableFuture<Void> disposed = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(current.sent.poll(5, SECONDS));
            current.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));

            // Assert: disposal does not rewrite either result from the failed attempt.
            disposed.get(5, SECONDS);
            assertThat(disposed).isNotSameAs(first).isNotSameAs(observer);
            assertThatThrownBy(() -> first.get(5, SECONDS)).hasRootCause(failure);
            assertThatThrownBy(() -> observer.get(5, SECONDS)).hasRootCause(failure);
            assertThat(transport.sent).isEmpty();
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
    void an_additional_call_queued_behind_a_refused_removal_fails_with_its_cause()
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
            assertThatThrownBy(() -> first.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttNotConnectedException.class);
            assertThatThrownBy(() -> second.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttNotConnectedException.class);
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

            // Assert: the original exchange still fails, and no additional removal was sent.
            assertThatThrownBy(() -> removing.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class);
            assertThatThrownBy(() -> publication.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));
            subscription.unsubscribe().get(5, SECONDS);
            assertThat(transport.sent).isEmpty();
        } finally {
            release.countDown();
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
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), delivery -> {
                delivered.add(delivery);
                handlers.add(Thread.currentThread());
                waitFor(releaseHandler);
            });
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            Subscription subscription = subscribing.get(5, SECONDS);
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
    void close_starts_one_removal_without_waiting_or_retaining_operation_slots()
            throws Exception {
        // Arrange
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), received::add);
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            Subscription subscription = subscribing.get(5, SECONDS);

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
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), received::add);
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            Subscription subscription = subscribing.get(5, SECONDS);
            holdActor(transport, entered, release);
            transport.receive(publication(new byte[] {1}));
            CompletableFuture<Void> removing = subscription.unsubscribe();

            // Act: additional calls take the remaining operation slots until one is refused.
            CompletableFuture<Void> refused = subscription.unsubscribe();
            for (int i = 0; i < 64 && !refused.isCompletedExceptionally(); i++) {
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
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), _ -> { });
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            Subscription subscription = subscribing.get(5, SECONDS);
            CompletableFuture<Void> first = subscription.unsubscribe();
            var unsubscribe = (Packet.Unsubscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            var observers = new ArrayList<CompletableFuture<Void>>();
            observers.add(first);
            // Setup completion threads may still be returning their slots. Refused observers
            // can retry without affecting the pending wire attempt.
            await().pollInterval(Duration.ofMillis(1)).until(() -> {
                CompletableFuture<Void> observer = subscription.unsubscribe();
                if (observer.isCompletedExceptionally()) {
                    assertThatThrownBy(observer::join)
                            .hasCauseInstanceOf(MqttCapacityException.class);
                } else {
                    observers.add(observer);
                }
                return observers.size() == 64;
            });
            for (CompletableFuture<Void> observer : observers) {
                continuations.add(observer.thenRun(() -> {
                    entered.incrementAndGet();
                    waitFor(release);
                }));
            }

            // Act & Assert: no further call is admitted while all operation slots are reserved.
            assertThatThrownBy(() -> subscription.unsubscribe().get(5, SECONDS))
                    .hasCauseInstanceOf(MqttCapacityException.class);

            // Act: acknowledge removal while every completion callback blocks.
            transport.receive(new Packet.UnsubAck(unsubscribe.packetId(), List.of(), EMPTY));

            // Assert: every callback starts independently and retains its own operation slot.
            await().untilAtomic(entered, is(64));
            assertThatThrownBy(() -> sut.publish(message(new byte[0])).join())
                    .hasCauseInstanceOf(MqttCapacityException.class);
            assertThat(transport.sent).isEmpty();
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
            CompletableFuture<Subscription> subscribing = sut.subscribe(request(), delivered::add);
            var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
            transport.receive(new Packet.SubAck(
                    subscribe.packetId(),
                    List.of(ReasonCode.SUCCESS),
                    EMPTY
            ));
            subscribing.get(5, SECONDS);
            CompletableFuture<PublishResult> publication = sut.publish(message(new byte[] {1}));
            assertThat(encoding.await(5, SECONDS)).isTrue();

            // Act & Assert: incoming delivery is independent of the blocked encoder.
            transport.receive(publication(new byte[] {2}));
            assertThat(requireNonNull(delivered.poll(5, SECONDS)).message())
                    .isEqualTo(message(new byte[] {2}));

            // Act: the actor registers the PING deadline and the next keep-alive check.
            scheduler.advance(Duration.ofSeconds(10));
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(2));
            scheduler.advance(Duration.ofSeconds(10));

            // Assert
            Disconnection end = requireNonNull(ends.poll(5, SECONDS));
            Throwable cause = requireNonNull(end.cause());
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(failure, false));
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
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(connectionLoss, false));
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
        CompletableFuture<ConnAck> connection = client.connect();
        ScriptTransport transport = nextTransport();
        transport.accept();
        connection.get(5, SECONDS);
        return transport;
    }

    private static Subscription subscribe(MqttClient client, ScriptTransport transport)
            throws Exception {
        CompletableFuture<Subscription> subscribing = client.subscribe(request(), _ -> { });
        var subscribe = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
        transport.receive(new Packet.SubAck(
                subscribe.packetId(),
                List.of(ReasonCode.SUCCESS),
                EMPTY
        ));
        return subscribing.get(5, SECONDS);
    }

    private static Subscription subscribePull(
            MqttClient client,
            ScriptTransport transport,
            Subscribe request
    ) throws Exception {
        CompletableFuture<Subscription> subscribing = client.subscribe(request);
        var packet = (Packet.Subscribe) requireNonNull(transport.sent.poll(5, SECONDS));
        transport.receive(new Packet.SubAck(packet.packetId(),
                List.of(ReasonCode.of(request.qos().level())), EMPTY));
        return subscribing.get(5, SECONDS);
    }

    private static void acknowledgeResubscription(
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

    private static MqttMessage message(byte[] bytes) {
        return new MqttMessage(new Topic("messages"), bytes, Qos.AT_MOST_ONCE, false);
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
                packetCodec
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

    private ScriptTransport nextTransport() throws Exception {
        ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
        assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Connect.class);
        return transport;
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

    private static final class ScriptTransport implements Transport {

        final LinkedBlockingQueue<Packet> sent = new LinkedBlockingQueue<>();
        volatile @Nullable Listener listener;
        volatile boolean closed;
        volatile Runnable beforeSend = () -> { };
        volatile Consumer<Packet> afterSend = _ -> { };
        Runnable onStart = () -> { };
        boolean connectOnStart = true;
        volatile SendResult sendResult = SendResult.ACCEPTED;
        volatile boolean completeSends = true;
        volatile long pendingTicket;
        @Nullable Throwable startFailure;

        Listener receiver() {
            return requireNonNull(listener);
        }

        void receive(Packet packet) {
            receiver().onBytes(codec.encode(packet));
        }

        void accept() {
            receiver().onBytes(codec.encode(new Packet.ConnAck(false, ReasonCode.SUCCESS, EMPTY)));
        }

        void completeSend() {
            long ticket = pendingTicket;
            pendingTicket = 0;
            receiver().onSent(ticket);
        }

        @Override
        public void start(URI endpoint, Duration timeout, Listener receiver) {
            listener = receiver;
            if (connectOnStart) {
                receiver.onConnected();
            }
            onStart.run();
            if (startFailure != null) {
                closed = true;
                receiver.onClosed(startFailure);
            }
        }

        @Override
        public SendResult send(long ticket, byte[] bytes) {
            beforeSend.run();
            if (closed) {
                return SendResult.CLOSED;
            }
            SendResult result = sendResult;
            if (result != SendResult.ACCEPTED) {
                return result;
            }
            if (pendingTicket != 0) {
                throw new IllegalStateException("A send is already outstanding");
            }
            pendingTicket = ticket;
            Packet packet = codec.decode(bytes);
            // Decide before a test can observe this send and change the flag for later sends.
            boolean complete = completeSends;
            sent.add(packet);
            if (complete) {
                completeSend();
            }
            afterSend.accept(packet);
            return SendResult.ACCEPTED;
        }

        @Override
        public void close() {
            closed = true;
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
