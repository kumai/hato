package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.ClientStats;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.MetricsListener;
import org.mqtt4j.hato.MetricsListener.DeliveryStart;
import org.mqtt4j.hato.MetricsListener.PublishCompletion;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttClientBuilder;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttNotConnectedException;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.ReceiveResult;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.TimerScheduler;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

class MetricsTest {

    // Runs a task on the thread that submits it.
    private static final Executor inPlace = Runnable::run;

    private final ManualTimerScheduler scheduler = new ManualTimerScheduler();
    private final LinkedBlockingQueue<ScriptTransport> transports = new LinkedBlockingQueue<>();

    // The threads on which the client read its clock.
    private final Set<Thread> clockReaders = ConcurrentHashMap.newKeySet();

    // Runs before each clock read with the reading thread; tests use it to fail or hold reads.
    private volatile Consumer<Thread> beforeTime = _ -> { };

    @Test
    void each_admitted_publication_reports_its_completion_once_with_its_duration()
            throws Exception {
        // Arrange
        var completions = new LinkedBlockingQueue<PublishCompletion>();
        try (MqttClient sut = builder(recording(completions, new LinkedBlockingQueue<>()))
                .build()) {
            ScriptTransport transport = connect(sut);

            // Act: QoS 0 completes when written, QoS 1 at PUBACK 5 ms later, and QoS 2 at PUBCOMP
            // 3 ms after PUBREC.
            sut.publish(message(Qos.AT_MOST_ONCE)).get(5, SECONDS);
            next(transport, Packet.Publish.class);
            CompletableFuture<PublishResult> qosOne = sut.publish(message(Qos.AT_LEAST_ONCE));
            var first = (Packet.Publish) next(transport, Packet.Publish.class);
            scheduler.advance(Duration.ofMillis(5));
            transport.receive(new Packet.PubAck(first.packetId(), ReasonCode.SUCCESS, EMPTY));
            qosOne.get(5, SECONDS);
            CompletableFuture<PublishResult> qosTwo = sut.publish(message(Qos.EXACTLY_ONCE));
            var second = (Packet.Publish) next(transport, Packet.Publish.class);
            transport.receive(new Packet.PubRec(second.packetId(), ReasonCode.SUCCESS, EMPTY));
            next(transport, Packet.PubRel.class);
            scheduler.advance(Duration.ofMillis(3));
            transport.receive(new Packet.PubComp(second.packetId(), ReasonCode.SUCCESS, EMPTY));
            qosTwo.get(5, SECONDS);

            // Assert
            assertThat(poll(completions)).isEqualTo(PublishCompletion.of(
                    Qos.AT_MOST_ONCE,
                    Duration.ZERO
            ));
            assertThat(poll(completions)).isEqualTo(PublishCompletion.of(
                    Qos.AT_LEAST_ONCE,
                    Duration.ofMillis(5)
            ));
            assertThat(poll(completions)).isEqualTo(PublishCompletion.of(
                    Qos.EXACTLY_ONCE,
                    Duration.ofMillis(3)
            ));
            assertThat(completions.poll(100, MILLISECONDS)).isNull();
        }
    }

    @Test
    void a_failed_publication_reports_the_failure_of_its_future() throws Exception {
        // Arrange
        var completions = new LinkedBlockingQueue<PublishCompletion>();
        try (MqttClient sut = builder(recording(completions, new LinkedBlockingQueue<>()))
                .build()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<PublishResult> published = sut.publish(message(Qos.AT_LEAST_ONCE));
            next(transport, Packet.Publish.class);

            // Act: the connection ends 2 ms later, before PUBACK.
            scheduler.advance(Duration.ofMillis(2));
            transport.finishClose();

            // Assert
            assertThatThrownBy(() -> published.get(5, SECONDS))
                    .hasCauseInstanceOf(MqttConnectionLostException.class);
            assertThat(poll(completions)).isEqualTo(PublishCompletion.of(
                    Qos.AT_LEAST_ONCE,
                    published.exceptionNow(),
                    Duration.ofMillis(2)
            ));
        }
    }

    @Test
    void a_publication_refused_at_admission_reports_nothing() throws Exception {
        // Arrange
        var completions = new LinkedBlockingQueue<PublishCompletion>();
        try (MqttClient sut = builder(recording(completions, new LinkedBlockingQueue<>()))
                .build()) {

            // Act: before a connection, the client refuses the publication at once.
            CompletableFuture<PublishResult> refused = sut.publish(message(Qos.AT_LEAST_ONCE));
            connect(sut);
            sut.publish(message(Qos.AT_MOST_ONCE)).get(5, SECONDS);

            // Assert: only the admitted publication reports its completion.
            assertThat(refused).isCompletedExceptionally();
            assertThatThrownBy(refused::join).hasCauseInstanceOf(MqttNotConnectedException.class);
            assertThat(poll(completions).qos()).isEqualTo(Qos.AT_MOST_ONCE);
            assertThat(completions.poll(100, MILLISECONDS)).isNull();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("modes")
    void each_delivery_reports_once_when_application_code_receives_it(Mode mode)
            throws Exception {
        // Arrange
        var starts = new LinkedBlockingQueue<DeliveryStart>();
        try (MqttClient sut = builder(recording(new LinkedBlockingQueue<>(), starts)).build()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = mode.subscribe().apply(
                    sut,
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_LEAST_ONCE)
            );
            acknowledgeSubscribes(transport, 1);
            subscription.established().get(5, SECONDS);

            // Act
            transport.receive(qosOnePublish("readings", 1));
            if (mode.pull()) {
                assertThat(subscription.receive(Duration.ofSeconds(5)))
                        .isInstanceOf(ReceiveResult.Received.class);
            }

            // Assert: the manual clock stood still, so the delivery waited no time.
            assertThat(poll(starts)).isEqualTo(DeliveryStart.of(subscription, Duration.ZERO));
            assertThat(starts.poll(100, MILLISECONDS)).isNull();
        }
    }

    @Test
    void a_delivery_waits_from_its_reception_until_its_handler_starts() throws Exception {
        // Arrange: the handler holds the first delivery while the second waits behind it.
        var starts = new LinkedBlockingQueue<DeliveryStart>();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder(recording(new LinkedBlockingQueue<>(), starts)).build()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_LEAST_ONCE),
                    _ -> {
                        entered.countDown();
                        waitFor(release);
                    }
            );
            acknowledgeSubscribes(transport, 1);
            subscription.established().get(5, SECONDS);
            transport.receive(qosOnePublish("readings", 1));
            assertThat(entered.await(5, SECONDS)).isTrue();
            transport.receive(qosOnePublish("readings", 2));

            // Act: 7 ms pass before the handler is free for the second delivery.
            scheduler.advance(Duration.ofMillis(7));
            release.countDown();

            // Assert
            assertThat(poll(starts)).isEqualTo(DeliveryStart.of(subscription, Duration.ZERO));
            assertThat(poll(starts))
                    .isEqualTo(DeliveryStart.of(subscription, Duration.ofMillis(7)));
        } finally {
            release.countDown();
        }
    }

    @Test
    void events_run_on_a_lane_of_their_own_and_a_slow_listener_loses_the_oldest()
            throws Exception {
        // Arrange: the listener holds the first event while 1,030 more arrive.
        var threads = new LinkedBlockingQueue<String>();
        var received = new AtomicInteger();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var slow = new MetricsListener() {
            @Override
            public void onDeliveryStarted(DeliveryStart start) {
                threads.add(Thread.currentThread().getName());
                received.incrementAndGet();
                entered.countDown();
                waitFor(release);
            }
        };
        var handled = new AtomicInteger();
        try (MqttClient sut = builder(slow).build()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_MOST_ONCE)
                            .withBufferCapacity(2048),
                    _ -> handled.incrementAndGet()
            );
            acknowledgeSubscribes(transport, 1);
            subscription.established().get(5, SECONDS);
            transport.receive(qosZeroPublish("readings"));
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            for (int i = 0; i < 1030; i++) {
                transport.receive(qosZeroPublish("readings"));
            }
            await().untilAsserted(() -> assertThat(handled.get()).isEqualTo(1031));

            // Assert: 1,024 events wait behind the running one, and the 6 oldest were dropped.
            ClientStats stats = sut.stats().get(5, SECONDS);
            assertThat(stats.droppedMetricEvents()).isEqualTo(6);
            release.countDown();
            await().untilAsserted(() -> assertThat(received.get()).isEqualTo(1025));
            assertThat(threads).containsOnly("hato-metrics-test-metrics");
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_throwing_listener_stops_neither_later_events_nor_the_client() throws Exception {
        // Arrange
        var completions = new LinkedBlockingQueue<PublishCompletion>();
        var throwing = new MetricsListener() {
            @Override
            public void onPublishCompleted(PublishCompletion completion) {
                completions.add(completion);
                if (completions.size() == 1) {
                    throw new IllegalStateException("Listener failed");
                }
            }
        };
        try (MqttClient sut = builder(throwing).build()) {
            connect(sut);

            // Act
            sut.publish(message(Qos.AT_MOST_ONCE)).get(5, SECONDS);
            sut.publish(message(Qos.AT_MOST_ONCE)).get(5, SECONDS);

            // Assert
            await().untilAsserted(() -> assertThat(completions).hasSize(2));
            sut.publish(message(Qos.AT_MOST_ONCE)).get(5, SECONDS);
            await().untilAsserted(() -> assertThat(completions).hasSize(3));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("failingReads")
    void a_failed_clock_read_leaves_only_its_event_unreported(FailingReads reads)
            throws Exception {
        // Arrange
        var completions = new LinkedBlockingQueue<PublishCompletion>();
        var starts = new LinkedBlockingQueue<DeliveryStart>();
        var handled = new AtomicInteger();
        try (MqttClient sut = builder(recording(completions, starts)).build()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_MOST_ONCE),
                    _ -> handled.incrementAndGet()
            );
            acknowledgeSubscribes(transport, 1);
            subscription.established().get(5, SECONDS);

            // Act: a publication and a delivery while the reads fail, then one of each after.
            beforeTime = thread -> {
                if (reads.threads().test(thread)) {
                    throw new IllegalStateException("Clock failed");
                }
            };
            sut.publish(message(Qos.AT_MOST_ONCE)).get(5, SECONDS);
            transport.receive(qosZeroPublish("readings"));
            await().untilAsserted(() -> assertThat(handled.get()).isOne());
            beforeTime = _ -> { };
            sut.publish(message(Qos.AT_MOST_ONCE)).get(5, SECONDS);
            transport.receive(qosZeroPublish("readings"));

            // Assert: the client went on, and only the events whose reads failed are missing.
            await().untilAsserted(() -> assertThat(handled.get()).isEqualTo(2));
            for (int i = 0; i < reads.completions(); i++) {
                poll(completions);
            }
            for (int i = 0; i < reads.starts(); i++) {
                poll(starts);
            }
            assertThat(completions.poll(100, MILLISECONDS)).isNull();
            assertThat(starts.poll(100, MILLISECONDS)).isNull();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource closes on failure.
    void termination_waits_for_a_publication_whose_completion_is_being_timed()
            throws Exception {
        // Arrange: a QoS 1 publication waits for its PUBACK, and its completion's clock read
        // will hold.
        var completions = new LinkedBlockingQueue<PublishCompletion>();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = builder(recording(completions, new LinkedBlockingQueue<>()))
                .build()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<PublishResult> published = sut.publish(message(Qos.AT_LEAST_ONCE));
            next(transport, Packet.Publish.class);
            beforeTime = thread -> {
                if (thread.getName().contains("-complete-")) {
                    entered.countDown();
                    waitFor(release);
                }
            };

            // Act: closing fails the publication, and its completion reads the clock.
            sut.close();
            CompletableFuture<Void> terminated = sut.terminated();

            // Assert
            assertThat(entered.await(5, SECONDS)).isTrue();
            await().until(() -> ((ClientActor) sut).terminationPending()
                    .equals(Set.of(ClientTermination.Pending.COMPLETIONS)));
            assertThat(terminated).isNotDone();
            release.countDown();
            terminated.get(5, SECONDS);
            assertThat(poll(completions).failure()).containsSame(published.exceptionNow());
        } finally {
            release.countDown();
        }
    }

    @Test
    void without_a_listener_no_timing_reads_the_clock() throws Exception {
        // Arrange: the threads that time publications and deliveries for a listener.
        var handlerThreads = new LinkedBlockingQueue<Thread>();
        var completionThreads = new LinkedBlockingQueue<Thread>();
        try (MqttClient sut = clientBuilder().build()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter("readings"), Qos.AT_LEAST_ONCE),
                    _ -> handlerThreads.add(Thread.currentThread())
            );
            acknowledgeSubscribes(transport, 1);
            subscription.established().get(5, SECONDS);
            clockReaders.clear();

            // Act
            CompletableFuture<PublishResult> published = sut.publish(message(Qos.AT_LEAST_ONCE));
            published.whenComplete((_, _) -> completionThreads.add(Thread.currentThread()));
            var publication = (Packet.Publish) next(transport, Packet.Publish.class);
            transport.receive(new Packet.PubAck(publication.packetId(), ReasonCode.SUCCESS, EMPTY));
            published.get(5, SECONDS);
            transport.receive(qosOnePublish("readings", 1));

            // Assert
            Thread handler = requireNonNull(handlerThreads.poll(5, SECONDS));
            Thread completion = requireNonNull(completionThreads.poll(5, SECONDS));
            assertThat(clockReaders).doesNotContain(Thread.currentThread(), handler, completion);
            assertThat(sut.stats().get(5, SECONDS).droppedMetricEvents()).isZero();
        }
    }

    @Test
    @SuppressWarnings("try") // Explicit close is under test; the resource closes on failure.
    void termination_waits_for_the_events_that_the_listener_has_not_received()
            throws Exception {
        // Arrange: the listener holds the completion of a publication.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var slow = new MetricsListener() {
            @Override
            public void onPublishCompleted(PublishCompletion completion) {
                entered.countDown();
                waitFor(release);
            }
        };
        try (MqttClient sut = builder(slow).build()) {
            connect(sut);
            sut.publish(message(Qos.AT_MOST_ONCE)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            sut.close();
            CompletableFuture<Void> terminated = sut.terminated();

            // Assert
            await().until(() -> ((ClientActor) sut).terminationPending()
                    .equals(Set.of(ClientTermination.Pending.LISTENER)));
            assertThat(terminated).isNotDone();
            release.countDown();
            terminated.get(5, SECONDS);
        } finally {
            release.countDown();
        }
    }

    static Stream<Mode> modes() {
        return Stream.of(
                new Mode("serial", (client, request) -> client.subscribe(request, _ -> { })),
                new Mode("executor", (client, request) -> client.subscribe(
                        request,
                        DeliveryOptions.serial().withExecutor(inPlace),
                        _ -> { }
                )),
                new Mode("striped", (client, request) -> client.subscribe(
                        request,
                        DeliveryOptions.striped(2),
                        _ -> { }
                )),
                new Mode("parallel", (client, request) -> client.subscribe(
                        request,
                        DeliveryOptions.parallel(2),
                        _ -> { }
                )),
                new Mode("lanes", (client, request) -> client.subscribe(
                        request,
                        DeliveryOptions.lanes(_ -> inPlace),
                        _ -> { }
                )),
                new Mode("hand-off", (client, request) -> client.handOff(request, _ -> true)),
                new Mode("pull", MqttClient::subscribe, true)
        );
    }

    static Stream<FailingReads> failingReads() {
        return Stream.of(
                new FailingReads(
                        "application threads",
                        thread -> !thread.getName().startsWith("hato-"),
                        1,
                        1
                ),
                new FailingReads(
                        "completion threads",
                        thread -> thread.getName().contains("-complete-"),
                        1,
                        2
                ),
                new FailingReads(
                        "delivery threads",
                        thread -> thread.getName().contains("-delivery-"),
                        2,
                        1
                )
        );
    }

    /// Threads whose clock reads fail, and how many of two publications and of two deliveries
    /// are then reported. The test's own thread publishes and, through the scripted transport,
    /// receives.
    record FailingReads(String name, Predicate<Thread> threads, int completions, int starts) {

        @Override
        public String toString() {
            return name;
        }
    }

    /// How a subscription takes its deliveries: through a handler, an acceptor, or receive().
    record Mode(
            String name,
            BiFunction<MqttClient, Subscribe, Subscription> subscribe,
            boolean pull
    ) {

        Mode(String name, BiFunction<MqttClient, Subscribe, Subscription> subscribe) {
            this(name, subscribe, false);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /// A listener that records the events it receives.
    private static MetricsListener recording(
            LinkedBlockingQueue<PublishCompletion> completions,
            LinkedBlockingQueue<DeliveryStart> starts
    ) {
        return new MetricsListener() {
            @Override
            public void onPublishCompleted(PublishCompletion completion) {
                completions.add(completion);
            }

            @Override
            public void onDeliveryStarted(DeliveryStart start) {
                starts.add(start);
            }
        };
    }

    private MqttClientBuilder builder(MetricsListener listener) {
        return clientBuilder().metricsListener(listener);
    }

    private MqttClientBuilder clientBuilder() {
        var clock = new TimerScheduler() {
            @Override
            public long nanoTime() {
                clockReaders.add(Thread.currentThread());
                beforeTime.accept(Thread.currentThread());
                return scheduler.nanoTime();
            }

            @Override
            public Cancellable schedule(Duration delay, Runnable task) {
                return scheduler.schedule(delay, task);
            }
        };
        return MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .clientId("metrics-test")
                .keepAlive(Duration.ZERO)
                .timerScheduler(clock)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                });
    }

    private ScriptTransport connect(MqttClient client) throws Exception {
        CompletableFuture<ConnAck> connected = client.connect();
        ScriptTransport transport = requireNonNull(transports.poll(5, SECONDS));
        assertThat(transport.sent.poll(5, SECONDS)).isInstanceOf(Packet.Connect.class);
        transport.accept();
        connected.get(5, SECONDS);
        return transport;
    }

    /// Answers the next `count` SUBSCRIBE packets with the QoS that each requested.
    private static void acknowledgeSubscribes(ScriptTransport transport, int count)
            throws Exception {
        for (int i = 0; i < count; i++) {
            var packet = (Packet.Subscribe) next(transport, Packet.Subscribe.class);
            transport.receive(new Packet.SubAck(
                    packet.packetId(),
                    List.of(ReasonCode.of(packet.subscriptions().getFirst().qos().level())),
                    EMPTY
            ));
        }
    }

    private static Packet next(ScriptTransport transport, Class<? extends Packet> type)
            throws Exception {
        Packet packet = requireNonNull(transport.sent.poll(5, SECONDS));
        assertThat(packet).isInstanceOf(type);
        return packet;
    }

    private static <T> T poll(LinkedBlockingQueue<T> events) throws InterruptedException {
        return requireNonNull(events.poll(5, SECONDS));
    }

    private static MqttMessage message(Qos qos) {
        return MqttMessage.of(new Topic("out"), new byte[] {1}, qos, false);
    }

    private static Packet.Publish qosZeroPublish(String topic) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1},
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                EMPTY
        );
    }

    private static Packet.Publish qosOnePublish(String topic, int packetId) {
        return new Packet.Publish(
                new Topic(topic),
                new byte[] {1},
                Qos.AT_LEAST_ONCE,
                false,
                false,
                packetId,
                EMPTY
        );
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            if (!latch.await(30, SECONDS)) {
                throw new AssertionError("Test failed to release the listener");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }
}
