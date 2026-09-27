package org.mqtt4j.mqtj.client.internal;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.mqtj.spi.Transport.SendResult.CLOSED;

class ClientActorTest {

    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    private final ManualTimerScheduler scheduler = new ManualTimerScheduler();
    private final LinkedBlockingQueue<Disconnection> ends = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<ScriptTransport> transports = new LinkedBlockingQueue<>();

    private volatile Runnable beforeActorTime = () -> { };

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
                    new Subscribe(new TopicFilter("messages"), Qos.AT_MOST_ONCE, 1),
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
            var other = new Subscribe(new TopicFilter("other"), Qos.AT_MOST_ONCE);
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
                    new Subscribe(new TopicFilter("other"), Qos.AT_MOST_ONCE),
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
                    new Subscribe(new TopicFilter("messages"), Qos.AT_MOST_ONCE, 1),
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
    void received_packet_size_counts_toward_the_delivery_byte_budget() throws Exception {
        // Arrange: three deliveries fit the subscription's slots but not the 32 MiB budget.
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            CompletableFuture<Subscription> subscribing = sut.subscribe(
                    new Subscribe(new TopicFilter("messages"), Qos.AT_MOST_ONCE, 3),
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
        // Arrange: graceful disconnect ends the clean session and its subscription.
        var received = new LinkedBlockingQueue<MqttDelivery>();
        try (MqttClient sut = client()) {
            ScriptTransport oldTransport = connect(sut);
            Subscription oldSubscription = subscribe(sut, oldTransport);
            sut.disconnect().get(5, SECONDS);
            assertThat(ends.poll(5, SECONDS)).isEqualTo(new Disconnection(null, true));

            // Act: dispose the old handle while disconnected and while reconnecting.
            oldSubscription.unsubscribe().get(5, SECONDS);
            CompletableFuture<ConnAck> connection = sut.connect();
            ScriptTransport current = nextTransport();
            oldSubscription.close();
            oldSubscription.unsubscribe().get(5, SECONDS);

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
    void connection_loss_fails_pending_removal_but_later_disposal_succeeds() throws Exception {
        // Arrange: two calls observe one removal that the broker has not acknowledged.
        var failure = new IOException("Peer reset during unsubscribe");
        try (MqttClient sut = client()) {
            ScriptTransport transport = connect(sut);
            Subscription subscription = subscribe(sut, transport);
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

            // Act: there is no subscription left for a later call to remove.
            CompletableFuture<Void> disposed = subscription.unsubscribe();
            subscription.close();

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
            for (int i = 0; i < 257; i++) {
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

    private static Subscribe request() {
        return new Subscribe(new TopicFilter("messages"), Qos.AT_MOST_ONCE);
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

        @Override
        public byte[] encode(Packet packet) {
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
            sent.add(packet);
            if (completeSends) {
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

        final Predicate<CompletableFuture<Void>> apply;

        ApplicationCompletion(Predicate<CompletableFuture<Void>> apply) {
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
}
