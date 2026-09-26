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
import org.mqtt4j.mqtj.MqttProtocolException;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.ReasonCode;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketCodec;
import org.mqtt4j.mqtj.client.internal.common.protocol.PacketDecodeException;
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
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;
import static org.mqtt4j.mqtj.spi.Transport.SendResult.CLOSED;

class ClientActorTest {

    private static final PacketCodec codec = PacketCodec.of(MqttVersion.V3_1_1);

    private final ManualTimerScheduler scheduler = new ManualTimerScheduler();
    private final LinkedBlockingQueue<Disconnection> ends = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<ScriptTransport> transports = new LinkedBlockingQueue<>();

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
    void an_actor_failure_closes_io_fails_operations_and_notifies_once() throws Exception {
        // Arrange
        var failure = new AssertionError("writer adapter failed");
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
    void operation_slots_remain_reserved_until_blocked_continuations_return() throws Exception {
        // Arrange
        var release = new CountDownLatch(1);
        var entered = new AtomicInteger();
        var continuations = new ArrayList<CompletableFuture<Void>>();
        try (MqttClient sut = client()) {
            // Act: retain each operation slot with a blocked continuation.
            for (int i = 0; i < 64; i++) {
                CompletableFuture<ConnAck> connection = sut.connect();
                continuations.add(connection.thenRun(() -> {
                    entered.incrementAndGet();
                    waitFor(release);
                }));
                ScriptTransport transport = nextTransport();
                transport.accept();
                int expected = i + 1;
                await().untilAtomic(entered, is(expected));
                if (i < 63) {
                    sut.disconnect().get(5, SECONDS);
                    requireNonNull(ends.poll(5, SECONDS));
                }
            }

            // Assert: saturation rejects admission until the continuations return.
            assertThatThrownBy(() -> sut.disconnect().join())
                    .hasCauseInstanceOf(MqttCapacityException.class);
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
        return MqttClient.builder()
                .server(URI.create("mqtt://localhost:1883"))
                .version(MqttVersion.V3_1_1)
                .clientId(clientId)
                .keepAlive(Duration.ofSeconds(10))
                .maxPacketSize(maxPacketSize)
                .timerScheduler(scheduler)
                .connectionListener(listener)
                .transportFactory(_ -> {
                    var transport = new ScriptTransport();
                    transports.add(transport);
                    return transport;
                })
                .build();
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

    private static final class ScriptTransport implements Transport {

        final LinkedBlockingQueue<Packet> sent = new LinkedBlockingQueue<>();
        volatile @Nullable Listener listener;
        volatile boolean closed;
        volatile Runnable beforeSend = () -> { };
        Runnable onStart = () -> { };
        boolean connectOnStart = true;
        volatile SendResult sendResult = SendResult.ACCEPTED;
        volatile boolean completeSends = true;
        volatile long pendingTicket;
        @Nullable Throwable startFailure;

        Listener receiver() {
            return requireNonNull(listener);
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
            sent.add(codec.decode(bytes));
            if (completeSends) {
                completeSend();
            }
            return SendResult.ACCEPTED;
        }

        @Override
        public void close() {
            closed = true;
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
