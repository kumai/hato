package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import javax.net.ssl.SSLServerSocket;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttClientBuilder;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.TimerScheduler;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;
import org.mqtt4j.hato.testsupport.Tls;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SuppressWarnings("try") // Explicit close starts the TLS close_notify grace period.
class TransportTimerTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void default_transports_measure_their_connection_deadline_on_the_injected_clock(boolean tls)
            throws Exception {
        // Arrange
        var clock = new RecordingClock();
        var timeout = Duration.ofSeconds(5);
        try (ServerSocket server = tls ? Tls.serverSocket() : ScriptedBroker.listen()) {
            String scheme = tls ? "mqtts" : "mqtt";
            MqttClientBuilder builder = MqttClient.builder()
                    .server(URI.create(scheme + "://localhost:" + server.getLocalPort()))
                    .version(MqttVersion.V3_1_1)
                    .reconnectPolicy(ReconnectPolicy.disabled())
                    .keepAlive(Duration.ZERO)
                    .connectionTimeout(timeout)
                    .timerScheduler(clock);
            if (tls) {
                builder.sslContext(Tls.clientContext());
            }
            try (MqttClient sut = builder.build()) {
                // Act
                try (ScriptedBroker _ = ScriptedBroker.connect(server, sut)) {
                    // Assert: the client and transport each schedule their connection deadline.
                    assertThat(clock.scheduled).containsExactly(timeout, timeout);
                    assertThat(clock.manual.pendingTaskCount()).isZero();
                }
            }
        }
    }

    @Test
    void the_default_tls_transport_measures_close_notification_grace_on_the_injected_clock()
            throws Exception {
        // Arrange
        var clock = new RecordingClock();
        try (SSLServerSocket server = Tls.serverSocket();
                MqttClient sut = MqttClient.builder()
                        .server(URI.create("mqtts://localhost:" + server.getLocalPort()))
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .sslContext(Tls.clientContext())
                        .keepAlive(Duration.ZERO)
                        .timerScheduler(clock)
                        .build()) {
            try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                clock.scheduled.clear();

                // Act
                sut.close();

                // Assert: provider close may finish sooner, but its grace always uses this clock.
                assertThat(clock.scheduled.poll(5, SECONDS)).isEqualTo(Duration.ofSeconds(1));
                clock.manual.advance(Duration.ofSeconds(1));
                peer.expectClosed();
                await().pollInterval(Duration.ofMillis(1)).untilAsserted(
                        () -> assertThat(clock.manual.pendingTaskCount()).isZero()
                );
            }
        }
    }

    @Test
    void stopping_the_borrowed_scheduler_does_not_prevent_tls_socket_cleanup() throws Exception {
        // Arrange
        try (var executor = new ScheduledThreadPoolExecutor(1);
                SSLServerSocket server = Tls.serverSocket()) {
            var clock = new TimerScheduler() {
                @Override
                public long nanoTime() {
                    return System.nanoTime();
                }

                @Override
                public Cancellable schedule(Duration delay, Runnable task) {
                    ScheduledFuture<?> scheduled = executor.schedule(
                            task,
                            delay.toNanos(),
                            NANOSECONDS
                    );
                    return () -> scheduled.cancel(false);
                }
            };
            try (MqttClient sut = MqttClient.builder()
                    .server(URI.create("mqtts://localhost:" + server.getLocalPort()))
                    .version(MqttVersion.V3_1_1)
                    .reconnectPolicy(ReconnectPolicy.disabled())
                    .sslContext(Tls.clientContext())
                    .keepAlive(Duration.ZERO)
                    .timerScheduler(clock)
                    .build()) {
                try (ScriptedBroker peer = ScriptedBroker.connect(server, sut)) {
                    // Act: close cannot reserve its TLS grace period on the stopped scheduler.
                    executor.shutdown();
                    sut.close();

                    // Assert: abort still closes the underlying TCP socket and releases the peer.
                    peer.expectClosed();
                }
            }
        }
    }

    private static final class RecordingClock implements TimerScheduler {

        final ManualTimerScheduler manual = new ManualTimerScheduler();
        final LinkedBlockingQueue<Duration> scheduled = new LinkedBlockingQueue<>();

        @Override
        public long nanoTime() {
            return manual.nanoTime();
        }

        @Override
        public Cancellable schedule(Duration delay, Runnable task) {
            Cancellable cancellation = manual.schedule(delay, task);
            scheduled.add(delay);
            return cancellation;
        }
    }
}
