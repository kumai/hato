package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.PublishResult;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReconnectPolicy;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.spi.Transport;
import org.mqtt4j.hato.testkit.FaultyTransport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/// The same public client contract runs against Mosquitto and the embedded broker.
abstract class ConnectionTest {

    @Test
    void connection_callbacks_can_publish_and_keep_their_order_while_the_client_progresses()
            throws Exception {
        // Arrange
        var client = new AtomicReference<MqttClient>();
        var events = new LinkedBlockingQueue<Object>();
        var release = new CountDownLatch(1);
        var published = new CompletableFuture<PublishResult>();
        var listener = new ConnectionListener() {
            @Override
            public void onConnected(ConnAck connAck) {
                events.add(connAck);
                if (published.isDone()) {
                    return;
                }
                try {
                    MqttMessage message = MqttMessage.of(
                            new Topic("listener/publication"),
                            new byte[] {1},
                            Qos.AT_LEAST_ONCE,
                            false
                    );
                    PublishResult result = requireNonNull(client.get()).publish(message)
                            .get(5, SECONDS);
                    published.complete(result);
                    release.await();
                } catch (Exception failure) {
                    published.completeExceptionally(failure);
                }
            }

            @Override
            public void onDisconnected(Disconnection disconnection) {
                events.add(disconnection);
            }
        };
        try (MqttClient sut = MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionListener(listener)
                .build()) {
            client.set(sut);
            try {
                // Act & Assert: the first onConnected publishes on its own connection.
                ConnAck first = sut.connect().get(5, SECONDS);
                assertThat(published.get(5, SECONDS)).isEqualTo(PublishResult.of());

                // Act: both operations finish while the first callback remains blocked.
                sut.disconnect().get(5, SECONDS);
                ConnAck second = sut.connect().get(5, SECONDS);

                // Assert: later callbacks wait without holding up I/O or completion.
                assertThat(events).containsExactly(first);
                assertThat(events.poll(5, SECONDS)).isEqualTo(first);
                release.countDown();
                assertThat(events.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
                assertThat(events.poll(5, SECONDS)).isEqualTo(second);
                sut.disconnect().get(5, SECONDS);
                assertThat(events.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 2));
                assertThat(events).isEmpty();
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void public_api_connects_pings_and_disconnects_with_virtual_time() throws Exception {
        // Arrange
        var scheduler = new ManualTimerScheduler();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .keepAlive(Duration.ofSeconds(10))
                .timerScheduler(scheduler)
                .connectionListener(ended::add)
                .build()) {
            // Act & Assert: keep the connection alive across successive ping exchanges.
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
            for (int i = 0; i < 3; i++) {
                scheduler.advance(Duration.ofSeconds(10));
                // CONNECT and PING timers are distinct; a returned PINGRESP cancels its deadline.
                await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
            }
            sut.disconnect().get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(null, true, 1));
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isZero());
        }
    }

    @Test
    void delayed_pingresp_closes_with_a_timeout_and_discards_delayed_input() throws Exception {
        // Arrange
        var scheduler = new ManualTimerScheduler();
        var ended = new LinkedBlockingQueue<Disconnection>();
        var transport = new AtomicReference<FaultyTransport>();
        try (MqttClient sut = MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .keepAlive(Duration.ofSeconds(10))
                .timerScheduler(scheduler)
                .connectionListener(ended::add)
                .transportFactory(id -> {
                    var faulty = new FaultyTransport(Transport.tcp(id), scheduler);
                    transport.set(faulty);
                    return faulty;
                })
                .build()) {
            sut.connect().get(5, SECONDS);
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));

            // Act: delay the response beyond its independent deadline.
            requireNonNull(transport.get()).receiveDelay(Duration.ofSeconds(30));
            scheduler.advance(Duration.ofSeconds(10));
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(3));
            scheduler.advance(Duration.ofSeconds(10));
            Disconnection disconnection = requireNonNull(ended.poll(5, SECONDS));

            // Assert
            assertThat(disconnection.cause().orElseThrow()).isInstanceOf(TimeoutException.class);
            assertThat(disconnection.userRequested()).isFalse();
            await().untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isZero());
            scheduler.advance(Duration.ofSeconds(30));
            assertThat(ended).isEmpty();
        }
    }

    abstract URI endpoint();
}
