package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testkit.FaultyTransport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;
import static org.mqtt4j.hato.client.internal.MessagingTest.waitFor;

/// Both brokers exercise the same persistent receiver without inspecting broker state, in both
/// protocol versions.
@Timeout(30)
@ParameterizedClass(name = "{0}")
@EnumSource(MqttVersion.class)
abstract class Qos2ReceptionIntegrationTest {

    private static final Duration DELAY = Duration.ofSeconds(30);

    private final MqttVersion version;

    Qos2ReceptionIntegrationTest(MqttVersion version) {
        this.version = version;
    }

    enum Cut {
        BEFORE_PUBLISH, BEFORE_PUBREC, BEFORE_PUBREL, BEFORE_PUBCOMP
    }

    @ParameterizedTest
    @EnumSource(Cut.class)
    void a_continued_exchange_delivers_the_publication_once(Cut cut) throws Exception {
        exchangeAcrossDisconnection(cut, true);
    }

    @Test
    void a_manual_condition_survives_disconnection_without_delivering_the_replay_again()
            throws Exception {
        // Arrange: the first delivery has left the pull queue but has not met its condition.
        String topic = uniqueTopic();
        var scheduler = new ManualTimerScheduler();
        var transports = new LinkedBlockingQueue<FaultyTransport>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient publisher = client();
                MqttClient sut = persistentClient(clientId(), scheduler, transports, ended)) {
            publisher.connect().get(5, SECONDS);
            sut.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.MANUAL)
            );
            subscription.established().get(5, SECONDS);
            publisher.publish(message(topic, (byte) 1)).get(5, SECONDS);
            MqttDelivery original = received(subscription.receive(Duration.ofSeconds(5)));
            FaultyTransport transport = requireNonNull(transports.poll(5, SECONDS));
            transport.disconnect(new IOException("before PUBREC"));
            assertThat(ended.poll(5, SECONDS)).isNotNull();

            // Act: the replay reaches the receiver before the original handle acknowledges it.
            assertSession(sut.connect().get(5, SECONDS), true, 2);
            publicationBarrier(sut);
            original.ack();
            publisher.publish(message(topic, (byte) 2)).get(5, SECONDS);

            // Assert: the marker follows the original publication through the same broker stream.
            MqttDelivery marker = received(subscription.receive(Duration.ofSeconds(5)));
            assertThat(marker.message()).isEqualTo(message(topic, (byte) 2));
            marker.ack();
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            assertThat(ended).isEmpty();
        }
    }

    @Test
    void a_failed_handler_is_retried_after_continuation_before_pubrec() throws Exception {
        // Arrange: failure stops the first reception before it can acknowledge the broker.
        String topic = uniqueTopic();
        var scheduler = new ManualTimerScheduler();
        var transports = new LinkedBlockingQueue<FaultyTransport>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        var calls = new AtomicInteger();
        var cause = new IllegalStateException("application failed before PUBREC");
        try (MqttClient publisher = client();
                MqttClient sut = persistentClient(clientId(), scheduler, transports, ended)) {
            publisher.connect().get(5, SECONDS);
            sut.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.AFTER_HANDLER),
                    delivery -> {
                        if (calls.incrementAndGet() == 1) {
                            throw cause;
                        }
                        delivered.add(delivery);
                    }
            ).established().get(5, SECONDS);
            publisher.publish(message(topic, (byte) 1)).get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(cause, false, 1));

            // Act
            assertSession(sut.connect().get(5, SECONDS), true, 2);
            assertThat(requireNonNull(delivered.poll(5, SECONDS)).message())
                    .isEqualTo(message(topic, (byte) 1));
            publicationBarrier(sut);
            publisher.publish(message(topic, (byte) 2)).get(5, SECONDS);

            // Assert: one retry finishes the reception; no further copy precedes the marker.
            assertThat(requireNonNull(delivered.poll(5, SECONDS)).message())
                    .isEqualTo(message(topic, (byte) 2));
            assertThat(delivered).isEmpty();
            assertThat(calls).hasValue(3);
            assertThat(ended).isEmpty();
        }
    }

    @Test
    void an_on_receive_failure_is_skipped_and_the_connection_continues() throws Exception {
        // Arrange: ON_RECEIVE admits PUBREC before the first handler fails, so the failure holds
        // back no acknowledgement.
        String topic = uniqueTopic();
        var ended = new LinkedBlockingQueue<Disconnection>();
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        var calls = new AtomicInteger();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var cause = new IllegalStateException("application failed after PUBREC");
        try (MqttClient publisher = client(); MqttClient sut = client(ended)) {
            publisher.connect().get(5, SECONDS);
            sut.connect().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.ON_RECEIVE),
                    delivery -> {
                        if (calls.incrementAndGet() == 1) {
                            entered.countDown();
                            waitFor(release);
                            throw cause;
                        }
                        delivered.add(delivery);
                    }
            ).established().get(5, SECONDS);
            publisher.publish(message(topic, (byte) 1)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();

            // Act
            release.countDown();
            await().until(() -> ((ClientActor) sut).reportedHandlerFailures() == 1);
            publicationBarrier(sut);
            publisher.publish(message(topic, (byte) 2)).get(5, SECONDS);

            // Assert: the failure is skipped, so the connection stays and only the new marker
            // starts another handler.
            assertThat(requireNonNull(delivered.poll(5, SECONDS)).message())
                    .isEqualTo(message(topic, (byte) 2));
            assertThat(delivered).isEmpty();
            assertThat(calls).hasValue(2);
            assertThat(ended).isEmpty();
        } finally {
            release.countDown();
        }
    }

    @Test
    void a_handler_can_join_its_qos2_publish_while_a_manual_reception_blocks_pubrec()
            throws Exception {
        // Arrange: a different subscription holds the head of incoming acknowledgement order.
        String base = uniqueTopic();
        var handled = new CompletableFuture<PublishResult>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient sut = client(ended); MqttClient publisher = client()) {
            sut.connect().get(5, SECONDS);
            publisher.connect().get(5, SECONDS);
            Subscription replies = publisher.subscribe(
                    Subscribe.of(new TopicFilter(base + "/reply"), Qos.EXACTLY_ONCE)
            );
            replies.established().get(5, SECONDS);
            Subscription manual = sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/head"), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.MANUAL)
            );
            manual.established().get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/work"), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.AFTER_HANDLER),
                    delivery -> handled.complete(sut.publish(
                            message(base + "/reply", delivery.message().payload()[0])
                    ).join())
            ).established().get(5, SECONDS);
            publisher.publish(message(base + "/head", (byte) 1)).get(5, SECONDS);
            MqttDelivery head = received(manual.receive(Duration.ofSeconds(5)));

            // Act: the handler's outbound PUBREL must not wait for the incoming PUBREC head.
            publisher.publish(message(base + "/work", (byte) 2)).get(5, SECONDS);

            // Assert: the handler completes before the manual reception is acknowledged.
            assertThat(handled.get(5, SECONDS)).isEqualTo(PublishResult.of());
            assertThat(received(replies.receive(Duration.ofSeconds(5))).message())
                    .isEqualTo(message(base + "/reply", (byte) 2));
            assertThat(ended).isEmpty();
            head.ack();
        }
    }

    void exchangeAcrossDisconnection(Cut cut, boolean sessionPresent) throws Exception {
        // Arrange: the client clock is independent of the network delay's virtual clock.
        String id = clientId();
        String topic = uniqueTopic();
        var scheduler = new ManualTimerScheduler();
        var transports = new LinkedBlockingQueue<FaultyTransport>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (MqttClient publisher = client();
                MqttClient sut = persistentClient(id, scheduler, transports, ended)) {
            publisher.connect().get(5, SECONDS);
            assertSession(sut.connect().get(5, SECONDS), false, 1);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.EXACTLY_ONCE)
            );
            subscription.established().get(5, SECONDS);
            assertThat(subscription.grantedQos()).isEqualTo(Qos.EXACTLY_ONCE);
            FaultyTransport transport = requireNonNull(transports.poll(5, SECONDS));
            if (cut == Cut.BEFORE_PUBLISH) {
                transport.receiveDelay(DELAY);
            } else {
                transport.sendDelay(DELAY);
            }
            publisher.publish(message(topic, (byte) 1)).get(5, SECONDS);
            if (cut != Cut.BEFORE_PUBLISH) {
                assertThat(received(subscription.receive(Duration.ofSeconds(5))).message())
                        .isEqualTo(message(topic, (byte) 1));
            }
            awaitDelay(scheduler);
            switch (cut) {
                case BEFORE_PUBLISH, BEFORE_PUBREC -> { }
                case BEFORE_PUBREL, BEFORE_PUBCOMP -> {
                    if (cut == Cut.BEFORE_PUBREL) {
                        transport.receiveDelay(DELAY);
                        transport.sendDelay(Duration.ZERO);
                    }
                    // Let PUBREC reach the broker; the next delay holds PUBREL or PUBCOMP.
                    scheduler.advance(DELAY);
                    awaitDelay(scheduler);
                }
            }

            // Act
            var cause = new IOException("cut " + cut);
            transport.disconnect(cause);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(cause, false, 1));
            assertThat(scheduler.pendingTaskCount()).isZero();
            if (!sessionPresent) {
                try (MqttClient replacement = MqttClient.builder()
                        .server(endpoint())
                        .version(version)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .clientId(id)
                        .build()) {
                    replacement.connect().get(5, SECONDS);
                    replacement.disconnect().get(5, SECONDS);
                }
            }
            assertSession(sut.connect().get(5, SECONDS), sessionPresent, 2);
            // This exchange confirms restoration before a different client sends the marker.
            publicationBarrier(sut);
            publisher.publish(message(topic, (byte) 2)).get(5, SECONDS);

            // Assert: Mosquitto's replay-before-marker order was observed before using it here.
            if (cut == Cut.BEFORE_PUBLISH && sessionPresent) {
                assertThat(received(subscription.receive(Duration.ofSeconds(5))).message())
                        .isEqualTo(message(topic, (byte) 1));
            }
            assertThat(received(subscription.receive(Duration.ofSeconds(5))).message())
                    .isEqualTo(message(topic, (byte) 2));
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            assertThat(ended).isEmpty();
        }
    }

    // An MQTT 5.0 broker may announce limits in its CONNACK, so only the session is compared.
    private static void assertSession(ConnAck connAck, boolean sessionPresent, int attempt) {
        assertThat(connAck.sessionPresent()).isEqualTo(sessionPresent);
        assertThat(connAck.connectionAttempt()).isEqualTo(attempt);
    }

    private static void awaitDelay(ManualTimerScheduler scheduler) {
        // These checks observe virtual-time progress; the default 100 ms only adds wall time.
        await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(1))
                .untilAsserted(() -> assertThat(scheduler.pendingTaskCount()).isEqualTo(1));
    }

    private MqttClient persistentClient(
            String id,
            ManualTimerScheduler scheduler,
            LinkedBlockingQueue<FaultyTransport> transports,
            LinkedBlockingQueue<Disconnection> ended
    ) {
        return MqttClient.builder()
                .server(endpoint())
                .version(version)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId(id)
                .sessionExpiry(Duration.ofSeconds(30))
                .connectionListener(ended::add)
                .transportFactory(connection -> {
                    var transport = new FaultyTransport(Transport.tcp(connection), scheduler);
                    transports.add(transport);
                    return transport;
                })
                .build();
    }

    private MqttClient client() {
        return MqttClient.builder()
                .server(endpoint())
                .version(version)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    private MqttClient client(LinkedBlockingQueue<Disconnection> ended) {
        return MqttClient.builder()
                .server(endpoint())
                .version(version)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .connectionListener(ended::add)
                .build();
    }

    private static String clientId() {
        return "hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19);
    }

    private static MqttMessage message(String topic, byte value) {
        return MqttMessage.of(new Topic(topic), new byte[] {value}, Qos.EXACTLY_ONCE, false);
    }

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }

    private static void publicationBarrier(MqttClient sut) throws Exception {
        sut.publish(message(uniqueTopic(), (byte) 0)).get(5, SECONDS);
    }

    abstract URI endpoint();
}
