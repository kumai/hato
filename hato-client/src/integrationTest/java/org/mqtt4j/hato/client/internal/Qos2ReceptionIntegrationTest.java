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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.spi.Transport;
import org.mqtt4j.hato.testkit.FaultyTransport;
import org.mqtt4j.hato.testkit.ManualTimerScheduler;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;
import static org.mqtt4j.hato.client.internal.MessagingTest.waitFor;

/// Both brokers exercise the same persistent receiver without inspecting broker state.
@Timeout(30)
abstract class Qos2ReceptionIntegrationTest {

    private static final Duration DELAY = Duration.ofSeconds(30);

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
            ).get(5, SECONDS);
            publisher.publish(message(topic, (byte) 1)).get(5, SECONDS);
            MqttDelivery original = received(subscription.receive(Duration.ofSeconds(5)));
            FaultyTransport transport = requireNonNull(transports.poll(5, SECONDS));
            transport.disconnect(new IOException("before PUBREC"));
            assertThat(ended.poll(5, SECONDS)).isNotNull();

            // Act: the replay reaches the receiver before the original handle acknowledges it.
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(true));
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
            ).get(5, SECONDS);
            publisher.publish(message(topic, (byte) 1)).get(5, SECONDS);
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(cause, false));

            // Act
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(true));
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
    void a_handler_failure_after_pubrec_does_not_redeliver_the_continued_reception()
            throws Exception {
        // Arrange: ON_RECEIVE admits PUBREC while the first handler is still running.
        String topic = uniqueTopic();
        var scheduler = new ManualTimerScheduler();
        var transports = new LinkedBlockingQueue<FaultyTransport>();
        var ended = new LinkedBlockingQueue<Disconnection>();
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        var calls = new AtomicInteger();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var cause = new IllegalStateException("application failed after PUBREC");
        try (MqttClient publisher = client();
                MqttClient sut = persistentClient(clientId(), scheduler, transports, ended)) {
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
            ).get(5, SECONDS);
            FaultyTransport transport = requireNonNull(transports.poll(5, SECONDS));
            transport.sendDelay(DELAY);
            publisher.publish(message(topic, (byte) 1)).get(5, SECONDS);
            assertThat(entered.await(5, SECONDS)).isTrue();
            awaitDelay(scheduler);
            transport.receiveDelay(DELAY);
            transport.sendDelay(Duration.ZERO);
            scheduler.advance(DELAY);
            // The delayed PUBREL proves the broker received PUBREC before the handler failed.
            awaitDelay(scheduler);

            // Act
            release.countDown();
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(cause, false));
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(true));
            publicationBarrier(sut);
            publisher.publish(message(topic, (byte) 2)).get(5, SECONDS);

            // Assert: the exchange resumes, but only the new marker starts another handler.
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
            ).get(5, SECONDS);
            Subscription manual = sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/head"), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.MANUAL)
            ).get(5, SECONDS);
            sut.subscribe(
                    Subscribe.of(new TopicFilter(base + "/work"), Qos.EXACTLY_ONCE)
                            .withAckMode(AckMode.AFTER_HANDLER),
                    delivery -> handled.complete(sut.publish(
                            message(base + "/reply", delivery.message().payload()[0])
                    ).join())
            ).get(5, SECONDS);
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
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false));
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.EXACTLY_ONCE)
            ).get(5, SECONDS);
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
            assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(cause, false));
            assertThat(scheduler.pendingTaskCount()).isZero();
            if (!sessionPresent) {
                try (MqttClient replacement = MqttClient.builder()
                        .server(endpoint())
                        .version(MqttVersion.V3_1_1)
                        .clientId(id)
                        .build()) {
                    replacement.connect().get(5, SECONDS);
                    replacement.disconnect().get(5, SECONDS);
                }
            }
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(sessionPresent));
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
                .version(MqttVersion.V3_1_1)
                .clientId(id)
                .cleanStart(false)
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
        return MqttClient.builder().server(endpoint()).version(MqttVersion.V3_1_1).build();
    }

    private MqttClient client(LinkedBlockingQueue<Disconnection> ended) {
        return MqttClient.builder()
                .server(endpoint())
                .version(MqttVersion.V3_1_1)
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
