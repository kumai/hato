package org.mqtt4j.hato.client.internal;

import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.ConnAck;
import org.mqtt4j.hato.ConnectionListener;
import org.mqtt4j.hato.Disconnection;
import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttConnectionLostException;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
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
import org.mqtt4j.hato.broker.MqttBroker;
import org.mqtt4j.hato.client.internal.common.protocol.Packet;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.endpoint;
import static org.mqtt4j.hato.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.hato.client.internal.common.protocol.Properties.EMPTY;

/// Raw peers expose replay order; real brokers verify the same client's persistent session.
@Timeout(30)
class SessionPersistenceIntegrationTest {

    private final String clientId = "hato" + UUID.randomUUID().toString().replace("-", "")
            .substring(0, 19);

    @Test
    void socket_loss_replays_the_original_identifiers_in_order_without_reviving_failed_futures()
            throws Exception {
        var ended = new LinkedBlockingQueue<Disconnection>();
        var publications = new ArrayList<CompletableFuture<PublishResult>>();
        var original = new ArrayList<Packet.Publish>();
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            try (ScriptedBroker peer = connect(server, sut, false)) {
                for (int value = 1; value <= 3; value++) {
                    publications.add(sut.publish(message("data", (byte) value)));
                }
                for (int value = 1; value <= 3; value++) {
                    var publication = assertInstanceOf(Packet.Publish.class, peer.receive());
                    assertThat(publication.payload()).containsExactly((byte) value);
                    assertThat(publication.dup()).isFalse();
                    original.add(publication);
                }
                assertThat(publications).allSatisfy(future -> assertThat(future).isNotDone());
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();
            var failures = new ArrayList<Throwable>();
            for (CompletableFuture<PublishResult> publication : publications) {
                Throwable failure = failureOf(publication);
                assertThat(failure).isInstanceOf(MqttConnectionLostException.class);
                failures.add(failure);
            }

            // Every replay precedes a newly published marker on the same output stream.
            try (ScriptedBroker peer = connect(server, sut, true)) {
                for (Packet.Publish previous : original) {
                    assertThat(peer.receive()).isEqualTo(
                            new Packet.Publish(
                                    previous.topic(),
                                    previous.payload(),
                                    previous.qos(),
                                    previous.retain(),
                                    true,
                                    previous.packetId(),
                                    previous.properties()
                            )
                    );
                    peer.send(new Packet.PubAck(previous.packetId(), ReasonCode.SUCCESS, EMPTY));
                }
                peer.roundTripBarrier(sut);
                for (int index = 0; index < publications.size(); index++) {
                    assertThat(failureOf(publications.get(index))).isSameAs(failures.get(index));
                }
            }
        }
    }

    @Test
    void a_missing_broker_session_discards_old_publications_before_a_new_marker() throws Exception {
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            CompletableFuture<PublishResult> publication;
            try (ScriptedBroker peer = connect(server, sut, false)) {
                publication = sut.publish(message("old", (byte) 1));
                assertThat(peer.receive()).isInstanceOf(Packet.Publish.class);
                assertThat(publication).isNotDone();
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();
            Throwable failure = failureOf(publication);
            assertThat(failure).isInstanceOf(MqttConnectionLostException.class);

            try (ScriptedBroker peer = connect(server, sut, false)) {
                // If the old publication were replayed, it would occupy the marker's position.
                peer.roundTripBarrier(sut);
                assertThat(failureOf(publication)).isSameAs(failure);
            }
        }
    }

    @Test
    void a_retained_session_uses_the_existing_subscription_without_sending_subscribe_again()
            throws Exception {
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            Subscription subscription;
            try (ScriptedBroker peer = connect(server, sut, false)) {
                subscription = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();

            try (ScriptedBroker peer = connect(server, sut, true)) {
                peer.roundTripBarrier(sut);
                peer.publish("data", 7);
                var received = assertInstanceOf(
                        ReceiveResult.Received.class,
                        subscription.receive(Duration.ofSeconds(5))
                );
                assertThat(received.delivery().message().payload()).containsExactly((byte) 1);
                assertThat(subscription.grantedQos()).isEqualTo(Qos.AT_LEAST_ONCE);
                peer.expectAcknowledgements(7, 1);
                peer.roundTripBarrier(sut);
            }
        }
    }

    @ParameterizedTest(name = "acknowledge while disconnected: {0}")
    @ValueSource(booleans = {false, true})
    void handed_off_manual_deliveries_join_retransmissions_and_keep_their_acknowledgements(
            boolean acknowledgeOffline
    ) throws Exception {
        // Arrange: both deliveries have left the pull queue before the connection ends.
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            Subscription subscription;
            MqttDelivery first;
            MqttDelivery second;
            try (ScriptedBroker peer = connect(server, sut, false)) {
                subscription = peer.subscribePull(sut, "data", AckMode.MANUAL, 2);
                peer.publishBurst("data", 7, 2, false);
                first = received(subscription.receive(Duration.ofSeconds(5)));
                second = received(subscription.receive(Duration.ofSeconds(5)));
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();
            if (acknowledgeOffline) {
                second.ack();
            }

            // Act: no acknowledgement belongs to the new wire order until its replay arrives.
            try (ScriptedBroker peer = connect(server, sut, true)) {
                peer.roundTripBarrier(sut);
                peer.publishBurst("data", 7, 2, true);
                peer.roundTripBarrier(sut);
                assertThat(subscription.receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.TimedOut());
                if (!acknowledgeOffline) {
                    second.ack();
                }
                peer.expectPublicationBarrier(sut);
                first.ack();

                // Assert: the original handles complete each replay once, in receive order.
                peer.expectAcknowledgements(7, 2);
                first.ack();
                second.ack();
                peer.roundTripBarrier(sut);
            }
        }
    }

    @Test
    void a_running_handler_finishes_the_reception_that_survived_reconnection() throws Exception {
        // Arrange
        var ended = new LinkedBlockingQueue<Disconnection>();
        var delivered = new LinkedBlockingQueue<MqttDelivery>();
        var release = new CountDownLatch(1);
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            try (ScriptedBroker peer = connect(server, sut, false)) {
                peer.subscribe(sut, "data", AckMode.AFTER_HANDLER, delivery -> {
                    delivered.add(delivery);
                    waitFor(release);
                });
                peer.publishBurst("data", 7, 1, false);
                assertThat(delivered.poll(5, SECONDS)).isNotNull();
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();

            // Act: the retransmission joins while the original handler still owns the delivery.
            try (ScriptedBroker peer = connect(server, sut, true)) {
                peer.publishBurst("data", 7, 1, true);
                peer.roundTripBarrier(sut);
                release.countDown();

                // Assert: a later callback on this lane proves the replay was not handed twice.
                peer.expectAcknowledgements(7, 1);
                sendMarker(peer, "data");
                assertThat(requireNonNull(delivered.poll(5, SECONDS)).message().qos())
                        .isEqualTo(Qos.AT_MOST_ONCE);
                assertThat(delivered).isEmpty();
                peer.roundTripBarrier(sut);
            }
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest(name = "failure after reconnect: {0}")
    @ValueSource(booleans = {false, true})
    void a_late_handler_failure_leaves_the_retained_reception_available_for_manual_retry(
            boolean connectedWhenFailed
    ) throws Exception {
        // Arrange: the first handler outlives the connection on which its delivery arrived.
        var ended = new LinkedBlockingQueue<Disconnection>();
        var workers = new LinkedBlockingQueue<Thread>();
        var retried = new LinkedBlockingQueue<MqttDelivery>();
        var calls = new AtomicInteger();
        var release = new CountDownLatch(1);
        var failure = new IllegalStateException("old handler failed");
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            Thread worker;
            try (ScriptedBroker peer = connect(server, sut, false)) {
                peer.subscribe(sut, "data", AckMode.AFTER_HANDLER, delivery -> {
                    if (calls.incrementAndGet() == 1) {
                        workers.add(Thread.currentThread());
                        waitFor(release);
                        throw failure;
                    }
                    retried.add(delivery);
                });
                peer.publishBurst("data", 7, 1, false);
                worker = requireNonNull(workers.poll(5, SECONDS));
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();

            // Act: the same failure ends a continued connection, or only retires offline work.
            if (connectedWhenFailed) {
                try (ScriptedBroker peer = connect(server, sut, true)) {
                    peer.publishBurst("data", 7, 1, true);
                    peer.roundTripBarrier(sut);
                    release.countDown();
                    peer.expectClosed();
                    assertThat(ended.poll(5, SECONDS))
                            .isEqualTo(Disconnection.of(failure, false, 2));
                }
            } else {
                release.countDown();
                await().until(() -> worker.getState() == Thread.State.WAITING);
                assertThat(ended).isEmpty();
            }

            // Assert: the next explicit connection retries the failed delivery and unblocks ACK.
            try (ScriptedBroker peer = connect(server, sut, true)) {
                peer.publishBurst("data", 7, 1, true);
                assertThat(requireNonNull(retried.poll(5, SECONDS)).dup()).isTrue();
                peer.expectAcknowledgements(7, 1);
                peer.roundTripBarrier(sut);
                assertThat(calls).hasValue(2);
                assertThat(ended).isEmpty();
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void reconnect_redistributes_only_recipients_that_had_not_taken_the_delivery()
            throws Exception {
        // Arrange: one overlapping pull subscription has completed both views; the other has none.
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            Subscription completed;
            Subscription queued;
            try (ScriptedBroker peer = connect(server, sut, false)) {
                completed = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 2);
                queued = peer.subscribePull(sut, "#", AckMode.MANUAL, 2);
                peer.publishBurst("data", 7, 2, false);
                received(completed.receive(Duration.ofSeconds(5)));
                received(completed.receive(Duration.ofSeconds(5)));
                peer.roundTripBarrier(sut);
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();
            assertThat(queued.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());

            // Act
            try (ScriptedBroker peer = connect(server, sut, true)) {
                peer.publishBurst("data", 7, 2, true);
                MqttDelivery first = received(queued.receive(Duration.ofSeconds(5)));
                MqttDelivery second = received(queued.receive(Duration.ofSeconds(5)));

                // Assert: only discarded views return, and their per-subscription order is stable.
                assertThat(first.message().payload()).containsExactly((byte) 7);
                assertThat(second.message().payload()).containsExactly((byte) 8);
                assertThat(completed.receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.TimedOut());
                second.ack();
                peer.expectPublicationBarrier(sut);
                first.ack();
                peer.expectAcknowledgements(7, 2);
                peer.roundTripBarrier(sut);
            }
        }
    }

    @Test
    void a_successful_overlap_is_kept_while_only_the_failed_handler_is_retried() throws Exception {
        // Arrange
        var ended = new LinkedBlockingQueue<Disconnection>();
        var successful = new LinkedBlockingQueue<MqttDelivery>();
        var failed = new LinkedBlockingQueue<MqttDelivery>();
        var calls = new AtomicInteger();
        var release = new CountDownLatch(1);
        var failure = new IllegalStateException("one recipient failed");
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            MqttDelivery old;
            try (ScriptedBroker peer = connect(server, sut, false)) {
                peer.subscribe(sut, "data", AckMode.MANUAL, successful::add);
                peer.subscribe(sut, "#", AckMode.MANUAL, delivery -> {
                    failed.add(delivery);
                    if (calls.incrementAndGet() == 1) {
                        waitFor(release);
                        throw failure;
                    }
                });
                peer.publishBurst("data", 7, 1, false);
                requireNonNull(successful.poll(5, SECONDS)).ack();
                old = requireNonNull(failed.poll(5, SECONDS));
                release.countDown();
                peer.expectClosed();
                assertThat(ended.poll(5, SECONDS)).isEqualTo(Disconnection.of(failure, false, 1));
            }

            // Act
            try (ScriptedBroker peer = connect(server, sut, true)) {
                peer.publishBurst("data", 7, 1, true);
                MqttDelivery retry = requireNonNull(failed.poll(5, SECONDS));
                sendMarker(peer, "data");

                // Assert: both lane sentinels exclude a second handoff to the successful view.
                assertThat(requireNonNull(successful.poll(5, SECONDS)).message().qos())
                        .isEqualTo(Qos.AT_MOST_ONCE);
                assertThat(requireNonNull(failed.poll(5, SECONDS)).message().qos())
                        .isEqualTo(Qos.AT_MOST_ONCE);
                assertThat(successful).isEmpty();
                old.ack();
                peer.expectPublicationBarrier(sut);
                retry.ack();
                peer.expectAcknowledgements(7, 1);
                peer.roundTripBarrier(sut);
            }
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest(name = "new publication DUP: {0}")
    @ValueSource(booleans = {false, true})
    void a_discarded_sessions_ack_cannot_complete_a_new_reception_with_the_same_identifier(
            boolean duplicate
    ) throws Exception {
        // Arrange
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            Subscription subscription;
            MqttDelivery old;
            try (ScriptedBroker peer = connect(server, sut, false)) {
                subscription = peer.subscribePull(sut, "data", AckMode.MANUAL, 1);
                peer.publish("data", 7);
                old = received(subscription.receive(Duration.ofSeconds(5)));
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();

            // Act: Session Present 0 restores the subscription with fresh receive state.
            try (ScriptedBroker peer = connect(server, sut, false)) {
                var restored = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                peer.send(
                        new Packet.SubAck(
                                restored.packetId(),
                                List.of(ReasonCode.GRANTED_QOS_1),
                                EMPTY
                        )
                );
                peer.publishBurst("data", 7, 1, duplicate);
                MqttDelivery current = received(subscription.receive(Duration.ofSeconds(5)));
                old.ack();

                // Assert
                peer.roundTripBarrier(sut);
                current.ack();
                peer.expectAcknowledgements(7, 1);
                assertThat(current).isNotSameAs(old);
                peer.roundTripBarrier(sut);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = AckMode.class, names = {"ON_RECEIVE", "MANUAL"})
    void a_continued_ack_head_does_not_retain_completed_pull_capacity(AckMode mode)
            throws Exception {
        // Arrange: the head is handed off, while a one-slot tail repeatedly returns its capacity.
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            MqttDelivery first;
            Subscription tail;
            try (ScriptedBroker peer = connect(server, sut, false)) {
                Subscription head = peer.subscribePull(sut, "head", AckMode.MANUAL, 1);
                tail = peer.subscribePull(sut, "tail", mode, 1);
                peer.publish("head", 1);
                first = received(head.receive(Duration.ofSeconds(5)));
                peer.publishBurst("tail", 2, 1, false);
                received(tail.receive(Duration.ofSeconds(5))).ack();
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();

            // Act: the retained tail view joins its replay without occupying the local slot.
            try (ScriptedBroker peer = connect(server, sut, true)) {
                peer.publishBurst("head", 1, 1, true);
                peer.publishBurst("tail", 2, 1, true);
                peer.roundTripBarrier(sut);
                assertThat(tail.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
                for (int id = 3; id <= 201; id++) {
                    peer.publish("tail", id);
                    received(tail.receive(Duration.ofSeconds(5))).ack();
                }

                // Assert: the separate wire order still waits for the original manual head.
                peer.expectPublicationBarrier(sut);
                first.ack();
                peer.expectAcknowledgements(1, 201);
                peer.roundTripBarrier(sut);
            }
        }
    }

    @ParameterizedTest(name = "cleanStart={0}, subscriptions={1}")
    @CsvSource({"true, 17", "false, 17", "true, 256", "false, 256"})
    void pending_resubacks_do_not_block_new_publications(
            boolean cleanStart,
            int subscriptionCount
    ) throws Exception {
        var subscriptions = new ArrayList<Subscription>();
        try (ServerSocket server = listen(); MqttClient sut = MqttClient.builder()
                .server(endpoint(server))
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId(clientId)
                .cleanStart(cleanStart)
                .sessionExpiry(cleanStart ? Duration.ZERO : Duration.ofSeconds(30))
                .build()) {
            try (ScriptedBroker peer = connect(server, sut, false, cleanStart)) {
                for (int index = 0; index < subscriptionCount; index++) {
                    subscriptions.add(
                            peer.subscribePull(
                                    sut,
                                    "restore/" + index,
                                    AckMode.ON_RECEIVE,
                                    1
                            )
                    );
                }
                sut.disconnect().get(5, SECONDS);
            }

            try (ScriptedBroker peer = connect(server, sut, false, cleanStart)) {
                var pending = new ArrayList<Packet.Subscribe>();
                for (int index = 0; index < 16; index++) {
                    var request = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                    assertThat(request.subscriptions().getFirst().filter())
                            .isEqualTo(subscriptions.get(index).filter());
                    pending.add(request);
                }

                // Withhold every SUBACK: the restoration window remains full throughout publish.
                var qosZero = MqttMessage.of(
                        new Topic("new/qos0"),
                        new byte[] {0},
                        Qos.AT_MOST_ONCE,
                        false
                );
                assertThat(sut.publish(qosZero).get(5, SECONDS)).isEqualTo(PublishResult.of());
                assertThat(peer.receive()).isEqualTo(
                        new Packet.Publish(
                                qosZero.topic(),
                                qosZero.payload(),
                                Qos.AT_MOST_ONCE,
                                false,
                                false,
                                0,
                                EMPTY
                        )
                );

                var qosOne = message("new/qos1", (byte) 1);
                CompletableFuture<PublishResult> published = sut.publish(qosOne);
                var publication = assertInstanceOf(Packet.Publish.class, peer.receive());
                assertThat(publication.topic()).isEqualTo(qosOne.topic());
                assertThat(publication.payload()).containsExactly((byte) 1);
                assertThat(publication.qos()).isEqualTo(Qos.AT_LEAST_ONCE);
                assertThat(published).isNotDone();
                peer.send(new Packet.PubAck(publication.packetId(), ReasonCode.SUCCESS, EMPTY));
                assertThat(published.get(5, SECONDS)).isEqualTo(PublishResult.of());

                // SUBACK now releases the window and admits the remaining registrations in order.
                for (Packet.Subscribe request : pending) {
                    peer.send(
                            new Packet.SubAck(
                                    request.packetId(),
                                    List.of(ReasonCode.GRANTED_QOS_1),
                                    EMPTY
                            )
                    );
                }
                for (int index = 16; index < subscriptionCount; index++) {
                    var request = assertInstanceOf(Packet.Subscribe.class, peer.receive());
                    assertThat(request.subscriptions().getFirst().filter())
                            .isEqualTo(subscriptions.get(index).filter());
                    peer.send(
                            new Packet.SubAck(
                                    request.packetId(),
                                    List.of(ReasonCode.GRANTED_QOS_1),
                                    EMPTY
                            )
                    );
                }
                peer.roundTripBarrier(sut);
                peer.publish("restore/" + (subscriptionCount - 1), 7);
                assertThat(subscriptions.getLast().receive(Duration.ofSeconds(5)))
                        .isInstanceOf(ReceiveResult.Received.class);
                peer.expectAcknowledgements(7, 1);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void an_offline_close_removes_a_retained_registration_only_when_the_session_survives(
            boolean sessionPresent
    ) throws Exception {
        var ended = new LinkedBlockingQueue<Disconnection>();
        try (ServerSocket server = listen();
                MqttClient sut = persistentClient(endpoint(server), ended::add)) {
            Subscription subscription;
            try (ScriptedBroker peer = connect(server, sut, false)) {
                subscription = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
            }
            assertThat(ended.poll(5, SECONDS)).isNotNull();
            subscription.close();
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.Ended());

            try (ScriptedBroker peer = connect(server, sut, sessionPresent)) {
                CompletableFuture<Void> removed = subscription.unsubscribe();
                if (sessionPresent) {
                    var request = assertInstanceOf(Packet.Unsubscribe.class, peer.receive());
                    assertThat(request.filters()).containsExactly(new TopicFilter("data"));
                    peer.roundTripBarrier(sut);
                    assertThat(removed).isNotDone();
                    peer.send(new Packet.UnsubAck(request.packetId(), List.of(), EMPTY));
                }
                removed.get(5, SECONDS);
                // SP=0 needs no wire removal; SP=1 needs no further removal after UNSUBACK.
                peer.roundTripBarrier(sut);
                Subscription replacement = peer.subscribePull(sut, "data", AckMode.ON_RECEIVE, 4);
                peer.publish("data", 8);
                assertThat(replacement.receive(Duration.ofSeconds(5)))
                        .isInstanceOf(ReceiveResult.Received.class);
                peer.expectAcknowledgements(8, 1);
                assertThat(subscription.receive(Duration.ZERO))
                        .isEqualTo(new ReceiveResult.Ended());
            }
        }
    }

    @Test
    void the_embedded_broker_continues_delivery_on_the_same_persistent_client() throws Exception {
        try (MqttBroker broker = MqttBroker.start()) {
            persistentDeliveryRoundTrip(URI.create("mqtt://localhost:" + broker.port()));
        }
    }

    @Test
    @Timeout(60)
    @EnabledIf(value = "dockerIsAvailableOrRequired", disabledReason = "Docker explicitly optional")
    void mosquitto_continues_delivery_on_the_same_persistent_client() throws Exception {
        try (GenericContainer<?> broker = Mosquitto.container()) {
            Mosquitto.start(broker);
            persistentDeliveryRoundTrip(
                    URI.create(
                            "mqtt://" + broker.getHost() + ":"
                                    + broker.getMappedPort(Mosquitto.MQTT_PORT)
                    )
            );
        }
    }

    static boolean dockerIsAvailableOrRequired() {
        return Mosquitto.availableOrRequired();
    }

    private void persistentDeliveryRoundTrip(URI endpoint) throws Exception {
        String topic = uniqueTopic();
        try (MqttClient sut = persistentClient(endpoint, _ -> { });
                MqttClient publisher = MqttClient.builder()
                        .server(endpoint)
                        .version(MqttVersion.V3_1_1)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .build()) {
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false, 1));
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.ON_RECEIVE)
            );
            subscription.established().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);

            var first = message(topic, (byte) 1);
            var second = message(topic, (byte) 2);
            publisher.publish(first).get(5, SECONDS);
            publisher.publish(second).get(5, SECONDS);
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(true, 2));
            var marker = message(topic, (byte) 3);
            publisher.publish(marker).get(5, SECONDS);

            for (MqttMessage expected : List.of(first, second, marker)) {
                var received = assertInstanceOf(
                        ReceiveResult.Received.class,
                        subscription.receive(Duration.ofSeconds(5))
                );
                assertThat(received.delivery().message()).isEqualTo(expected);
            }
            assertThat(subscription.receive(Duration.ZERO)).isEqualTo(new ReceiveResult.TimedOut());
            subscription.unsubscribe().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);
            publisher.disconnect().get(5, SECONDS);
        }
    }

    private MqttClient persistentClient(URI endpoint, ConnectionListener listener) {
        return MqttClient.builder()
                .server(endpoint)
                .version(MqttVersion.V3_1_1)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .clientId(clientId)
                .cleanStart(false)
                .sessionExpiry(Duration.ofSeconds(30))
                .connectionListener(listener)
                .build();
    }

    private ScriptedBroker connect(
            ServerSocket server,
            MqttClient sut,
            boolean present
    ) throws Exception {
        return connect(server, sut, present, false);
    }

    private ScriptedBroker connect(
            ServerSocket server,
            MqttClient sut,
            boolean present,
            boolean cleanStart
    ) throws Exception {
        return ScriptedBroker.connect(server, sut, present, request -> {
            assertThat(request.clientId()).isEqualTo(clientId);
            assertThat(request.cleanStart()).isEqualTo(cleanStart);
        });
    }

    static Throwable failureOf(CompletableFuture<?> operation) throws Exception {
        return requireNonNull(operation.handle((_, failure) -> failure).get(5, SECONDS));
    }

    private static MqttMessage message(String topic, byte value) {
        return MqttMessage.of(new Topic(topic), new byte[] {value}, Qos.AT_LEAST_ONCE, false);
    }

    private static MqttDelivery received(ReceiveResult result) {
        return assertInstanceOf(ReceiveResult.Received.class, result).delivery();
    }

    private static void sendMarker(ScriptedBroker peer, String topic) throws Exception {
        peer.send(
                new Packet.Publish(
                        new Topic(topic),
                        new byte[] {0},
                        Qos.AT_MOST_ONCE,
                        false,
                        false,
                        0,
                        EMPTY
                )
        );
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, SECONDS)).isTrue();
        } catch (InterruptedException cause) {
            Thread.currentThread().interrupt();
            throw new AssertionError(cause);
        }
    }
}
