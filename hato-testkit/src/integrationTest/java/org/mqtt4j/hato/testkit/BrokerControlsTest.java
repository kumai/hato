package org.mqtt4j.hato.testkit;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/// The test controls of the embedded broker, used as an application test uses them: through
/// the client, against a broker that the test starts.
@Timeout(60)
class BrokerControlsTest {

    @Test
    void a_refused_filter_subscribes_to_nothing_until_its_rule_is_cleared() throws Exception {
        var filter = new TopicFilter("refused/#");
        try (var broker = MqttBroker.start();
                MqttClient subscriber = connected(broker, MqttVersion.V3_1_1);
                MqttClient publisher = connected(broker, MqttVersion.V3_1_1)) {
            // Arrange
            var sut = BrokerControls.of(broker);
            sut.refuseSubscriptions(filter);

            // Act
            PullSubscription refused = subscriber.subscribe(
                    Subscribe.of(filter, Qos.AT_LEAST_ONCE)
            );

            // Assert: the refusal fails the subscription, and clearing the rule lets the same
            // filter subscribe while the clients stay connected.
            assertThatThrownBy(() -> refused.established().get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(MqttSubscriptionRefusedException.class);
            sut.clearSubscriptionRule(filter);
            PullSubscription accepted = subscribed(subscriber, filter, Qos.AT_LEAST_ONCE);
            MqttMessage message = message("refused/now-accepted", Qos.AT_LEAST_ONCE);
            publisher.publish(message).get(5, SECONDS);
            assertThat(receive(accepted)).isEqualTo(message);
        }
    }

    @Test
    void a_filter_granted_at_most_a_qos_receives_its_deliveries_at_that_qos() throws Exception {
        var filter = new TopicFilter("limited/#");
        try (var broker = MqttBroker.start();
                MqttClient subscriber = connected(broker, MqttVersion.V3_1_1);
                MqttClient publisher = connected(broker, MqttVersion.V3_1_1)) {
            // Arrange
            var sut = BrokerControls.of(broker);
            sut.grantAtMost(filter, Qos.AT_LEAST_ONCE);

            // Act
            PullSubscription subscription = subscribed(subscriber, filter, Qos.EXACTLY_ONCE);
            publisher.publish(message("limited/topic", Qos.EXACTLY_ONCE)).get(5, SECONDS);

            // Assert
            assertThat(subscription.grantedQos()).contains(Qos.AT_LEAST_ONCE);
            assertThat(receive(subscription).qos()).isEqualTo(Qos.AT_LEAST_ONCE);
        }
    }

    @ParameterizedTest
    @EnumSource(ResponseKind.class)
    void a_held_response_waits_until_the_test_releases_it(ResponseKind kind) throws Exception {
        String clientId = clientId();
        try (var broker = MqttBroker.start();
                MqttClient sut = connected(broker, MqttVersion.V3_1_1, clientId)) {
            // Arrange: unsubscribing needs a subscription, whose SUBACK is not held.
            var controls = BrokerControls.of(broker);
            PullSubscription subscription = subscribed(
                    sut,
                    new TopicFilter("held/topic"),
                    Qos.AT_LEAST_ONCE
            );
            controls.holdResponses(kind, clientId);

            // Act
            CompletableFuture<?> request = switch (kind) {
                case SUBACK -> sut.subscribe(Subscribe.of(
                        new TopicFilter("held/other"),
                        Qos.AT_LEAST_ONCE
                )).established();
                case UNSUBACK -> subscription.unsubscribe();
                case PUBACK -> sut.publish(message("held/topic", Qos.AT_LEAST_ONCE));
                case PUBREC, PUBCOMP -> sut.publish(message("held/topic", Qos.EXACTLY_ONCE));
            };

            // Assert: the broker shows the response that it holds, and the request completes
            // once the test releases it.
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(controls.heldResponses())
                            .extracting(HeldResponse::kind, HeldResponse::clientId)
                            .containsExactly(tuple(kind, clientId)));
            assertThat(request).isNotDone();
            controls.releaseResponses(kind, clientId);
            assertThat(request).succeedsWithin(Duration.ofSeconds(5));
            assertThat(controls.heldResponses()).isEmpty();
        }
    }

    @Test
    void responses_held_in_order_take_no_room_in_the_send_queue() throws Exception {
        // More than the 128 responses that the broker's send queue keeps room for.
        int count = 200;
        String clientId = clientId();
        try (var broker = MqttBroker.start();
                MqttClient sut = MqttClient.builder()
                        .server(endpoint(broker))
                        .version(MqttVersion.V3_1_1)
                        .clientId(clientId)
                        .maxInflightPublishes(count)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .build()) {
            // Arrange
            sut.connect().get(5, SECONDS);
            var controls = BrokerControls.of(broker);
            controls.holdResponses(ResponseKind.PUBACK, clientId);
            var publications = new ArrayList<CompletableFuture<PublishResult>>();
            for (int index = 0; index < count; index++) {
                publications.add(sut.publish(message("ordered/" + index, Qos.AT_LEAST_ONCE)));
            }

            // Act
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(controls.heldResponses()).hasSize(count));
            List<HeldResponse> held = controls.heldResponses();
            controls.releaseResponses(ResponseKind.PUBACK, clientId);

            // Assert: the broker held the acknowledgements in the order of the publications,
            // kept the connection open, and sends them all once released.
            assertThat(held).extracting(HeldResponse::packetId).isSorted();
            for (CompletableFuture<PublishResult> publication : publications) {
                assertThat(publication).succeedsWithin(Duration.ofSeconds(5));
            }
            assertThat(controls.connectedClients()).contains(clientId);
        }
    }

    @Test
    void a_connection_that_ends_drops_the_responses_held_for_it() throws Exception {
        String clientId = clientId();
        try (var broker = MqttBroker.start()) {
            // Arrange
            var controls = BrokerControls.of(broker);
            controls.holdResponses(ResponseKind.PUBACK, clientId);
            MqttClient sut = connected(broker, MqttVersion.V3_1_1, clientId);
            CompletableFuture<PublishResult> publication = sut.publish(
                    message("dropped", Qos.AT_LEAST_ONCE)
            );
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(controls.heldResponses()).hasSize(1));

            // Act: closing the client ends its connection.
            sut.close();

            // Assert
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(controls.heldResponses()).isEmpty());
            assertThat(publication).failsWithin(Duration.ofSeconds(5));
        }
    }

    @Test
    void the_connected_clients_are_those_with_an_open_connection() throws Exception {
        String stays = clientId();
        String leaves = clientId();
        try (var broker = MqttBroker.start();
                MqttClient staying = connected(broker, MqttVersion.V3_1_1, stays);
                MqttClient leaving = connected(broker, MqttVersion.V5, leaves)) {
            // Arrange
            var sut = BrokerControls.of(broker);
            assertThat(sut.connectedClients()).containsExactlyInAnyOrder(stays, leaves);

            // Act
            leaving.disconnect().get(5, SECONDS);

            // Assert
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(sut.connectedClients()).containsExactly(stays));
            assertThat(staying.whenConnected()).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void disconnecting_a_client_ends_its_connection_and_publishes_its_will(MqttVersion version)
            throws Exception {
        String clientId = clientId();
        MqttMessage will = message("controls/will", Qos.AT_LEAST_ONCE);
        var ends = new LinkedBlockingQueue<Disconnection>();
        try (var broker = MqttBroker.start();
                MqttClient subscriber = connected(broker, MqttVersion.V3_1_1);
                MqttClient client = MqttClient.builder()
                        .server(endpoint(broker))
                        .version(version)
                        .clientId(clientId)
                        .will(will)
                        .connectionListener(ends::add)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .build()) {
            // Arrange
            PullSubscription subscription = subscribed(
                    subscriber,
                    new TopicFilter(will.topic().name()),
                    Qos.AT_LEAST_ONCE
            );
            client.connect().get(5, SECONDS);
            var sut = BrokerControls.of(broker);

            // Act
            sut.disconnect(clientId);

            // Assert: an MQTT 5.0 client learns the broker's reason.
            Disconnection end = ends.poll(5, SECONDS);
            assertThat(end).isNotNull();
            assertThat(end.userRequested()).isFalse();
            if (version == MqttVersion.V5) {
                assertThat(end.cause()).get().isInstanceOfSatisfying(
                        MqttBrokerDisconnectException.class,
                        ending -> assertThat(ending.reasonCode())
                                .isEqualTo(ReasonCode.ADMINISTRATIVE_ACTION)
                );
            }
            assertThat(receive(subscription)).isEqualTo(will);
            assertThat(sut.connectedClients()).doesNotContain(clientId);
        }
    }

    @Test
    void an_announcement_applies_to_the_mqtt_5_clients_that_connect_afterward() throws Exception {
        try (var broker = MqttBroker.start();
                MqttClient before = connected(broker, MqttVersion.V5);
                MqttClient after = client(broker, MqttVersion.V5, clientId())) {
            // Arrange
            var sut = BrokerControls.of(broker);
            BrokerCapabilities narrowed = BrokerCapabilities.defaults()
                    .withMaximumQos(Qos.AT_LEAST_ONCE)
                    .withRetainAvailable(false)
                    .withWildcardSubscriptionAvailable(false)
                    .withMaximumPacketSize(512);

            // Act
            sut.announce(narrowed);
            ConnAck ack = after.connect().get(5, SECONDS);

            // Assert: the client that connected before keeps what it learned then, so the
            // broker still takes a retained QoS 2 publication from it.
            ConnAck expected = ConnAck.of(false, 1)
                    .withMaximumQos(Qos.AT_LEAST_ONCE)
                    .withRetainAvailable(false)
                    .withWildcardSubscriptionAvailable(false)
                    .withSharedSubscriptionAvailable(false)
                    .withMaximumPacketSize(512);
            assertThat(ack).isEqualTo(expected);
            MqttMessage retained = MqttMessage.of(
                    new Topic("announced/before"),
                    new byte[] {1},
                    Qos.EXACTLY_ONCE,
                    true
            );
            assertThat(before.publish(retained)).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    @Test
    void an_mqtt_3_1_1_client_is_held_to_no_announcement() throws Exception {
        try (var broker = MqttBroker.start();
                MqttClient client = client(broker, MqttVersion.V3_1_1, clientId())) {
            // Arrange
            var sut = BrokerControls.of(broker);
            BrokerCapabilities narrowed = BrokerCapabilities.defaults()
                    .withMaximumQos(Qos.AT_MOST_ONCE)
                    .withRetainAvailable(false);
            sut.announce(narrowed);
            MqttMessage retained = MqttMessage.of(
                    new Topic("announced/v311"),
                    new byte[] {1},
                    Qos.EXACTLY_ONCE,
                    true
            );

            // Act
            ConnAck ack = client.connect().get(5, SECONDS);
            CompletableFuture<PublishResult> publication = client.publish(retained);

            // Assert
            assertThat(ack).isEqualTo(ConnAck.of(false, 1));
            assertThat(publication).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    @Test
    void held_back_responses_count_against_the_receive_maximum() throws Exception {
        String clientId = clientId();
        try (var broker = MqttBroker.start();
                MqttClient client = client(broker, MqttVersion.V5, clientId)) {
            // Arrange
            var sut = BrokerControls.of(broker);
            sut.announce(BrokerCapabilities.defaults().withReceiveMaximum(1));
            sut.holdResponses(ResponseKind.PUBACK, clientId);
            client.connect().get(5, SECONDS);

            // Act
            var publications = new ArrayList<CompletableFuture<PublishResult>>();
            for (int index = 0; index < 3; index++) {
                publications.add(client.publish(message("counted/" + index, Qos.AT_LEAST_ONCE)));
            }

            // Assert: the client sends a publication only once the broker has answered the one
            // before, so the broker holds one response at a time until the test releases them.
            await().during(Duration.ofMillis(300))
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(sut.heldResponses()).hasSize(1));
            sut.releaseResponses(ResponseKind.PUBACK, clientId);
            for (CompletableFuture<PublishResult> publication : publications) {
                assertThat(publication).succeedsWithin(Duration.ofSeconds(5));
            }
        }
    }

    private static MqttClient connected(MqttBroker broker, MqttVersion version) throws Exception {
        return connected(broker, version, clientId());
    }

    private static MqttClient connected(MqttBroker broker, MqttVersion version, String clientId)
            throws Exception {
        MqttClient client = client(broker, version, clientId);
        try {
            client.connect().get(5, SECONDS);
            return client;
        } catch (Exception | AssertionError failure) {
            client.close();
            throw failure;
        }
    }

    private static MqttClient client(MqttBroker broker, MqttVersion version, String clientId) {
        return MqttClient.builder()
                .server(endpoint(broker))
                .version(version)
                .clientId(clientId)
                .reconnectPolicy(ReconnectPolicy.disabled())
                .build();
    }

    private static URI endpoint(MqttBroker broker) {
        return URI.create("mqtt://localhost:" + broker.port());
    }

    private static String clientId() {
        return "hato" + UUID.randomUUID().toString().replace("-", "").substring(0, 19);
    }

    private static MqttMessage message(String topic, Qos qos) {
        return MqttMessage.of(new Topic(topic), topic.getBytes(UTF_8), qos, false);
    }

    private static PullSubscription subscribed(MqttClient client, TopicFilter filter, Qos qos)
            throws Exception {
        PullSubscription subscription = client.subscribe(Subscribe.of(filter, qos));
        subscription.established().get(5, SECONDS);
        return subscription;
    }

    private static MqttMessage receive(PullSubscription subscription) throws Exception {
        ReceiveResult.Received received = assertInstanceOf(
                ReceiveResult.Received.class,
                subscription.receive(Duration.ofSeconds(5))
        );
        return received.delivery().message();
    }
}
