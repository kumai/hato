package org.mqtt4j.mqtj.client.internal;

import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.mqtj.AckMode;
import org.mqtt4j.mqtj.ConnAck;
import org.mqtt4j.mqtj.ConnectionListener;
import org.mqtt4j.mqtj.Disconnection;
import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttConnectionLostException;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.PublishResult;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.ReasonCode;
import org.mqtt4j.mqtj.ReceiveResult;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.Subscription;
import org.mqtt4j.mqtj.Topic;
import org.mqtt4j.mqtj.TopicFilter;
import org.mqtt4j.mqtj.broker.MqttBroker;
import org.mqtt4j.mqtj.client.internal.common.protocol.Packet;
import org.mqtt4j.mqtj.testsupport.Mosquitto;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mqtt4j.mqtj.client.internal.MessagingTest.uniqueTopic;
import static org.mqtt4j.mqtj.client.internal.ScriptedBroker.endpoint;
import static org.mqtt4j.mqtj.client.internal.ScriptedBroker.listen;
import static org.mqtt4j.mqtj.client.internal.common.protocol.Properties.EMPTY;

/// Raw peers expose replay order; real brokers verify the same client's persistent session.
@Timeout(30)
class SessionPersistenceIntegrationTest {

    private final String clientId = "mqtj" + UUID.randomUUID().toString().replace("-", "")
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
        try (var broker = Mosquitto.container()) {
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
                        .build()) {
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(false));
            publisher.connect().get(5, SECONDS);
            Subscription subscription = sut.subscribe(
                    Subscribe.of(new TopicFilter(topic), Qos.AT_LEAST_ONCE)
                            .withAckMode(AckMode.ON_RECEIVE)
        )
                .get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);

            var first = message(topic, (byte) 1);
            var second = message(topic, (byte) 2);
            publisher.publish(first).get(5, SECONDS);
            publisher.publish(second).get(5, SECONDS);
            assertThat(sut.connect().get(5, SECONDS)).isEqualTo(ConnAck.of(true));
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

    private static Throwable failureOf(CompletableFuture<?> operation) throws Exception {
        return requireNonNull(operation.handle((_, failure) -> failure).get(5, SECONDS));
    }

    private static MqttMessage message(String topic, byte value) {
        return MqttMessage.of(new Topic(topic), new byte[] {value}, Qos.AT_LEAST_ONCE, false);
    }
}
