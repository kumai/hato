package org.mqtt4j.hato.testkit;

import java.net.URI;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// The refusals and disconnections with a reason that the test controls make the embedded broker
/// give, seen as an application test sees them: through the client.
@Timeout(60)
class BrokerRefusalsTest {

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_LEAST_ONCE", "EXACTLY_ONCE"})
    void a_refused_publication_fails_with_the_rules_reason_and_reaches_no_subscriber(Qos qos)
            throws Exception {
        try (var broker = MqttBroker.start();
                MqttClient subscriber = connected(broker, MqttVersion.V5);
                MqttClient publisher = connected(broker, MqttVersion.V5)) {
            // Arrange
            PullSubscription subscription = subscribed(subscriber, "refused/#");
            var sut = BrokerControls.of(broker);
            sut.refusePublications(
                    new TopicFilter("refused/secret/#"),
                    BrokerReason.of(ReasonCode.QUOTA_EXCEEDED).withReasonString("over quota")
            );

            // Act
            CompletableFuture<PublishResult> refused = publisher.publish(
                    message("refused/secret/data", qos)
            );

            // Assert: the next publication of the same publisher is the first to arrive.
            assertThatThrownBy(() -> refused.get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(MqttPublicationRefusedException.class, refusal -> {
                        assertThat(refusal.reasonCode()).isEqualTo(ReasonCode.QUOTA_EXCEEDED);
                        assertThat(refusal.reasonString()).contains("over quota");
                    });
            MqttMessage open = message("refused/open", qos);
            publisher.publish(open).get(5, SECONDS);
            assertThat(receive(subscription)).isEqualTo(open);
        }
    }

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void a_refusal_without_a_reason_to_report_still_reaches_no_subscriber(MqttVersion version)
            throws Exception {
        // Arrange: MQTT 3.1.1 cannot report the refusal of a QoS 1 publication, and QoS 0 has no
        // answer in either version.
        Qos qos = (version == MqttVersion.V3_1_1) ? Qos.AT_LEAST_ONCE : Qos.AT_MOST_ONCE;
        try (var broker = MqttBroker.start();
                MqttClient subscriber = connected(broker, MqttVersion.V5);
                MqttClient publisher = connected(broker, version)) {
            PullSubscription subscription = subscribed(subscriber, "refused/#");
            var sut = BrokerControls.of(broker);
            sut.refusePublications(
                    new TopicFilter("refused/secret/#"),
                    BrokerReason.of(ReasonCode.NOT_AUTHORIZED)
            );

            // Act
            publisher.publish(message("refused/secret/data", qos)).get(5, SECONDS);

            // Assert
            MqttMessage open = message("refused/open", qos);
            publisher.publish(open).get(5, SECONDS);
            assertThat(receive(subscription)).isEqualTo(open);
        }
    }

    @Test
    void a_topic_that_several_publication_rules_match_takes_the_rule_set_last() throws Exception {
        try (var broker = MqttBroker.start();
                MqttClient publisher = connected(broker, MqttVersion.V5)) {
            // Arrange
            var sut = BrokerControls.of(broker);
            var any = new TopicFilter("rules/#");
            var one = new TopicFilter("rules/one");
            sut.refusePublications(any, BrokerReason.of(ReasonCode.NOT_AUTHORIZED));
            sut.refusePublications(one, BrokerReason.of(ReasonCode.QUOTA_EXCEEDED));

            // Act & Assert: setting a rule again makes it the last one set, and clearing the
            // rules lets the publication through.
            assertThat(refusalOf(publisher, "rules/one")).isEqualTo(ReasonCode.QUOTA_EXCEEDED);
            assertThat(refusalOf(publisher, "rules/two")).isEqualTo(ReasonCode.NOT_AUTHORIZED);
            sut.refusePublications(any, BrokerReason.of(ReasonCode.NOT_AUTHORIZED));
            assertThat(refusalOf(publisher, "rules/one")).isEqualTo(ReasonCode.NOT_AUTHORIZED);
            sut.clearPublicationRule(any);
            sut.clearPublicationRule(one);
            assertThat(publisher.publish(message("rules/one", Qos.AT_LEAST_ONCE)))
                    .succeedsWithin(Duration.ofSeconds(5));
        }
    }

    @Test
    void a_refused_subscription_fails_with_the_rules_reason() throws Exception {
        var filter = new TopicFilter("guarded/#");
        try (var broker = MqttBroker.start();
                MqttClient subscriber = connected(broker, MqttVersion.V5)) {
            // Arrange
            var sut = BrokerControls.of(broker);
            sut.refuseSubscriptions(
                    filter,
                    BrokerReason.of(ReasonCode.NOT_AUTHORIZED).withReasonString("no access")
            );

            // Act
            PullSubscription refused = subscriber.subscribe(Subscribe.of(filter, Qos.AT_MOST_ONCE));

            // Assert
            assertThatThrownBy(() -> refused.established().get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(MqttSubscriptionRefusedException.class, refusal -> {
                        assertThat(refusal.reasonCode()).isEqualTo(ReasonCode.NOT_AUTHORIZED);
                        assertThat(refusal.reasonString()).contains("no access");
                    });
        }
    }

    @Test
    void a_refused_connection_fails_with_the_rules_reason_until_the_rule_is_cleared()
            throws Exception {
        String clientId = clientId();
        try (var broker = MqttBroker.start();
                MqttClient client = client(broker, MqttVersion.V5, clientId)) {
            // Arrange
            var sut = BrokerControls.of(broker);
            sut.refuseConnections(
                    clientId,
                    BrokerReason.of(ReasonCode.USE_ANOTHER_SERVER)
                            .withReasonString("maintenance")
                            .withServerReference("other.example:1883")
            );

            // Act
            CompletableFuture<ConnAck> refused = client.connect();

            // Assert
            assertThatThrownBy(() -> refused.get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(MqttConnectException.class, refusal -> {
                        assertThat(refusal.reasonCode()).contains(ReasonCode.USE_ANOTHER_SERVER);
                        assertThat(refusal.reasonString()).contains("maintenance");
                        assertThat(refusal.serverReference()).contains("other.example:1883");
                    });
            sut.clearConnectionRule(clientId);
            assertThat(client.connect()).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    @Test
    void an_mqtt_3_1_1_client_is_refused_with_the_return_code_of_the_reason() throws Exception {
        String clientId = clientId();
        try (var broker = MqttBroker.start();
                MqttClient client = client(broker, MqttVersion.V3_1_1, clientId)) {
            // Arrange
            var sut = BrokerControls.of(broker);
            sut.refuseConnections(clientId, BrokerReason.of(ReasonCode.NOT_AUTHORIZED));

            // Act
            CompletableFuture<ConnAck> refused = client.connect();

            // Assert
            assertThatThrownBy(() -> refused.get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(MqttConnectException.class, refusal ->
                            assertThat(refusal.reasonCode()).contains(ReasonCode.NOT_AUTHORIZED));
        }
    }

    @Test
    void an_mqtt_3_1_1_client_refused_for_a_reason_without_a_return_code_loses_its_connection()
            throws Exception {
        String clientId = clientId();
        try (var broker = MqttBroker.start();
                MqttClient client = client(broker, MqttVersion.V3_1_1, clientId)) {
            // Arrange
            var sut = BrokerControls.of(broker);
            sut.refuseConnections(clientId, BrokerReason.of(ReasonCode.BAD_AUTHENTICATION_METHOD));

            // Act
            CompletableFuture<ConnAck> refused = client.connect();

            // Assert: the broker closes the connection without CONNACK, so no reason arrives.
            assertThatThrownBy(() -> refused.get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(MqttConnectException.class, refusal ->
                            assertThat(refusal.reasonCode()).isEmpty());
        }
    }

    @Test
    void disconnecting_with_a_reason_tells_an_mqtt_5_client_the_reason() throws Exception {
        String clientId = clientId();
        var ends = new LinkedBlockingQueue<Disconnection>();
        try (var broker = MqttBroker.start();
                MqttClient client = MqttClient.builder()
                        .server(endpoint(broker))
                        .version(MqttVersion.V5)
                        .clientId(clientId)
                        .connectionListener(ends::add)
                        .reconnectPolicy(ReconnectPolicy.disabled())
                        .build()) {
            // Arrange
            client.connect().get(5, SECONDS);
            var sut = BrokerControls.of(broker);

            // Act
            sut.disconnect(
                    clientId,
                    BrokerReason.of(ReasonCode.SERVER_MOVED)
                            .withReasonString("moved for good")
                            .withServerReference("new.example:1883")
            );

            // Assert
            Disconnection end = ends.poll(5, SECONDS);
            assertThat(end).isNotNull();
            assertThat(end.cause()).get().isInstanceOfSatisfying(
                    MqttBrokerDisconnectException.class,
                    ending -> {
                        assertThat(ending.reasonCode()).isEqualTo(ReasonCode.SERVER_MOVED);
                        assertThat(ending.reasonString()).contains("moved for good");
                        assertThat(ending.serverReference()).contains("new.example:1883");
                    }
            );
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unfitReasons")
    void an_operation_refuses_a_reason_that_its_packet_cannot_carry(Unfit unfit)
            throws Exception {
        try (var broker = MqttBroker.start()) {
            // Arrange
            var sut = BrokerControls.of(broker);

            // Act & Assert
            assertThatThrownBy(() -> unfit.operation().accept(sut))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    static Stream<Unfit> unfitReasons() {
        var filter = new TopicFilter("unfit/#");
        BrokerReason redirect = BrokerReason.of(ReasonCode.NOT_AUTHORIZED)
                .withServerReference("elsewhere");
        return Stream.of(
                new Unfit(
                        "a publication refused with success",
                        sut -> sut.refusePublications(filter, BrokerReason.of(ReasonCode.SUCCESS))
                ),
                new Unfit(
                        "a publication refused with No matching subscribers",
                        sut -> sut.refusePublications(
                                filter,
                                BrokerReason.of(ReasonCode.NO_MATCHING_SUBSCRIBERS)
                        )
                ),
                new Unfit(
                        "a publication refused with a reason that PUBACK lacks",
                        sut -> sut.refusePublications(
                                filter,
                                BrokerReason.of(ReasonCode.SESSION_TAKEN_OVER)
                        )
                ),
                new Unfit(
                        "a publication refused with a Server Reference",
                        sut -> sut.refusePublications(
                                filter,
                                BrokerReason.of(ReasonCode.USE_ANOTHER_SERVER)
                                        .withServerReference("elsewhere")
                        )
                ),
                new Unfit(
                        "a subscription refused with a granted QoS",
                        sut -> sut.refuseSubscriptions(
                                filter,
                                BrokerReason.of(ReasonCode.GRANTED_QOS_1)
                        )
                ),
                new Unfit(
                        "a subscription refused with a reason that SUBACK lacks",
                        sut -> sut.refuseSubscriptions(
                                filter,
                                BrokerReason.of(ReasonCode.PAYLOAD_FORMAT_INVALID)
                        )
                ),
                new Unfit(
                        "a connection refused with success",
                        sut -> sut.refuseConnections("unfit", BrokerReason.of(ReasonCode.SUCCESS))
                ),
                new Unfit(
                        "a connection refused with a reason that CONNACK lacks",
                        sut -> sut.refuseConnections(
                                "unfit",
                                BrokerReason.of(ReasonCode.KEEP_ALIVE_TIMEOUT)
                        )
                ),
                new Unfit(
                        "a Server Reference with a reason that is no redirection",
                        sut -> sut.refuseConnections("unfit", redirect)
                ),
                new Unfit(
                        "a disconnection with Disconnect with Will Message",
                        sut -> sut.disconnect(
                                "unfit",
                                BrokerReason.of(ReasonCode.DISCONNECT_WITH_WILL_MESSAGE)
                        )
                ),
                new Unfit(
                        "a disconnection with a reason that DISCONNECT lacks",
                        sut -> sut.disconnect(
                                "unfit",
                                BrokerReason.of(ReasonCode.CLIENT_IDENTIFIER_NOT_VALID)
                        )
                ),
                new Unfit(
                        "a Reason String that MQTT cannot carry",
                        sut -> sut.refuseConnections(
                                "unfit",
                                BrokerReason.of(ReasonCode.NOT_AUTHORIZED).withReasonString("\0")
                        )
                )
        );
    }

    /// An operation of the controls with a reason that the packet it answers with cannot carry.
    record Unfit(String name, Consumer<BrokerControls> operation) {

        @Override
        public String toString() {
            return name;
        }
    }

    /// The reason code with which the broker refuses a QoS 1 publication to `topic`.
    private static ReasonCode refusalOf(MqttClient publisher, String topic) {
        CompletableFuture<PublishResult> refused = publisher.publish(
                message(topic, Qos.AT_LEAST_ONCE)
        );
        ExecutionException failure = assertThrows(
                ExecutionException.class,
                () -> refused.get(5, SECONDS)
        );
        return assertInstanceOf(MqttPublicationRefusedException.class, failure.getCause())
                .reasonCode();
    }

    private static MqttClient connected(MqttBroker broker, MqttVersion version) throws Exception {
        MqttClient client = client(broker, version, clientId());
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

    private static PullSubscription subscribed(MqttClient client, String filter) throws Exception {
        PullSubscription subscription = client.subscribe(
                Subscribe.of(new TopicFilter(filter), Qos.EXACTLY_ONCE)
        );
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
