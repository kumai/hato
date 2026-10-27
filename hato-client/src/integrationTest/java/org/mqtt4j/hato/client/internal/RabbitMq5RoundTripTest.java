package org.mqtt4j.hato.client.internal;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.RabbitMq;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mqtt4j.hato.client.internal.MessagingTest.uniqueTopic;

/// The client's MQTT 5.0 connections to RabbitMQ, the limits that RabbitMQ announces, the
/// sessions that it keeps, and the QoS 2 publications and shared subscriptions that RabbitMQ does
/// not take.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker explicitly optional"
)
class RabbitMq5RoundTripTest extends Mqtt5RoundTripTest {

    private static final GenericContainer<?> broker = RabbitMq.container();

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void start() {
        Docker.start(broker);
    }

    @AfterAll
    static void stop() {
        broker.stop();
    }

    // RabbitMQ announces a Maximum QoS of 1, so publications round trip at QoS 0 and 1 only. The
    // tests below cover QoS 2.
    @Override
    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_MOST_ONCE", "AT_LEAST_ONCE"})
    void publications_round_trip_at_every_qos(Qos qos) throws Exception {
        super.publications_round_trip_at_every_qos(qos);
    }

    @Override
    @ParameterizedTest
    @EnumSource(value = Qos.class, names = "AT_LEAST_ONCE")
    void a_publication_without_subscribers_reports_the_brokers_reason_code(Qos qos)
            throws Exception {
        super.a_publication_without_subscribers_reports_the_brokers_reason_code(qos);
    }

    @Test
    void the_connack_reports_the_limits_that_rabbitmq_announces() throws Exception {
        try (MqttClient sut = client("mqtt5-limits", Duration.ZERO)) {
            // Act
            ConnAck ack = sut.connect().get(5, SECONDS);

            // Assert: recorded from the pinned RabbitMQ 4.4.0 image, which announces a Maximum QoS
            // of 1, no shared subscriptions, and a maximum packet size of 16 MiB.
            assertThat(ack).isEqualTo(ConnAck.of(false, 1)
                    .withMaximumQos(Qos.AT_LEAST_ONCE)
                    .withSharedSubscriptionAvailable(false)
                    .withMaximumPacketSize(16_777_216));
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void a_session_without_subscriptions_is_not_kept() throws Exception {
        try (MqttClient sut = client("mqtt5-no-subscriptions", Duration.ofMinutes(1))) {
            // Arrange
            sut.connect().get(5, SECONDS);
            sut.disconnect().get(5, SECONDS);

            // Act
            ConnAck reconnected = sut.connect().get(5, SECONDS);

            // Assert: recorded from the pinned RabbitMQ 4.4.0 image, which keeps no session
            // without a subscription, although the Session Expiry Interval asks for one.
            assertThat(reconnected.sessionPresent()).isFalse();
            sut.disconnect().get(5, SECONDS);
        }
    }

    @Test
    void a_qos_2_publication_is_refused_before_it_is_sent_and_the_connection_goes_on()
            throws Exception {
        var topic = new Topic(uniqueTopic());
        try (MqttClient sut = client("mqtt5-qos2", Duration.ZERO)) {
            // Arrange: RabbitMQ would end the connection with QoS not supported.
            sut.connect().get(5, SECONDS);

            // Act
            CompletableFuture<PublishResult> publication = sut.publish(MqttMessage.of(
                    topic,
                    new byte[] {2},
                    Qos.EXACTLY_ONCE,
                    false
            ));

            // Assert
            assertThatThrownBy(() -> publication.get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(
                            MqttPublicationRefusedException.class,
                            refusal -> assertThat(refusal.reasonCode())
                                    .isEqualTo(ReasonCode.QOS_NOT_SUPPORTED)
                    );
            assertThat(sut.publish(MqttMessage.of(
                    topic,
                    new byte[] {1},
                    Qos.AT_LEAST_ONCE,
                    false
            ))).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    @Test
    void a_shared_subscription_is_refused_before_it_is_sent_and_the_connection_goes_on()
            throws Exception {
        try (MqttClient sut = client("mqtt5-shared", Duration.ZERO)) {
            // Arrange: RabbitMQ would grant the subscription as a filter that matches nothing.
            sut.connect().get(5, SECONDS);

            // Act
            Subscription refused = sut.subscribe(
                    Subscribe.of(new TopicFilter("$share/hato/" + uniqueTopic()), Qos.AT_LEAST_ONCE)
            );

            // Assert
            assertThatThrownBy(() -> refused.established().get(5, SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOfSatisfying(
                            MqttSubscriptionRefusedException.class,
                            refusal -> assertThat(refusal.reasonCode())
                                    .isEqualTo(ReasonCode.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED)
                    );
            assertThat(sut.subscribe(
                    Subscribe.of(new TopicFilter(uniqueTopic()), Qos.AT_LEAST_ONCE)
            ).established()).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    // RabbitMQ 4.4.0 reports no matching subscribers in PUBACK.
    @Override
    ReasonCode unmatchedPublicationReason(Qos qos) {
        return ReasonCode.NO_MATCHING_SUBSCRIBERS;
    }

    @Override
    URI endpoint() {
        return URI.create(
                "mqtt://" + broker.getHost() + ":" + broker.getMappedPort(RabbitMq.MQTT_PORT)
        );
    }
}
