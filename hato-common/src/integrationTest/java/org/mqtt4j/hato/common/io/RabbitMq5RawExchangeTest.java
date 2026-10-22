package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.protocol.Property;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.RabbitMq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.protocol.PropertyId.MAXIMUM_PACKET_SIZE;
import static org.mqtt4j.hato.common.protocol.PropertyId.MAXIMUM_QOS;
import static org.mqtt4j.hato.common.protocol.PropertyId.SESSION_EXPIRY_INTERVAL;
import static org.mqtt4j.hato.common.protocol.PropertyId.SHARED_SUBSCRIPTION_AVAILABLE;
import static org.mqtt4j.hato.common.protocol.PropertyId.TOPIC_ALIAS_MAXIMUM;
import static org.mqtt4j.hato.common.value.ReasonCode.NO_MATCHING_SUBSCRIBERS;
import static org.mqtt4j.hato.common.value.ReasonCode.NO_SUBSCRIPTION_EXISTED;
import static org.mqtt4j.hato.common.value.ReasonCode.QOS_NOT_SUPPORTED;
import static org.mqtt4j.hato.common.value.ReasonCode.SESSION_TAKEN_OVER;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Records how the pinned RabbitMQ 4.4.0 image, with its MQTT plugin, answers MQTT 5.0 clients.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker is not available and hato.docker.required=false"
)
class RabbitMq5RawExchangeTest extends Mqtt5RawExchangeTest {

    private static final GenericContainer<?> rabbitMq = RabbitMq.container();

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void startBroker() {
        Docker.start(rabbitMq);
    }

    @AfterAll
    static void stopBroker() {
        rabbitMq.stop();
    }

    @Override
    RawSession open() throws IOException {
        return RawSession.open(codec, rabbitMq, RabbitMq.MQTT_PORT);
    }

    @Override
    Qos maximumQos() {
        return Qos.AT_LEAST_ONCE;
    }

    // RabbitMQ has no QoS 2, so publications cross it at QoS 0 and 1 only. What a QoS 2
    // publication meets is recorded below.
    @Override
    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_MOST_ONCE", "AT_LEAST_ONCE"})
    void publications_and_properties_cross_the_broker_at_every_qos(Qos qos) throws Exception {
        super.publications_and_properties_cross_the_broker_at_every_qos(qos);
    }

    @Test
    void the_connack_announces_qos_1_without_shared_subscriptions_and_its_limits()
            throws Exception {
        // Act
        var connAck = (Packet.ConnAck) codec.decode(connAckFrame("hato-rabbitmq-connack", EMPTY));

        // Assert: RabbitMQ also repeats the Session Expiry Interval that the client asked for.
        assertThat(connAck.reasonCode()).isEqualTo(SUCCESS);
        assertThat(connAck.sessionPresent()).isFalse();
        assertThat(connAck.properties().entries()).containsExactlyInAnyOrder(
                new Property.ByteValue(SHARED_SUBSCRIPTION_AVAILABLE, 0),
                new Property.ByteValue(MAXIMUM_QOS, 1),
                new Property.FourByteValue(MAXIMUM_PACKET_SIZE, 16_777_216),
                new Property.TwoByteValue(TOPIC_ALIAS_MAXIMUM, 16),
                new Property.FourByteValue(SESSION_EXPIRY_INTERVAL, 0)
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {16, 65_535})
    void aliases_go_to_the_first_16_topics_whatever_the_client_allows(int topicAliasMaximum)
            throws Exception {
        // Act
        List<String> deliveries = aliasesForTopicsSentTwice(topicAliasMaximum);

        // Assert: RabbitMQ does not reassign an alias once its own maximum of 16 is reached.
        assertThat(deliveries).containsExactlyElementsOf(aliasesForTheFirst(16));
    }

    @Test
    void overlapping_subscriptions_share_one_copy_with_both_identifiers() throws Exception {
        // Act
        List<List<Integer>> copies = identifiersOfOverlappingCopies(
                "identified",
                identifier(1),
                identifier(2)
        );

        // Assert
        assertThat(copies).singleElement().satisfies(
                identifiers -> assertThat(identifiers).containsExactlyInAnyOrder(1, 2)
        );
    }

    @Test
    void overlapping_subscriptions_without_identifiers_share_one_copy() throws Exception {
        // Act
        List<List<Integer>> copies = identifiersOfOverlappingCopies("plain", EMPTY, EMPTY);

        // Assert
        assertThat(copies).containsExactly(List.of());
    }

    @Test
    void a_publication_without_subscribers_is_answered_with_no_matching_subscribers()
            throws Exception {
        assertThat(answerToUnmatchedPublication(Qos.AT_LEAST_ONCE))
                .isEqualTo(new Packet.PubAck(11, NO_MATCHING_SUBSCRIBERS, EMPTY));
    }

    @Test
    void a_qos_2_publication_ends_the_connection_with_qos_not_supported() throws Exception {
        assertThat(answerToUnmatchedPublication(Qos.EXACTLY_ONCE))
                .isEqualTo(new Packet.Disconnect(QOS_NOT_SUPPORTED, EMPTY));
    }

    @Test
    void unsubscribing_from_a_filter_never_subscribed_is_answered_with_no_subscription_existed()
            throws Exception {
        assertThat(answerToUnsubscribingWhatWasNeverSubscribed())
                .isEqualTo(new Packet.UnsubAck(1, List.of(NO_SUBSCRIPTION_EXISTED), EMPTY));
    }

    @Test
    void a_shared_subscription_is_granted_although_unavailable_and_reaches_no_member()
            throws Exception {
        var topic = new Topic("hato/shared/rabbitmq");
        try (
                RawSession member = connected("hato-shared-member");
                RawSession publisher = connected("hato-shared-publisher")
        ) {
            // Arrange: the CONNACK announces no shared subscriptions, yet RabbitMQ grants this
            // one at QoS 0.
            subscribe(member, 1, new TopicFilter("$share/hato/" + topic.name()), EMPTY);

            // Act
            publisher.send(publication(topic, 0));

            // Assert
            assertThat(member.poll(Duration.ofSeconds(1))).isEmpty();
        }
    }

    @Test
    void a_taken_over_connection_receives_session_taken_over_and_its_will_is_published()
            throws Exception {
        // Act
        Takeover takeover = takeOver("hato-rabbitmq-taken-over");

        // Assert
        assertThat(takeover.toTakenOverConnection())
                .containsExactly(new Packet.Disconnect(SESSION_TAKEN_OVER, EMPTY));
        assertThat(takeover.willPublished()).isTrue();
    }
}
