package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.protocol.Property;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Emqx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.protocol.PropertyId.MAXIMUM_PACKET_SIZE;
import static org.mqtt4j.hato.common.protocol.PropertyId.RECEIVE_MAXIMUM;
import static org.mqtt4j.hato.common.protocol.PropertyId.RETAIN_AVAILABLE;
import static org.mqtt4j.hato.common.protocol.PropertyId.SHARED_SUBSCRIPTION_AVAILABLE;
import static org.mqtt4j.hato.common.protocol.PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE;
import static org.mqtt4j.hato.common.protocol.PropertyId.TOPIC_ALIAS_MAXIMUM;
import static org.mqtt4j.hato.common.protocol.PropertyId.WILDCARD_SUBSCRIPTION_AVAILABLE;
import static org.mqtt4j.hato.common.value.ReasonCode.NO_MATCHING_SUBSCRIBERS;
import static org.mqtt4j.hato.common.value.ReasonCode.NO_SUBSCRIPTION_EXISTED;
import static org.mqtt4j.hato.common.value.ReasonCode.RECEIVE_MAXIMUM_EXCEEDED;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Records how the pinned EMQX 6.3.2 image answers MQTT 5.0 clients.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker is not available and hato.docker.required=false"
)
class Emqx5RawExchangeTest extends Mqtt5RawExchangeTest {

    private static final GenericContainer<?> emqx = Emqx.container();

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void startBroker() {
        Docker.start(emqx);
    }

    @AfterAll
    static void stopBroker() {
        emqx.stop();
    }

    @Override
    RawSession open() throws IOException {
        return RawSession.open(codec, emqx, Emqx.MQTT_PORT);
    }

    @Test
    void the_connack_announces_every_capability_with_its_limits() throws Exception {
        // Act
        var connAck = (Packet.ConnAck) codec.decode(connAckFrame("hato-emqx-connack", EMPTY));

        // Assert: EMQX announces even the capabilities that MQTT 5.0 assumes. The order of the
        // properties changes between starts of the image, so only the properties are compared.
        assertThat(connAck.reasonCode()).isEqualTo(SUCCESS);
        assertThat(connAck.sessionPresent()).isFalse();
        assertThat(connAck.properties().entries()).containsExactlyInAnyOrder(
                new Property.TwoByteValue(RECEIVE_MAXIMUM, 32),
                new Property.ByteValue(RETAIN_AVAILABLE, 1),
                new Property.ByteValue(WILDCARD_SUBSCRIPTION_AVAILABLE, 1),
                new Property.ByteValue(SHARED_SUBSCRIPTION_AVAILABLE, 1),
                new Property.ByteValue(SUBSCRIPTION_IDENTIFIER_AVAILABLE, 1),
                new Property.FourByteValue(MAXIMUM_PACKET_SIZE, 1_048_576),
                new Property.TwoByteValue(TOPIC_ALIAS_MAXIMUM, 65_535)
        );
    }

    @Test
    void the_announced_receive_maximum_shrinks_to_a_smaller_one_from_the_client()
            throws Exception {
        // Arrange
        var receiveMaximum = new Property.TwoByteValue(RECEIVE_MAXIMUM, 5);

        // Act
        var connAck = (Packet.ConnAck) codec.decode(
                connAckFrame("hato-emqx-receive-maximum", properties(receiveMaximum))
        );

        // Assert: EMQX announces the smaller of its own 32 and the client's value.
        assertThat(connAck.properties().find(RECEIVE_MAXIMUM)).contains(receiveMaximum);
    }

    @ParameterizedTest
    @CsvSource({
            "16, 16",
            "65535, 20"
    })
    void aliases_go_to_as_many_topics_as_the_client_allows(int topicAliasMaximum, int aliased)
            throws Exception {
        // Act
        List<String> deliveries = aliasesForTopicsSentTwice(topicAliasMaximum);

        // Assert: EMQX does not reassign an alias once the client's maximum is reached.
        assertThat(deliveries).containsExactlyElementsOf(aliasesForTheFirst(aliased));
    }

    @Test
    void overlapping_subscriptions_get_a_copy_each_with_its_own_identifier() throws Exception {
        // Act
        List<List<Integer>> copies = identifiersOfOverlappingCopies(
                "identified",
                identifier(1),
                identifier(2)
        );

        // Assert
        assertThat(copies).containsExactlyInAnyOrder(List.of(1), List.of(2));
    }

    @Test
    void overlapping_subscriptions_without_identifiers_get_a_copy_each() throws Exception {
        // Act
        List<List<Integer>> copies = identifiersOfOverlappingCopies("plain", EMPTY, EMPTY);

        // Assert
        assertThat(copies).containsExactly(List.of(), List.of());
    }

    @Test
    void a_publication_without_subscribers_is_answered_with_no_matching_subscribers()
            throws Exception {
        assertThat(answerToUnmatchedPublication(Qos.AT_LEAST_ONCE))
                .isEqualTo(new Packet.PubAck(11, NO_MATCHING_SUBSCRIBERS, EMPTY));
    }

    @Test
    void a_qos_2_publication_without_subscribers_is_answered_with_no_matching_subscribers()
            throws Exception {
        assertThat(answerToUnmatchedPublication(Qos.EXACTLY_ONCE))
                .isEqualTo(new Packet.PubRec(11, NO_MATCHING_SUBSCRIBERS, EMPTY));
    }

    @Test
    void unsubscribing_from_a_filter_never_subscribed_is_answered_with_no_subscription_existed()
            throws Exception {
        assertThat(answerToUnsubscribingWhatWasNeverSubscribed())
                .isEqualTo(new Packet.UnsubAck(1, List.of(NO_SUBSCRIPTION_EXISTED), EMPTY));
    }

    @Test
    void a_shared_subscription_hands_the_messages_to_its_members_in_turn() throws Exception {
        // Assert: which member takes the first message varies between runs.
        assertThat(recipientsOfSharedMessages()).isIn(
                List.of(0, 1, 0, 1, 0, 1, 0, 1, 0, 1),
                List.of(1, 0, 1, 0, 1, 0, 1, 0, 1, 0)
        );
    }

    @Test
    void qos_2_publications_awaiting_release_end_the_connection_beyond_100() throws Exception {
        // Act: EMQX announces a Receive Maximum of 32 but takes up to 100 QoS 2 publications
        // that await release.
        Packet answer = answerToOneMoreUnreleasedPublication(100);

        // Assert
        assertThat(answer).isEqualTo(new Packet.Disconnect(RECEIVE_MAXIMUM_EXCEEDED, EMPTY));
    }
}
