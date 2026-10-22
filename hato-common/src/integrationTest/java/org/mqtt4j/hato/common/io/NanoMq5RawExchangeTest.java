package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.protocol.Property;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.NanoMq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.protocol.PropertyId.MAXIMUM_PACKET_SIZE;
import static org.mqtt4j.hato.common.protocol.PropertyId.RECEIVE_MAXIMUM;
import static org.mqtt4j.hato.common.protocol.PropertyId.SHARED_SUBSCRIPTION_AVAILABLE;
import static org.mqtt4j.hato.common.protocol.PropertyId.SUBSCRIPTION_IDENTIFIER_AVAILABLE;
import static org.mqtt4j.hato.common.protocol.PropertyId.TOPIC_ALIAS_MAXIMUM;
import static org.mqtt4j.hato.common.protocol.PropertyId.WILDCARD_SUBSCRIPTION_AVAILABLE;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Records how the pinned NanoMQ 0.25.7 image answers MQTT 5.0 clients.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker is not available and hato.docker.required=false"
)
class NanoMq5RawExchangeTest extends Mqtt5RawExchangeTest {

    private static final GenericContainer<?> nanoMq = NanoMq.container();

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void startBroker() {
        Docker.start(nanoMq);
    }

    @AfterAll
    static void stopBroker() {
        nanoMq.stop();
    }

    @Override
    RawSession open() throws IOException {
        return RawSession.open(codec, nanoMq, NanoMq.MQTT_PORT);
    }

    // NanoMQ forwards on several threads, so publications on different topics can swap places.
    @Override
    boolean keepsOrderAcrossTopics() {
        return false;
    }

    @Test
    void the_connack_announces_identifiers_and_wildcard_and_shared_subscriptions_with_its_limits()
            throws Exception {
        // Act
        var connAck = (Packet.ConnAck) codec.decode(connAckFrame("hato-nanomq-connack", EMPTY));

        // Assert: no Receive Maximum, Maximum QoS, or Retain Available, so MQTT 5.0 assumes
        // 65,535, QoS 2, and retained messages.
        assertThat(connAck.reasonCode()).isEqualTo(SUCCESS);
        assertThat(connAck.sessionPresent()).isFalse();
        assertThat(connAck.properties().entries()).containsExactlyInAnyOrder(
                new Property.ByteValue(SHARED_SUBSCRIPTION_AVAILABLE, 1),
                new Property.ByteValue(SUBSCRIPTION_IDENTIFIER_AVAILABLE, 1),
                new Property.ByteValue(WILDCARD_SUBSCRIPTION_AVAILABLE, 1),
                new Property.TwoByteValue(TOPIC_ALIAS_MAXIMUM, 1024),
                new Property.FourByteValue(MAXIMUM_PACKET_SIZE, 268_435_456)
        );
    }

    @Test
    void the_announced_maximum_packet_size_and_receive_maximum_are_those_of_the_client()
            throws Exception {
        // Arrange
        var maximumPacketSize = new Property.FourByteValue(MAXIMUM_PACKET_SIZE, 1_000);
        var receiveMaximum = new Property.TwoByteValue(RECEIVE_MAXIMUM, 5);

        // Act
        var connAck = (Packet.ConnAck) codec.decode(connAckFrame(
                "hato-nanomq-client-limits",
                properties(maximumPacketSize, receiveMaximum)
        ));

        // Assert: NanoMQ repeats the client's limits as its own, although they describe what the
        // client receives.
        assertThat(connAck.properties().entries()).contains(maximumPacketSize, receiveMaximum);
    }

    @Test
    void a_session_resumes_only_when_it_did_not_start_with_clean_start() throws Exception {
        // Assert: a Session Expiry Interval keeps a session that Clean Start began, by MQTT 5.0
        // [MQTT-3.1.2-23], but NanoMQ ends such a session with its connection.
        assertThat(resumesSession(true)).isFalse();
        assertThat(resumesSession(false)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {16, 65_535})
    void no_aliases_reach_a_client_that_allows_them(int topicAliasMaximum) throws Exception {
        // Act
        List<String> deliveries = aliasesForTopicsSentTwice(topicAliasMaximum);

        // Assert: each topic arrives twice, by its name alone, in an order that differs between
        // runs.
        assertThat(deliveries).containsExactlyInAnyOrderElementsOf(aliasesForTheFirst(0));
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
    void a_publication_without_subscribers_is_answered_with_success() throws Exception {
        assertThat(answerToUnmatchedPublication(Qos.AT_LEAST_ONCE))
                .isEqualTo(new Packet.PubAck(11, SUCCESS, EMPTY));
    }

    @Test
    void a_qos_2_publication_without_subscribers_is_answered_with_success() throws Exception {
        assertThat(answerToUnmatchedPublication(Qos.EXACTLY_ONCE))
                .isEqualTo(new Packet.PubRec(11, SUCCESS, EMPTY));
    }

    @Test
    void unsubscribing_from_a_filter_never_subscribed_is_answered_with_success()
            throws Exception {
        assertThat(answerToUnsubscribingWhatWasNeverSubscribed())
                .isEqualTo(new Packet.UnsubAck(1, List.of(SUCCESS), EMPTY));
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
    void a_taken_over_connection_is_closed_without_a_disconnect_and_its_will_published()
            throws Exception {
        // Act
        Takeover takeover = takeOver("hato-nanomq-taken-over");

        // Assert: MQTT 5.0 lets the broker close the connection without the DISCONNECT that
        // reports Session taken over.
        assertThat(takeover.toTakenOverConnection()).isEmpty();
        assertThat(takeover.willPublished()).isTrue();
    }
}
