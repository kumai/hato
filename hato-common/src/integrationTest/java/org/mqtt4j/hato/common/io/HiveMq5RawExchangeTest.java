package org.mqtt4j.hato.common.io;

import java.io.IOException;
import java.util.HexFormat;
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
import org.mqtt4j.hato.testsupport.HiveMq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.protocol.PropertyId.REASON_STRING;
import static org.mqtt4j.hato.common.value.ReasonCode.NO_MATCHING_SUBSCRIBERS;
import static org.mqtt4j.hato.common.value.ReasonCode.RECEIVE_MAXIMUM_EXCEEDED;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Records how the pinned HiveMQ Community Edition 2026.5 image answers MQTT 5.0 clients.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker is not available and hato.docker.required=false"
)
class HiveMq5RawExchangeTest extends Mqtt5RawExchangeTest {

    private static final GenericContainer<?> hiveMq = HiveMq.container();

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void startBroker() {
        Docker.start(hiveMq);
    }

    @AfterAll
    static void stopBroker() {
        hiveMq.stop();
    }

    @Override
    RawSession open() throws IOException {
        return RawSession.open(codec, hiveMq, HiveMq.MQTT_PORT);
    }

    @Test
    void the_connack_announces_a_receive_maximum_of_10_and_5_topic_aliases() throws Exception {
        // Act
        byte[] wire = connAckFrame("hato-hivemq-connack", EMPTY);

        // Assert: receive maximum 10 and alias maximum 5, in this order, and nothing else.
        assertThat(wire).isEqualTo(HexFormat.of().parseHex("200900000621000a220005"));
    }

    @ParameterizedTest
    @ValueSource(ints = {16, 65_535})
    void no_aliases_reach_a_client_that_allows_them(int topicAliasMaximum) throws Exception {
        // Act
        List<String> deliveries = aliasesForTopicsSentTwice(topicAliasMaximum);

        // Assert
        assertThat(deliveries).containsExactlyElementsOf(aliasesForTheFirst(0));
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
    void a_qos_2_publication_without_subscribers_is_answered_with_no_matching_subscribers()
            throws Exception {
        assertThat(answerToUnmatchedPublication(Qos.EXACTLY_ONCE))
                .isEqualTo(new Packet.PubRec(11, NO_MATCHING_SUBSCRIBERS, EMPTY));
    }

    @Test
    void unsubscribing_from_a_filter_never_subscribed_is_answered_with_success()
            throws Exception {
        assertThat(answerToUnsubscribingWhatWasNeverSubscribed())
                .isEqualTo(new Packet.UnsubAck(1, List.of(SUCCESS), EMPTY));
    }

    @Test
    void a_shared_subscription_hands_each_message_to_one_member() throws Exception {
        // Assert: every message reaches exactly one member, which the helper checks. HiveMQ does
        // not take turns, and a quick run of messages often goes to a single member.
        assertThat(recipientsOfSharedMessages()).hasSize(SHARED_MESSAGES);
    }

    @Test
    void a_qos_2_publication_beyond_the_receive_maximum_ends_the_connection() throws Exception {
        // Act: HiveMQ announces a Receive Maximum of 10.
        Packet answer = answerToOneMoreUnreleasedPublication(10);

        // Assert
        var reason = new Property.StringValue(
                REASON_STRING,
                "Too many concurrent PUBLISH messages sent."
        );
        assertThat(answer)
                .isEqualTo(new Packet.Disconnect(RECEIVE_MAXIMUM_EXCEEDED, properties(reason)));
    }
}
