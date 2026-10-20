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
import org.mqtt4j.hato.common.protocol.Properties;
import org.mqtt4j.hato.common.protocol.Property;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.Mosquitto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.protocol.PropertyId.REASON_STRING;
import static org.mqtt4j.hato.common.value.ReasonCode.NO_MATCHING_SUBSCRIBERS;
import static org.mqtt4j.hato.common.value.ReasonCode.NO_SUBSCRIPTION_EXISTED;
import static org.mqtt4j.hato.common.value.ReasonCode.QUOTA_EXCEEDED;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Records how the pinned Mosquitto 2.1.2 image answers MQTT 5.0 clients.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker is not available and hato.docker.required=false"
)
class Mosquitto5RawExchangeTest extends Mqtt5RawExchangeTest {

    private static final GenericContainer<?> mosquitto = Mosquitto.container();

    static boolean dockerIsAvailableOrRequired() {
        return Docker.availableOrRequired();
    }

    @BeforeAll
    static void startBroker() {
        Docker.start(mosquitto);
    }

    @AfterAll
    static void stopBroker() {
        mosquitto.stop();
    }

    @Override
    RawSession open() throws IOException {
        return RawSession.open(codec, mosquitto, Mosquitto.MQTT_PORT);
    }

    @Test
    void connack_properties_and_disconnect_are_exchanged_with_mosquitto() throws Exception {
        try (RawSession client = open()) {
            client.send(connect("hato-five-wire", EMPTY));

            byte[] wire = client.nextFrame();
            // Alias maximum 10, maximum packet size 2,000,000, and receive maximum 20, in this
            // order.
            assertThat(wire).isEqualTo(HexFormat.of().parseHex("200e00000b22000a27001e8480210014"));
            var connAck = (Packet.ConnAck) codec.decode(wire);
            assertThat(connAck.reasonCode()).isEqualTo(SUCCESS);
            assertThat(connAck.sessionPresent()).isFalse();
            assertThat(connAck.properties().entries()).isNotEmpty();
            client.send(new Packet.PingReq());
            assertThat(client.expect(Packet.PingResp.class)).isEqualTo(new Packet.PingResp());
            Properties properties = properties(new Property.StringValue(REASON_STRING, "finished"));
            client.send(new Packet.Disconnect(SUCCESS, properties));
            client.expectEndOfStream();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {16, 65_535})
    void aliases_go_to_the_first_ten_topics_whatever_the_client_allows(int topicAliasMaximum)
            throws Exception {
        // Act
        List<String> deliveries = aliasesForTopicsSentTwice(topicAliasMaximum);

        // Assert: Mosquitto keeps at most 10 aliases for each client, its default broker-side
        // maximum, and does not reassign them.
        assertThat(deliveries).containsExactlyElementsOf(aliasesForTheFirst(10));
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
    void a_qos_2_publication_without_subscribers_is_answered_with_success() throws Exception {
        assertThat(answerToUnmatchedPublication(Qos.EXACTLY_ONCE))
                .isEqualTo(new Packet.PubRec(11, SUCCESS, EMPTY));
    }

    @Test
    void unsubscribing_from_a_filter_never_subscribed_is_answered_with_no_subscription_existed()
            throws Exception {
        assertThat(answerToUnsubscribingWhatWasNeverSubscribed())
                .isEqualTo(new Packet.UnsubAck(1, List.of(NO_SUBSCRIPTION_EXISTED), EMPTY));
    }

    @Test
    void a_shared_subscription_hands_the_messages_to_its_members_in_turn() throws Exception {
        assertThat(recipientsOfSharedMessages()).isIn(
                List.of(0, 1, 0, 1, 0, 1, 0, 1, 0, 1),
                List.of(1, 0, 1, 0, 1, 0, 1, 0, 1, 0)
        );
    }

    @Test
    void a_qos_2_publication_beyond_the_receive_maximum_is_refused_with_quota_exceeded()
            throws Exception {
        // Act: Mosquitto announces a Receive Maximum of 20.
        Packet answer = answerToOneMoreUnreleasedPublication(20);

        // Assert: Mosquitto refuses this publication alone, with a PUBREC rather than a
        // DISCONNECT.
        assertThat(answer).isEqualTo(new Packet.PubRec(21, QUOTA_EXCEEDED, EMPTY));
    }
}
