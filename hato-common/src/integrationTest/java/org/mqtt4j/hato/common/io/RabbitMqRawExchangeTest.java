package org.mqtt4j.hato.common.io;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;

import org.mqtt4j.hato.common.protocol.Packet;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.testsupport.Docker;
import org.mqtt4j.hato.testsupport.RabbitMq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Records how the pinned RabbitMQ 4.4.0 image, with its MQTT plugin, answers MQTT 3.1.1
/// clients.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker is not available and hato.docker.required=false"
)
class RabbitMqRawExchangeTest extends MqttRawExchangeTest {

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

    @Test
    void a_qos_2_publication_is_accepted_at_qos_1_with_puback_and_the_connection_stays()
            throws Exception {
        // The helper checks that a PINGREQ is still answered afterwards.
        assertThat(answerToQos2Publication()).isEqualTo(new Packet.PubAck(1, SUCCESS, EMPTY));
    }

    @Test
    void overlapping_subscriptions_share_one_copy() throws Exception {
        assertThat(copiesForOverlappingSubscriptions()).hasSize(1);
    }

    @Test
    void a_retained_message_reaches_a_new_subscription_to_its_topic_after_the_suback()
            throws Exception {
        // Arrange
        String prefix = "hato/three/rabbitmq/exact";

        // Act & Assert
        assertThat(retainedForNewSubscription(prefix, prefix + "/topic"))
                .containsExactly("SUBACK", "retained");
    }

    @ParameterizedTest
    @CsvSource({
            "multi, #",
            "single, +"
    })
    void a_retained_message_reaches_no_new_subscription_with_a_wildcard(
            String name,
            String wildcard
    ) throws Exception {
        // Arrange
        String prefix = "hato/three/rabbitmq/" + name;

        // Act & Assert
        assertThat(retainedForNewSubscription(prefix, prefix + "/" + wildcard))
                .containsExactly("SUBACK");
    }

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_MOST_ONCE", "AT_LEAST_ONCE"})
    void a_resumed_session_delivers_the_message_sent_while_away(Qos qos) throws Exception {
        assertThat(resume(qos)).isEqualTo(new Resumption(true, 1));
    }

    @Test
    void a_taken_over_connection_is_closed_and_its_will_published() throws Exception {
        // Act
        Takeover takeover = takeOver("hato-rabbitmq-taken-over");

        // Assert
        assertThat(takeover.toTakenOverConnection()).isEmpty();
        assertThat(takeover.willPublished()).isTrue();
    }
}
