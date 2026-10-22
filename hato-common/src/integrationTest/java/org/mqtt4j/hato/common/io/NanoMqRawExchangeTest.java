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
import org.mqtt4j.hato.testsupport.NanoMq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.common.protocol.Properties.EMPTY;
import static org.mqtt4j.hato.common.value.ReasonCode.SUCCESS;

/// Records how the pinned NanoMQ 0.25.7 image answers MQTT 3.1.1 clients.
@EnabledIf(
        value = "dockerIsAvailableOrRequired",
        disabledReason = "Docker is not available and hato.docker.required=false"
)
class NanoMqRawExchangeTest extends MqttRawExchangeTest {

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
    void a_qos_2_publication_is_answered_with_pubrec() throws Exception {
        assertThat(answerToQos2Publication()).isEqualTo(new Packet.PubRec(1, SUCCESS, EMPTY));
    }

    @Test
    void overlapping_subscriptions_get_a_copy_each() throws Exception {
        assertThat(copiesForOverlappingSubscriptions()).hasSize(2);
    }

    @ParameterizedTest
    @CsvSource({
            "exact, topic",
            "multi, #",
            "single, +"
    })
    void a_retained_message_reaches_a_new_subscription_before_its_suback(
            String name,
            String lastLevel
    ) throws Exception {
        // Arrange
        String prefix = "hato/three/nanomq/" + name;

        // Act & Assert: MQTT lets the broker send it before the SUBACK [MQTT-3.8.4-3].
        assertThat(retainedForNewSubscription(prefix, prefix + "/" + lastLevel))
                .containsExactly("retained", "SUBACK");
    }

    @ParameterizedTest
    @EnumSource(value = Qos.class, names = {"AT_MOST_ONCE", "AT_LEAST_ONCE"})
    void a_resumed_session_keeps_its_subscription_but_no_message_sent_while_away(Qos qos)
            throws Exception {
        // Assert: MQTT 3.1.1 asks the broker to keep QoS 1 and 2 messages for the session
        // [MQTT-3.1.2-5], but NanoMQ drops even those.
        assertThat(resume(qos)).isEqualTo(new Resumption(true, 0));
    }

    @Test
    void a_retained_qos_0_publication_reaches_an_established_subscription_still_retained()
            throws Exception {
        // Assert: NanoMQ clears RETAIN for an established subscription at QoS 1 only.
        assertThat(retainOfForwardedRetainedMessage(Qos.AT_MOST_ONCE)).isTrue();
        assertThat(retainOfForwardedRetainedMessage(Qos.AT_LEAST_ONCE)).isFalse();
    }

    @Test
    void a_taken_over_connection_is_closed_and_its_will_published() throws Exception {
        // Act
        Takeover takeover = takeOver("hato-nanomq-taken-over");

        // Assert
        assertThat(takeover.toTakenOverConnection()).isEmpty();
        assertThat(takeover.willPublished()).isTrue();
    }
}
