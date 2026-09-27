package org.mqtt4j.mqtj;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttMessageTest {

    @Test
    void payload_is_unchanged_by_input_and_accessor_mutation() {
        // Arrange
        var payload = new byte[] {1, 2};
        var sut = new MqttMessage(new Topic("events/a"), payload, Qos.AT_MOST_ONCE, false);

        // Act
        payload[0] = 9;
        byte[] received = sut.payload();
        received[1] = 9;

        // Assert
        assertThat(sut.payload()).containsExactly(1, 2);
    }

    @Test
    void equality_and_hash_code_compare_payload_contents() {
        // Arrange
        var sut = new MqttMessage(new Topic("events/a"), new byte[] {1}, Qos.AT_MOST_ONCE, false);
        var same = new MqttMessage(new Topic("events/a"), new byte[] {1}, Qos.AT_MOST_ONCE, false);

        // Act & Assert
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(same).isEqualTo(sut);
    }

    static Stream<MqttMessage> differentMessages() {
        return Stream.of(
                new MqttMessage(new Topic("events/b"), new byte[] {1}, Qos.AT_MOST_ONCE, false),
                new MqttMessage(new Topic("events/a"), new byte[] {2}, Qos.AT_MOST_ONCE, false),
                new MqttMessage(new Topic("events/a"), new byte[] {1}, Qos.AT_LEAST_ONCE, false),
                new MqttMessage(new Topic("events/a"), new byte[] {1}, Qos.AT_MOST_ONCE, true)
        );
    }

    @ParameterizedTest
    @MethodSource("differentMessages")
    void differing_message_components_are_not_equal(MqttMessage different) {
        // Arrange
        var sut = new MqttMessage(new Topic("events/a"), new byte[] {1}, Qos.AT_MOST_ONCE, false);

        // Act & Assert
        assertThat(sut).isNotEqualTo(different);
    }

    @Test
    void string_representation_exposes_size_without_payload_contents() {
        // Arrange
        var sut = new MqttMessage(
                new Topic("events/a"),
                "s3cret".getBytes(StandardCharsets.UTF_8),
                Qos.AT_MOST_ONCE,
                false
        );

        // Act & Assert
        assertThat(sut.toString()).contains("payloadBytes=6").doesNotContain("s3cret");
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the constructor's non-null contract.
    void requires_topic_payload_and_qos() {
        // Arrange
        var topic = new Topic("events/a");

        // Act & Assert
        assertThatThrownBy(() -> new MqttMessage(null, new byte[0], Qos.AT_MOST_ONCE, false))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new MqttMessage(topic, null, Qos.AT_MOST_ONCE, false))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new MqttMessage(topic, new byte[0], null, false))
                .isInstanceOf(NullPointerException.class);
    }
}
