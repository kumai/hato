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
        var sut = MqttMessage.of(new Topic("events/a"), payload, Qos.AT_MOST_ONCE, false);

        // Act
        payload[0] = 9;
        byte[] received = sut.payload();
        received[1] = 9;

        // Assert
        assertThat(sut.payload()).containsExactly(1, 2);
        assertThat(sut.payloadSize()).isEqualTo(2);
    }

    @Test
    void with_methods_change_only_the_requested_component_and_leave_the_source_unchanged() {
        // Arrange
        var topic = new Topic("events/a");
        var otherTopic = new Topic("events/b");
        var sut = MqttMessage.of(topic, new byte[] {1}, Qos.AT_MOST_ONCE, false);

        // Act & Assert
        assertThat(sut.withTopic(otherTopic)).isEqualTo(
                MqttMessage.of(otherTopic, new byte[] {1}, Qos.AT_MOST_ONCE, false));
        assertThat(sut.withPayload(new byte[] {2})).isEqualTo(
                MqttMessage.of(topic, new byte[] {2}, Qos.AT_MOST_ONCE, false));
        assertThat(sut.withQos(Qos.EXACTLY_ONCE)).isEqualTo(
                MqttMessage.of(topic, new byte[] {1}, Qos.EXACTLY_ONCE, false));
        assertThat(sut.withRetain(true)).isEqualTo(
                MqttMessage.of(topic, new byte[] {1}, Qos.AT_MOST_ONCE, true));
        assertThat(sut).isEqualTo(
                MqttMessage.of(topic, new byte[] {1}, Qos.AT_MOST_ONCE, false));
    }

    @Test
    void replacement_payload_is_unchanged_by_input_and_accessor_mutation() {
        // Arrange
        var sut = MqttMessage.of(new Topic("events/a"), new byte[] {1, 2},
                Qos.AT_MOST_ONCE, false);
        var replacement = new byte[] {3, 4, 5};

        // Act
        MqttMessage changed = sut.withPayload(replacement);
        replacement[0] = 9;
        changed.payload()[1] = 9;

        // Assert
        assertThat(changed.payload()).containsExactly(3, 4, 5);
        assertThat(changed.payloadSize()).isEqualTo(3);
        assertThat(sut.payload()).containsExactly(1, 2);
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null parameter contracts.
    void with_methods_require_topic_payload_and_qos() {
        // Arrange
        var sut = MqttMessage.of(new Topic("events/a"), new byte[0], Qos.AT_MOST_ONCE, false);

        // Act & Assert
        assertThatThrownBy(() -> sut.withTopic(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.withPayload(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.withQos(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void equality_and_hash_code_compare_payload_contents() {
        // Arrange
        var sut = MqttMessage.of(new Topic("events/a"), new byte[] {1}, Qos.AT_MOST_ONCE, false);
        var same = MqttMessage.of(new Topic("events/a"), new byte[] {1}, Qos.AT_MOST_ONCE, false);

        // Act & Assert
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(same).isEqualTo(sut);
    }

    static Stream<MqttMessage> differentMessages() {
        return Stream.of(
                MqttMessage.of(new Topic("events/b"), new byte[] {1}, Qos.AT_MOST_ONCE, false),
                MqttMessage.of(new Topic("events/a"), new byte[] {2}, Qos.AT_MOST_ONCE, false),
                MqttMessage.of(new Topic("events/a"), new byte[] {1}, Qos.AT_LEAST_ONCE, false),
                MqttMessage.of(new Topic("events/a"), new byte[] {1}, Qos.AT_MOST_ONCE, true)
        );
    }

    @ParameterizedTest
    @MethodSource("differentMessages")
    void differing_message_components_are_not_equal(MqttMessage different) {
        // Arrange
        var sut = MqttMessage.of(new Topic("events/a"), new byte[] {1}, Qos.AT_MOST_ONCE, false);

        // Act & Assert
        assertThat(sut).isNotEqualTo(different);
    }

    @Test
    void string_representation_exposes_size_without_payload_contents() {
        // Arrange
        var sut = MqttMessage.of(
                new Topic("events/a"),
                "s3cret".getBytes(StandardCharsets.UTF_8),
                Qos.AT_MOST_ONCE,
                false
        );

        // Act & Assert
        assertThat(sut.toString()).contains("payloadBytes=6").doesNotContain("s3cret");
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the factory's non-null contract.
    void requires_topic_payload_and_qos() {
        // Arrange
        var topic = new Topic("events/a");

        // Act & Assert
        assertThatThrownBy(() -> MqttMessage.of(null, new byte[0], Qos.AT_MOST_ONCE, false))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> MqttMessage.of(topic, null, Qos.AT_MOST_ONCE, false))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> MqttMessage.of(topic, new byte[0], null, false))
                .isInstanceOf(NullPointerException.class);
    }
}
