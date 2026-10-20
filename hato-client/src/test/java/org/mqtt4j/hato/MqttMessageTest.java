package org.mqtt4j.hato;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttMessageTest {

    private static final Duration MAX_EXPIRY = Duration.ofSeconds(4_294_967_295L);

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
    void a_new_message_has_no_properties() {
        // Act
        var sut = MqttMessage.of(new Topic("events/a"), new byte[] {1}, Qos.AT_MOST_ONCE, false);

        // Assert
        assertThat(sut.payloadUtf8()).isFalse();
        assertThat(sut.messageExpiry()).isEmpty();
        assertThat(sut.contentType()).isEmpty();
        assertThat(sut.responseTopic()).isEmpty();
        assertThat(sut.correlationData()).isEmpty();
        assertThat(sut.userProperties()).isEmpty();
    }

    @Test
    void with_methods_set_one_property_each_and_keep_the_other_components() {
        // Arrange
        var userProperties = List.of(new UserProperty("a", "1"), new UserProperty("a", "2"));

        // Act
        MqttMessage sut = message()
                .withPayloadUtf8(true)
                .withMessageExpiry(Duration.ofSeconds(30))
                .withContentType("text/plain")
                .withResponseTopic(new Topic("replies/a"))
                .withCorrelationData(new byte[] {7})
                .withUserProperties(userProperties)
                .withTopic(new Topic("events/b"))
                .withPayload(new byte[] {2})
                .withQos(Qos.EXACTLY_ONCE)
                .withRetain(true);

        // Assert
        assertThat(sut.topic()).isEqualTo(new Topic("events/b"));
        assertThat(sut.payload()).containsExactly(2);
        assertThat(sut.qos()).isEqualTo(Qos.EXACTLY_ONCE);
        assertThat(sut.retain()).isTrue();
        assertThat(sut.payloadUtf8()).isTrue();
        assertThat(sut.messageExpiry()).contains(Duration.ofSeconds(30));
        assertThat(sut.contentType()).contains("text/plain");
        assertThat(sut.responseTopic()).contains(new Topic("replies/a"));
        assertThat(sut.correlationData()).hasValueSatisfying(
                data -> assertThat(data).containsExactly(7)
        );
        assertThat(sut.userProperties()).isEqualTo(userProperties);
    }

    static Stream<Named<UnaryOperator<MqttMessage>>> propertyBoundaries() {
        return Stream.of(
                Named.of("no lifetime at all", m -> m.withMessageExpiry(Duration.ZERO)),
                Named.of("the longest lifetime", m -> m.withMessageExpiry(MAX_EXPIRY)),
                Named.of(
                        "a fraction below the longest lifetime",
                        m -> m.withMessageExpiry(MAX_EXPIRY.minusMillis(500))
                ),
                Named.of("an empty content type", m -> m.withContentType("")),
                Named.of(
                        "a content type of 65535 bytes",
                        m -> m.withContentType("x".repeat(65_535))
                ),
                Named.of("empty correlation data", m -> m.withCorrelationData(new byte[0])),
                Named.of(
                        "correlation data of 65535 bytes",
                        m -> m.withCorrelationData(new byte[65_535])
                )
        );
    }

    @ParameterizedTest
    @MethodSource("propertyBoundaries")
    void properties_at_the_limits_are_accepted(UnaryOperator<MqttMessage> change) {
        // Act & Assert
        assertThatCode(() -> change.apply(message())).doesNotThrowAnyException();
    }

    static Stream<Named<UnaryOperator<MqttMessage>>> propertiesBeyondLimits() {
        return Stream.of(
                Named.of(
                        "a negative lifetime",
                        m -> m.withMessageExpiry(Duration.ofNanos(-1))
                ),
                Named.of(
                        "a lifetime a nanosecond beyond 4294967295 seconds",
                        m -> m.withMessageExpiry(MAX_EXPIRY.plusNanos(1))
                ),
                Named.of(
                        "a content type with U+0000",
                        m -> m.withContentType("text\0plain")
                ),
                Named.of(
                        "a content type with an unpaired surrogate",
                        m -> m.withContentType("text\uD800")
                ),
                Named.of(
                        "a content type of 65536 UTF-8 bytes",
                        m -> m.withContentType("\u00e9".repeat(32_768))
                ),
                Named.of(
                        "correlation data of 65536 bytes",
                        m -> m.withCorrelationData(new byte[65_536])
                )
        );
    }

    @ParameterizedTest
    @MethodSource("propertiesBeyondLimits")
    void properties_beyond_the_limits_are_rejected(UnaryOperator<MqttMessage> change) {
        // Act & Assert
        assertThatThrownBy(() -> change.apply(message()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void correlation_data_and_user_properties_are_unchanged_by_input_and_accessor_mutation() {
        // Arrange
        var correlationData = new byte[] {1, 2};
        var userProperties = new ArrayList<>(List.of(new UserProperty("a", "1")));
        MqttMessage sut = message()
                .withCorrelationData(correlationData)
                .withUserProperties(userProperties);

        // Act
        correlationData[0] = 9;
        sut.correlationData().orElseThrow()[1] = 9;
        userProperties.add(new UserProperty("b", "2"));

        // Assert
        assertThat(sut.correlationData()).hasValueSatisfying(
                data -> assertThat(data).containsExactly(1, 2)
        );
        assertThat(sut.userProperties()).containsExactly(new UserProperty("a", "1"));
        assertThatThrownBy(() -> sut.userProperties().add(new UserProperty("c", "3")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void equality_and_hash_code_include_the_properties() {
        // Arrange
        MqttMessage sut = withAllProperties(message());
        MqttMessage same = withAllProperties(message());

        // Act & Assert
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(sut).isNotEqualTo(message());
    }

    static Stream<MqttMessage> messagesWithOneDifferentProperty() {
        MqttMessage base = withAllProperties(message());
        return Stream.of(
                base.withPayloadUtf8(false),
                base.withMessageExpiry(Duration.ofSeconds(31)),
                base.withContentType("text/html"),
                base.withResponseTopic(new Topic("replies/b")),
                base.withCorrelationData(new byte[] {8}),
                base.withUserProperties(List.of(
                        new UserProperty("source", "relay"),
                        new UserProperty("source", "sensor")
                ))
        );
    }

    @ParameterizedTest
    @MethodSource("messagesWithOneDifferentProperty")
    void a_differing_property_makes_messages_unequal(MqttMessage different) {
        // Act & Assert: the last case differs only in the order of its User Properties.
        assertThat(withAllProperties(message())).isNotEqualTo(different);
    }

    @Test
    void string_representation_shows_metadata_but_only_the_sizes_of_application_data() {
        // Arrange
        MqttMessage sut = message()
                .withPayloadUtf8(true)
                .withMessageExpiry(Duration.ofSeconds(30))
                .withContentType("text/plain")
                .withResponseTopic(new Topic("replies/a"))
                .withCorrelationData("request-17".getBytes(StandardCharsets.UTF_8))
                .withUserProperties(List.of(new UserProperty("token", "s3cret")));

        // Act
        String text = sut.toString();

        // Assert
        assertThat(text)
                .contains(
                        "payloadUtf8=true",
                        "messageExpiry=PT30S",
                        "contentType=text/plain",
                        "responseTopic=replies/a",
                        "correlationDataBytes=10",
                        "userProperties=1"
                )
                .doesNotContain("request-17", "token", "s3cret");
        assertThat(message().toString())
                .doesNotContain("payloadUtf8", "messageExpiry", "contentType", "responseTopic")
                .doesNotContain("correlationData", "userProperties");
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null parameter contracts.
    void property_with_methods_require_their_values() {
        // Arrange
        MqttMessage sut = message();

        // Act & Assert
        assertThatThrownBy(() -> sut.withMessageExpiry(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.withContentType(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.withResponseTopic(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.withCorrelationData(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.withUserProperties(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.withUserProperties(Arrays.asList((UserProperty) null)))
                .isInstanceOf(NullPointerException.class);
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

    private static MqttMessage message() {
        return MqttMessage.of(new Topic("events/a"), new byte[] {1}, Qos.AT_LEAST_ONCE, false);
    }

    private static MqttMessage withAllProperties(MqttMessage message) {
        return message.withPayloadUtf8(true)
                .withMessageExpiry(Duration.ofSeconds(30))
                .withContentType("text/plain")
                .withResponseTopic(new Topic("replies/a"))
                .withCorrelationData(new byte[] {7})
                .withUserProperties(List.of(
                        new UserProperty("source", "sensor"),
                        new UserProperty("source", "relay")
                ));
    }
}
