package org.mqtt4j.hato;

import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserPropertyTest {

    @Test
    void names_and_values_may_be_empty_or_take_65535_bytes() {
        // Act
        var empty = new UserProperty("", "");
        var largest = new UserProperty("n".repeat(65_535), "v".repeat(65_535));

        // Assert
        assertThat(empty.name()).isEmpty();
        assertThat(largest.value()).hasSize(65_535);
    }

    static Stream<Named<String>> stringsThatMqttCannotCarry() {
        return Stream.of(
                Named.of("U+0000", "text\0more"),
                Named.of("an unpaired surrogate", "text\uD800"),
                Named.of("65536 UTF-8 bytes", "é".repeat(32_768))
        );
    }

    @ParameterizedTest
    @MethodSource("stringsThatMqttCannotCarry")
    void a_name_or_value_that_mqtt_cannot_carry_is_rejected(String invalid) {
        // Act & Assert
        assertThatThrownBy(() -> new UserProperty(invalid, "value"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new UserProperty("name", invalid))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void user_properties_with_the_same_name_and_value_are_equal() {
        // Arrange
        var sut = new UserProperty("name", "value");

        // Act & Assert
        assertThat(sut)
                .isEqualTo(new UserProperty("name", "value"))
                .hasSameHashCodeAs(new UserProperty("name", "value"))
                .isNotEqualTo(new UserProperty("name", "other"))
                .isNotEqualTo(new UserProperty("other", "value"));
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null component contracts.
    void requires_a_name_and_a_value() {
        // Act & Assert
        assertThatThrownBy(() -> new UserProperty(null, "value"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new UserProperty("name", null))
                .isInstanceOf(NullPointerException.class);
    }
}
