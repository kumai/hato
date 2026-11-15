package org.mqtt4j.hato;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttCredentialsTest {

    @Test
    void password_is_unchanged_by_input_and_accessor_mutation() {
        // Arrange
        var password = new byte[] {0, -1, 2};
        var sut = MqttCredentials.of("user", password);

        // Act
        password[0] = 9;
        byte[] returned = sut.password().orElseThrow();
        returned[1] = 9;

        // Assert
        assertThat(sut.username()).isEqualTo("user");
        assertThat(sut.password().orElseThrow()).containsExactly(0, -1, 2);
    }

    @Test
    void an_absent_password_and_an_empty_password_are_distinct() {
        var absent = MqttCredentials.of("");
        var empty = MqttCredentials.of("", new byte[0]);

        assertThat(absent.username()).isEmpty();
        assertThat(absent.password()).isEmpty();
        assertThat(empty.password()).isPresent();
        assertThat(empty.password().orElseThrow()).isEmpty();
        assertThat(absent).isNotEqualTo(empty);
    }

    @Test
    void equality_and_hash_code_compare_password_contents() {
        var sut = MqttCredentials.of("user", new byte[] {1, 2});
        var same = MqttCredentials.of("user", new byte[] {1, 2});

        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(same).isEqualTo(sut);
        assertThat(MqttCredentials.of("user"))
                .isEqualTo(MqttCredentials.of("user"))
                .hasSameHashCodeAs(MqttCredentials.of("user"));
    }

    static Stream<MqttCredentials> differentCredentials() {
        return Stream.of(
                MqttCredentials.of("another", new byte[] {1, 2}),
                MqttCredentials.of("user", new byte[] {1, 3}),
                MqttCredentials.of("user")
        );
    }

    @ParameterizedTest
    @MethodSource("differentCredentials")
    void differing_credentials_are_not_equal(MqttCredentials different) {
        var sut = MqttCredentials.of("user", new byte[] {1, 2});

        assertThat(sut).isNotEqualTo(different);
    }

    @Test
    void string_representation_hides_both_credentials() {
        var sut = MqttCredentials.of(
                "private-user",
                "private-secret".getBytes(StandardCharsets.UTF_8)
        );

        assertThat(sut.toString()).doesNotContain("private-user", "private-secret");
    }

    @Test
    void maximum_length_credentials_are_accepted() {
        String username = "あ".repeat(21_845);
        var password = new byte[65_535];

        var sut = MqttCredentials.of(username, password);

        assertThat(sut.username()).isEqualTo(username);
        assertThat(sut.password().orElseThrow()).hasSize(65_535);
    }

    @Test
    void oversized_credentials_are_rejected() {
        assertThatThrownBy(() -> MqttCredentials.of("あ".repeat(21_846)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MqttCredentials.of("user", new byte[65_536]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"private\0user", "private\uD800user", "private\uDC00user"})
    void malformed_usernames_are_rejected_without_exposing_them(String username) {
        assertThatThrownBy(() -> MqttCredentials.of(username))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("private");
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the factories' non-null contracts.
    void factories_require_non_null_credentials() {
        assertThatThrownBy(() -> MqttCredentials.of(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> MqttCredentials.of("user", null))
                .isInstanceOf(NullPointerException.class);
    }
}
