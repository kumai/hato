package org.mqtt4j.mqtj.common.value;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "a", "a/b", "/", "//", "/a", "a/", "a//b", " ", " a ", "$SYS/status",
            "今日はカレー", "A\uD869\uDED4", "\uFEFFa", "a\u0001b", "a\uFFFFb"
    })
    void valid_names_are_preserved_exactly(String name) {
        // MQTT 4.7.3 and [MQTT-1.5.3-3]: preserve empty levels, whitespace, and BOM.
        var sut = new Topic(name);

        assertThat(sut.name()).isEqualTo(name);
        assertThat(sut.toString()).isEqualTo(name);
    }

    @Test
    void a_null_name_is_rejected() {
        assertThatThrownBy(() -> new Topic(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void an_empty_name_is_rejected() {
        // [MQTT-4.7.3-1]
        assertThatThrownBy(() -> new Topic("")).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\0", "\0a", "a\0", "a\0b"})
    void names_containing_nul_are_rejected(String name) {
        // [MQTT-4.7.3-2]
        assertThatThrownBy(() -> new Topic(name)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"+", "#", "a/+", "a/#", "a+b", "a#b"})
    void names_containing_wildcards_are_rejected(String name) {
        // [MQTT-4.7.1-1]: wildcards belong only in topic filters.
        assertThatThrownBy(() -> new Topic(name)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "\uD800", "\uDC00", "a\uD800b", "a\uDC00b", "\uDC00\uD800",
            "\uD800\uD800", "\uDC00\uDC00"
    })
    void names_containing_unpaired_surrogates_are_rejected(String name) {
        // [MQTT-1.5.3-1]: the name must be encodable as well-formed UTF-8.
        assertThatThrownBy(() -> new Topic(name)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"a, 1", "é, 2", "あ, 3", "\uD869\uDED4, 4"})
    void names_of_exactly_65535_utf8_bytes_are_accepted(String character, int bytesPerCharacter) {
        // [MQTT-4.7.3-3]: exercise one-, two-, three-, and four-byte characters.
        // Arrange
        String name = character.repeat(65535 / bytesPerCharacter)
                + "a".repeat(65535 % bytesPerCharacter);
        // Act
        var sut = new Topic(name);
        // Assert
        assertThat(sut.name()).isEqualTo(name);
    }

    @ParameterizedTest
    @CsvSource({"a, 1", "é, 2", "あ, 3", "\uD869\uDED4, 4"})
    void names_of_65536_utf8_bytes_are_rejected(String character, int bytesPerCharacter) {
        // [MQTT-4.7.3-3]: the limit is in bytes, not Java string length.
        // Arrange
        String name = character.repeat(65536 / bytesPerCharacter)
                + "a".repeat(65536 % bytesPerCharacter);
        // Act & Assert
        assertThatThrownBy(() -> new Topic(name)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void names_are_case_sensitive_and_are_not_normalized() {
        var sut = new Topic("café");

        assertThat(sut).isNotEqualTo(new Topic("CAFÉ"));
        assertThat(sut).isNotEqualTo(new Topic("cafe\u0301"));
    }
}
