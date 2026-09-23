package org.mqtt4j.mqtj.common.value;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicFilterTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "a", "a/b", "/", "//", "/a", "a/", "a//b", " ", " a ", "$SYS/#",
            "+", "#", "+/+", "+/#", "/+", "/#", "a/+/b", "a/+", "a/#", "a//+/#",
            "$share", "$sharex/+", "$SHARE/g/+", "温度", "A\uD869\uDED4", "\uFEFFa",
            "a\u0001b", "a\uFFFFb"
    })
    void valid_filters_are_preserved_exactly(String pattern) {
        // MQTT 4.7 and [MQTT-1.5.3-3]: preserve empty levels, whitespace, and BOM.
        var sut = new TopicFilter(pattern);

        assertThat(sut.pattern()).isEqualTo(pattern);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "$share/g/a", "$share/g/+", "$share/g/#", "$share/g/a/+/#", "$share/g//",
            "$share/g//finance", "$share/g/ ", "$share/ /a", "$share/カツ/カレー/+",
            "$share/", "$share//", "$share//a", "$share/g", "$share/g/", "$share/+/a", "$share/#"
    })
    void dollar_prefixed_filters_are_preserved_without_shared_subscription_validation(
            String pattern
    ) {
        // MQTT 3.1.1 section 4.7 does not give $share/ any special meaning.
        var sut = new TopicFilter(pattern);

        assertThat(sut.pattern()).isEqualTo(pattern);
    }

    @Test
    void a_null_filter_is_rejected() {
        assertThatThrownBy(() -> new TopicFilter(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void an_empty_filter_is_rejected() {
        // [MQTT-4.7.3-1]
        assertThatThrownBy(() -> new TopicFilter("")).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\0", "a\0b", "$share/g\0/a", "$share/g/a\0"})
    void filters_containing_nul_are_rejected(String pattern) {
        // [MQTT-4.7.3-2]
        assertThatThrownBy(() -> new TopicFilter(pattern))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "\uD800", "\uDC00", "a\uD800b", "a\uDC00b", "\uDC00\uD800",
            "\uD800\uD800", "\uDC00\uDC00", "$share/g\uD800/a", "$share/g/\uDC00"
    })
    void filters_containing_unpaired_surrogates_are_rejected(String pattern) {
        // [MQTT-1.5.3-1]
        assertThatThrownBy(() -> new TopicFilter(pattern))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "a+", "+a", "++", "a/+b", "a/b+", "a/++", "+a/",
            "$share/g+/a", "$share/g/a+", "$share/g/+a", "$share/g/++"
    })
    void a_single_level_wildcard_must_occupy_an_entire_level(String pattern) {
        // [MQTT-4.7.1-3]
        assertThatThrownBy(() -> new TopicFilter(pattern))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "a#", "#a", "##", "#/a", "a/#/b", "a/#/", "a/#/#", "a/##",
            "$share/#/a", "$share/g#/a", "$share/g/a#", "$share/g/#/a",
            "$share/g/a/#/", "$share/g/##"
    })
    void a_multi_level_wildcard_must_occupy_the_entire_final_level(String pattern) {
        // [MQTT-4.7.1-2]
        assertThatThrownBy(() -> new TopicFilter(pattern))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"a, 1", "é, 2", "あ, 3", "\uD869\uDED4, 4"})
    void filters_of_exactly_65535_utf8_bytes_are_accepted(String character, int bytesPerCharacter) {
        // [MQTT-4.7.3-3]
        String pattern = character.repeat(65535 / bytesPerCharacter)
                + "a".repeat(65535 % bytesPerCharacter);

        var sut = new TopicFilter(pattern);

        assertThat(sut.pattern()).isEqualTo(pattern);
    }

    @ParameterizedTest
    @CsvSource({"a, 1", "é, 2", "あ, 3", "\uD869\uDED4, 4"})
    void filters_of_65536_utf8_bytes_are_rejected(String character, int bytesPerCharacter) {
        // [MQTT-4.7.3-3]
        String pattern = character.repeat(65536 / bytesPerCharacter)
                + "a".repeat(65536 % bytesPerCharacter);

        assertThatThrownBy(() -> new TopicFilter(pattern))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"$share/g/, 9", "$share/\uD869\uDED4/, 12"})
    void prefixed_filters_of_exactly_65535_total_utf8_bytes_are_accepted(String prefix, int bytes) {
        // [MQTT-4.7.3-3]: the prefix counts towards the limit.
        String pattern = prefix + "a".repeat(65535 - bytes);

        var sut = new TopicFilter(pattern);

        assertThat(sut.pattern()).isEqualTo(pattern);
    }

    @ParameterizedTest
    @CsvSource({"$share/g/, 9", "$share/\uD869\uDED4/, 12"})
    void prefixed_filters_of_65536_total_utf8_bytes_are_rejected(String prefix, int bytes) {
        // [MQTT-4.7.3-3]
        String pattern = prefix + "a".repeat(65536 - bytes);

        assertThatThrownBy(() -> new TopicFilter(pattern))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({
            "sport/tennis/player1/#, sport/tennis/player1",
            "sport/tennis/player1/#, sport/tennis/player1/ranking",
            "sport/tennis/player1/#, sport/tennis/player1/score/wimbledon",
            "sport/#, sport", "sport/#, sport/", "#, sport/tennis/player1",
            "sport/tennis/+, sport/tennis/player1", "sport/tennis/+, sport/tennis/player2",
            "sport/+, sport/", "+/+, /finance", "/+, /finance",
            "+/tennis/#, sport/tennis", "sport/+/player1, sport/tennis/player1",
            "/, /", "//, //", "a/, a/", "a//b, a//b", "+/+, /", "/#, /",
            "a/+/#, a/", "a/+/#, a//b", "+/#, a", "#, /", "+/+, /$SYS",
            "a/+, a/$SYS", "温度/+, 温度/\uD869\uDED4", "a.b, a.b", "a*b, a*b",
            "' a ', ' a '", "' ', ' '", "\uFEFFa, \uFEFFa"
    })
    void filters_match_topics_at_the_requested_levels(String pattern, String topicName) {
        // MQTT 4.7.1 examples, including [MQTT-4.7.1-2] and [MQTT-4.7.1-3].
        var sut = new TopicFilter(pattern);

        assertThat(sut.matches(new Topic(topicName))).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
            "sport/tennis/+, sport/tennis/player1/ranking", "sport/+, sport",
            "+, /finance", "+, /", "+/+, a", "a/+/#, a",
            "sport/#, sports", "sport/+/player1, sport/tennis/player2",
            "a, a/b", "a/b, a", "a/, a", "a, a/", "/a, a", "a, /a",
            "a//b, a/b", "a/b, a//b", "/, //", "//, /",
            "ACCOUNTS, Accounts", "' a ', a", "\uFEFFa, a", "café, cafe\u0301",
            "a.b, axb", "a*b, axxb", "a?b, acb"
    })
    void mismatched_levels_and_literal_text_do_not_match(String pattern, String topicName) {
        // MQTT 4.7.1 and [MQTT-4.7.3-4]: matching does not normalize names.
        var sut = new TopicFilter(pattern);

        assertThat(sut.matches(new Topic(topicName))).isFalse();
    }

    @ParameterizedTest
    @CsvSource({
            "#, $SYS/monitor/Clients", "+/monitor/Clients, $SYS/monitor/Clients",
            "+/#, $SYS", "+, $SYS", "#, $OTHER/status",
            "#, $share/g/a", "+/g/a, $share/g/a", "+/#, $share"
    })
    void a_leading_wildcard_does_not_match_a_topic_starting_with_dollar(
            String pattern,
            String topicName
    ) {
        // [MQTT-4.7.2-1]
        var sut = new TopicFilter(pattern);

        assertThat(sut.matches(new Topic(topicName))).isFalse();
    }

    @ParameterizedTest
    @CsvSource({
            "$SYS/#, $SYS/monitor/Clients", "$SYS/monitor/+, $SYS/monitor/Clients",
            "$SYS/#, $SYS", "$OTHER/+, $OTHER/status", "$share, $share",
            "$share/g/$SYS/#, $share/g/$SYS/monitor/Clients", "$share/g/$SYS/#, $share/g/$SYS"
    })
    void an_explicit_dollar_prefix_can_match_a_topic_starting_with_dollar(
            String pattern,
            String topicName
    ) {
        // MQTT 4.7.2 examples.
        var sut = new TopicFilter(pattern);

        assertThat(sut.matches(new Topic(topicName))).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
            "$share/+/a, $share/g/a", "$share/+/a, $share//a",
            "$share/g, $share/g", "$share/g/a, $share/g/a",
            "$share/, $share/", "$share//a, $share//a", "$share/g/, $share/g/",
            "$share/#, $share", "$share/g/sport/#, $share/g/sport",
            "$share/g/+, $share/g/a", "$share/g/#, $share/g/a/b", "$share/g//, $share/g//",
            "$share/g/a/+/#, $share/g/a/", "$share/g/a/+, $share/g/a/$SYS",
            "$share/カツ/カレー/+, $share/カツ/カレー/大盛り"
    })
    void dollar_prefixed_filters_match_all_their_levels(String pattern, String topicName) {
        var sut = new TopicFilter(pattern);

        assertThat(sut.matches(new Topic(topicName))).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
            "$share/g/a, a", "$share/g/a, g/a", "$share/g/sport/#, sport/tennis",
            "$share/g//finance, /finance", "$share/g/$SYS/#, $SYS/monitor/Clients",
            "$share/g/a/+, $share/g/a", "$share/g/a/+, $share/g/a/b/c",
            "$share/g/a, $share/h/a", "$share/g/a, $share/g/a/"
    })
    void dollar_prefixes_are_not_removed_or_ignored_when_matching(
            String pattern, String topicName
    ) {
        var sut = new TopicFilter(pattern);

        assertThat(sut.matches(new Topic(topicName))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"#", "a/+", "$share/g/#"})
    void a_null_topic_is_rejected_even_by_a_wildcard_filter(String pattern) {
        var sut = new TopicFilter(pattern);

        assertThatThrownBy(() -> sut.matches(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void matching_supports_deeply_nested_topics_within_the_byte_limit() {
        // MQTT 4.7.3 imposes no separate limit on the number of levels.
        String name = "/".repeat(65535);
        var sut = new TopicFilter(name);

        assertThat(sut.matches(new Topic(name))).isTrue();
    }
}
