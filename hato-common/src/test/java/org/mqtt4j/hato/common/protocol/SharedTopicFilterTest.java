package org.mqtt4j.hato.common.protocol;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SharedTopicFilterTest {

    @ParameterizedTest
    @CsvSource({
            "$share/g/a, g, a",
            "$share/consumer1/sport/tennis/+, consumer1, sport/tennis/+",
            "$share/g/#, g, #",
            "$share/g//, g, /",
            "$share/g//finance, g, /finance",
            "$share/g/$SYS/#, g, $SYS/#",
            "'$share/ /a', ' ', a",
            "'$share/g/ ', g, ' '",
            "$share/カツ/カレー/+, カツ, カレー/+"
    })
    void a_shared_filter_is_split_into_its_share_name_and_filter(
            String pattern,
            String shareName,
            String filter
    ) {
        // MQTT 5.0 section 4.8.2: $share/{ShareName}/{filter}.
        Optional<SharedTopicFilter> result = SharedTopicFilter.parse(new TopicFilter(pattern));

        assertThat(result).contains(new SharedTopicFilter(shareName, new TopicFilter(filter)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "a/b", "#", "$share", "$sharex/g/a", "$SHARE/g/a", "$SYS/#", "share/g/a", "/$share/g/a"
    })
    void filters_without_the_share_prefix_are_ordinary_filters(String pattern) {
        var filter = new TopicFilter(pattern);

        assertThat(SharedTopicFilter.parse(filter)).isEmpty();
        assertThat(SharedTopicFilter.isShared(filter)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "$share/", "$share//", "$share//a", "$share/g", "$share/g/", "$share/+/a", "$share/#",
            "$share/+"
    })
    void a_share_prefix_without_a_valid_shared_subscription_is_rejected(String pattern) {
        // MQTT 5.0 [MQTT-4.8.2-1, MQTT-4.8.2-2]: a non-empty share name, then '/' and a filter.
        // Each pattern is still a valid ordinary filter, as MQTT 3.1.1 reads it, and still asks
        // for a shared subscription.
        var filter = new TopicFilter(pattern);

        assertThatThrownBy(() -> SharedTopicFilter.parse(filter))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(SharedTopicFilter.isShared(filter)).isTrue();
    }

    @ParameterizedTest(name = "{0} against {1}: {2}")
    @CsvSource({
            "$share/g/sport/#, sport, true",
            "$share/g/sport/#, sport/tennis/player1, true",
            "$share/g/+/tennis, sport/tennis, true",
            "$share/g/a, $share/g/a, false",
            "$share/g/#, $SYS/monitor/Clients, false",
            "$share/g/$SYS/#, $SYS/monitor/Clients, true"
    })
    void deliveries_are_matched_by_the_filter_after_the_share_name(
            String pattern,
            String topicName,
            boolean expected
    ) {
        // Brokers deliver under the published topic name, so only the filter after the share
        // name can route a delivery. [MQTT-4.7.2-1] applies to that filter as well.
        SharedTopicFilter sut = SharedTopicFilter.parse(new TopicFilter(pattern)).orElseThrow();

        assertThat(sut.filter().matches(new Topic(topicName))).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "/", "a/b", "+", "#", "g+", "g#"})
    void share_names_are_validated_on_construction(String shareName) {
        var filter = new TopicFilter("a");

        assertThatThrownBy(() -> new SharedTopicFilter(shareName, filter))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null contracts.
    void the_share_name_the_filter_and_the_parsed_filter_are_required() {
        var filter = new TopicFilter("a");

        assertThatThrownBy(() -> new SharedTopicFilter(null, filter))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SharedTopicFilter("g", null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SharedTopicFilter.parse(null))
                .isInstanceOf(NullPointerException.class);
    }
}
