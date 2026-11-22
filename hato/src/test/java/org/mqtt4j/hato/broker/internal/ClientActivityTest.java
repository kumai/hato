package org.mqtt4j.hato.broker.internal;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;

/// How the record of a client's activity keeps the topics and filters whose length the client
/// chooses: whole up to its length, and beyond it as their beginning, marked as truncated.
class ClientActivityTest {

    @Test
    void a_text_within_the_length_is_kept_whole() {
        // Arrange
        var sut = new ClientActivity();
        String topic = "a".repeat(ClientActivity.MAX_TEXT);

        // Act
        sut.received(new Topic(topic), 0);

        // Assert
        BrokerServer.ActivityValues values = sut.values(0, 0);
        assertThat(values.lastTopic()).isEqualTo(topic);
        assertThat(values.lastTopicTruncated()).isFalse();
    }

    @Test
    void a_longer_text_shows_as_its_beginning_marked_as_truncated() {
        // Arrange: a topic that goes on past the length, whose beginning is a whole topic too.
        var sut = new ClientActivity();
        String beginning = "a".repeat(ClientActivity.MAX_TEXT);

        // Act
        sut.received(new Topic(beginning + "/state"), 0);

        // Assert
        BrokerServer.ActivityValues values = sut.values(0, 0);
        assertThat(values.lastTopic()).isEqualTo(beginning);
        assertThat(values.lastTopicTruncated()).isTrue();
    }

    @Test
    void a_long_text_after_another_shows_its_own_beginning() {
        // Arrange
        var sut = new ClientActivity();
        sut.received(new Topic("a".repeat(ClientActivity.MAX_TEXT) + "/state"), 0);

        // Act
        sut.received(new Topic("b".repeat(ClientActivity.MAX_TEXT) + "/state"), 0);

        // Assert
        BrokerServer.ActivityValues values = sut.values(0, 0);
        assertThat(values.lastTopic()).isEqualTo("b".repeat(ClientActivity.MAX_TEXT));
        assertThat(values.lastTopicTruncated()).isTrue();
    }

    @Test
    void the_beginning_of_a_longer_text_does_not_part_a_surrogate_pair() {
        // Arrange: a character beyond the Basic Multilingual Plane, whose two halves straddle the
        // end of what shows.
        var sut = new ClientActivity();
        String beginning = "a".repeat(ClientActivity.MAX_TEXT - 1);

        // Act
        sut.refusedSubscription(new TopicFilter(beginning + Character.toString(0x1F600) + "b"));

        // Assert
        BrokerServer.ActivityValues values = sut.values(0, 0);
        assertThat(values.lastRefusedFilter()).isEqualTo(beginning);
        assertThat(values.lastRefusedFilterTruncated()).isTrue();
    }
}
