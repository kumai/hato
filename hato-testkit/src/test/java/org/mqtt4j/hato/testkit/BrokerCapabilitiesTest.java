package org.mqtt4j.hato.testkit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrokerCapabilitiesTest {

    @Test
    void the_bounds_that_mqtt_5_can_announce_are_kept() {
        // Arrange
        var sut = BrokerCapabilities.defaults();

        // Act
        BrokerCapabilities lowest = sut.withReceiveMaximum(1)
                .withMaximumPacketSize(1)
                .withTopicAliasMaximum(0);
        BrokerCapabilities highest = sut.withReceiveMaximum(65_535)
                .withMaximumPacketSize(268_435_460)
                .withTopicAliasMaximum(65_535);

        // Assert
        assertThat(lowest)
                .extracting(
                        BrokerCapabilities::receiveMaximum,
                        BrokerCapabilities::maximumPacketSize,
                        BrokerCapabilities::topicAliasMaximum
                )
                .containsExactly(1, 1, 0);
        assertThat(highest)
                .extracting(
                        BrokerCapabilities::receiveMaximum,
                        BrokerCapabilities::maximumPacketSize,
                        BrokerCapabilities::topicAliasMaximum
                )
                .containsExactly(65_535, 268_435_460, 65_535);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 65_536})
    void a_receive_maximum_that_mqtt_5_cannot_announce_is_refused(int receiveMaximum) {
        // Arrange
        var sut = BrokerCapabilities.defaults();

        // Act & Assert
        assertThatThrownBy(() -> sut.withReceiveMaximum(receiveMaximum))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 268_435_461})
    void a_maximum_packet_size_that_mqtt_5_cannot_announce_is_refused(int maximumPacketSize) {
        // Arrange
        var sut = BrokerCapabilities.defaults();

        // Act & Assert
        assertThatThrownBy(() -> sut.withMaximumPacketSize(maximumPacketSize))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 65_536})
    void a_topic_alias_maximum_that_mqtt_5_cannot_announce_is_refused(int topicAliasMaximum) {
        // Arrange
        var sut = BrokerCapabilities.defaults();

        // Act & Assert
        assertThatThrownBy(() -> sut.withTopicAliasMaximum(topicAliasMaximum))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
