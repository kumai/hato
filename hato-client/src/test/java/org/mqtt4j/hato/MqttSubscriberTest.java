package org.mqtt4j.hato;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttSubscriberTest {

    @Test
    void a_subscriber_that_only_provides_handlers_rejects_hand_off() {
        // Arrange
        MqttSubscriber sut = (_, _) -> {
            throw new AssertionError("Only the hand-off method is called");
        };
        var request = Subscribe.of(new TopicFilter("events"), Qos.AT_MOST_ONCE);

        // Act & Assert
        assertThatThrownBy(() -> sut.handOff(request, _ -> true))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
