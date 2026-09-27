package org.mqtt4j.mqtj;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubscribeTest {

    @ParameterizedTest
    @EnumSource(Qos.class)
    void default_capacity_is_256_for_every_representable_qos(Qos qos) {
        // Act
        var sut = new Subscribe(new TopicFilter("events/#"), qos);

        // Assert
        assertThat(sut.bufferCapacity()).isEqualTo(256);
        assertThat(sut.qos()).isEqualTo(qos);
    }

    @ParameterizedTest(name = "accepts {0}")
    @ValueSource(strings = {"events/#", "$share", "$SHARE/group/events/#", "$share/group/events/#"})
    void preserves_ordinary_and_shared_filters(String pattern) {
        // Act
        var sut = new Subscribe(new TopicFilter(pattern), Qos.AT_MOST_ONCE, 1);

        // Assert
        assertThat(sut.filter().pattern()).isEqualTo(pattern);
        assertThat(sut.bufferCapacity()).isEqualTo(1);
    }

    @ParameterizedTest(name = "rejects {0}")
    @ValueSource(strings = {"$share/group", "$share//events", "$share/group/", "$share/+/events"})
    void rejects_malformed_shared_syntax(String pattern) {
        // Arrange: these are legal ordinary topic filters, but invalid shared subscriptions.
        var filter = new TopicFilter(pattern);

        // Act & Assert
        assertThatThrownBy(() -> new Subscribe(filter, Qos.AT_MOST_ONCE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "rejects {0}")
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void capacity_must_be_positive(int capacity) {
        // Act & Assert
        assertThatThrownBy(() -> new Subscribe(
                new TopicFilter("events/#"),
                Qos.AT_MOST_ONCE,
                capacity
        ))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the constructor's non-null contract.
    void requires_filter_and_qos() {
        // Act & Assert
        assertThatThrownBy(() -> new Subscribe(null, Qos.AT_MOST_ONCE))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Subscribe(new TopicFilter("events/#"), null))
                .isInstanceOf(NullPointerException.class);
    }
}
