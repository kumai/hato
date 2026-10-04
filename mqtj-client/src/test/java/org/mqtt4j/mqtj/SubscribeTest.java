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
    void defaults_leave_the_ack_mode_open_and_use_capacity_1024_for_every_qos(Qos qos) {
        // Act
        var sut = Subscribe.of(new TopicFilter("events/#"), qos);

        // Assert
        assertThat(sut.qos()).isEqualTo(qos);
        assertThat(sut.ackMode()).isEmpty();
        assertThat(sut.bufferCapacity()).isEqualTo(1024);
    }

    @Test
    void with_methods_return_changed_copies_that_compare_by_value() {
        // Arrange
        var filter = new TopicFilter("events/#");
        var sut = Subscribe.of(filter, Qos.AT_LEAST_ONCE);

        // Act
        Subscribe changed = sut.withAckMode(AckMode.MANUAL).withBufferCapacity(8);

        // Assert
        assertThat(sut.ackMode()).isEmpty();
        assertThat(sut.bufferCapacity()).isEqualTo(1024);
        assertThat(changed.ackMode()).contains(AckMode.MANUAL);
        assertThat(changed.bufferCapacity()).isEqualTo(8);
        Subscribe same = Subscribe.of(filter, Qos.AT_LEAST_ONCE)
                .withBufferCapacity(8)
                .withAckMode(AckMode.MANUAL);
        assertThat(changed).isEqualTo(same).hasSameHashCodeAs(same).isNotEqualTo(sut);
        assertThat(sut)
                .isNotEqualTo(Subscribe.of(new TopicFilter("events/+"), Qos.AT_LEAST_ONCE))
                .isNotEqualTo(Subscribe.of(filter, Qos.AT_MOST_ONCE));
    }

    @Test
    void an_explicit_after_handler_mode_stays_distinguishable_from_the_default() {
        // Arrange
        var sut = Subscribe.of(new TopicFilter("events/#"), Qos.AT_LEAST_ONCE);

        // Act
        Subscribe explicit = sut.withAckMode(AckMode.AFTER_HANDLER);

        // Assert: a receiving style with another default can still reject this choice.
        assertThat(explicit.ackMode()).contains(AckMode.AFTER_HANDLER);
        assertThat(explicit).isNotEqualTo(sut);
    }

    @ParameterizedTest(name = "accepts {0}")
    @ValueSource(strings = {"events/#", "$share", "$SHARE/group/events/#", "$share/group/events/#"})
    void preserves_ordinary_and_shared_filters(String pattern) {
        // Act
        var sut = Subscribe.of(new TopicFilter(pattern), Qos.AT_MOST_ONCE);

        // Assert
        assertThat(sut.filter().pattern()).isEqualTo(pattern);
    }

    @ParameterizedTest(name = "rejects {0}")
    @ValueSource(strings = {"$share/group", "$share//events", "$share/group/", "$share/+/events"})
    void rejects_malformed_shared_syntax(String pattern) {
        // Arrange: these are legal ordinary topic filters, but invalid shared subscriptions.
        var filter = new TopicFilter(pattern);

        // Act & Assert
        assertThatThrownBy(() -> Subscribe.of(filter, Qos.AT_MOST_ONCE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "rejects {0}")
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void capacity_must_be_positive(int capacity) {
        // Arrange
        var sut = Subscribe.of(new TopicFilter("events/#"), Qos.AT_MOST_ONCE);

        // Act & Assert
        assertThatThrownBy(() -> sut.withBufferCapacity(capacity))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null parameter contracts.
    void requires_filter_qos_and_acknowledgement_mode() {
        // Arrange
        var sut = Subscribe.of(new TopicFilter("events/#"), Qos.AT_LEAST_ONCE);

        // Act & Assert
        assertThatThrownBy(() -> Subscribe.of(null, Qos.AT_MOST_ONCE))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Subscribe.of(new TopicFilter("events/#"), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.withAckMode(null))
                .isInstanceOf(NullPointerException.class);
    }
}
