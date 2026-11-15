package org.mqtt4j.hato;

import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
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
        Subscribe changed = sut.withAckMode(AckMode.MANUAL)
                .withBufferCapacity(8)
                .withFailurePolicy(FailurePolicy.SKIP);

        // Assert
        assertThat(sut.ackMode()).isEmpty();
        assertThat(sut.bufferCapacity()).isEqualTo(1024);
        assertThat(sut.failurePolicy()).isEqualTo(FailurePolicy.STOP);
        assertThat(changed.ackMode()).contains(AckMode.MANUAL);
        assertThat(changed.bufferCapacity()).isEqualTo(8);
        assertThat(changed.failurePolicy()).isEqualTo(FailurePolicy.SKIP);
        Subscribe same = Subscribe.of(filter, Qos.AT_LEAST_ONCE)
                .withFailurePolicy(FailurePolicy.SKIP)
                .withBufferCapacity(8)
                .withAckMode(AckMode.MANUAL);
        assertThat(changed).isEqualTo(same).hasSameHashCodeAs(same).isNotEqualTo(sut);
        assertThat(changed).isNotEqualTo(same.withFailurePolicy(FailurePolicy.STOP));
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
    void requires_filter_qos_acknowledgement_mode_and_failure_policy() {
        // Arrange
        var sut = Subscribe.of(new TopicFilter("events/#"), Qos.AT_LEAST_ONCE);

        // Act & Assert
        assertThatThrownBy(() -> Subscribe.of(null, Qos.AT_MOST_ONCE))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Subscribe.of(new TopicFilter("events/#"), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.withAckMode(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> sut.withFailurePolicy(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void the_subscription_options_default_to_those_of_mqtt_5() {
        // Act
        var sut = Subscribe.of(new TopicFilter("events/#"), Qos.AT_LEAST_ONCE);

        // Assert
        assertThat(sut.noLocal()).isFalse();
        assertThat(sut.retainAsPublished()).isFalse();
        assertThat(sut.retainHandling()).isEqualTo(RetainHandling.SEND);
    }

    @Test
    void with_methods_set_the_subscription_options_and_keep_the_other_settings() {
        // Arrange
        var filter = new TopicFilter("events/#");
        Subscribe base = Subscribe.of(filter, Qos.AT_LEAST_ONCE).withBufferCapacity(8);

        // Act
        Subscribe sut = base.withNoLocal(true)
                .withRetainAsPublished(true)
                .withRetainHandling(RetainHandling.DO_NOT_SEND);

        // Assert
        assertThat(sut.noLocal()).isTrue();
        assertThat(sut.retainAsPublished()).isTrue();
        assertThat(sut.retainHandling()).isEqualTo(RetainHandling.DO_NOT_SEND);
        assertThat(sut.bufferCapacity()).isEqualTo(8);
        Subscribe same = Subscribe.of(filter, Qos.AT_LEAST_ONCE)
                .withRetainHandling(RetainHandling.DO_NOT_SEND)
                .withRetainAsPublished(true)
                .withNoLocal(true)
                .withBufferCapacity(8);
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same).isNotEqualTo(base);
        assertThat(sut.toString()).contains(
                "noLocal=true",
                "retainAsPublished=true",
                "retainHandling=DO_NOT_SEND"
        );
    }

    static Stream<Named<UnaryOperator<Subscribe>>> oneChangedOption() {
        return Stream.of(
                Named.of("No Local", request -> request.withNoLocal(true)),
                Named.of("Retain As Published", request -> request.withRetainAsPublished(true)),
                Named.of(
                        "Retain Handling",
                        request -> request.withRetainHandling(
                                RetainHandling.SEND_IF_NEW_SUBSCRIPTION
                        )
                )
        );
    }

    @ParameterizedTest
    @MethodSource("oneChangedOption")
    void requests_that_differ_in_one_option_are_not_equal(UnaryOperator<Subscribe> change) {
        // Arrange
        var sut = Subscribe.of(new TopicFilter("events/#"), Qos.AT_LEAST_ONCE);

        // Act & Assert
        assertThat(change.apply(sut)).isNotEqualTo(sut);
    }

    @Test
    void a_shared_subscription_cannot_set_no_local() {
        // Arrange
        var sut = Subscribe.of(new TopicFilter("$share/workers/events/#"), Qos.AT_LEAST_ONCE);

        // Act & Assert
        assertThatThrownBy(() -> sut.withNoLocal(true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(sut.withNoLocal(false).noLocal()).isFalse();
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null parameter contract.
    void retain_handling_cannot_be_null() {
        // Arrange
        var sut = Subscribe.of(new TopicFilter("events/#"), Qos.AT_LEAST_ONCE);

        // Act & Assert
        assertThatThrownBy(() -> sut.withRetainHandling(null))
                .isInstanceOf(NullPointerException.class);
    }
}
