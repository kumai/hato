package org.mqtt4j.hato;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class ConnAckTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void preserves_session_presence_and_attempt_and_compares_by_value(boolean sessionPresent) {
        // Act
        var sut = ConnAck.of(sessionPresent, 3);
        var same = ConnAck.of(sessionPresent, 3);

        // Assert
        assertThat(sut.sessionPresent()).isEqualTo(sessionPresent);
        assertThat(sut.connectionAttempt()).isEqualTo(3);
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same)
                .isNotEqualTo(ConnAck.of(!sessionPresent, 3))
                .isNotEqualTo(ConnAck.of(sessionPresent, 4));
        assertThat(same).isEqualTo(sut);
        assertThat(sut.toString())
                .contains("sessionPresent=" + sessionPresent, "connectionAttempt=3");
    }

    @Test
    void attempt_numbers_start_at_one() {
        assertThatIllegalArgumentException().isThrownBy(() -> ConnAck.of(false, 0));
    }

    @Test
    void a_broker_without_announced_limits_allows_what_mqtt_allows() {
        // Act
        var sut = ConnAck.of(false, 1);

        // Assert
        assertThat(sut.maximumQos()).isEqualTo(Qos.EXACTLY_ONCE);
        assertThat(sut.retainAvailable()).isTrue();
        assertThat(sut.wildcardSubscriptionAvailable()).isTrue();
        assertThat(sut.sharedSubscriptionAvailable()).isTrue();
        assertThat(sut.maximumPacketSize()).isEqualTo(268_435_460);
    }

    @Test
    void preserves_announced_limits_and_compares_them_by_value() {
        // Act
        ConnAck sut = ConnAck.of(true, 2)
                .withMaximumQos(Qos.AT_LEAST_ONCE)
                .withRetainAvailable(false)
                .withWildcardSubscriptionAvailable(false)
                .withMaximumPacketSize(1024);

        // Assert
        assertThat(sut.maximumQos()).isEqualTo(Qos.AT_LEAST_ONCE);
        assertThat(sut.retainAvailable()).isFalse();
        assertThat(sut.wildcardSubscriptionAvailable()).isFalse();
        assertThat(sut.sharedSubscriptionAvailable()).isTrue();
        assertThat(sut.maximumPacketSize()).isEqualTo(1024);
        assertThat(List.of(
                sut.withMaximumQos(Qos.EXACTLY_ONCE),
                sut.withRetainAvailable(true),
                sut.withWildcardSubscriptionAvailable(true),
                sut.withSharedSubscriptionAvailable(false),
                sut.withMaximumPacketSize(1025)
        )).doesNotContain(sut);
        // The copies leave the original as it was.
        ConnAck same = ConnAck.of(true, 2)
                .withMaximumQos(Qos.AT_LEAST_ONCE)
                .withRetainAvailable(false)
                .withWildcardSubscriptionAvailable(false)
                .withMaximumPacketSize(1024);
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(sut.toString()).contains(
                "maximumQos=AT_LEAST_ONCE",
                "retainAvailable=false",
                "wildcardSubscriptionAvailable=false",
                "sharedSubscriptionAvailable=true",
                "maximumPacketSize=1024"
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 268_435_461})
    void packet_sizes_outside_what_mqtt_carries_are_rejected(int size) {
        assertThatIllegalArgumentException().isThrownBy(
                () -> ConnAck.of(false, 1).withMaximumPacketSize(size)
        );
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violates the non-null contract.
    void the_maximum_qos_cannot_be_null() {
        assertThatNullPointerException().isThrownBy(
                () -> ConnAck.of(false, 1).withMaximumQos(null)
        );
    }
}
