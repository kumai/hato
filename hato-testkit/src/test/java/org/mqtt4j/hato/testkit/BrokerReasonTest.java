package org.mqtt4j.hato.testkit;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.ReasonCode;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerReasonTest {

    @Test
    void the_with_methods_return_copies_that_add_the_strings() {
        // Arrange
        var sut = BrokerReason.of(ReasonCode.USE_ANOTHER_SERVER);

        // Act
        BrokerReason described = sut.withReasonString("maintenance");
        BrokerReason redirected = described.withServerReference("other.example:1883");

        // Assert
        assertThat(sut.reasonString()).isEmpty();
        assertThat(described.serverReference()).isEmpty();
        assertThat(redirected.reasonCode()).isEqualTo(ReasonCode.USE_ANOTHER_SERVER);
        assertThat(redirected.reasonString()).contains("maintenance");
        assertThat(redirected.serverReference()).contains("other.example:1883");
    }

    @Test
    void reasons_with_the_same_code_and_strings_are_equal() {
        // Arrange
        BrokerReason sut = BrokerReason.of(ReasonCode.NOT_AUTHORIZED)
                .withReasonString("no access");

        // Act
        BrokerReason same = BrokerReason.of(ReasonCode.NOT_AUTHORIZED)
                .withReasonString("no access");
        BrokerReason other = BrokerReason.of(ReasonCode.NOT_AUTHORIZED)
                .withReasonString("denied");

        // Assert
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same).isNotEqualTo(other);
    }
}
