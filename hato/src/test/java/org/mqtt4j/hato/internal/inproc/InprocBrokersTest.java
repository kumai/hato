package org.mqtt4j.hato.internal.inproc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InprocBrokersTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "a",
        "fleet",
        "site-a.fleet",
        "0",
        "a-0.b-1.c-2",
        // A label of 63 characters.
        "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk"
    })
    void accepts_a_host_name_in_lower_case(String name) {
        // Act & Assert
        assertThatCode(() -> InprocBrokers.check(name)).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "Fleet",
        "fleet_a",
        "fleet a",
        "-fleet",
        "fleet-",
        "site.-a",
        "site-.a",
        ".fleet",
        "fleet.",
        "site..fleet",
        "flëet",
        "[::1]",
        "fleet:1883",
        // A label of 64 characters.
        "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl"
    })
    void rejects_anything_else(String name) {
        // Act & Assert
        assertThatThrownBy(() -> InprocBrokers.check(name))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void accepts_a_name_of_253_characters_and_rejects_one_more() {
        // Arrange: labels of 63 characters and one more of 1, 253 characters with the periods.
        String label = "a".repeat(63);
        String longest = label + "." + label + "." + label + "." + "a".repeat(61);
        // Act & Assert
        assertThat(longest).hasSize(253);
        assertThatCode(() -> InprocBrokers.check(longest)).doesNotThrowAnyException();
        assertThatThrownBy(() -> InprocBrokers.check(longest + "a"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_name_that_a_broker_holds_cannot_be_taken_until_it_is_given_back() {
        // Arrange
        InprocBrokers.Registration first = InprocBrokers.take("held-name-test");
        try {
            // Act & Assert
            assertThatThrownBy(() -> InprocBrokers.take("held-name-test"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("held-name-test");
        } finally {
            first.release();
        }
        InprocBrokers.Registration second = InprocBrokers.take("held-name-test");
        second.release();
    }

    @Test
    void giving_a_name_back_again_leaves_the_broker_that_holds_it_now() {
        // Arrange: a name given back, and taken by another broker.
        InprocBrokers.Registration first = InprocBrokers.take("returned-name-test");
        first.release();
        InprocBrokers.Registration second = InprocBrokers.take("returned-name-test");
        try {
            // Act
            first.release();
            // Assert
            assertThatThrownBy(() -> InprocBrokers.take("returned-name-test"))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            second.release();
        }
    }

    @Test
    void an_automatic_name_skips_a_name_that_a_broker_holds() {
        // Arrange: the name that comes after the next automatic one, taken by its own name.
        InprocBrokers.Registration probe = InprocBrokers.takeNext();
        long number = Long.parseLong(probe.name().substring("hato-".length()));
        InprocBrokers.Registration given = InprocBrokers.take("hato-" + (number + 1));
        try {
            // Act
            InprocBrokers.Registration automatic = InprocBrokers.takeNext();
            // Assert
            assertThat(automatic.name()).isEqualTo("hato-" + (number + 2));
            automatic.release();
        } finally {
            given.release();
            probe.release();
        }
    }
}
