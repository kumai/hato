package org.mqtt4j.hato.client.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.hato.client.internal.Subscriptions.MAX_SUBSCRIPTION_IDENTIFIER;

class SubscriptionsTest {

    @Test
    void the_search_for_a_free_identifier_starts_where_it_is_asked_to() {
        assertThat(Subscriptions.freeIdentifier(7, _ -> false)).isEqualTo(7);
    }

    @Test
    void the_search_for_a_free_identifier_continues_from_1_and_skips_identifiers_in_use() {
        // Act
        int identifier = Subscriptions.freeIdentifier(
                MAX_SUBSCRIPTION_IDENTIFIER,
                candidate -> candidate == MAX_SUBSCRIPTION_IDENTIFIER || candidate == 1
        );

        // Assert
        assertThat(identifier).isEqualTo(2);
    }
}
