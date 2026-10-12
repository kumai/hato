package org.mqtt4j.hato.common.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PropertiesTest {

    @Test
    void empty_properties_represent_packets_without_mqtt_5_properties() {
        assertThat(Properties.EMPTY.isEmpty()).isTrue();
        assertThat(Properties.EMPTY.entries()).isEmpty();
        assertThat(Properties.EMPTY).isEqualTo(new Properties(List.of()));
    }

    @Test
    void user_properties_keep_order_and_duplicates_in_an_immutable_list() {
        var first = new Property.UserProperty("key", "first");
        var second = new Property.UserProperty("key", "second");
        var entries = new ArrayList<Property>(List.of(first, second, first));
        var sut = new Properties(entries);

        entries.clear();

        assertThat(sut.isEmpty()).isFalse();
        assertThat(sut.entries()).containsExactly(first, second, first);
        assertThatThrownBy(() -> sut.entries().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void user_property_names_and_values_can_be_empty() {
        var sut = new Property.UserProperty("", "");

        assertThat(sut.name()).isEmpty();
        assertThat(sut.value()).isEmpty();
    }

    @Test
    @SuppressWarnings("NullAway") // Exercise runtime rejection of invalid non-null inputs.
    void properties_and_user_property_components_cannot_be_null() {
        assertThatThrownBy(() -> new Properties(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Properties(
                Arrays.asList(new Property.UserProperty("k", "v"), null)
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Property.UserProperty(null, "v"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Property.UserProperty("k", null))
                .isInstanceOf(NullPointerException.class);
    }
}
