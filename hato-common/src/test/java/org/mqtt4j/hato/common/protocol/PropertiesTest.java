package org.mqtt4j.hato.common.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
    void finds_the_first_property_with_an_identifier() {
        var first = new Property.UserProperty("key", "first");
        var keepAlive = new Property.TwoByteValue(PropertyId.SERVER_KEEP_ALIVE, 30);
        var sut = new Properties(List.of(
                first,
                keepAlive,
                new Property.UserProperty("key", "second")
        ));

        assertThat(sut.find(PropertyId.USER_PROPERTY)).contains(first);
        assertThat(sut.find(PropertyId.SERVER_KEEP_ALIVE)).contains(keepAlive);
        assertThat(sut.find(PropertyId.REASON_STRING)).isEmpty();
    }

    @Test
    void user_property_names_and_values_can_be_empty() {
        var sut = new Property.UserProperty("", "");

        assertThat(sut.name()).isEmpty();
        assertThat(sut.value()).isEmpty();
    }

    @Test
    void property_identifiers_must_match_the_value_encoding() {
        assertThatThrownBy(() -> new Property.ByteValue(PropertyId.TOPIC_ALIAS, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.TwoByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.FourByteValue(PropertyId.TOPIC_ALIAS, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.VariableByteValue(PropertyId.TOPIC_ALIAS, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.StringValue(PropertyId.USER_PROPERTY, "v"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.BinaryValue(PropertyId.CONTENT_TYPE, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"-1", "2", "255", "256"})
    void byte_properties_accept_only_zero_and_one(int value) {
        assertThatThrownBy(() -> new Property.ByteValue(PropertyId.PAYLOAD_FORMAT_INDICATOR, value))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void numeric_properties_enforce_unsigned_ranges_and_nonzero_requirements() {
        assertThatThrownBy(() -> new Property.TwoByteValue(PropertyId.TOPIC_ALIAS_MAXIMUM, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.TwoByteValue(PropertyId.SERVER_KEEP_ALIVE, 65536))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.TwoByteValue(PropertyId.RECEIVE_MAXIMUM, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.FourByteValue(PropertyId.MESSAGE_EXPIRY_INTERVAL, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.FourByteValue(
                PropertyId.SESSION_EXPIRY_INTERVAL,
                0x1_0000_0000L
        )).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.FourByteValue(PropertyId.MAXIMUM_PACKET_SIZE, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Property.VariableByteValue(
                PropertyId.SUBSCRIPTION_IDENTIFIER,
                268435456
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void binary_properties_own_their_bytes_and_compare_by_content() {
        // Arrange
        var original = new byte[]{1, 2};
        var sut = new Property.BinaryValue(PropertyId.CORRELATION_DATA, original);
        var equal = new Property.BinaryValue(PropertyId.CORRELATION_DATA, original);

        // Act
        original[0] = 9;
        sut.value()[1] = 9;

        // Assert
        assertThat(sut.value()).containsExactly((byte) 1, (byte) 2);
        assertThat(sut).isEqualTo(equal).hasSameHashCodeAs(equal);
    }

    @Test
    void binary_properties_are_limited_to_65535_bytes() {
        assertThatThrownBy(() -> new Property.BinaryValue(
                PropertyId.CORRELATION_DATA,
                new byte[65536]
        )).isInstanceOf(IllegalArgumentException.class);
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
