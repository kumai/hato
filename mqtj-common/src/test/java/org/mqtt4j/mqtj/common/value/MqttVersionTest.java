package org.mqtt4j.mqtj.common.value;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttVersionTest {

    @Test
    void connect_protocol_levels_follow_the_mqtt_specification() {
        assertThat(MqttVersion.of(4)).isSameAs(MqttVersion.V3_1_1);
        assertThat(MqttVersion.of(5)).isSameAs(MqttVersion.V5);
        assertThat(MqttVersion.V3_1_1.protocolLevel()).isEqualTo(4);
        assertThat(MqttVersion.V5.protocolLevel()).isEqualTo(5);
    }

    @Test
    void unsupported_protocol_levels_are_rejected() {
        for (int level : new int[] {-1, 0, 3, 6, 255}) {
            assertThatThrownBy(() -> MqttVersion.of(level))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
