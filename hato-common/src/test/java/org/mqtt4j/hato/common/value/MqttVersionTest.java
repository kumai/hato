package org.mqtt4j.hato.common.value;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MqttVersionTest {

    @Test
    void connect_protocol_levels_follow_the_mqtt_specification() {
        assertThat(MqttVersion.V3_1_1.protocolLevel()).isEqualTo(4);
        assertThat(MqttVersion.V5.protocolLevel()).isEqualTo(5);
    }
}
