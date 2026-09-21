package org.mqtt4j.mqtj.common.value;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QosTest {

    @Test
    void qos_levels_round_trip_through_their_wire_values() {
        for (Qos qos : Qos.values()) {
            assertThat(Qos.of(qos.level())).isSameAs(qos);
        }
    }

    @Test
    void an_unknown_qos_level_is_rejected() {
        for (int level : new int[] {-1, 3, 255}) {
            assertThatThrownBy(() -> Qos.of(level)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
