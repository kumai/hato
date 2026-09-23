package org.mqtt4j.mqtj.common.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PacketIdsTest {

    @Test
    void identifiers_start_at_one_and_are_marked_in_use() {
        var sut = new PacketIds();

        assertThat(sut.isInUse(1)).isFalse();
        assertThat(sut.isInUse(65535)).isFalse();
        assertThat(sut.allocate()).hasValue(1);
        assertThat(sut.allocate()).hasValue(2);
        assertThat(sut.isInUse(1)).isTrue();
        assertThat(sut.isInUse(2)).isTrue();
        assertThat(sut.isInUse(3)).isFalse();
    }

    @Test
    void all_nonzero_two_byte_identifiers_can_be_allocated_once_before_exhaustion() {
        // MQTT 3.1.1 [MQTT-2.3.1-1, MQTT-2.3.1-2].
        var sut = new PacketIds();

        for (int id = 1; id <= 65535; id++) {
            assertThat(sut.allocate()).hasValue(id);
            assertThat(sut.isInUse(id)).isTrue();
        }

        assertThat(sut.allocate()).isEmpty();
        assertThat(sut.allocate()).isEmpty();
        assertThat(sut.isInUse(1)).isTrue();
        assertThat(sut.isInUse(65535)).isTrue();
    }

    @Test
    void releasing_an_earlier_identifier_does_not_rewind_the_allocation_cursor() {
        var sut = new PacketIds();
        int first = sut.allocate().orElseThrow();
        int second = sut.allocate().orElseThrow();

        sut.release(first);

        assertThat(sut.isInUse(first)).isFalse();
        assertThat(sut.isInUse(second)).isTrue();
        assertThat(sut.allocate()).hasValue(3);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 63, 64, 65, 32768, 65535})
    void a_released_identifier_is_reusable_after_exhaustion(int id) {
        // MQTT 3.1.1 [MQTT-2.3.1-3]: the owner releases an ID when its exchange completes.
        PacketIds sut = exhaustedIds();
        assertThat(sut.allocate()).isEmpty();

        sut.release(id);

        assertThat(sut.isInUse(id)).isFalse();
        assertThat(sut.allocate()).hasValue(id);
        assertThat(sut.isInUse(id)).isTrue();
        assertThat(sut.allocate()).isEmpty();
    }

    @Test
    void allocation_skips_occupied_identifiers_and_wraps_to_an_earlier_gap() {
        PacketIds sut = exhaustedIds();
        sut.release(100);
        sut.release(60000);

        assertThat(sut.allocate()).hasValue(100);
        assertThat(sut.allocate()).hasValue(60000);

        sut.release(1);

        assertThat(sut.allocate()).hasValue(1);
        assertThat(sut.allocate()).isEmpty();
        assertThat(sut.isInUse(2)).isTrue();
        assertThat(sut.isInUse(65535)).isTrue();
    }

    @Test
    void allocation_resumes_at_one_after_returning_65535() {
        PacketIds sut = exhaustedIds();
        sut.release(65535);
        assertThat(sut.allocate()).hasValue(65535);
        sut.release(1);
        sut.release(2);

        assertThat(sut.allocate()).hasValue(1);
        assertThat(sut.allocate()).hasValue(2);
        assertThat(sut.allocate()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 100, 65535})
    void identifiers_that_were_never_allocated_cannot_be_released(int id) {
        var sut = new PacketIds();

        assertThatThrownBy(() -> sut.release(id)).isInstanceOf(IllegalStateException.class);
        assertThat(sut.isInUse(id)).isFalse();
        assertThat(sut.allocate()).hasValue(1);
    }

    @Test
    void a_second_release_is_rejected_without_releasing_other_identifiers() {
        var sut = new PacketIds();
        int first = sut.allocate().orElseThrow();
        int second = sut.allocate().orElseThrow();
        sut.release(first);

        assertThatThrownBy(() -> sut.release(first)).isInstanceOf(IllegalStateException.class);
        assertThat(sut.isInUse(first)).isFalse();
        assertThat(sut.isInUse(second)).isTrue();
        assertThat(sut.allocate()).hasValue(3);
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 65536, Integer.MAX_VALUE})
    void identifiers_outside_the_wire_range_are_rejected_without_changing_state(int id) {
        var sut = new PacketIds();
        assertThat(sut.allocate()).hasValue(1);

        assertThatThrownBy(() -> sut.release(id)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.isInUse(id)).isInstanceOf(IllegalArgumentException.class);
        assertThat(sut.isInUse(1)).isTrue();
        assertThat(sut.allocate()).hasValue(2);
    }

    @Test
    void senders_have_independent_identifier_spaces() {
        // MQTT 3.1.1 section 2.3.1: the client and server assign IDs independently.
        var client = new PacketIds();
        var server = new PacketIds();

        assertThat(client.allocate()).hasValue(1);
        assertThat(server.allocate()).hasValue(1);
        client.release(1);

        assertThat(client.isInUse(1)).isFalse();
        assertThat(server.isInUse(1)).isTrue();
    }

    private static PacketIds exhaustedIds() {
        var ids = new PacketIds();
        for (int i = 0; i < 65535; i++) {
            ids.allocate().orElseThrow();
        }
        return ids;
    }
}
