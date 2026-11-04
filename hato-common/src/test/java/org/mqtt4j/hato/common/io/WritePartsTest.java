package org.mqtt4j.hato.common.io;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WritePartsTest {

    @Test
    void small_arrays_join_into_one_part_and_large_ones_leave_from_where_they_lie() {
        // Arrange
        var sut = new WriteParts();
        var large = new byte[WriteParts.JOIN_BYTES];
        var small = new byte[WriteParts.JOIN_BYTES - 1];

        // Act
        sut.add(new byte[] {1});
        sut.add(new byte[] {2, 3});
        sut.add(large);
        sut.add(small);
        sut.add(new byte[] {4});
        ByteBuffer[] parts = sut.toArray();

        // Assert: the arrays keep their order, and only the large one is a part of its own.
        assertThat(parts).hasSize(3);
        assertThat(parts[0].array()).containsExactly(1, 2, 3);
        assertThat(parts[1].array()).isSameAs(large);
        assertThat(parts[2].remaining()).isEqualTo(WriteParts.JOIN_BYTES);
        assertThat(parts[2].get(WriteParts.JOIN_BYTES - 1)).isEqualTo((byte) 4);
    }
}
