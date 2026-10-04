package org.mqtt4j.mqtj.common.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mqtt4j.mqtj.common.io.SendQueue.OfferResult.ACCEPTED;
import static org.mqtt4j.mqtj.common.io.SendQueue.OfferResult.CLOSED;
import static org.mqtt4j.mqtj.common.io.SendQueue.OfferResult.FULL;

class SendQueueTest {

    @Test
    void capacity_checks_do_not_reserve_and_preserve_closed_precedence() {
        // Arrange
        var sut = new SendQueue<byte[]>(1, 10, 1, 2);

        // Act & Assert: checks leave both quotas available for the subsequent offers.
        assertThat(sut.checkCapacity(Long.MAX_VALUE, false)).isEqualTo(FULL);
        assertThat(sut.checkCapacity(10, false)).isEqualTo(ACCEPTED);
        assertThat(sut.checkCapacity(10, false)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(1, new byte[10], 10, false)).isEqualTo(ACCEPTED);
        sut.poll().orElseThrow();
        assertThat(sut.checkCapacity(0, false)).isEqualTo(FULL);
        assertThat(sut.checkCapacity(3, true)).isEqualTo(FULL);
        assertThat(sut.checkCapacity(2, true)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(2, new byte[2], 2, true)).isEqualTo(ACCEPTED);

        // Act & Assert: completion returns capacity; closure takes precedence over capacity.
        sut.completed(1);
        assertThat(sut.checkCapacity(10, false)).isEqualTo(ACCEPTED);
        sut.close();
        assertThat(sut.checkCapacity(Long.MAX_VALUE, false)).isEqualTo(CLOSED);
        assertThat(sut.checkCapacity(Long.MAX_VALUE, true)).isEqualTo(CLOSED);
    }

    @Test
    void active_write_retains_its_bytes_and_reserved_sends_follow_fifo() {
        // Arrange
        var sut = new SendQueue<String>(2, 10, 1, 2);
        assertThat(sut.hasReady()).isFalse();

        // Act & Assert: a dequeued write still owns its quota.
        assertThat(sut.offer(1, "first", 10, false)).isEqualTo(ACCEPTED);
        assertThat(sut.hasReady()).isTrue();
        SendQueue.Send<String> active = sut.poll().orElseThrow();
        assertThat(active.value()).isEqualTo("first");
        assertThat(active.byteCount()).isEqualTo(10);
        assertThat(sut.offer(2, "refused", 1, false)).isEqualTo(FULL);
        assertThat(sut.offer(3, "reserved", 2, true)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(4, "refused", 1, true)).isEqualTo(FULL);
        assertThat(sut.hasReady()).isFalse();
        assertThat(sut.poll()).isEmpty();

        // Act: writing returns capacity without changing FIFO order.
        sut.completed(active.ticket());

        // Assert
        assertThat(sut.hasReady()).isTrue();
        assertThat(sut.offer(5, "last", 10, false)).isEqualTo(ACCEPTED);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(3);
        sut.completed(3);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(5);
        sut.close();
        assertThat(sut.hasReady()).isFalse();
        assertThat(sut.poll()).isEmpty();
        assertThat(sut.offer(6, "closed", 1, true)).isEqualTo(CLOSED);
        sut.ended();
        assertThat(sut.hasReady()).isFalse();
    }

    @Test
    void closed_queues_retain_only_active_capacity_in_the_shared_budget() {
        // Arrange: two connections share capacity, but each owns its own active send.
        var budget = new SendQueue.Budget(2, 10, 1, 2);
        var previous = new SendQueue<String>(budget);
        var sut = new SendQueue<String>(budget);
        assertThat(previous.offer(1, "active", 6, false)).isEqualTo(ACCEPTED);
        previous.poll().orElseThrow();
        assertThat(previous.offer(2, "waiting", 4, false)).isEqualTo(ACCEPTED);
        assertThat(sut.checkCapacity(0, false)).isEqualTo(FULL);
        assertThat(sut.offer(3, "reserved", 2, true)).isEqualTo(ACCEPTED);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(3);
        assertThat(previous.offer(4, "reserved", 1, true)).isEqualTo(FULL);

        // Act: closing the old connection discards its waiting send, but keeps its active send.
        previous.close();

        // Assert: the new connection can use only the returned capacity and stays open.
        assertThat(previous.offer(5, "closed", 1, false)).isEqualTo(CLOSED);
        assertThat(previous.poll()).isEmpty();
        assertThat(sut.offer(6, "next", 4, false)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(7, "full", 1, false)).isEqualTo(FULL);

        // Act: the old sender stops using its value; duplicate shutdown callbacks are harmless.
        previous.ended();
        previous.ended();
        previous.completed(1);

        // Assert: the old active send's capacity is returned exactly once.
        assertThat(sut.offer(8, "last", 6, false)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(9, "full", 1, false)).isEqualTo(FULL);
        assertThat(sut.offer(10, "reserved", 1, true)).isEqualTo(FULL);
        sut.completed(3);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(6);
        sut.completed(6);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(8);
        sut.ended();
    }

    @Test
    void completing_an_active_send_after_close_returns_shared_capacity() {
        // Arrange
        var budget = new SendQueue.Budget(1, 10, 0, 0);
        var previous = new SendQueue<String>(budget);
        var sut = new SendQueue<String>(budget);
        assertThat(previous.offer(1, "active", 10, false)).isEqualTo(ACCEPTED);
        previous.poll().orElseThrow();
        previous.close();
        assertThat(sut.offer(2, "full", 10, false)).isEqualTo(FULL);

        // Act: normal write completion can release the old send before its shutdown callback.
        previous.completed(1);

        // Assert
        assertThat(sut.offer(3, "next", 10, false)).isEqualTo(ACCEPTED);
        previous.ended();
        assertThat(sut.offer(4, "full", 1, false)).isEqualTo(FULL);
        sut.ended();
    }

    @Test
    void headroom_counts_active_sends_across_connections_and_preserves_fifo() {
        // Arrange: one old active send and one new send consume the non-headroom quota.
        var budget = new SendQueue.Budget(1, 10, 4, 100);
        var previous = new SendQueue<String>(budget);
        var sut = new SendQueue<String>(budget);
        assertThat(previous.offer(1, "old", 1, true, 2, 0)).isEqualTo(ACCEPTED);
        previous.poll().orElseThrow();
        previous.close();
        assertThat(sut.offer(2, "regular", 1, false)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(3, "ack", 1, true, 2, 0)).isEqualTo(ACCEPTED);

        // Act & Assert: only sends without headroom restrictions can use the remaining slots.
        assertThat(sut.offer(4, "blocked ack", 1, true, 2, 0)).isEqualTo(FULL);
        assertThat(sut.offer(5, "control", 1, true)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(6, "control", 1, true)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(7, "full", 1, true)).isEqualTo(FULL);
        previous.completed(1);
        assertThat(sut.offer(8, "still blocked ack", 1, true, 2, 0)).isEqualTo(FULL);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(2);
        sut.completed(2);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(3);
        sut.completed(3);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(5);
        sut.completed(5);
        assertThat(sut.offer(9, "ack", 1, true, 2, 0)).isEqualTo(ACCEPTED);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(6);
        sut.ended();
    }

    @Test
    void byte_headroom_is_shared_and_rejected_sends_reserve_nothing() {
        // Arrange
        var budget = new SendQueue.Budget(0, 0, 10, 100);
        var previous = new SendQueue<String>(budget);
        var sut = new SendQueue<String>(budget);
        previous.offer(1, "old", 30, true);
        previous.poll().orElseThrow();
        previous.close();

        // Act & Assert: an ACK must leave 40 bytes even when the old active send owns 30.
        assertThat(sut.offer(2, "too large", 31, true, 0, 40)).isEqualTo(FULL);
        assertThat(sut.offer(3, "ack", 30, true, 0, 40)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(4, "blocked ack", 1, true, 0, 40)).isEqualTo(FULL);
        assertThat(sut.offer(5, "control", 40, true)).isEqualTo(ACCEPTED);
        sut.close();
        previous.completed(1);
        var next = new SendQueue<String>(budget);
        assertThat(next.offer(6, "ack", 60, true, 0, 40)).isEqualTo(ACCEPTED);
        assertThat(next.offer(7, "control", 40, true)).isEqualTo(ACCEPTED);
        next.ended();
    }

    @Test
    void invalid_or_excessive_headroom_cannot_expand_capacity() {
        // Arrange
        var sut = new SendQueue<String>(0, 0, 2, 10);

        // Act & Assert
        assertThatIllegalArgumentException()
                .isThrownBy(() -> sut.offer(1, "invalid", 0, true, -1, 0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> sut.offer(1, "invalid", 0, true, 0, -1));
        assertThat(sut.offer(1, "full", 0, true, Integer.MAX_VALUE, 0)).isEqualTo(FULL);
        assertThat(sut.offer(1, "full", 0, true, 0, Long.MAX_VALUE)).isEqualTo(FULL);
        assertThat(sut.offer(1, "valid", 10, true)).isEqualTo(ACCEPTED);
        sut.close();
        assertThat(sut.offer(1, "closed", 0, true, -1, -1)).isEqualTo(CLOSED);
    }

    @ParameterizedTest
    @CsvSource({"-1, 1, 1, 1", "1, -1, 1, 1", "1, 1, -1, 1", "1, 1, 1, -1"})
    void negative_capacity_is_rejected(
            int regularCount,
            long regularBytes,
            int reservedCount,
            long reservedBytes
    ) {
        // Act & Assert
        assertThatIllegalArgumentException().isThrownBy(() -> new SendQueue.Budget(
                regularCount,
                regularBytes,
                reservedCount,
                reservedBytes
        ));
    }

    @Test
    void negative_send_sizes_cannot_increase_the_available_capacity() {
        // Arrange
        var sut = new SendQueue<String>(1, 10, 0, 0);

        // Act & Assert
        assertThatIllegalArgumentException().isThrownBy(() -> sut.checkCapacity(-1, false));
        assertThatIllegalArgumentException().isThrownBy(() -> sut.offer(1, "invalid", -1, false));
        assertThat(sut.offer(2, "valid", 10, false)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(3, "full", 1, false)).isEqualTo(FULL);
        sut.ended();
    }
}
