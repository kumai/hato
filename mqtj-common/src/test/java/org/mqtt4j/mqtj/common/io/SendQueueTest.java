package org.mqtt4j.mqtj.common.io;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mqtt4j.mqtj.common.io.SendQueue.OfferResult.ACCEPTED;
import static org.mqtt4j.mqtj.common.io.SendQueue.OfferResult.CLOSED;
import static org.mqtt4j.mqtj.common.io.SendQueue.OfferResult.FULL;

class SendQueueTest {

    @Test
    void active_write_retains_its_bytes_and_reserved_sends_follow_fifo() {
        // Arrange
        var sut = new SendQueue(2, 10, 1, 2);

        // Act & Assert: a dequeued write still owns its quota.
        assertThat(sut.offer(1, new byte[10], false)).isEqualTo(ACCEPTED);
        SendQueue.Send active = sut.poll().orElseThrow();
        assertThat(sut.offer(2, new byte[1], false)).isEqualTo(FULL);
        assertThat(sut.offer(3, new byte[2], true)).isEqualTo(ACCEPTED);
        assertThat(sut.offer(4, new byte[1], true)).isEqualTo(FULL);
        assertThat(sut.poll()).isEmpty();

        // Act: writing returns capacity without changing FIFO order.
        sut.completed(active.ticket());

        // Assert
        assertThat(sut.offer(5, new byte[10], false)).isEqualTo(ACCEPTED);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(3);
        sut.completed(3);
        assertThat(sut.poll().orElseThrow().ticket()).isEqualTo(5);
        sut.close();
        assertThat(sut.poll()).isEmpty();
        assertThat(sut.offer(6, new byte[1], true)).isEqualTo(CLOSED);
        sut.ended();
    }
}
