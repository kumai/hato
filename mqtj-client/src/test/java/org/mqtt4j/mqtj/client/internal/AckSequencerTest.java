package org.mqtt4j.mqtj.client.internal;

import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AckSequencerTest {

    @Test
    void only_the_ready_prefix_can_be_released() {
        // Arrange
        var sut = new AckSequencer(3);
        sut.add(1, 7);
        sut.add(2, 8);
        sut.add(3, 9);

        // Act
        sut.ready(3);
        sut.ready(2);

        // Assert
        assertThat(sut.nextReady()).isEmpty();

        // Act
        sut.ready(1);

        // Assert
        assertThat(sut.nextReady().orElseThrow().packetId()).isEqualTo(7);
        sut.submitted(10);
        assertThat(sut.nextReady().orElseThrow().packetId()).isEqualTo(8);
        sut.submitted(11);
        assertThat(sut.nextReady().orElseThrow().packetId()).isEqualTo(9);
        sut.submitted(12);
        assertThat(sut.nextReady()).isEmpty();
    }

    @Test
    void completed_followers_retain_capacity_until_the_head_is_ready() {
        // Arrange
        var sut = new AckSequencer(2);
        assertThat(sut.add(1, 7)).isTrue();
        assertThat(sut.add(2, 8)).isTrue();

        // Act
        sut.ready(2);

        // Assert
        assertThat(sut.add(3, 9)).isFalse();

        // Act
        sut.ready(1);
        sut.submitted(10);

        // Assert: queue admission still occupies the identifier and acknowledgement capacity.
        assertThat(sut.contains(7)).isTrue();
        assertThat(sut.add(3, 9)).isFalse();

        // Act
        sut.written(10);

        // Assert
        assertThat(sut.contains(7)).isFalse();
        assertThat(sut.add(3, 9)).isTrue();
        assertThat(sut.nextReady().orElseThrow().token()).isEqualTo(2);
        sut.submitted(11);
        assertThat(sut.nextReady()).isEmpty();
    }

    @Test
    void an_identifier_is_occupied_until_written_and_old_tokens_cannot_ack_its_reuse() {
        // Arrange
        var sut = new AckSequencer(2);
        assertThat(sut.add(1, 7)).isTrue();
        assertThat(sut.add(2, 7)).isFalse();
        sut.ready(1);
        sut.submitted(10);

        // Assert
        assertThat(sut.contains(7)).isTrue();
        assertThat(sut.add(2, 7)).isFalse();
        assertThat(sut.writingCount()).isEqualTo(1);
        assertThat(sut.nextReady()).isEmpty();

        // Act
        sut.written(10);
        assertThat(sut.add(2, 7)).isTrue();
        sut.ready(1);
        sut.written(10);

        // Assert
        assertThat(sut.contains(7)).isTrue();
        assertThat(sut.nextReady()).isEmpty();

        // Act
        sut.ready(2);

        // Assert
        assertThat(sut.nextReady().orElseThrow().token()).isEqualTo(2);
    }

    @Test
    void superseding_an_unqueued_acknowledgement_lets_a_ready_follower_lead() {
        // Arrange
        var sut = new AckSequencer(2);
        sut.add(1, 7);
        sut.add(2, 8);
        sut.ready(2);

        // Act
        OptionalLong superseded = sut.supersede(7);

        // Assert
        assertThat(superseded).hasValue(1);
        assertThat(sut.contains(7)).isFalse();
        assertThat(sut.nextReady().orElseThrow().token()).isEqualTo(2);
        assertThat(sut.add(3, 7)).isTrue();
    }

    @Test
    void a_superseded_queued_acknowledgement_keeps_capacity_but_not_the_identifier() {
        // Arrange
        var sut = new AckSequencer(2);
        sut.add(1, 7);
        sut.ready(1);
        sut.submitted(10);

        // Act
        OptionalLong superseded = sut.supersede(7);
        boolean added = sut.add(2, 7);

        // Assert: the write still counts, but its completion leaves the new reception alone.
        assertThat(superseded).isEmpty();
        assertThat(added).isTrue();
        assertThat(sut.add(3, 9)).isFalse();
        sut.written(10);
        assertThat(sut.contains(7)).isTrue();
        assertThat(sut.add(3, 9)).isTrue();
    }

    @Test
    void clear_releases_the_whole_sequence_and_ignores_old_tokens() {
        // Arrange
        var sut = new AckSequencer(1);
        sut.add(1, 7);
        sut.ready(1);
        sut.submitted(10);

        // Act
        sut.clear();
        assertThat(sut.add(2, 7)).isTrue();
        sut.ready(1);
        sut.written(10);

        // Assert
        assertThat(sut.contains(7)).isTrue();
        assertThat(sut.writingCount()).isZero();
        assertThat(sut.nextReady()).isEmpty();

        // Act
        sut.ready(2);

        // Assert
        assertThat(sut.nextReady().orElseThrow().token()).isEqualTo(2);
    }
}
