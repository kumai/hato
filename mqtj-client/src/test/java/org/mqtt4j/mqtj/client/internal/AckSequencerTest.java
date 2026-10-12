package org.mqtt4j.mqtj.client.internal;

import org.junit.jupiter.api.Test;

import org.mqtt4j.mqtj.client.internal.ReceiveTable.Entry;

import static org.assertj.core.api.Assertions.assertThat;

class AckSequencerTest {

    @Test
    void only_the_ready_prefix_can_be_released() {
        // Arrange
        var receptions = new ReceiveTable(3);
        var sut = new AckSequencer();
        sut.add(receptions.add(1, 7).orElseThrow());
        sut.add(receptions.add(2, 8).orElseThrow());
        sut.add(receptions.add(3, 9).orElseThrow());

        // Act
        receptions.ready(3);
        receptions.ready(2);

        // Assert
        assertThat(sut.nextReady()).isEmpty();

        // Act
        receptions.ready(1);

        // Assert
        assertThat(sut.nextReady().orElseThrow().packetId()).isEqualTo(7);
        sut.submitted(10);
        assertThat(sut.nextReady().orElseThrow().packetId()).isEqualTo(8);
        sut.submitted(11);
        assertThat(sut.nextReady().orElseThrow().packetId()).isEqualTo(9);
        sut.submitted(12);
        assertThat(sut.nextReady()).isEmpty();
        assertThat(sut.writingCount()).isEqualTo(3);
    }

    @Test
    void removing_an_unqueued_acknowledgement_lets_a_ready_follower_lead() {
        // Arrange
        var receptions = new ReceiveTable(2);
        var sut = new AckSequencer();
        Entry earlier = receptions.add(1, 7).orElseThrow();
        Entry following = receptions.add(2, 8).orElseThrow();
        sut.add(earlier);
        sut.add(following);
        receptions.ready(2);
        assertThat(sut.contains(earlier)).isTrue();

        // Act
        boolean removed = sut.remove(earlier);

        // Assert
        assertThat(removed).isTrue();
        assertThat(sut.contains(earlier)).isFalse();
        assertThat(sut.contains(following)).isTrue();
        assertThat(sut.nextReady()).containsSame(following);
        assertThat(sut.remove(earlier)).isFalse();
    }

    @Test
    void a_submitted_acknowledgement_cannot_be_withdrawn() {
        // Arrange
        var receptions = new ReceiveTable(1);
        var sut = new AckSequencer();
        Entry entry = receptions.add(1, 7).orElseThrow();
        sut.add(entry);
        receptions.ready(1);
        sut.submitted(10);

        // Act
        boolean removed = sut.remove(entry);

        // Assert
        assertThat(removed).isFalse();
        assertThat(sut.contains(entry)).isTrue();
        assertThat(sut.nextReady()).isEmpty();
        assertThat(sut.writingCount()).isEqualTo(1);
        assertThat(sut.written(10)).containsSame(entry);
        assertThat(sut.contains(entry)).isFalse();
        assertThat(sut.written(10)).isEmpty();
        assertThat(sut.writingCount()).isZero();
    }

    @Test
    void clear_releases_the_whole_sequence_and_ignores_old_tickets() {
        // Arrange
        var receptions = new ReceiveTable(3);
        var sut = new AckSequencer();
        sut.add(receptions.add(1, 7).orElseThrow());
        sut.add(receptions.add(2, 8).orElseThrow());
        receptions.ready(1);
        sut.submitted(10);

        // Act
        sut.clear();
        Entry current = receptions.add(3, 9).orElseThrow();
        sut.add(current);
        receptions.ready(2);

        // Assert
        assertThat(sut.written(10)).isEmpty();
        assertThat(sut.writingCount()).isZero();
        assertThat(sut.nextReady()).isEmpty();
        assertThat(sut.contains(current)).isTrue();
        assertThat(sut.contains(receptions.find(8).orElseThrow())).isFalse();

        // Act
        receptions.ready(3);

        // Assert
        assertThat(sut.nextReady()).containsSame(current);
    }
}
