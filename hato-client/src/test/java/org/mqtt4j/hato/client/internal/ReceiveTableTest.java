package org.mqtt4j.hato.client.internal;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.client.internal.ReceiveTable.Entry;

import static org.assertj.core.api.Assertions.assertThat;

class ReceiveTableTest {

    @Test
    void readiness_can_be_revoked_without_releasing_the_reception() {
        // Arrange
        var sut = new ReceiveTable(1);
        Entry entry = sut.add(1, 7).orElseThrow();
        sut.ready(1);

        // Act
        sut.ready(1, false);

        // Assert
        assertThat(entry.ready()).isFalse();
        assertThat(sut.find(7)).containsSame(entry);
        assertThat(sut.add(2, 8)).isEmpty();

        // Act
        sut.ready(1, true);

        // Assert
        assertThat(entry.ready()).isTrue();
    }

    @Test
    void a_snapshot_keeps_superseded_receptions_and_survives_their_removal() {
        // Arrange
        var sut = new ReceiveTable(2);
        Entry earlier = sut.add(1, 7).orElseThrow();
        sut.supersede(7);
        Entry current = sut.add(2, 7).orElseThrow();

        // Act
        List<Entry> snapshot = sut.entries();
        snapshot.forEach(sut::remove);

        // Assert
        assertThat(snapshot).containsExactlyInAnyOrder(earlier, current);
        assertThat(sut.entries()).isEmpty();
        assertThat(sut.find(7)).isEmpty();
    }

    @Test
    void readiness_does_not_release_acknowledgement_capacity() {
        // Arrange
        var sut = new ReceiveTable(2);
        Entry first = sut.add(1, 7).orElseThrow();
        Entry following = sut.add(2, 8).orElseThrow();

        // Act
        sut.ready(2);
        sut.ready(1);

        // Assert
        assertThat(first.ready()).isTrue();
        assertThat(following.ready()).isTrue();
        assertThat(sut.find(7)).containsSame(first);
        assertThat(sut.add(3, 9)).isEmpty();

        // Act
        sut.remove(first);

        // Assert
        assertThat(sut.find(7)).isEmpty();
        assertThat(sut.add(3, 9)).isPresent();
        assertThat(sut.find(8)).containsSame(following);
    }

    @Test
    void an_identifier_is_occupied_until_removed_and_old_tokens_cannot_ack_its_reuse() {
        // Arrange
        var sut = new ReceiveTable(2);
        Entry earlier = sut.add(1, 7).orElseThrow();
        assertThat(sut.add(2, 7)).isEmpty();
        sut.ready(1);
        sut.remove(earlier);
        Entry current = sut.add(2, 7).orElseThrow();

        // Act
        sut.ready(1);
        sut.remove(earlier);

        // Assert
        assertThat(sut.find(7)).containsSame(current);
        assertThat(current.ready()).isFalse();

        // Act
        sut.ready(2);

        // Assert
        assertThat(current.ready()).isTrue();
    }

    @Test
    void a_superseded_reception_keeps_capacity_but_not_the_identifier() {
        // Arrange
        var sut = new ReceiveTable(2);
        Entry earlier = sut.add(1, 7).orElseThrow();

        // Act
        Optional<Entry> superseded = sut.supersede(7);
        Entry current = sut.add(2, 7).orElseThrow();

        // Assert: an older pending write still counts without owning the reused identifier.
        assertThat(superseded).containsSame(earlier);
        assertThat(sut.find(7)).containsSame(current);
        assertThat(sut.add(3, 9)).isEmpty();

        // Act
        sut.remove(earlier);

        // Assert
        assertThat(sut.find(7)).containsSame(current);
        assertThat(sut.add(3, 9)).isPresent();
    }

    @Test
    void clear_releases_identifiers_capacity_and_old_tokens() {
        // Arrange
        var sut = new ReceiveTable(1);
        Entry earlier = sut.add(1, 7).orElseThrow();
        sut.ready(1);

        // Act
        sut.clear();
        Entry current = sut.add(2, 7).orElseThrow();
        sut.ready(1);
        sut.remove(earlier);

        // Assert
        assertThat(sut.find(7)).containsSame(current);
        assertThat(current.ready()).isFalse();

        // Act
        sut.ready(2);

        // Assert
        assertThat(current.ready()).isTrue();
    }
}
