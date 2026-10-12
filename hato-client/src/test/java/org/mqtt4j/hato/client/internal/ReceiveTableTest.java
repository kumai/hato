package org.mqtt4j.hato.client.internal;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.client.internal.ReceiveTable.Entry;
import org.mqtt4j.hato.client.internal.ReceiveTable.Stage;

import static org.assertj.core.api.Assertions.assertThat;

class ReceiveTableTest {

    @Test
    void readiness_can_be_revoked_without_releasing_the_reception() {
        // Arrange
        var sut = new ReceiveTable(1);
        Entry entry = sut.add(1, 7, Qos.AT_LEAST_ONCE).orElseThrow();
        sut.ready(1);

        // Act
        sut.ready(1, false);

        // Assert
        assertThat(entry.ready()).isFalse();
        assertThat(sut.find(7)).containsSame(entry);
        assertThat(sut.add(2, 8, Qos.AT_LEAST_ONCE)).isEmpty();

        // Act
        sut.ready(1, true);

        // Assert
        assertThat(entry.ready()).isTrue();
    }

    @Test
    void a_snapshot_keeps_superseded_receptions_and_survives_their_removal() {
        // Arrange
        var sut = new ReceiveTable(2);
        Entry earlier = sut.add(1, 7, Qos.AT_LEAST_ONCE).orElseThrow();
        sut.supersede(7);
        Entry current = sut.add(2, 7, Qos.AT_LEAST_ONCE).orElseThrow();

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
        Entry first = sut.add(1, 7, Qos.AT_LEAST_ONCE).orElseThrow();
        Entry following = sut.add(2, 8, Qos.AT_LEAST_ONCE).orElseThrow();

        // Act
        sut.ready(2);
        sut.ready(1);

        // Assert
        assertThat(first.ready()).isTrue();
        assertThat(following.ready()).isTrue();
        assertThat(sut.find(7)).containsSame(first);
        assertThat(sut.add(3, 9, Qos.AT_LEAST_ONCE)).isEmpty();

        // Act
        sut.remove(first);

        // Assert
        assertThat(sut.find(7)).isEmpty();
        assertThat(sut.add(3, 9, Qos.AT_LEAST_ONCE)).isPresent();
        assertThat(sut.find(8)).containsSame(following);
    }

    @Test
    void an_identifier_is_occupied_until_removed_and_old_tokens_cannot_ack_its_reuse() {
        // Arrange
        var sut = new ReceiveTable(2);
        Entry earlier = sut.add(1, 7, Qos.AT_LEAST_ONCE).orElseThrow();
        assertThat(sut.add(2, 7, Qos.AT_LEAST_ONCE)).isEmpty();
        sut.ready(1);
        sut.remove(earlier);
        Entry current = sut.add(2, 7, Qos.AT_LEAST_ONCE).orElseThrow();

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
        Entry earlier = sut.add(1, 7, Qos.AT_LEAST_ONCE).orElseThrow();

        // Act
        Optional<Entry> superseded = sut.supersede(7);
        Entry current = sut.add(2, 7, Qos.AT_LEAST_ONCE).orElseThrow();

        // Assert: an older pending write still counts without owning the reused identifier.
        assertThat(superseded).containsSame(earlier);
        assertThat(sut.find(7)).containsSame(current);
        assertThat(sut.add(3, 9, Qos.AT_LEAST_ONCE)).isEmpty();

        // Act
        sut.remove(earlier);

        // Assert
        assertThat(sut.find(7)).containsSame(current);
        assertThat(sut.add(3, 9, Qos.AT_LEAST_ONCE)).isPresent();
    }

    @Test
    void clear_releases_identifiers_capacity_and_old_tokens() {
        // Arrange
        var sut = new ReceiveTable(1);
        Entry earlier = sut.add(1, 7, Qos.AT_LEAST_ONCE).orElseThrow();
        sut.ready(1);

        // Act
        sut.clear();
        Entry current = sut.add(2, 7, Qos.AT_LEAST_ONCE).orElseThrow();
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
    void pubrec_waits_for_transport_acceptance_and_keeps_the_identifier_and_capacity() {
        // Arrange
        var sut = new ReceiveTable(1);
        Entry entry = sut.add(1, 7, Qos.EXACTLY_ONCE).orElseThrow();
        var receipt = new SendReceipt();
        sut.ready(1);
        entry.submitted(receipt);

        // Act
        entry.observeReceipt();

        // Assert
        assertThat(entry.stage()).isEqualTo(Stage.RECEIVING);

        // Act
        receipt.markAccepted();
        entry.observeReceipt();
        sut.ready(1, false);

        // Assert
        assertThat(entry.stage()).isEqualTo(Stage.WAITING_PUBREL);
        assertThat(entry.ready()).isTrue();
        assertThat(sut.find(7)).containsSame(entry);
        assertThat(sut.add(2, 8, Qos.AT_LEAST_ONCE)).isEmpty();
    }

    @Test
    void a_written_pubrec_establishes_the_release_wait_without_a_receipt_notification() {
        // Arrange
        var sut = new ReceiveTable(1);
        Entry entry = sut.add(1, 7, Qos.EXACTLY_ONCE).orElseThrow();
        entry.submitted(new SendReceipt());

        // Act
        entry.pubrecAccepted();

        // Assert
        assertThat(entry.stage()).isEqualTo(Stage.WAITING_PUBREL);
        assertThat(sut.find(7)).containsSame(entry);
    }

    @Test
    void a_repeated_pubrec_cannot_erase_the_first_transport_acceptance() {
        // Arrange
        var sut = new ReceiveTable(1);
        Entry entry = sut.add(1, 7, Qos.EXACTLY_ONCE).orElseThrow();
        var original = new SendReceipt();
        entry.submitted(original);
        original.markAccepted();
        var repetition = new SendReceipt();

        // Act
        entry.submitted(repetition);
        entry.observeReceipt();

        // Assert
        assertThat(entry.receipt()).isSameAs(repetition);
        assertThat(entry.stage()).isEqualTo(Stage.WAITING_PUBREL);
    }

    @Test
    void pubrel_releases_the_identifier_but_pubcomp_keeps_capacity_until_written() {
        // Arrange
        var sut = new ReceiveTable(2);
        Entry earlier = sut.add(1, 7, Qos.EXACTLY_ONCE).orElseThrow();
        earlier.pubrecAccepted();

        // Act
        sut.complete(earlier);
        Entry current = sut.add(2, 7, Qos.EXACTLY_ONCE).orElseThrow();
        sut.ready(1, false);
        earlier.pubrecAccepted();

        // Assert
        assertThat(earlier.stage()).isEqualTo(Stage.COMPLETING);
        assertThat(earlier.ready()).isTrue();
        assertThat(sut.find(7)).containsSame(current);
        assertThat(sut.add(3, 8, Qos.AT_LEAST_ONCE)).isEmpty();

        // Act: repeated release and old write completion cannot touch the new id occupant.
        sut.complete(earlier);
        sut.remove(earlier);

        // Assert
        assertThat(sut.find(7)).containsSame(current);
        assertThat(current.ready()).isFalse();
        assertThat(sut.add(3, 8, Qos.AT_LEAST_ONCE)).isPresent();
    }

    @Test
    void a_puback_receipt_does_not_turn_qos_one_into_a_release_wait() {
        // Arrange
        var sut = new ReceiveTable(1);
        Entry entry = sut.add(1, 7, Qos.AT_LEAST_ONCE).orElseThrow();
        var receipt = new SendReceipt();
        entry.submitted(receipt);

        // Act
        receipt.markAccepted();
        entry.observeReceipt();

        // Assert
        assertThat(entry.qos()).isEqualTo(Qos.AT_LEAST_ONCE);
        assertThat(entry.stage()).isEqualTo(Stage.RECEIVING);
    }
}
