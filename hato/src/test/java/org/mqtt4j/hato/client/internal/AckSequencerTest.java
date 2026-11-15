package org.mqtt4j.hato.client.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.client.internal.AckSequencer.Acknowledgement;
import org.mqtt4j.hato.client.internal.AckSequencer.Kind;
import org.mqtt4j.hato.client.internal.ReceiveTable.Entry;

import static org.assertj.core.api.Assertions.assertThat;

class AckSequencerTest {

    @Test
    void only_the_ready_prefix_can_be_released() {
        // Arrange
        var receptions = new ReceiveTable(3);
        var sut = new AckSequencer();
        sut.add(receptions.add(1, 7, Qos.AT_LEAST_ONCE, 0).orElseThrow(), Kind.PUBACK);
        sut.add(receptions.add(2, 8, Qos.AT_LEAST_ONCE, 0).orElseThrow(), Kind.PUBACK);
        sut.add(receptions.add(3, 9, Qos.AT_LEAST_ONCE, 0).orElseThrow(), Kind.PUBACK);

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
        Entry earlier = receptions.add(1, 7, Qos.AT_LEAST_ONCE, 0).orElseThrow();
        Entry following = receptions.add(2, 8, Qos.AT_LEAST_ONCE, 0).orElseThrow();
        sut.add(earlier, Kind.PUBACK);
        sut.add(following, Kind.PUBACK);
        receptions.ready(2);
        assertThat(sut.contains(earlier, Kind.PUBACK)).isTrue();

        // Act
        boolean removed = sut.remove(earlier, Kind.PUBACK);

        // Assert
        assertThat(removed).isTrue();
        assertThat(sut.contains(earlier, Kind.PUBACK)).isFalse();
        assertThat(sut.contains(following, Kind.PUBACK)).isTrue();
        assertThat(sut.nextReady()).containsSame(following);
        assertThat(sut.remove(earlier, Kind.PUBACK)).isFalse();
    }

    @Test
    void a_submitted_acknowledgement_cannot_be_withdrawn() {
        // Arrange
        var receptions = new ReceiveTable(1);
        var sut = new AckSequencer();
        Entry entry = receptions.add(1, 7, Qos.AT_LEAST_ONCE, 0).orElseThrow();
        sut.add(entry, Kind.PUBACK);
        receptions.ready(1);
        sut.submitted(10);

        // Act
        boolean removed = sut.remove(entry, Kind.PUBACK);

        // Assert
        assertThat(removed).isFalse();
        assertThat(sut.contains(entry, Kind.PUBACK)).isTrue();
        assertThat(sut.nextReady()).isEmpty();
        assertThat(sut.writingCount()).isEqualTo(1);
        assertThat(sut.written(10).map(Acknowledgement::entry)).containsSame(entry);
        assertThat(sut.contains(entry, Kind.PUBACK)).isFalse();
        assertThat(sut.written(10)).isEmpty();
        assertThat(sut.writingCount()).isZero();
    }

    @Test
    void an_unready_puback_holds_back_a_later_pubrec_but_not_pubcomp() {
        // Arrange
        var receptions = new ReceiveTable(3);
        var sut = new AckSequencer();
        Entry first = receptions.add(1, 7, Qos.AT_LEAST_ONCE, 0).orElseThrow();
        Entry publication = receptions.add(2, 8, Qos.EXACTLY_ONCE, 0).orElseThrow();
        Entry completion = receptions.add(3, 9, Qos.EXACTLY_ONCE, 0).orElseThrow();
        receptions.complete(completion);
        sut.add(first, Kind.PUBACK);
        sut.add(publication, Kind.PUBREC);
        sut.add(completion, Kind.PUBCOMP);
        receptions.ready(2);

        // Act & Assert
        assertThat(sut.completing(9)).isTrue();
        assertThat(next(sut)).contains(new Acknowledgement(Kind.PUBCOMP, completion));
        sut.submitted(10);
        assertThat(sut.completing(9)).isFalse();
        assertThat(sut.nextReady()).isEmpty();
        receptions.ready(1);
        assertThat(next(sut)).contains(new Acknowledgement(Kind.PUBACK, first));
        sut.submitted(11);
        assertThat(next(sut)).contains(new Acknowledgement(Kind.PUBREC, publication));
        sut.submitted(12);
        assertThat(sut.nextReady()).isEmpty();
    }

    @Test
    void a_pubrec_write_keeps_its_kind_after_pubrel_completes_the_reception() {
        // Arrange
        var receptions = new ReceiveTable(1);
        var sut = new AckSequencer();
        Entry entry = receptions.add(1, 7, Qos.EXACTLY_ONCE, 0).orElseThrow();
        receptions.ready(1);
        sut.add(entry, Kind.PUBREC);
        sut.submitted(10);

        // Act
        entry.pubrecAccepted(0);
        receptions.complete(entry);
        sut.add(entry, Kind.PUBCOMP);

        // Assert: the two writes refer to the same reception but have different lifetimes.
        assertThat(sut.contains(entry, Kind.PUBREC)).isTrue();
        assertThat(sut.contains(entry, Kind.PUBCOMP)).isTrue();
        assertThat(sut.written(10)).contains(new Acknowledgement(Kind.PUBREC, entry));
        assertThat(next(sut)).contains(new Acknowledgement(Kind.PUBCOMP, entry));
        sut.submitted(11);
        assertThat(sut.written(11)).contains(new Acknowledgement(Kind.PUBCOMP, entry));
    }

    @Test
    void repeated_pubrec_responses_keep_their_receive_position() {
        // Arrange
        var receptions = new ReceiveTable(2);
        var sut = new AckSequencer();
        Entry entry = receptions.add(1, 7, Qos.EXACTLY_ONCE, 0).orElseThrow();
        Entry following = receptions.add(2, 8, Qos.AT_LEAST_ONCE, 0).orElseThrow();
        entry.pubrecAccepted(0);
        sut.add(entry, Kind.PUBREC);
        sut.submitted(10);
        sut.add(entry, Kind.PUBREC);
        sut.add(following, Kind.PUBACK);

        // Act
        sut.add(entry, Kind.PUBREC);
        receptions.ready(2);

        // Assert
        assertThat(next(sut)).contains(new Acknowledgement(Kind.PUBREC, entry));
        sut.submitted(11);
        assertThat(next(sut)).contains(new Acknowledgement(Kind.PUBACK, following));
        sut.submitted(12);
        assertThat(sut.nextReady()).isEmpty();
        assertThat(sut.writingCount()).isEqualTo(3);
    }

    @Test
    void removing_a_reception_preserves_writes_and_cannot_remove_a_reused_identifier() {
        // Arrange
        var receptions = new ReceiveTable(2);
        var sut = new AckSequencer();
        Entry earlier = receptions.add(1, 7, Qos.EXACTLY_ONCE, 0).orElseThrow();
        earlier.pubrecAccepted(0);
        sut.add(earlier, Kind.PUBREC);
        sut.submitted(10);
        sut.add(earlier, Kind.PUBREC);
        receptions.complete(earlier);
        Entry current = receptions.add(2, 7, Qos.AT_LEAST_ONCE, 0).orElseThrow();
        sut.add(current, Kind.PUBACK);
        receptions.ready(2);

        // Act
        boolean removed = sut.remove(earlier, Kind.PUBREC);

        // Assert
        assertThat(removed).isTrue();
        assertThat(sut.contains(earlier, Kind.PUBREC)).isTrue();
        assertThat(next(sut)).contains(new Acknowledgement(Kind.PUBACK, current));
        assertThat(sut.written(10)).contains(new Acknowledgement(Kind.PUBREC, earlier));
        assertThat(sut.contains(earlier, Kind.PUBREC)).isFalse();
    }

    @Test
    void a_connection_end_lets_each_reception_await_an_acknowledgement_again() {
        // Arrange
        var receptions = new ReceiveTable(3);
        var sut = new AckSequencer();
        Entry written = receptions.add(1, 7, Qos.AT_LEAST_ONCE, 0).orElseThrow();
        Entry waiting = receptions.add(2, 8, Qos.AT_LEAST_ONCE, 0).orElseThrow();
        Entry completion = receptions.add(3, 9, Qos.EXACTLY_ONCE, 0).orElseThrow();
        receptions.complete(completion);
        sut.add(written, Kind.PUBACK);
        sut.add(waiting, Kind.PUBACK);
        sut.add(completion, Kind.PUBCOMP);
        receptions.ready(1);
        sut.submitted(10);

        // Act
        sut.clear();

        // Assert: nothing of the ended connection remains, and the next one queues afresh.
        assertThat(sut.nextReady()).isEmpty();
        assertThat(sut.completing(9)).isFalse();
        assertThat(sut.written(10)).isEmpty();
        assertThat(sut.writingCount()).isZero();
        sut.add(waiting, Kind.PUBACK);
        sut.add(written, Kind.PUBACK);
        receptions.ready(2);
        assertThat(next(sut)).contains(new Acknowledgement(Kind.PUBACK, waiting));
        sut.submitted(11);
        assertThat(next(sut)).contains(new Acknowledgement(Kind.PUBACK, written));
    }

    @Test
    void writes_beyond_sixteen_finish_in_any_order_with_their_kinds() {
        // Arrange: every third reception is QoS 2, so a finished write can leave its slot to a
        // write of the other kind.
        var receptions = new ReceiveTable(20);
        var sut = new AckSequencer();
        var acknowledgements = new ArrayList<Acknowledgement>();
        for (int number = 1; number <= 20; number++) {
            boolean exactlyOnce = number % 3 == 0;
            Entry entry = receptions.add(
                    number,
                    number,
                    exactlyOnce ? Qos.EXACTLY_ONCE : Qos.AT_LEAST_ONCE,
                    0
            ).orElseThrow();
            Kind kind = exactlyOnce ? Kind.PUBREC : Kind.PUBACK;
            sut.add(entry, kind);
            receptions.ready(number);
            acknowledgements.add(new Acknowledgement(kind, entry));
        }
        for (int number = 1; number <= 20; number++) {
            sut.submitted(100 + number);
        }
        // The last write first, then from both ends towards the middle.
        List<Long> order = List.of(
                120L, 101L, 119L, 102L, 110L, 111L, 103L, 118L, 104L, 117L,
                105L, 116L, 106L, 115L, 107L, 114L, 108L, 113L, 109L, 112L
        );

        // Act
        List<Optional<Acknowledgement>> finished = order.stream()
                .map(sut::written)
                .toList();

        // Assert
        assertThat(finished).containsExactlyElementsOf(order.stream()
                .map(ticket -> Optional.of(acknowledgements.get((int) (ticket - 101))))
                .toList());
        assertThat(sut.writingCount()).isZero();
        assertThat(sut.contains(acknowledgements.getFirst().entry(), Kind.PUBACK)).isFalse();
    }

    /// The next ready acknowledgement, with the kind that its reception awaits.
    private static Optional<Acknowledgement> next(AckSequencer sut) {
        return sut.nextReady().map(entry -> new Acknowledgement(sut.pendingKind(entry), entry));
    }
}
