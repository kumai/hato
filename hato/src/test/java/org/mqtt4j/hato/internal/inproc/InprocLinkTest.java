package org.mqtt4j.hato.internal.inproc;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.internal.protocol.Packet;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class InprocLinkTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test
    void a_reader_that_waits_after_its_own_side_ended_returns_at_once() {
        // Arrange: the client's side ends before any reader waits, so the end wakes nobody.
        var sut = new InprocLink();
        sut.endClient();
        // Act & Assert
        assertTimeoutPreemptively(WAIT, sut::awaitPackets);
    }

    @Test
    void a_broker_whose_packets_fill_the_queue_is_woken_once_the_client_takes_one() {
        // Arrange: a queue to the client that is full.
        var wakes = new AtomicInteger();
        var sut = new InprocLink();
        sut.accept(wakes::incrementAndGet);
        int delivered = 0;
        while (sut.deliver(small())) {
            delivered++;
        }
        int before = wakes.get();
        // Act
        InprocLink.Item taken = sut.receive();
        // Assert
        assertThat(delivered).isEqualTo(128);
        assertThat(taken).isNotNull();
        assertThat(wakes).hasValue(before + 1);
        assertThat(sut.deliver(small())).isTrue();
    }

    @Test
    void an_empty_queue_takes_one_packet_beyond_its_bytes_and_nothing_after_it() {
        // Arrange
        var sut = new InprocLink();
        var large = new InprocLink.Item(new Packet.PingReq(), 1024 * 1024);
        // Act & Assert
        assertThat(sut.send(large)).isTrue();
        assertThat(sut.send(small())).isFalse();
        assertThat(sut.take()).isSameAs(large);
        assertThat(sut.send(small())).isTrue();
    }

    @Test
    void a_sender_that_waits_for_room_goes_on_once_the_broker_takes_a_packet() throws Exception {
        // Arrange: a queue to the broker that is full, and a sender that waits for room.
        var sut = new InprocLink();
        while (sut.send(small())) {
            // Fills the queue.
        }
        CompletableFuture<Void> waiting = CompletableFuture.runAsync(() -> sut.awaitRoom(2));
        // Act
        assertThat(sut.take()).isNotNull();
        // Assert
        assertThatCode(() -> waiting.get(5, SECONDS)).doesNotThrowAnyException();
    }

    private static InprocLink.Item small() {
        return new InprocLink.Item(new Packet.PingReq(), 2);
    }
}
