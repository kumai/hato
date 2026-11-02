package org.mqtt4j.hato.client.internal;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

class PlatformReadersTest {

    // The tests pass the time themselves. Each reader counts its reads over windows of 100 ns:
    // five reads in one make a connection busy, and fewer than two quiet.
    private static final long WINDOW = 100;

    private final ReadingClient client = new ReadingClient();

    @ParameterizedTest(name = "{0} reads in a window move it: {1}")
    @CsvSource({"4, false", "5, true"})
    void a_connection_moves_to_a_free_platform_thread_once_it_reads_busily(
            int reads,
            boolean moves
    ) {
        // Arrange
        PlatformReaders sut = moving(1, true);
        PlatformReaders.Watch virtual = watch(sut, false);
        // Act & Assert: the reading moves after the read that ends a busy window.
        assertThat(readsUntilMove(virtual, 0, reads)).isEqualTo(moves ? reads : 0);
    }

    @ParameterizedTest(name = "{0} reads in a window give the thread up: {1}")
    @CsvSource({"0, true", "1, true", "2, false", "10, false"})
    void a_platform_reader_gives_its_thread_up_to_a_connection_that_asks_only_while_quiet(
            int reads,
            boolean givesUp
    ) {
        // Arrange: the only platform thread is taken, and a busy connection asks for it.
        PlatformReaders sut = moving(1, true);
        assertThat(sut.take()).isTrue();
        PlatformReaders.Watch platform = watch(sut, true);
        PlatformReaders.Watch virtual = watch(sut, false);
        assertThat(readsUntilMove(virtual, 0, 10)).isZero();
        // Act
        boolean moves = reads == 0
                ? platform.afterTimeout(WINDOW)
                : readsUntilMove(platform, 0, reads) == reads;
        // Assert
        assertThat(moves).isEqualTo(givesUp);
    }

    @Test
    void a_quiet_platform_reader_keeps_its_thread_while_no_busy_connection_asks() {
        // Arrange: the only platform thread is taken, and another connection reads, but not
        // busily.
        PlatformReaders sut = moving(1, true);
        assertThat(sut.take()).isTrue();
        PlatformReaders.Watch platform = watch(sut, true);
        PlatformReaders.Watch virtual = watch(sut, false);
        assertThat(readsUntilMove(virtual, 0, 4)).isZero();
        // Act & Assert
        assertThat(platform.afterTimeout(WINDOW)).isFalse();
    }

    @Test
    void a_connection_that_reads_less_busily_no_longer_asks() {
        // Arrange: the only platform thread is taken, and a connection asks for it in one
        // window but reads less in the next.
        PlatformReaders sut = moving(1, true);
        assertThat(sut.take()).isTrue();
        PlatformReaders.Watch platform = watch(sut, true);
        PlatformReaders.Watch virtual = watch(sut, false);
        assertThat(readsUntilMove(virtual, 0, 10)).isZero();
        assertThat(readsUntilMove(virtual, WINDOW, 4)).isZero();
        // Act & Assert: the quiet platform reader keeps its thread.
        assertThat(platform.afterTimeout(2 * WINDOW)).isFalse();
    }

    @ParameterizedTest(name = "handlers run after reads: {0}")
    @ValueSource(booleans = {false, true})
    void with_few_processors_only_a_connection_whose_handlers_run_after_reads_asks(
            boolean runsHandlers
    ) {
        // Arrange: the only platform thread is taken by a quiet connection, and another reads
        // busily.
        PlatformReaders sut = moving(1, false);
        assertThat(sut.take()).isTrue();
        PlatformReaders.Watch platform = watch(sut, true);
        client.runsHandlers = runsHandlers;
        PlatformReaders.Watch virtual = watch(sut, false);
        assertThat(readsUntilMove(virtual, 0, 10)).isZero();
        // Act & Assert: the quiet connection gives its thread up only to a connection that asks.
        assertThat(platform.afterTimeout(WINDOW)).isEqualTo(runsHandlers);
    }

    @Test
    void a_pause_of_the_reading_starts_the_count_again() {
        // Arrange: the only platform thread is taken by a connection that read once and then
        // paused, while a busy connection asks for it.
        PlatformReaders sut = moving(1, true);
        assertThat(sut.take()).isTrue();
        PlatformReaders.Watch platform = watch(sut, true);
        PlatformReaders.Watch virtual = watch(sut, false);
        assertThat(readsUntilMove(virtual, 0, 10)).isZero();
        assertThat(platform.afterRead(10)).isFalse();
        // Act: the pause ends long after the window.
        platform.restart(5 * WINDOW);
        // Assert: the first read after the pause starts a new window rather than end a quiet one.
        assertThat(platform.afterRead(5 * WINDOW + 10)).isFalse();
    }

    @Test
    void a_reading_that_did_not_move_tries_again_a_window_later() {
        // Arrange: a busy connection whose reading was due to move to the free platform thread.
        PlatformReaders sut = moving(1, true);
        PlatformReaders.Watch virtual = watch(sut, false);
        assertThat(readsUntilMove(virtual, 0, 10)).isEqualTo(10);
        // Act: no thread could take the reading.
        virtual.stay(WINDOW);
        // Assert: the next attempt comes only with the read that ends the next busy window.
        assertThat(readsUntilMove(virtual, WINDOW, 10)).isEqualTo(10);
    }

    private static PlatformReaders moving(int slots, boolean everyConnection) {
        return new PlatformReaders(
                slots,
                new PlatformReaders.Moves(
                        Duration.ofNanos(WINDOW),
                        5,
                        2,
                        Duration.ofMillis(10),
                        everyConnection
                )
        );
    }

    // A watch from time 0 on a connection of `client`.
    private PlatformReaders.Watch watch(PlatformReaders sut, boolean platform) {
        return requireNonNull(sut.watch(platform, client, 0));
    }

    // Reads `reads` times, evenly over the window that starts at `start`, the last at its end, and
    // returns after how many reads the reading should have moved, or 0 if it should not.
    private static int readsUntilMove(PlatformReaders.Watch watch, long start, int reads) {
        for (int read = 1; read <= reads; read++) {
            if (watch.afterRead(start + WINDOW * read / reads)) {
                return read;
            }
        }
        return 0;
    }

    private static final class ReadingClient implements BorrowingListener {

        volatile boolean runsHandlers;

        @Override
        public void onConnected() {
            throw new AssertionError("Unexpected connection");
        }

        @Override
        public void onBytes(byte[] bytes) {
            throw new AssertionError("Unexpected bytes");
        }

        @Override
        public void onBorrowedBytes(byte[] buffer, int offset, int length) {
            throw new AssertionError("Unexpected bytes");
        }

        @Override
        public void afterBorrowedBytes() {
            throw new AssertionError("Unexpected read");
        }

        @Override
        public boolean runsHandlersAfterReads() {
            return runsHandlers;
        }

        @Override
        public void onSent(long ticket) {
            throw new AssertionError("Unexpected send completion");
        }

        @Override
        public void onClosed(Throwable cause) {
            throw new AssertionError("Unexpected close");
        }
    }
}
