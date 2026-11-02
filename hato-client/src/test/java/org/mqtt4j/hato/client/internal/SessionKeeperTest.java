package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.mqtt4j.hato.MqttVersion;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.TopicFilter;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class SessionKeeperTest {

    private static final SessionState STATE = new SessionState(
            List.of(new SessionState.Registration(new TopicFilter("a/#"), 1, false)),
            2,
            List.of(new SessionState.Reception(5, Qos.AT_LEAST_ONCE))
    );

    private static final SessionState LATER_STATE = new SessionState(
            List.of(new SessionState.Registration(new TopicFilter("b/#"), 2, false)),
            3,
            List.of()
    );

    private static final SessionCodec codec = new SessionCodec("client", MqttVersion.V5);

    @TempDir
    Path directory;

    private final Semaphore woken = new Semaphore(0);

    private final List<SessionKeeper> keepers = new ArrayList<>();

    @AfterEach
    void closeKeepers() {
        keepers.forEach(SessionKeeper::close);
    }

    @Test
    void a_claim_hands_over_the_state_that_an_earlier_client_object_saved() throws Exception {
        // Arrange: an earlier client object claimed the session and saved its state.
        Path file = directory.resolve("session");
        SessionKeeper earlier = keeper(new FileSessionStore(file));
        earlier.claim();
        next(earlier);
        earlier.save(STATE, List.of());
        next(earlier);
        SessionKeeper sut = keeper(new FileSessionStore(file));

        // Act
        sut.claim();

        // Assert
        assertThat(next(sut)).isEqualTo(new SessionKeeper.Outcome.Claimed(STATE, false, null));
        assertThat(stored(file)).isEmpty();
    }

    @Test
    void a_claim_while_another_client_object_holds_the_store_waits_for_its_save()
            throws Exception {
        // Arrange: an earlier client object claimed the session and holds the store.
        Path file = directory.resolve("session");
        SessionKeeper earlier = keeper(new FileSessionStore(file));
        earlier.claim();
        next(earlier);
        SessionKeeper sut = keeper(new FileSessionStore(file));

        // Act
        sut.claim();
        SessionKeeper.Outcome whileHeld = next(sut);
        earlier.save(STATE, List.of());
        next(earlier);
        sut.claim();

        // Assert: the store refuses the claim until the earlier client object saves the session,
        // which the claim then takes over.
        assertThat(whileHeld).isEqualTo(new SessionKeeper.Outcome.Refused());
        assertThat(next(sut)).isEqualTo(new SessionKeeper.Outcome.Claimed(STATE, false, null));
    }

    @Test
    void a_closed_keeper_gives_the_store_up_once_no_transport_is_open() throws Exception {
        // Arrange: an earlier client object claimed the session and closes without saving it,
        // while its transport is still open.
        Path file = directory.resolve("session");
        var transportOpen = new AtomicBoolean(true);
        SessionKeeper earlier = keeper(new FileSessionStore(file), transportOpen::get);
        earlier.claim();
        next(earlier);
        earlier.close();
        SessionKeeper sut = keeper(new FileSessionStore(file));

        // Act
        sut.claim();
        SessionKeeper.Outcome whileOpen = next(sut);
        transportOpen.set(false);
        earlier.transportsClosed();
        sut.claim();

        // Assert: the claim then finds the earlier client object's mark instead of a saved state.
        assertThat(whileOpen).isEqualTo(new SessionKeeper.Outcome.Refused());
        assertThat(next(sut)).isEqualTo(new SessionKeeper.Outcome.Claimed(null, false, null));
    }

    @Test
    void a_save_keeps_the_store_until_no_transport_is_open() throws Exception {
        // Arrange: this client object claimed the session. The transport of a connection that it
        // gave up, whose CONNECT may not have left, is still open.
        Path file = directory.resolve("session");
        var transportOpen = new AtomicBoolean(true);
        SessionKeeper sut = keeper(new FileSessionStore(file), transportOpen::get);
        sut.claim();
        next(sut);
        SessionKeeper other = keeper(new FileSessionStore(file));

        // Act
        sut.save(STATE, List.of());
        next(sut);
        other.claim();
        SessionKeeper.Outcome whileOpen = next(other);
        transportOpen.set(false);
        sut.transportsClosed();
        other.claim();

        // Assert: another client object takes the saved session over once the transport closed.
        assertThat(whileOpen).isEqualTo(new SessionKeeper.Outcome.Refused());
        assertThat(next(other)).isEqualTo(new SessionKeeper.Outcome.Claimed(STATE, false, null));
    }

    @Test
    void a_keeper_that_closes_during_a_store_call_gives_the_store_up_once_it_returns()
            throws Exception {
        // Arrange: the claim's read waits until the test lets it go.
        var reading = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);
        var releases = new AtomicInteger();
        SessionKeeper sut = keeper(new SessionStore() {
            @Override
            public boolean hold() {
                return true;
            }

            @Override
            public void release() {
                releases.incrementAndGet();
            }

            @Override
            public Optional<Stored> read() throws IOException {
                reading.countDown();
                try {
                    if (!proceed.await(5, SECONDS)) {
                        throw new IOException("The read was never let go");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException(interrupted);
                }
                return Optional.empty();
            }

            @Override
            public boolean replace(long expected, long next, byte[] content) {
                return true;
            }
        });
        sut.claim();
        assertThat(reading.await(5, SECONDS)).isTrue();

        // Act: the client closes while the claim still runs.
        sut.close();
        int whileTheClaimRuns = releases.get();
        proceed.countDown();

        // Assert
        assertThat(whileTheClaimRuns).isZero();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(releases).hasValue(1));
    }

    @Test
    void a_claim_of_its_own_latest_save_says_so() throws Exception {
        // Arrange
        SessionKeeper sut = keeper(new FileSessionStore(directory.resolve("session")));
        sut.claim();
        next(sut);
        sut.save(STATE, List.of());
        next(sut);

        // Act
        sut.claim();

        // Assert
        assertThat(next(sut)).isEqualTo(new SessionKeeper.Outcome.Claimed(null, true, null));
    }

    @Test
    void a_check_tells_whether_the_record_still_carries_the_claim() throws Exception {
        // Arrange: this client object claims the session, and then a writer that ignores the
        // hold claims it too.
        var record = new AtomicReference<SessionStore.@Nullable Stored>();
        var releases = new AtomicInteger();
        SessionKeeper sut = keeper(new UnheldStore(record, releases));
        sut.claim();
        next(sut);
        sut.check(3);
        SessionKeeper.Outcome beforeTheOtherClaim = next(sut);
        SessionKeeper other = keeper(new UnheldStore(record, new AtomicInteger()));
        other.claim();
        next(other);

        // Act
        sut.check(4);

        // Assert: the check that finds the other claim gives the store up.
        assertThat(beforeTheOtherClaim)
                .isEqualTo(new SessionKeeper.Outcome.Checked(3, true, null));
        assertThat(next(sut)).isEqualTo(new SessionKeeper.Outcome.Checked(4, false, null));
        assertThat(releases).hasValue(1);
    }

    @Test
    void a_claim_after_the_file_was_deleted_takes_no_other_save_for_its_own() throws Exception {
        // Arrange: this client object saved the session. Someone deleted the file, and another
        // client object claimed and saved the session once.
        Path file = directory.resolve("session");
        SessionKeeper sut = keeper(new FileSessionStore(file));
        sut.claim();
        next(sut);
        sut.save(STATE, List.of());
        next(sut);
        Files.delete(file);
        SessionKeeper other = keeper(new FileSessionStore(file));
        other.claim();
        next(other);
        other.save(LATER_STATE, List.of());
        next(other);

        // Act
        sut.claim();

        // Assert
        assertThat(next(sut))
                .isEqualTo(new SessionKeeper.Outcome.Claimed(LATER_STATE, false, null));
    }

    @Test
    void a_save_after_the_record_was_deleted_and_claimed_again_changes_nothing()
            throws Exception {
        // Arrange: this client object claimed the session. Someone deleted the record, and a
        // writer that ignores the hold claimed the session.
        var record = new AtomicReference<SessionStore.@Nullable Stored>();
        SessionKeeper sut = keeper(new UnheldStore(record, new AtomicInteger()));
        sut.claim();
        next(sut);
        record.set(null);
        SessionKeeper other = keeper(new UnheldStore(record, new AtomicInteger()));
        other.claim();
        next(other);

        // Act
        sut.save(STATE, List.of());
        next(sut);

        // Assert: the record still marks the session as the other client object's.
        assertThat(codec.decode(requireNonNull(record.get()).content())).isEmpty();
    }

    @Test
    void a_store_that_cannot_be_read_fails_the_claim_and_goes_once_no_transport_is_open()
            throws Exception {
        // Arrange: the store fails the claim while the transport of the connection that asked
        // for it is open.
        var failure = new IOException("disk failure");
        var releases = new AtomicInteger();
        var transportOpen = new AtomicBoolean(true);
        SessionStore failing = new SessionStore() {
            @Override
            public boolean hold() {
                return true;
            }

            @Override
            public void release() {
                releases.incrementAndGet();
            }

            @Override
            public Optional<Stored> read() throws IOException {
                throw failure;
            }

            @Override
            public boolean replace(long expected, long next, byte[] content) {
                return true;
            }
        };
        SessionKeeper sut = keeper(failing, transportOpen::get);

        // Act
        sut.claim();
        SessionKeeper.Outcome claimed = next(sut);
        int whileOpen = releases.get();
        transportOpen.set(false);
        sut.transportsClosed();

        // Assert
        assertThat(claimed).isEqualTo(new SessionKeeper.Outcome.Claimed(null, false, failure));
        assertThat(whileOpen).isZero();
        assertThat(releases).hasValue(1);
    }

    @Test
    void a_failed_save_still_completes_its_disconnections_and_gives_the_store_up()
            throws Exception {
        // Arrange: the store takes the claim and then fails.
        var replacements = new ArrayList<byte[]>();
        var releases = new AtomicInteger();
        SessionKeeper sut = keeper(new SessionStore() {
            @Override
            public boolean hold() {
                return true;
            }

            @Override
            public void release() {
                releases.incrementAndGet();
            }

            @Override
            public Optional<Stored> read() {
                return Optional.empty();
            }

            @Override
            public boolean replace(long expected, long next, byte[] content) throws IOException {
                if (!replacements.isEmpty()) {
                    throw new IOException("disk full");
                }
                replacements.add(content);
                return true;
            }
        });
        sut.claim();
        next(sut);
        var disconnections = List.of(new SessionCore.Completion(4, null));

        // Act
        sut.save(STATE, disconnections);

        // Assert
        assertThat(next(sut)).isEqualTo(new SessionKeeper.Outcome.Saved(disconnections));
        assertThat(releases).hasValue(1);
    }

    /// A keeper of `store` for a client without open transports.
    private SessionKeeper keeper(SessionStore store) {
        return keeper(store, () -> false);
    }

    private SessionKeeper keeper(SessionStore store, BooleanSupplier transportsOpen) {
        var keeper = new SessionKeeper(
                "session-test",
                "client",
                store,
                codec,
                woken::release,
                () -> {},
                transportsOpen
        );
        keepers.add(keeper);
        return keeper;
    }

    /// The next result of `keeper`, once its worker reports one and has nothing left to run, when
    /// the keeper has given up the store if it should.
    private SessionKeeper.Outcome next(SessionKeeper keeper) throws InterruptedException {
        assertThat(woken.tryAcquire(5, SECONDS)).isTrue();
        await().atMost(5, SECONDS).until(keeper::idle);
        return keeper.take().getFirst();
    }

    /// The state that the session file saved, or empty when it marks the session as owned.
    private static Optional<SessionState> stored(Path file) throws IOException {
        byte[] content = new FileSessionStore(file).read().orElseThrow().content();
        return codec.decode(content);
    }

    /// A store of `record` that every client object holds at once, as a network file system may
    /// let them, and that counts how often its client object gives it up in `releases`.
    private record UnheldStore(
            AtomicReference<SessionStore.@Nullable Stored> record,
            AtomicInteger releases
    ) implements SessionStore {

        @Override
        public boolean hold() {
            return true;
        }

        @Override
        public void release() {
            releases.incrementAndGet();
        }

        @Override
        public Optional<Stored> read() {
            return Optional.ofNullable(record.get());
        }

        @Override
        public boolean replace(long expected, long next, byte[] content) {
            @Nullable Stored current = record.get();
            long version = current == null ? 0 : current.version();
            return version == expected && record.compareAndSet(current, new Stored(next, content));
        }
    }
}
