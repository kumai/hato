package org.mqtt4j.hato.client.internal;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.common.log.Log;

/// Keeps the local state of a persistent session in a [SessionStore] between client objects.
/// Before the client sends CONNECT, it claims the stored record: it holds the store, which keeps
/// other client objects from claiming the session, marks the record as this client object's, and
/// hands over the state that an earlier one saved. While the client owns the session, it instead
/// checks that the record still marks the session as its own, which only a writer that ignores
/// the hold can change. When a disconnection completes, it saves the client's state over the
/// record. One worker thread runs the store's calls in request order, so that a claim never
/// overtakes the save before it, and the results wait here until the actor takes them.
///
/// The keeper gives the store up once the client no longer owns the session, after a save, a
/// failed claim, a check that finds another claim, or closing, and none of the client's
/// transports is open, so that no CONNECT that a connection of this client object may still
/// send follows another client object's claim. A claim of the next connection keeps the store
/// instead.
///
/// Closing drops the requests that wait and the results, but lets a call that has started
/// finish: a replacement that stopped halfway would leave the record whole anyway, and finishing
/// it keeps the client's termination from waiting on an interrupted store.
final class SessionKeeper implements AutoCloseable {

    /// What a store call came to.
    sealed interface Outcome {

        /// A claim of the stored record. Without `failure`, the record now marks the session as
        /// this client object's, and `state` is what an earlier client object saved for this
        /// client, if anything. `ownSave` tells that the record held this client object's own
        /// latest save, so its state is already the client's.
        record Claimed(
                @Nullable SessionState state,
                boolean ownSave,
                @Nullable Throwable failure
        ) implements Outcome {}

        /// A claim that took nothing, because another client object holds the store.
        record Refused() implements Outcome {}

        /// A check of the stored record for the connection numbered `connection`. Without
        /// `failure`, `held` tells whether the record still marks the session as this client
        /// object's, as its latest claim left it.
        record Checked(
                long connection,
                boolean held,
                @Nullable Throwable failure
        ) implements Outcome {}

        /// A save of the client's state, whether or not it reached the store, with the
        /// disconnections that waited for it.
        record Saved(List<SessionCore.Completion> disconnections) implements Outcome {}
    }

    // A request that the worker serves.
    private sealed interface Request {

        record Claim() implements Request {}

        record Check(long connection) implements Request {}

        record Save(
                SessionState state,
                List<SessionCore.Completion> disconnections
        ) implements Request {}
    }

    private static final Log log = Log.of(SessionKeeper.class);

    // Picks the versions of the records that keepers write.
    private static final SecureRandom versions = new SecureRandom();

    private final String threadName;

    // The kind of the worker thread, that of the client's core: the claim before CONNECT then
    // waits for no carrier while the core runs on platform threads.
    private final CoreThreads cores;

    private final String clientId;

    private final SessionStore store;

    private final SessionCodec codec;

    // Wakes the actor to take the results; null once closed.
    private @Nullable Runnable ready;

    // Called without the monitor once the worker has exited, even after close.
    private final Runnable onIdle;

    private final ArrayDeque<Request> requests = new ArrayDeque<>();

    private final ArrayDeque<Outcome> outcomes = new ArrayDeque<>();

    private @Nullable Thread worker;

    // Whether a transport of the client may still send, which keeps the store held.
    private final BooleanSupplier transportsOpen;

    // Whether the client owns the session, as the latest claim, check, or save left it; never
    // once the keeper has closed.
    private boolean owned;

    // The worker's own. The record version that this client object's latest claim gave the
    // record, which each check looks for and its next save replaces, and the version that its
    // latest save gave it, which tells its own save from another client object's; 0 before
    // either.
    private long claimedVersion;

    private long savedVersion;

    SessionKeeper(
            String threadName,
            CoreThreads cores,
            String clientId,
            SessionStore store,
            SessionCodec codec,
            Runnable ready,
            Runnable onIdle,
            BooleanSupplier transportsOpen
    ) {
        this.threadName = threadName;
        this.cores = cores;
        this.clientId = clientId;
        this.store = store;
        this.codec = codec;
        this.ready = ready;
        this.onIdle = onIdle;
        this.transportsOpen = transportsOpen;
    }

    synchronized void claim() {
        request(new Request.Claim());
    }

    synchronized void check(long connection) {
        request(new Request.Check(connection));
    }

    synchronized void save(SessionState state, List<SessionCore.Completion> disconnections) {
        request(new Request.Save(state, disconnections));
    }

    /// Returns the results that wait, in the order of their requests, once.
    synchronized List<Outcome> take() {
        List<Outcome> taken = List.copyOf(outcomes);
        outcomes.clear();
        return taken;
    }

    @Override
    public synchronized void close() {
        ready = null;
        requests.clear();
        outcomes.clear();
        owned = false;
        releaseIfDue();
    }

    /// None of the client's transports is open any longer, so a store that the client no longer
    /// needs can go, at once or when the store call that runs has returned.
    synchronized void transportsClosed() {
        releaseIfDue();
    }

    /// No store call is running.
    synchronized boolean idle() {
        return worker == null;
    }

    // Caller holds this monitor.
    private void request(Request request) {
        if (ready == null) {
            return;
        }
        requests.addLast(request);
        if (worker == null) {
            worker = cores.builder().name(threadName).start(this::run);
        }
    }

    private void run() {
        while (true) {
            Request request;
            synchronized (this) {
                request = requests.pollFirst();
                if (request == null) {
                    worker = null;
                    releaseIfDue();
                    break;
                }
            }
            Outcome outcome = switch (request) {
                case Request.Claim _ -> claimRecord();
                case Request.Check check -> checkRecord(check.connection());
                case Request.Save save -> saveState(save.state(), save.disconnections());
            };
            synchronized (this) {
                owned = ready != null && ownedAfter(outcome, owned);
            }
            complete(outcome);
        }
        onIdle.run();
    }

    /// Whether the client owns the session after `outcome`, which found it owned when `owned`.
    private static boolean ownedAfter(Outcome outcome, boolean owned) {
        return switch (outcome) {
            case Outcome.Claimed claimed -> claimed.failure() == null;
            case Outcome.Refused _ -> owned;
            // A check that fails keeps the claim, which the next connection checks again.
            case Outcome.Checked checked -> owned && (checked.held() || checked.failure() != null);
            case Outcome.Saved _ -> false;
        };
    }

    /// Gives the store up when the client no longer owns the session, no store call runs, and
    /// none of the client's transports is open. Caller holds this monitor.
    private void releaseIfDue() {
        if (!owned && worker == null && !transportsOpen.getAsBoolean()) {
            store.release();
        }
    }

    /// Holds the store, marks the stored record as this client object's, and returns what the
    /// record held.
    private Outcome claimRecord() {
        try {
            if (!store.hold()) {
                return new Outcome.Refused();
            }
            Optional<SessionStore.Stored> stored = store.read();
            long version = stored.map(SessionStore.Stored::version).orElse(0L);
            long next = nextVersion();
            if (!store.replace(version, next, codec.owned())) {
                // Only a writer that ignores the hold replaces the record meanwhile.
                return new Outcome.Claimed(
                        null,
                        false,
                        new IOException("The stored session changed during the claim")
                );
            }
            claimedVersion = next;
            if (version != 0 && version == savedVersion) {
                return new Outcome.Claimed(null, true, null);
            }
            SessionState state = stored
                    .flatMap(record -> codec.decode(record.content()))
                    .orElse(null);
            return new Outcome.Claimed(state, false, null);
        } catch (IOException | RuntimeException failure) {
            return new Outcome.Claimed(null, false, failure);
        }
    }

    /// Tells whether the stored record still carries this client object's latest claim, for the
    /// connection numbered `connection`. When it no longer does, only a writer that ignores the
    /// hold can have taken the session over.
    private Outcome.Checked checkRecord(long connection) {
        try {
            long version = store.read().map(SessionStore.Stored::version).orElse(0L);
            return new Outcome.Checked(connection, version == claimedVersion, null);
        } catch (IOException | RuntimeException failure) {
            return new Outcome.Checked(connection, false, failure);
        }
    }

    /// Replaces the record that this client object claimed with `state`, unless a writer that
    /// ignores the hold has replaced it since. The client owns the session no more, and its next
    /// connection claims it again.
    private Outcome.Saved saveState(
            SessionState state,
            List<SessionCore.Completion> disconnections
    ) {
        try {
            long next = nextVersion();
            if (store.replace(claimedVersion, next, codec.encode(state))) {
                savedVersion = next;
            } else {
                log.warn("Did not save the session for {}: another client object claimed it",
                        clientId);
            }
        } catch (IOException | RuntimeException failure) {
            log.warn("Could not save the session for {}", clientId, failure);
        }
        return new Outcome.Saved(disconnections);
    }

    /// A new version for the record. A random one almost surely differs from every earlier
    /// version, even one that the record had before someone deleted it or it was found corrupt,
    /// so that a claim, check, or save never takes another client object's record for the one
    /// that it wrote last.
    private static long nextVersion() {
        long version;
        do {
            version = versions.nextLong();
        } while (version == 0);
        return version;
    }

    /// Keeps `outcome` for the actor and wakes it, unless the keeper was closed meanwhile. A
    /// separate frame ends before the next call, so the worker's stack keeps no reference.
    private void complete(Outcome outcome) {
        Runnable wake;
        synchronized (this) {
            if (ready == null) {
                return;
            }
            outcomes.addLast(outcome);
            wake = ready;
        }
        try {
            wake.run();
        } catch (Throwable cause) {
            // Keep the single worker; the attempt's deadline reports a lost claim.
            log.warn("Session store result notification failed", cause);
        }
    }
}
