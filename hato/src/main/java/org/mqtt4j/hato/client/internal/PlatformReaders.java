package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.Transport;

/// The platform threads that the JVM spares for reading connections, which every TCP transport
/// shares, and the rules by which they move between connections.
///
/// A virtual thread that waits for its socket is woken by the JDK's poller, a hand-off that each
/// read pays when it arrives after a pause. On Linux, where the poller runs on virtual threads
/// too, a connection receiving 20,000 messages a second spent 40 to 60 percent less CPU per
/// message when a platform thread read it, blocking in the read itself. A platform thread per
/// connection does not scale, though: with a thousand of them, a message to every connection
/// took 14 to 50 times as long to reach them all. So as many connections as the JVM has
/// processors read on platform threads of their own, and the others on virtual threads; a shared
/// event loop would serve every connection without either cost.
///
/// A connection takes a free platform thread once connected, busy or not, so the connections
/// that come up first take them all. The threads then move: a connection that reads busily on a
/// virtual thread takes one as one comes free and asks for one while none is, and a connection
/// that reads little on one gives it back while more connections ask than find one free. A
/// platform reader reads with a timeout, so that it also looks up from a connection that receives
/// nothing; the timeout is no failure of the connection. The reading moves only between reads,
/// once the client has handled what the last one returned.
///
/// On Linux with one or two processors, a platform reader cut the CPU per message of a
/// connection whose handlers ran on threads of their own by 7 to 24 percent at 1,000 to 5,000
/// messages a second, and added 7 to 9 percent at 20,000; for handlers that run as part of
/// receiving, on the reading thread, it cut 23 to 55 percent at every rate. There, only a
/// connection whose handlers run so asks for one, and the others read where they started.
///
/// The build's single-carrier suite sets hato.client.platformReaders to 0, so that it tests
/// virtual readers on one carrier; the property is not a supported setting.
final class PlatformReaders {

    /// How the platform readers move. Each reader counts its reads over `window`: a connection
    /// that reads on a virtual thread and reads at least `busyReads` times in it asks for a
    /// platform thread, and one that reads on a platform thread and reads fewer than
    /// `quietReads` times gives it up to one that asks. A platform reader reads with
    /// `readTimeout`. With `everyConnection`, every busy connection asks; otherwise only one
    /// whose handlers run as part of receiving.
    record Moves(
            Duration window,
            int busyReads,
            int quietReads,
            Duration readTimeout,
            boolean everyConnection
    ) {}

    // A read that a platform thread makes in place of a virtual one saves some tens of
    // microseconds, so at a hundred reads a second a connection saves milliseconds of CPU each
    // second, and soon makes up for the start of a thread. A connection that reads a quarter of
    // that keeps its thread, so that one near either bound does not move back and forth. The
    // reads are counted over ten seconds, so that the threads go to connections that stay busy,
    // and a connection that asks waits ten to twenty seconds. Counted over one second, they
    // followed connections that took turns at being busy every two seconds, arrived about as
    // each fell quiet, and cost more CPU than they saved. An idle platform reader looks up every
    // five seconds, which ends its window at most that late and seldom wakes it.
    private static final Duration WINDOW = Duration.ofSeconds(10);

    private static final int BUSY_READS = 1_000;

    private static final int QUIET_READS = 250;

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    private final Semaphore slots;
    private final @Nullable Moves moves;
    private final long windowNanos;
    // The connections that read on virtual threads, would take a platform thread, and found none
    // free.
    private final AtomicInteger asks = new AtomicInteger();

    /// `slots` platform readers that stay with the connections that take them.
    PlatformReaders(int slots) {
        this(slots, null);
    }

    /// `slots` platform readers that move as `moves` says, or stay where `moves` is null.
    PlatformReaders(int slots, @Nullable Moves moves) {
        this.slots = new Semaphore(slots);
        this.moves = moves;
        windowNanos = moves == null ? 0 : moves.window().toNanos();
    }

    /// The JVM's platform readers: as many as it has processors, unless the internal property
    /// says otherwise.
    static PlatformReaders forThisJvm() {
        int processors = Runtime.getRuntime().availableProcessors();
        int slots = Integer.getInteger("hato.client.platformReaders", processors);
        return new PlatformReaders(
                slots,
                slots == 0
                        ? null
                        : new Moves(WINDOW, BUSY_READS, QUIET_READS, READ_TIMEOUT, processors > 2)
        );
    }

    /// Takes a free platform reader for one connection; false when none is free.
    boolean take() {
        return slots.tryAcquire();
    }

    /// Gives back a platform reader that [#take()] took.
    void giveBack() {
        slots.release();
    }

    /// A watch on the reads of a connection that `receiver` takes the bytes of, from `now` on,
    /// for the thread that reads it, a platform thread of its own when `platform`; null where
    /// the platform readers do not move.
    @Nullable Watch watch(boolean platform, Transport.Listener receiver, long now) {
        return moves == null ? null : new Watch(moves, platform, receiver, now);
    }

    /// Follows the reads of one connection on the thread that reads it, and tells that thread
    /// when its reading should move to a thread of the other kind. Used by that thread alone.
    ///
    /// The bounds are fixed rather than set against the connections that ask: a connection
    /// that reads a little more than the quiet bound keeps its thread against a busier one.
    /// Comparing them would take each asking connection's count.
    final class Watch {

        private final Moves moves;
        private final boolean platform;
        private final Transport.Listener receiver;
        // When the current count started, and the reads since.
        private long counting;
        private int reads;
        // Whether this connection, read on a virtual thread, would take a platform thread: it read
        // busily in the last full window.
        private boolean busy;
        // Whether this connection counts among the asks.
        private boolean asking;

        private Watch(Moves moves, boolean platform, Transport.Listener receiver, long now) {
            this.moves = moves;
            this.platform = platform;
            this.receiver = receiver;
            counting = now;
        }

        /// The timeout for each read of this thread: a platform reader's, or none.
        Duration readTimeout() {
            return platform ? moves.readTimeout() : Duration.ZERO;
        }

        /// After a read that returned at `now` and whose bytes the client has handled: whether
        /// the reading should now move to a thread of the other kind.
        boolean afterRead(long now) {
            reads++;
            return moveDue(now);
        }

        /// After a read that waited out the timeout at `now`: whether the reading should move.
        boolean afterTimeout(long now) {
            return moveDue(now);
        }

        /// After the reader waited for the client to resume reading, which says nothing about
        /// the traffic: counting starts again at `now`.
        void restart(long now) {
            counting = now;
            reads = 0;
        }

        /// The reading did not move when due at `now`: the transport has closed, another
        /// connection took the free platform thread first, or no thread could start. Counting
        /// starts again, so that the reader tries again a window later at the earliest rather
        /// than at every read.
        void stay(long now) {
            busy = false;
            restart(now);
        }

        /// The reading leaves this thread, or ends: the connection no longer asks from it.
        void stop() {
            if (asking) {
                asking = false;
                asks.decrementAndGet();
            }
        }

        private boolean moveDue(long now) {
            long elapsed = now - counting;
            if (elapsed >= windowNanos) {
                // The reads per window, over what can be a longer time after a silence. Against
                // whole bounds, the quotient rounded down compares as the exact one would.
                long perWindow = reads * windowNanos / elapsed;
                counting = now;
                reads = 0;
                if (platform) {
                    // Only for a connection that would take the thread: one that asks and finds
                    // none free.
                    return perWindow < moves.quietReads() && asks.get() > slots.availablePermits();
                }
                busy = perWindow >= moves.busyReads()
                        && (moves.everyConnection()
                                || (receiver instanceof BorrowingListener lender
                                        && lender.runsHandlersAfterReads()));
                if (!busy) {
                    stop();
                }
            }
            if (!busy) {
                return false;
            }
            if (slots.availablePermits() > 0) {
                // The reader takes it as the reading moves.
                stop();
                return true;
            }
            if (!asking) {
                asking = true;
                asks.incrementAndGet();
            }
            return false;
        }
    }
}
