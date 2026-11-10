package org.mqtt4j.hato.client.internal;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;

/// The kind of thread that runs the core of one client: its actor, its sender, the thread that
/// completes its operations and observations, and the thread that connects its TCP or TLS
/// transport and then reads it. A core on platform threads keeps the client answering however
/// the application uses virtual threads, and the JVM spares places for a limited number of such
/// clients at once.
///
/// Virtual threads are not preempted: one that computes keeps its carrier until it blocks. An
/// application that computes on virtual threads of its own, as a server that serves each request
/// on one does, can keep every carrier busy, and a core that waits for a carrier then stops. With
/// two processors kept three quarters busy so, each publication waited about 130 ms for its
/// result; with more such work than the processors could do, the client sent no PINGREQ and lost
/// its connection, and the future of stats() waited until that work ended, as it did for good
/// while class initializers that waited for it held every carrier. A core on platform threads,
/// which the operating system runs beside the carriers, answered within a few milliseconds in
/// these cases, and a handler that computed on the client's own virtual threads no longer held
/// up its keep-alive. Without such work, both kinds cost about the same per message, and with
/// processors that other processes keep busy, a core on platform threads published QoS 1
/// messages five times as fast.
///
/// A core on platform threads holds up to four threads of its own; a continuation that holds the
/// thread that completes operations hands them to a virtual thread, as Completions describes.
/// Their stacks took about half a megabyte on Linux, which a thousand clients in one JVM, as a
/// device simulator runs, would multiply. So the JVM spares [#PLATFORM_CORES] places. A client
/// takes one, if one is free, as it is built, and gives it back as its actor ends, after it has
/// closed its transports and stopped its sender; a thread that completes operations ends once it
/// has been idle for a while. A client that finds no free place runs its core on virtual threads,
/// which share their carriers as CarrierShare describes and read as PlatformReaders describes.
///
/// The build's single-carrier suite sets hato.client.platformCores to 0, so that it tests cores
/// on virtual threads on one carrier; the property is not a supported setting.
public final class CoreThreads {

    /// How many clients the JVM runs with a core on platform threads at once, unless the internal
    /// property says otherwise.
    static final int PLATFORM_CORES = 64;

    private static final Places places = new Places(
            Integer.getInteger("hato.client.platformCores", PLATFORM_CORES)
    );

    // The places that this core took one of, or null for a core on virtual threads.
    private final @Nullable Places taken;

    private final AtomicBoolean givenBack = new AtomicBoolean();

    private CoreThreads(@Nullable Places taken) {
        this.taken = taken;
    }

    /// The core of a new client: on platform threads while the JVM has a place free, which the
    /// client then holds until [#giveBack()], and otherwise on virtual threads.
    public static CoreThreads take() {
        return places.take();
    }

    /// A core on virtual threads, which holds no place.
    public static CoreThreads virtual() {
        return new CoreThreads(null);
    }

    /// Whether the core runs on platform threads.
    public boolean platform() {
        return taken != null;
    }

    /// A builder for one thread of the core: a daemon platform thread, or a virtual thread.
    Thread.Builder builder() {
        return platform() ? Thread.ofPlatform().daemon() : Thread.ofVirtual();
    }

    /// Gives the place back once the core has ended, so that another client can take it. Only the
    /// first call gives it back, and a core on virtual threads has none to give.
    public void giveBack() {
        Places owner = taken;
        if (owner != null && givenBack.compareAndSet(false, true)) {
            owner.free.release();
        }
    }

    /// Places for cores on platform threads.
    static final class Places {

        private final Semaphore free;

        Places(int count) {
            free = new Semaphore(count);
        }

        /// A core on platform threads that holds one of these places, while one is free, and
        /// otherwise a core on virtual threads.
        CoreThreads take() {
            return new CoreThreads(free.tryAcquire() ? this : null);
        }
    }
}
