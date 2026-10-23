package org.mqtt4j.hato.client.internal;

import java.util.concurrent.locks.LockSupport;

/// Lets a virtual thread that works without blocking share its carrier.
///
/// Virtual threads are not preempted, so a busy one keeps its carrier until it blocks. With a
/// single carrier, the other threads of the client, its timers, and the work that application
/// threads hand over would wait out a whole receive backlog, and a PINGREQ queued in time would
/// not be written in time. Parking after a slice of continuous work lets them run;
/// Thread.yield() alone did not when measured with one carrier, because a yielding thread stays
/// in its carrier's own queue, which the JDK's virtual-thread I/O pollers also keep busy. With 250
/// microseconds, a one-carrier burst of small publications stayed within the default queue of a
/// fast handler; with one millisecond, it overflowed. Real time measures carrier use, which
/// TimerScheduler time cannot express.
final class CarrierShare {

    private static final long SLICE_NANOS = 250_000;

    private CarrierShare() {
    }

    /// Whether a thread that has worked for `workedNanos` since it last shared its carrier has
    /// used up its slice and should let others run. The thread counts only its work, such as
    /// handling what a socket read returned: waiting frees the carrier anyway.
    static boolean spent(long workedNanos) {
        return workedNanos >= SLICE_NANOS;
    }

    /// Parks for a moment, which lets the other virtual threads run.
    static void share() {
        LockSupport.parkNanos(1);
    }
}
