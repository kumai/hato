package org.mqtt4j.hato.broker.internal;

import java.util.concurrent.locks.LockSupport;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.BrokerListener;
import org.mqtt4j.hato.internal.log.Log;

// Tells the application's listener of a failure that stopped the broker, on a thread of its own.
// The notice waits for no other call of the listener, such as a report of dropped deliveries
// that takes its time, and the broker ends meanwhile without waiting for the notice.
//
// The thread runs from the start and waits, so that a failure that comes when the process lacks
// what a new thread takes, as it may after running out of memory, still has one to tell it. It
// ends with the broker, telling nothing if the application closed the broker first.
final class FailureNotice {

    private static final Log log = Log.of(FailureNotice.class);

    private final BrokerListener listener;

    // The port that the broker listens on, for the thread's name and logs.
    private final int port;

    // Where the thread reports its end.
    private final Termination termination;

    private final Thread thread;

    // The failure that stopped the broker, set before the thread is let go.
    private volatile @Nullable Throwable failure;

    private volatile boolean ended;

    FailureNotice(BrokerListener listener, int port, Termination termination) {
        this.listener = listener;
        this.port = port;
        this.termination = termination;
        thread = Thread.ofPlatform().daemon()
                .name("hato-broker-" + port + "-failure")
                .unstarted(this::run);
    }

    void start() {
        thread.start();
    }

    // Lets the thread tell the listener of `failure`, which stopped the broker.
    void fail(Throwable failure) {
        this.failure = failure;
        end();
    }

    // Lets the thread end, after the notice of a failure if fail() came first.
    void end() {
        ended = true;
        LockSupport.unpark(thread);
    }

    private void run() {
        try {
            while (!ended) {
                LockSupport.park(this);
                // An interrupted thread no longer parks, and this one has nothing to stop for.
                boolean _ = Thread.interrupted();
            }
            Throwable stopped = failure;
            if (stopped != null) {
                tell(stopped);
            }
        } finally {
            termination.partEnded();
        }
    }

    private void tell(Throwable stopped) {
        try {
            listener.onFailed(stopped);
        } catch (Throwable listenerFailure) {
            // The listener's failure is the application's, and the broker has ended anyway.
            log.warn("Broker on port {} could not tell its listener that it failed",
                    port, listenerFailure);
        }
    }
}
