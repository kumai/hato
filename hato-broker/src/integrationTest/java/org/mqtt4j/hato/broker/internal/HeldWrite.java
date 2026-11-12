package org.mqtt4j.hato.broker.internal;

import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/// Keeps back the first write that carries a delivery after [#hold()], until [#release()]. The
/// write has taken its sends from the connection's queue, where they keep their room. Only the
/// writes of that connection wait: its selector goes on and reads the connection's packets too,
/// as it does for a peer that sends while it takes no more bytes.
final class HeldWrite implements Predicate<Connection> {

    private final CountDownLatch reached = new CountDownLatch(1);

    // The selectors and the test take this object's monitor for what follows.

    private boolean holding;

    private boolean released;

    private @Nullable Connection held;

    @Override
    public synchronized boolean test(Connection connection) {
        if (!holding || released) {
            return false;
        }
        if (held == null) {
            held = connection;
            reached.countDown();
        }
        return held == connection;
    }

    synchronized void hold() {
        holding = true;
    }

    void awaitHeld() throws InterruptedException {
        assertThat(reached.await(5, SECONDS)).as("a delivery came to its write").isTrue();
    }

    synchronized void release() {
        released = true;
        if (held != null) {
            held.wakeNow();
        }
    }
}
