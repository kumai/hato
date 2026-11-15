package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.SplittableRandom;
import java.util.random.RandomGenerator;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// Actor-owned Full Jitter sequence. Only a stable connection resets the ceiling.
/// The builder creates one per client from its validated reconnect policy.
public final class ReconnectBackoff {

    // Nanoseconds; zero when the client never reconnects.
    private final long initial;

    private final long maximum;

    private final @Nullable RandomGenerator random;

    private long ceiling;

    ReconnectBackoff(Duration initial, Duration maximum, RandomGenerator random) {
        this.initial = initial.toNanos();
        this.maximum = maximum.toNanos();
        this.random = requireNonNull(random);
        ceiling = this.initial;
    }

    private ReconnectBackoff() {
        initial = 0;
        maximum = 0;
        random = null;
    }

    /// Never reconnects.
    public static ReconnectBackoff disabled() {
        return new ReconnectBackoff();
    }

    /// Doubles the ceiling from `initial` up to `maximum`, which the policy has validated, and
    /// draws each delay from a random source of this client. The source is created here, at
    /// run time, so a native image cannot bake its seed in.
    public static ReconnectBackoff exponential(Duration initial, Duration maximum) {
        return new ReconnectBackoff(initial, maximum, new SplittableRandom());
    }

    boolean enabled() {
        return initial != 0;
    }

    Duration next() {
        long delay = requireNonNull(random, "Reconnection is disabled").nextLong(ceiling + 1);
        ceiling = ceiling >= maximum - ceiling ? maximum : ceiling * 2;
        return Duration.ofNanos(delay);
    }

    void reset() {
        ceiling = initial;
    }
}
