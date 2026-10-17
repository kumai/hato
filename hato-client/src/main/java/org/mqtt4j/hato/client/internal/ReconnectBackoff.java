package org.mqtt4j.hato.client.internal;

import java.time.Duration;
import java.util.random.RandomGenerator;

import static java.util.Objects.requireNonNull;

/// Actor-owned Full Jitter sequence. Only a stable connection resets the ceiling.
final class ReconnectBackoff {

    private static final Duration MAX_DELAY = Duration.ofMillis(Integer.MAX_VALUE);

    private final long initial;

    private final long maximum;

    private final RandomGenerator random;

    private long ceiling;

    private ReconnectBackoff(RandomGenerator random) {
        initial = 0;
        maximum = 0;
        this.random = requireNonNull(random);
    }

    static ReconnectBackoff disabled(RandomGenerator random) {
        return new ReconnectBackoff(random);
    }

    boolean enabled() {
        return initial != 0;
    }

    ReconnectBackoff(Duration initial, Duration maximum, RandomGenerator random) {
        if (!initial.isPositive()
                || maximum.compareTo(initial) < 0
                || maximum.compareTo(MAX_DELAY) > 0) {
            throw new IllegalArgumentException("Invalid reconnect delay range");
        }
        this.initial = initial.toNanos();
        this.maximum = maximum.toNanos();
        this.random = requireNonNull(random);
        ceiling = this.initial;
    }

    Duration next() {
        long delay = random.nextLong(ceiling + 1);
        ceiling = ceiling >= maximum - ceiling ? maximum : ceiling * 2;
        return Duration.ofNanos(delay);
    }

    void reset() {
        ceiling = initial;
    }
}
