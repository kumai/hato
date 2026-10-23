package org.mqtt4j.hato.comparison;

/// When each message of a run is due. Messages are numbered from 0, and times count in
/// microseconds from the moment sending starts.
sealed interface Schedule {

    /// Every message at once: each goes out as soon as the connection takes it.
    record Flood() implements Schedule {}

    /// `rate` messages per second: message `n` is due `n / rate` seconds after the start.
    record Steady(long rate) implements Schedule {

        public Steady {
            if (rate < 1) {
                throw new IllegalArgumentException("Rate must be positive: " + rate);
            }
        }
    }

    /// A steady `rate` that `size` extra messages interrupt after the first `after` messages, the
    /// way a device that reconnects sends what it held back: the extra messages are all due
    /// together, when message `after` would have been, and the steady messages then go on as if
    /// the burst had not happened.
    record Burst(long rate, long after, long size) implements Schedule {

        public Burst {
            if (rate < 1 || after < 0 || size < 0) {
                throw new IllegalArgumentException(
                        "Rate must be positive, and the burst not negative: "
                                + rate + ", " + after + ", " + size
                );
            }
        }
    }

    /// Whether messages wait until they are due, rather than all being due at once.
    default boolean paced() {
        return !(this instanceof Flood);
    }

    /// How many messages are due `elapsedMicros` after the start: those numbered below the
    /// result.
    default long dueBy(long elapsedMicros) {
        return switch (this) {
            case Flood _ -> Long.MAX_VALUE;
            case Steady(long rate) -> elapsedMicros * rate / 1_000_000 + 1;
            case Burst(long rate, long after, long size) -> {
                long steady = elapsedMicros * rate / 1_000_000 + 1;
                yield steady <= after ? steady : steady + size;
            }
        };
    }

    /// When message `sequence` is due, in microseconds after the start; 0 for a flood. The time
    /// rounds up to the first whole microsecond at which [#dueBy] counts the message.
    default long dueMicros(long sequence) {
        return switch (this) {
            case Flood _ -> 0;
            case Steady(long rate) -> ceilingMicros(sequence, rate);
            case Burst(long rate, long after, long size) -> {
                // The steady message that this one stands in for: the burst shares one.
                long steady = sequence < after ? sequence : Math.max(after, sequence - size);
                yield ceilingMicros(steady, rate);
            }
        };
    }

    /// `messages / rate` seconds in microseconds, rounded up.
    private static long ceilingMicros(long messages, long rate) {
        return (messages * 1_000_000 + rate - 1) / rate;
    }
}
