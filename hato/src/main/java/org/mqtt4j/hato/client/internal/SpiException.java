package org.mqtt4j.hato.client.internal;

import java.io.Serial;

/// Unwinds a core command before the actor ends the affected connection.
/// Only exceptions caught at SPI calls use this path; internal failures still stop the actor.
final class SpiException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final long generation;

    private final boolean schedulerFailure;

    SpiException(long generation, RuntimeException cause) {
        this(generation, cause, false);
    }

    private SpiException(long generation, RuntimeException cause, boolean schedulerFailure) {
        super(cause);
        this.generation = generation;
        this.schedulerFailure = schedulerFailure;
    }

    static SpiException scheduler(long generation, RuntimeException cause) {
        return new SpiException(generation, cause, true);
    }

    long generation() {
        return generation;
    }

    boolean schedulerFailure() {
        return schedulerFailure;
    }
}
