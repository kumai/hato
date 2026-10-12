package org.mqtt4j.hato.client.internal;

import java.io.Serial;

/// Unwinds a core command before the actor ends the affected connection.
/// Only exceptions caught at SPI calls use this path; internal failures still stop the actor.
final class SpiException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final long generation;

    SpiException(long generation, RuntimeException cause) {
        super(cause);
        this.generation = generation;
    }

    long generation() {
        return generation;
    }
}
