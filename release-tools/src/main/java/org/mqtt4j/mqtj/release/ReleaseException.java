package org.mqtt4j.mqtj.release;

import java.io.Serial;

/// A release step failed for a reason the maintainer can act on. The message says what to do.
final class ReleaseException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    ReleaseException(String message) {
        super(message);
    }
}
