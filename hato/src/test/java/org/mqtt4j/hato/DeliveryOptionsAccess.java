package org.mqtt4j.hato;

import org.mqtt4j.hato.client.internal.HandlerExecution;

/// Reads the handler execution that delivery options chose, as the builder lets the client do,
/// for tests that construct the client directly.
public final class DeliveryOptionsAccess {

    private DeliveryOptionsAccess() {
    }

    public static HandlerExecution execution(DeliveryOptions options) {
        return options.execution();
    }
}
