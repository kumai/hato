package org.mqtt4j.hato.client.internal;

import java.util.concurrent.Executor;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttMessage;

import static java.util.Objects.requireNonNull;

/// How a push subscription's handler runs, as the application's delivery options chose.
public sealed interface HandlerExecution {

    /// The subscription's dispatcher runs each handler in place, on its internal virtual thread.
    record Internal() implements HandlerExecution {}

    /// Each handler runs on the application's executor, one at a time in receive order.
    record OnExecutor(Executor executor) implements HandlerExecution {

        public OnExecutor {
            requireNonNull(executor, "executor");
        }
    }

    /// Each handler runs on the executor that `selector` returns for its message, one at a time
    /// in receive order per executor.
    record Lanes(Function<MqttMessage, Executor> selector) implements HandlerExecution {

        public Lanes {
            requireNonNull(selector, "selector");
        }
    }

    /// Deliveries with equal keys run one at a time in receive order, on one of `stripes` fixed
    /// stripes that the key's hash chooses. A stripe runs on `executor`, or on an internal
    /// virtual thread while it has deliveries when `executor` is null.
    record Striped(
            int stripes,
            Function<MqttMessage, ?> key,
            @Nullable Executor executor
    ) implements HandlerExecution {

        /// Bounds the handlers that one subscription runs at once. With the client's 256
        /// subscriptions, at most 65,536 handlers run at once.
        public static final int MAX_STRIPES = 256;

        public Striped {
            if (stripes < 1 || stripes > MAX_STRIPES) {
                throw new IllegalArgumentException(
                        "Stripes must be from 1 to " + MAX_STRIPES + ": " + stripes
                );
            }
            requireNonNull(key, "key");
        }
    }
}
