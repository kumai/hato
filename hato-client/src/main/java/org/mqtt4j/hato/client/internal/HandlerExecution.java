package org.mqtt4j.hato.client.internal;

import java.util.concurrent.Executor;
import java.util.function.Function;

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
}
