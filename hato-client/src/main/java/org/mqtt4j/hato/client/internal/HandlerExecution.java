package org.mqtt4j.hato.client.internal;

import java.util.concurrent.Executor;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttMessage;

import static java.util.Objects.requireNonNull;

/// How a push subscription's handler runs, as the application's delivery options chose.
public sealed interface HandlerExecution {

    /// Bounds the handlers that one subscription runs at once, as the stripes of [Striped] or the
    /// parallelism of [Parallel]. With the client's 256 subscriptions, at most 65,536 handlers
    /// run at once.
    int MAX_PARALLELISM = 256;

    /// The subscription's dispatcher runs each handler in place, on its internal virtual thread.
    record Internal() implements HandlerExecution {}

    /// As [Internal], except that the thread that reads the connection may run a handler itself,
    /// right after the read that brought its delivery, while the dispatcher sleeps. The handler
    /// then holds up the connection's reading until it returns.
    record Inline() implements HandlerExecution {}

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

        public Striped {
            checkParallelism("Stripes", stripes);
            requireNonNull(key, "key");
        }
    }

    /// Up to `parallelism` handlers run at once, taking the deliveries in receive order from one
    /// queue, so they can finish in any order. They run on `executor`, or, when it is null, on
    /// internal virtual threads while deliveries wait.
    record Parallel(int parallelism, @Nullable Executor executor) implements HandlerExecution {

        public Parallel {
            checkParallelism("Parallelism", parallelism);
        }
    }

    private static void checkParallelism(String name, int value) {
        if (value < 1 || value > MAX_PARALLELISM) {
            throw new IllegalArgumentException(
                    name + " must be from 1 to " + MAX_PARALLELISM + ": " + value
            );
        }
    }
}
