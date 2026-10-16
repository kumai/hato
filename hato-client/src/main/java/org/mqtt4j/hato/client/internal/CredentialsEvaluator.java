package org.mqtt4j.hato.client.internal;

import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.MqttCredentials;
import org.mqtt4j.hato.client.internal.common.log.Log;

import static java.util.Objects.requireNonNull;

/// Serializes application calls even when an expired supplier ignores interruption.
/// Only the latest pending attempt is retained; its connection deadline still applies while
/// an earlier call is blocked. No new worker starts until that call returns.
///
/// The latest current result waits here until the actor takes it. Closing drops the result
/// and every reference to the client, so a call that never returns cannot keep it reachable.
final class CredentialsEvaluator implements AutoCloseable {

    record Result(
            ConnectionAttempt attempt,
            @Nullable MqttCredentials value,
            @Nullable Throwable failure
    ) {}

    private static final Log log = Log.of(CredentialsEvaluator.class);

    private final String threadName;
    private final Supplier<MqttCredentials> supplier;

    // Wakes the actor to take the result; null once closed.
    private @Nullable Runnable ready;

    private @Nullable ConnectionAttempt requested;

    // Held here rather than on the worker's stack, so closing also releases it.
    private @Nullable ConnectionAttempt evaluating;

    private @Nullable Result completed;

    private @Nullable Thread worker;

    CredentialsEvaluator(String threadName, Supplier<MqttCredentials> supplier, Runnable ready) {
        this.threadName = threadName;
        this.supplier = supplier;
        this.ready = ready;
    }

    synchronized void evaluate(ConnectionAttempt attempt) {
        if (ready == null) {
            return;
        }
        requested = attempt;
        if (worker == null) {
            worker = Thread.ofVirtual().name(threadName).start(this::run);
        }
    }

    synchronized void cancel(ConnectionAttempt attempt) {
        if (requested == attempt) {
            requested = null;
            if (worker != null) {
                worker.interrupt();
            }
        }
    }

    /// Returns the latest result of a still requested attempt once.
    synchronized @Nullable Result take() {
        Result result = completed;
        completed = null;
        return result;
    }

    @Override
    public synchronized void close() {
        ready = null;
        requested = null;
        evaluating = null;
        completed = null;
        if (worker != null) {
            worker.interrupt();
        }
    }

    private void run() {
        while (true) {
            synchronized (this) {
                if (requested == null) {
                    worker = null;
                    return;
                }
                evaluating = requested;
                // Interrupt status left by an abandoned call must not reach the next one.
                Thread.interrupted();
            }
            MqttCredentials value = null;
            Throwable failure = null;
            try {
                value = requireNonNull(supplier.get(), "Credentials supplier returned null");
            } catch (Throwable cause) {
                failure = cause;
            }
            complete(value, failure);
        }
    }

    /// Keeps the result of a call whose attempt is still requested and wakes the actor. A
    /// separate frame ends before the next call, so the worker's stack keeps no reference.
    private void complete(@Nullable MqttCredentials value, @Nullable Throwable failure) {
        Runnable wake;
        synchronized (this) {
            ConnectionAttempt attempt = evaluating;
            evaluating = null;
            if (attempt == null || requested != attempt) {
                return;
            }
            requested = null;
            completed = new Result(attempt, value, failure);
            wake = requireNonNull(ready);
        }
        try {
            wake.run();
        } catch (Throwable cause) {
            // Keep the single worker; the attempt's deadline reports the lost wake.
            log.warn("Credentials result notification failed", cause);
        }
    }
}
