package org.mqtt4j.hato;

import java.util.concurrent.Executor;

import org.mqtt4j.hato.client.internal.HandlerExecution;

import static java.util.Objects.requireNonNull;

/// Where and how a push subscription runs its handler, for
/// [MqttSubscriber#subscribe(Subscribe, DeliveryOptions, java.util.function.Consumer)].
/// Instances are immutable and can be shared between subscriptions; they do not define equality.
///
/// [#serial()] runs one delivery at a time, in receive order, on an internal virtual thread, as
/// [MqttSubscriber#subscribe(Subscribe, java.util.function.Consumer)] does.
/// [#withExecutor(Executor)] keeps that order but runs each handler on the application's
/// executor.
///
/// An executor that runs handlers receives one task per subscription at a time. The task runs
/// one delivery's handler and, while further deliveries wait, asks the executor again, so the
/// executor's queue holds at most one task per subscription, and waiting deliveries count
/// against [Subscribe#bufferCapacity()] as with an internal thread. While no delivery waits, the
/// subscription holds none of the executor's threads. The executor must run each task it
/// accepts, or throw from [Executor#execute(Runnable)]: an exception of any type ends the
/// delivery that the task would have run as a handler failure, with the consequences that
/// [MqttSubscriber#subscribe(Subscribe, java.util.function.Consumer)] describes. An executor that
/// silently discards a task leaves the subscription waiting until it closes. An executor that
/// runs tasks on the calling thread, such as a direct executor or one with a caller-runs policy,
/// keeps the order and the capacity without deepening the stack.
///
/// The application owns the executor; the client never shuts it down. Once the subscription or
/// the client has closed, a task that the executor still holds does nothing when it runs, so the
/// tasks that [java.util.concurrent.ExecutorService#shutdownNow()] returns need not run, and
/// neither [Subscription#terminated()] nor [MqttClient#terminated()] waits for them. They do wait
/// for a handler still running on the executor.
///
/// Acknowledgement follows the request's [AckMode] wherever the handler runs: [AckMode#ON_RECEIVE]
/// just before the handler starts on its executor, and [AckMode#AFTER_HANDLER] when it returns
/// there.
public final class DeliveryOptions {

    private static final DeliveryOptions SERIAL =
            new DeliveryOptions(new HandlerExecution.Internal());

    private final HandlerExecution execution;

    private DeliveryOptions(HandlerExecution execution) {
        this.execution = execution;
    }

    /// One delivery at a time, in receive order, on an internal virtual thread: the behavior of
    /// [MqttSubscriber#subscribe(Subscribe, java.util.function.Consumer)].
    public static DeliveryOptions serial() {
        return SERIAL;
    }

    /// The same order, one delivery at a time, with each handler running on `executor` instead
    /// of an internal virtual thread. Replaces an executor chosen earlier.
    ///
    /// @throws NullPointerException if `executor` is null
    public DeliveryOptions withExecutor(Executor executor) {
        requireNonNull(executor, "executor");
        return new DeliveryOptions(new HandlerExecution.OnExecutor(executor));
    }

    /// The client reads the choice through its builder in this package, so applications see only
    /// the factories.
    HandlerExecution execution() {
        return execution;
    }
}
