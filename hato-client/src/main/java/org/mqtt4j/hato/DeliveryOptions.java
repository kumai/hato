package org.mqtt4j.hato;

import java.util.concurrent.Executor;
import java.util.function.Function;

import org.mqtt4j.hato.client.internal.HandlerExecution;

import static java.util.Objects.requireNonNull;

/// Where and how a push subscription runs its handler, for
/// [MqttSubscriber#subscribe(Subscribe, DeliveryOptions, java.util.function.Consumer)].
/// Instances are immutable and can be shared between subscriptions; they do not define equality.
///
/// [#serial()] runs one delivery at a time, in receive order, on an internal virtual thread, as
/// [MqttSubscriber#subscribe(Subscribe, java.util.function.Consumer)] does.
/// [#withExecutor(Executor)] keeps that order but runs each handler on the application's
/// executor. [#lanes(Function)] runs each delivery on the executor that the application chooses
/// for its message, its lane: deliveries on the same lane keep that order, and different lanes
/// can run at the same time.
///
/// An executor that runs handlers receives at most one task per subscription and lane at a time.
/// The task runs one delivery's handler and, while further deliveries wait for the same lane,
/// asks the executor again, so the executor's queue holds at most one task per subscription and
/// lane, and waiting deliveries count against [Subscribe#bufferCapacity()] as with an internal
/// thread. While no delivery waits, the subscription holds none of the executor's threads. The
/// executor must run each task it accepts, or throw from [Executor#execute(Runnable)]: an
/// exception of any type ends the delivery that the task would have run as a handler failure,
/// with the consequences that [MqttSubscriber#subscribe(Subscribe, java.util.function.Consumer)]
/// describes. An executor that silently discards a task leaves the subscription waiting until it
/// closes. An executor that runs tasks on the calling thread, such as a direct executor or one
/// with a caller-runs policy, keeps the order and the capacity without deepening the stack.
///
/// A handler failure on any lane stops the subscription's other lanes too. Failures on other
/// lanes that were already running are logged, and the first failure remains the cause that the
/// client reports.
///
/// The application owns the executor; the client never shuts it down. Once the subscription or
/// the client has closed, a task that the executor still holds does nothing when it runs, so the
/// tasks that [java.util.concurrent.ExecutorService#shutdownNow()] returns need not run, and
/// neither [Subscription#terminated()] nor [MqttClient#terminated()] waits for them. They do wait
/// for a handler still running on the executor.
///
/// Acknowledgement follows the request's [AckMode] wherever the handler runs: [AckMode#ON_RECEIVE]
/// just before the handler starts on its executor, and [AckMode#AFTER_HANDLER] when it returns
/// there. PUBACK and PUBREC still follow receive order across lanes, so a slow lane holds back
/// the acknowledgement of later deliveries on other lanes; see [AckMode].
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

    /// Runs each delivery's handler on the executor that `selector` returns for its message: the
    /// delivery's lane. Deliveries on the same lane run one at a time in receive order. Lanes are
    /// told apart by identity, so return the same executor for the same lane; a new executor
    /// object for every message gives every message a lane of its own.
    ///
    /// The selector runs on an internal thread, once for each delivery in receive order. It
    /// receives the message rather than the delivery, so it cannot acknowledge it. An exception
    /// or a null result ends that delivery as a handler failure.
    ///
    /// @throws NullPointerException if `selector` is null
    public static DeliveryOptions lanes(Function<MqttMessage, Executor> selector) {
        requireNonNull(selector, "selector");
        return new DeliveryOptions(new HandlerExecution.Lanes(selector));
    }

    /// The same order, one delivery at a time, with each handler running on `executor` instead
    /// of an internal virtual thread. Replaces an executor chosen earlier.
    ///
    /// @throws NullPointerException if `executor` is null
    /// @throws IllegalArgumentException if these options are [#lanes(Function)], whose selector
    ///         chooses the executors
    public DeliveryOptions withExecutor(Executor executor) {
        requireNonNull(executor, "executor");
        return switch (execution) {
            case HandlerExecution.Internal _, HandlerExecution.OnExecutor _ ->
                    new DeliveryOptions(new HandlerExecution.OnExecutor(executor));
            case HandlerExecution.Lanes _ -> throw new IllegalArgumentException(
                    "Lanes take their executors from the selector"
            );
        };
    }

    /// The client reads the choice through its builder in this package, so applications see only
    /// the factories.
    HandlerExecution execution() {
        return execution;
    }
}
