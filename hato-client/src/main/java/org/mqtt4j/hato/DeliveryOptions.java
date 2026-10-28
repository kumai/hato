package org.mqtt4j.hato;

import java.util.concurrent.Executor;
import java.util.function.Function;

import org.mqtt4j.hato.client.internal.HandlerExecution;

import static java.util.Objects.requireNonNull;

/// Where and how a push subscription runs its handler, for
/// [MqttSubscriber#subscribe(Subscribe, DeliveryOptions, java.util.function.Consumer)].
/// Instances are immutable and can be shared between subscriptions; they do not define equality.
///
/// [#serial()] runs one delivery at a time, in receive order, on an internal thread, as
/// [MqttSubscriber#subscribe(Subscribe, java.util.function.Consumer)] does.
/// [#withExecutor(Executor)] keeps that order but runs each handler on the application's
/// executor. [#lanes(Function)] runs each delivery on the executor that the application chooses
/// for its message, its lane: deliveries on the same lane keep that order, and different lanes
/// can run at the same time. [#striped(int)] keeps that order for deliveries with equal keys, by
/// default their topics, and runs different keys at the same time on a fixed number of stripes;
/// each stripe is a lane in what follows. [#parallel(int)] runs up to a given number of
/// deliveries at the same time in no particular order, as one lane that runs that many at once.
/// [#inline()] runs one delivery at a time, in receive order, for a handler that returns at
/// once: the client may run it as part of receiving, and the connection waits for it meanwhile.
/// Any other handler on an internal thread may block: meanwhile only its own lane waits.
///
/// An executor that runs handlers receives at most one task per subscription and lane at a time,
/// or, for [#parallel(int)], as many as its parallelism. A task runs one delivery's handler and,
/// while further deliveries wait for the same lane, asks the executor again, so the executor's
/// queue holds no more tasks than that, and waiting deliveries count against
/// [Subscribe#bufferCapacity()] as with an internal thread. While no delivery waits, the
/// subscription holds none of the executor's threads. The
/// executor must run each task it accepts, or throw from [Executor#execute(Runnable)]: an
/// exception of any type ends the delivery that the task would have run as a handler failure
/// under [FailurePolicy#STOP], whatever the subscription's policy, since a refusing executor has
/// processed nothing. An executor that silently discards a task leaves the subscription waiting
/// until it closes. An executor that runs tasks on the calling thread, such as a direct executor
/// or one with a caller-runs policy, keeps the order and the capacity without deepening the
/// stack.
///
/// A failure that stops the client, on any lane, stops the subscription's other lanes too.
/// Failures on other lanes that were already running are logged, and the first failure remains
/// the cause that the client reports.
///
/// The application owns the executor; the client never shuts it down. Once the subscription or
/// the client has closed, a task that the executor still holds does nothing when it runs, so the
/// tasks that [java.util.concurrent.ExecutorService#shutdownNow()] returns need not run, and
/// neither [Subscription#terminated()] nor [MqttClient#terminated()] waits for them. They do wait
/// for a handler still running on the executor.
///
/// Acknowledgement follows the request's [AckMode] wherever the handler runs: [AckMode#ON_RECEIVE]
/// just before the handler starts on its executor, and [AckMode#AFTER_HANDLER] when it returns
/// there. PUBACK and PUBREC still follow receive order, so a slow handler holds back the
/// acknowledgement of later deliveries, on other lanes too; see [AckMode].
public final class DeliveryOptions {

    private static final DeliveryOptions SERIAL =
            new DeliveryOptions(new HandlerExecution.Internal());

    private static final DeliveryOptions INLINE =
            new DeliveryOptions(new HandlerExecution.Inline());

    private final HandlerExecution execution;

    private DeliveryOptions(HandlerExecution execution) {
        this.execution = execution;
    }

    /// One delivery at a time, in receive order, on an internal thread: the behavior of
    /// [MqttSubscriber#subscribe(Subscribe, java.util.function.Consumer)].
    ///
    /// @return
    ///     the options for serial delivery
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
    /// @param selector
    ///     the function that chooses the executor for each message
    /// @return
    ///     the options for the lanes that `selector` chooses
    /// @throws NullPointerException
    ///     if `selector` is null
    public static DeliveryOptions lanes(Function<MqttMessage, Executor> selector) {
        requireNonNull(selector, "selector");
        return new DeliveryOptions(new HandlerExecution.Lanes(selector));
    }

    /// Runs deliveries with equal topics one at a time in receive order, and deliveries with
    /// different topics possibly at the same time, at most `stripes` at once. Each topic's
    /// deliveries go to one of `stripes` fixed stripes that its hash chooses, so different topics
    /// can share a stripe and then wait for each other. A stripe runs on an internal thread while
    /// it has deliveries, or on the executor that [#withExecutor(Executor)] sets.
    ///
    /// @param stripes
    ///     the number of stripes
    /// @return
    ///     the options for `stripes` stripes keyed by topic
    /// @throws IllegalArgumentException
    ///     if `stripes` is less than 1 or greater than 256
    public static DeliveryOptions striped(int stripes) {
        return striped(stripes, MqttMessage::topic);
    }

    /// Like [#striped(int)], with the key that `key` returns for each message in place of its
    /// topic; keys are compared with [equals][Object#equals(Object)] and
    /// [hashCode][Object#hashCode()]. The key function runs on an internal thread, once for each
    /// delivery in receive order. It receives the message rather than the delivery, so it cannot
    /// acknowledge it. An exception or a null result ends that delivery as a handler failure.
    ///
    /// @param stripes
    ///     the number of stripes
    /// @param key
    ///     the function that returns the key of each message
    /// @return
    ///     the options for `stripes` stripes keyed by `key`
    /// @throws IllegalArgumentException
    ///     if `stripes` is less than 1 or greater than 256
    /// @throws NullPointerException
    ///     if `key` is null
    public static DeliveryOptions striped(int stripes, Function<MqttMessage, ?> key) {
        return new DeliveryOptions(new HandlerExecution.Striped(stripes, key, null));
    }

    /// Runs up to `parallelism` deliveries at the same time, in no particular order. Handlers run
    /// on internal threads while deliveries wait, or on the executor that
    /// [#withExecutor(Executor)] sets.
    ///
    /// @param parallelism
    ///     the largest number of deliveries that run at the same time
    /// @return
    ///     the options for up to `parallelism` deliveries at the same time
    /// @throws IllegalArgumentException
    ///     if `parallelism` is less than 1 or greater than 256
    public static DeliveryOptions parallel(int parallelism) {
        return new DeliveryOptions(new HandlerExecution.Parallel(parallelism, null));
    }

    /// One delivery at a time, in receive order, as [#serial()], for a handler that returns at
    /// once and never waits for this client. In return, the client may run the handler as part
    /// of receiving its message, rather than hand the delivery to another thread. While such a
    /// handler runs, the connection may read nothing else: neither messages for other
    /// subscriptions nor the broker's answers to publications, subscriptions, and keep-alive
    /// requests.
    ///
    /// Inside the handler, [join()][java.util.concurrent.CompletableFuture#join()] and
    /// [get()][java.util.concurrent.CompletableFuture#get()] of a future that this client returned,
    /// or of a future made from one, and the [receive][PullSubscription#receive()] methods of this
    /// client's pull subscriptions throw [IllegalStateException], even when they would not wait.
    /// Read a completed result with
    /// [getNow][java.util.concurrent.CompletableFuture#getNow(Object)], or chain the next step
    /// instead. Other waits are not detected: a handler that blocks, or that waits for anything
    /// that needs this connection, holds up every subscription of the connection, and one that runs
    /// for about the keep-alive interval can lose the connection.
    ///
    /// A delivery that the client cannot run as part of receiving runs on an internal thread, in
    /// the same order and under the same rules. The subscription's buffer, acknowledgement mode,
    /// and failure policy work as for [#serial()]. [#withExecutor(Executor)] rejects these
    /// options.
    ///
    /// @return
    ///     the options for inline delivery
    public static DeliveryOptions inline() {
        return INLINE;
    }

    /// Keeps the order, stripes, or parallelism of these options, with each handler running on
    /// `executor` instead of an internal thread. Replaces an executor chosen earlier.
    ///
    /// @param executor
    ///     the executor that runs the handlers
    /// @return
    ///     options like these that run each handler on `executor`
    /// @throws NullPointerException
    ///     if `executor` is null
    /// @throws IllegalArgumentException
    ///     if these options are [#lanes(Function)], whose selector chooses the executors, or
    ///     [#inline()], whose handler runs as part of receiving
    public DeliveryOptions withExecutor(Executor executor) {
        requireNonNull(executor, "executor");
        return switch (execution) {
            case HandlerExecution.Internal _, HandlerExecution.OnExecutor _ ->
                    new DeliveryOptions(new HandlerExecution.OnExecutor(executor));
            case HandlerExecution.Striped striped -> new DeliveryOptions(
                    new HandlerExecution.Striped(striped.stripes(), striped.key(), executor)
            );
            case HandlerExecution.Parallel parallel -> new DeliveryOptions(
                    new HandlerExecution.Parallel(parallel.parallelism(), executor)
            );
            case HandlerExecution.Lanes _ -> throw new IllegalArgumentException(
                    "Lanes take their executors from the selector"
            );
            case HandlerExecution.Inline _ -> throw new IllegalArgumentException(
                    "Inline handlers run as part of receiving, not on an executor"
            );
        };
    }

    /// The client reads the choice through its builder in this package, so applications see only
    /// the factories.
    HandlerExecution execution() {
        return execution;
    }
}
