package org.mqtt4j.hato.testkit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.AckMode;
import org.mqtt4j.hato.DeliveryOptions;
import org.mqtt4j.hato.MqttDelivery;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.MqttSubscriber;
import org.mqtt4j.hato.MqttSubscriptionRefusedException;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.ReasonCode;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Subscription;
import org.mqtt4j.hato.TopicFilter;

import static java.util.Objects.requireNonNull;

/// A stand-in for the subscribing role of a client, for tests of code that takes an
/// [MqttSubscriber]. It records each subscription with its handler, or with its acceptor for a
/// hand-off subscription, and grants it at once at the requested QoS. Rules like those of
/// [BrokerControls] refuse later subscriptions to a filter or grant them a lower QoS. A test
/// delivers a message to a subscription with [FakeSubscription#deliver(MqttMessage)], which runs
/// the handler or the acceptor on the test's thread, and then checks what the code under test did
/// with the delivery: whether it called [MqttDelivery#ack()], whether the acceptor refused it, and
/// what was thrown.
///
/// The fake records these facts without applying a client's acknowledgement rules: what the
/// [AckMode] and the [org.mqtt4j.hato.FailurePolicy] of the recorded [Subscribe] make of them is
/// for the test to check. A subscription with [DeliveryOptions] runs its handler on the test's
/// thread as well, whatever the options say. Pull subscriptions throw
/// [UnsupportedOperationException], as the role's default method does. It is not a client: apart
/// from rejecting null arguments and [AckMode#AFTER_HANDLER] for a hand-off subscription, and
/// refusing a second subscription to the filter of an active one, it checks nothing that a client
/// checks. All methods may be called from any thread.
public final class FakeMqttSubscriber implements MqttSubscriber {

    private final List<FakeSubscription> subscriptions = new ArrayList<>();
    private final Map<TopicFilter, Rule> rules = new HashMap<>();

    /// Creates a fake with no subscriptions and no rules, which grants each subscription at once
    /// at the requested QoS.
    public FakeMqttSubscriber() {
    }

    /// Records the subscription and returns its handle, which [#subscriptions()] also lists. The
    /// subscription is granted the requested QoS at once, or the lower QoS of a rule. A refused
    /// subscription, or a second one to the filter of an active subscription, ends at once:
    /// [Subscription#established()] fails with [MqttSubscriptionRefusedException] or
    /// [IllegalStateException].
    ///
    /// @param request
    ///     the subscription request
    /// @param handler
    ///     the handler that receives the deliveries
    /// @return
    ///     the handle of the recorded subscription
    /// @throws NullPointerException
    ///     if `request` or `handler` is null
    @Override
    public FakeSubscription subscribe(Subscribe request, Consumer<MqttDelivery> handler) {
        requireNonNull(request, "request must not be null");
        requireNonNull(handler, "handler must not be null");
        return register(request, handler::accept);
    }

    /// Records the subscription as [#subscribe(Subscribe, Consumer)] does. Its handler runs on the
    /// test's thread, whatever `options` say.
    ///
    /// @param request
    ///     the subscription request
    /// @param options
    ///     where and how the handler would run on a client, which the fake ignores
    /// @param handler
    ///     the handler that receives the deliveries
    /// @return
    ///     the handle of the recorded subscription
    /// @throws NullPointerException
    ///     if an argument is null
    @Override
    public FakeSubscription subscribe(
            Subscribe request,
            DeliveryOptions options,
            Consumer<MqttDelivery> handler
    ) {
        requireNonNull(options, "options must not be null");
        return subscribe(request, handler);
    }

    /// Records the hand-off subscription as [#subscribe(Subscribe, Consumer)] records a
    /// subscription with a handler. [FakeSubscription#deliver(MqttMessage)] offers each delivery to
    /// `acceptor` on the test's thread, and the delivery shows whether the acceptor refused it.
    ///
    /// @param request
    ///     the subscription request
    /// @param acceptor
    ///     the predicate that accepts each delivery by returning true
    /// @return
    ///     the handle of the recorded subscription
    /// @throws NullPointerException
    ///     if `request` or `acceptor` is null
    /// @throws IllegalArgumentException
    ///     if the request acknowledges with [AckMode#AFTER_HANDLER], as
    ///     [MqttSubscriber#handOff(Subscribe, Predicate)] states
    @Override
    public FakeSubscription handOff(Subscribe request, Predicate<MqttDelivery> acceptor) {
        requireNonNull(request, "request must not be null");
        requireNonNull(acceptor, "acceptor must not be null");
        if (request.ackMode().orElse(AckMode.ON_RECEIVE) == AckMode.AFTER_HANDLER) {
            throw new IllegalArgumentException(
                    "A hand-off subscription cannot see when its deliveries are processed"
            );
        }
        return register(request, delivery -> {
            if (!acceptor.test(delivery)) {
                delivery.refuse();
            }
        });
    }

    // Records a subscription whose deliveries go to `receiver`, granted or ended at once.
    private synchronized FakeSubscription register(
            Subscribe request,
            Consumer<FakeMqttDelivery> receiver
    ) {
        TopicFilter filter = request.filter();
        // A client refuses a second subscription to a filter itself, before a broker could.
        boolean duplicate = subscriptions.stream()
                .anyMatch(active -> !active.ended() && active.filter().equals(filter));
        FakeSubscription subscription;
        if (duplicate) {
            subscription = FakeSubscription.refused(
                    request,
                    receiver,
                    new IllegalStateException("Already subscribed to " + filter.pattern())
            );
        } else {
            subscription = switch (rules.get(filter)) {
                case null -> FakeSubscription.granted(request, receiver, request.qos());
                case Refuse _ -> FakeSubscription.refused(
                        request,
                        receiver,
                        new MqttSubscriptionRefusedException(ReasonCode.UNSPECIFIED_ERROR)
                );
                case GrantAtMost(var maximum) -> FakeSubscription.granted(
                        request,
                        receiver,
                        request.qos().level() <= maximum.level() ? request.qos() : maximum
                );
            };
        }
        subscriptions.add(subscription);
        return subscription;
    }

    /// Refuses later subscriptions to exactly `filter`, as written, with Unspecified error:
    /// their [Subscription#established()] fails with [MqttSubscriptionRefusedException], and
    /// they end. A subscription made to the filter before stays as it was.
    ///
    /// @param filter
    ///     the topic filter to refuse
    /// @throws NullPointerException
    ///     if `filter` is null
    public synchronized void refuseSubscriptions(TopicFilter filter) {
        rules.put(requireNonNull(filter, "filter must not be null"), new Refuse());
    }

    /// Grants later subscriptions to exactly `filter` the lower of `maximum` and the requested
    /// QoS.
    ///
    /// @param filter
    ///     the topic filter to limit
    /// @param maximum
    ///     the highest QoS to grant
    /// @throws NullPointerException
    ///     if `filter` or `maximum` is null
    public synchronized void grantAtMost(TopicFilter filter, Qos maximum) {
        requireNonNull(filter, "filter must not be null");
        requireNonNull(maximum, "maximum must not be null");
        rules.put(filter, new GrantAtMost(maximum));
    }

    /// Removes the rule for `filter`, if there is one, so that later subscriptions to it are
    /// granted as requested.
    ///
    /// @param filter
    ///     the topic filter whose rule to remove
    /// @throws NullPointerException
    ///     if `filter` is null
    public synchronized void clearSubscriptionRule(TopicFilter filter) {
        rules.remove(requireNonNull(filter, "filter must not be null"));
    }

    /// {@return the subscriptions made so far, ended ones included, in the order made, as an
    /// unmodifiable list that later subscriptions do not affect}
    public synchronized List<FakeSubscription> subscriptions() {
        return List.copyOf(subscriptions);
    }

    /// {@return the most recent subscription made to exactly `filter`, ended or not, or empty if
    /// none was made}
    ///
    /// @param filter
    ///     the topic filter of the subscription
    /// @throws NullPointerException
    ///     if `filter` is null
    public synchronized Optional<FakeSubscription> subscription(TopicFilter filter) {
        requireNonNull(filter, "filter must not be null");
        return subscriptions.reversed().stream()
                .filter(subscription -> subscription.filter().equals(filter))
                .findFirst();
    }

    /// A subscription that the fake recorded. Besides the [Subscription] methods that the code
    /// under test calls, it lets the test see the request, deliver messages to the handler or the
    /// acceptor, and see whether the subscription has ended.
    public static final class FakeSubscription implements Subscription {

        private final Subscribe request;
        // The handler, or the acceptor of a hand-off subscription with its answer recorded.
        private final Consumer<FakeMqttDelivery> receiver;
        // Null when the fake refused the subscription, which then ended at once.
        private final @Nullable Qos granted;
        private final CompletableFuture<Void> terminated = new CompletableFuture<>();
        // What establishment fails with once the subscription has ended; the first ending
        // decides it. Null while the subscription is active.
        private @Nullable RuntimeException ending;
        // Deliveries whose handler or acceptor is running now.
        private int running;

        private FakeSubscription(
                Subscribe request,
                Consumer<FakeMqttDelivery> receiver,
                @Nullable Qos granted,
                @Nullable RuntimeException ending
        ) {
            this.request = request;
            this.receiver = receiver;
            this.granted = granted;
            this.ending = ending;
            if (ending != null) {
                terminated.complete(null);
            }
        }

        private static FakeSubscription granted(
                Subscribe request,
                Consumer<FakeMqttDelivery> receiver,
                Qos granted
        ) {
            return new FakeSubscription(request, receiver, granted, null);
        }

        private static FakeSubscription refused(
                Subscribe request,
                Consumer<FakeMqttDelivery> receiver,
                RuntimeException refusal
        ) {
            return new FakeSubscription(request, receiver, null, refusal);
        }

        /// {@return the request that the code under test subscribed with}
        public Subscribe request() {
            return request;
        }

        /// Delivers `message` to the handler, or offers it to the acceptor of a hand-off
        /// subscription, on the calling thread, and returns the delivery once that returns. The
        /// delivery has no DUP flag, connection attempt 1, and the filter of this subscription.
        /// What the handler or the acceptor throws is recorded in the delivery instead of being
        /// thrown here. Calls from several threads run the handler at the same time, which a
        /// client does only where the subscription's [DeliveryOptions] let deliveries run at once.
        ///
        /// @param message
        ///     the message to deliver
        /// @return
        ///     the delivery that the handler or the acceptor received
        /// @throws NullPointerException
        ///     if `message` is null
        /// @throws IllegalStateException
        ///     if this subscription has ended
        public FakeMqttDelivery deliver(MqttMessage message) {
            requireNonNull(message, "message must not be null");
            synchronized (this) {
                if (ending != null) {
                    throw new IllegalStateException(
                            "The subscription to " + filter().pattern() + " has ended"
                    );
                }
                running++;
            }
            var delivery = new FakeMqttDelivery(message, filter());
            try {
                receiver.accept(delivery);
            } catch (Throwable failure) {
                // A client catches whatever escapes the handler or the acceptor, too.
                delivery.failed(failure);
            } finally {
                boolean done;
                synchronized (this) {
                    running--;
                    done = ending != null && running == 0;
                }
                if (done) {
                    terminated.complete(null);
                }
            }
            return delivery;
        }

        /// {@return whether this subscription has ended: the code under test closed or
        /// unsubscribed it, or the fake refused it}
        public synchronized boolean ended() {
            return ending != null;
        }

        @Override
        public TopicFilter filter() {
            return request.filter();
        }

        @Override
        public Optional<Qos> grantedQos() {
            return Optional.ofNullable(granted);
        }

        /// {@return a future completed with the granted QoS while this subscription is active}
        /// Once it has ended, the future fails with the refusal, or with [IllegalStateException]
        /// after the code under test closed or unsubscribed it.
        @Override
        public synchronized CompletableFuture<Qos> established() {
            if (ending != null) {
                return CompletableFuture.failedFuture(ending);
            }
            // An active subscription was granted a QoS.
            return CompletableFuture.completedFuture(requireNonNull(granted));
        }

        /// Ends this subscription at once and returns a completed future.
        ///
        /// @return
        ///     a completed future
        @Override
        public CompletableFuture<Void> unsubscribe() {
            end(new IllegalStateException("The subscription was removed"));
            return CompletableFuture.completedFuture(null);
        }

        /// {@return a future that completes once this subscription has ended and neither its
        /// handler nor its acceptor is running}
        @Override
        public CompletableFuture<Void> terminated() {
            return terminated.copy();
        }

        /// Ends this subscription at once.
        @Override
        public void close() {
            end(new IllegalStateException("The subscription was closed"));
        }

        private void end(RuntimeException reason) {
            boolean done;
            synchronized (this) {
                if (ending == null) {
                    ending = reason;
                }
                done = running == 0;
            }
            // Completing the future runs the code under test that waits for it, so it happens
            // outside the lock.
            if (done) {
                terminated.complete(null);
            }
        }
    }

    /// A delivery that a test made with [FakeSubscription#deliver(MqttMessage)]. Besides the
    /// [MqttDelivery] methods that the code under test calls, it shows what that code did with it.
    public static final class FakeMqttDelivery implements MqttDelivery {

        private final MqttMessage message;
        private final TopicFilter filter;
        private volatile boolean acked;
        private volatile boolean refused;
        private volatile @Nullable Throwable failure;

        private FakeMqttDelivery(MqttMessage message, TopicFilter filter) {
            this.message = message;
            this.filter = filter;
        }

        @Override
        public MqttMessage message() {
            return message;
        }

        /// {@return false, as the fake delivers no duplicates}
        @Override
        public boolean dup() {
            return false;
        }

        /// {@return 1}
        @Override
        public long connectionAttempt() {
            return 1;
        }

        @Override
        public TopicFilter filter() {
            return filter;
        }

        /// Records the call, which [#acked()] then shows. Repeated calls change nothing.
        @Override
        public void ack() {
            acked = true;
        }

        /// {@return whether [#ack()] has been called, by the handler or the acceptor, or later by
        /// any thread}
        public boolean acked() {
            return acked;
        }

        /// {@return whether the acceptor of a hand-off subscription returned false for this
        /// delivery, which is always false for a subscription with a handler}
        public boolean refused() {
            return refused;
        }

        /// {@return what the handler or the acceptor threw for this delivery, or empty if it
        /// returned normally}
        public Optional<Throwable> failure() {
            return Optional.ofNullable(failure);
        }

        private void refuse() {
            refused = true;
        }

        private void failed(Throwable thrown) {
            failure = thrown;
        }
    }

    // A rule for later subscriptions to one filter. The alternatives are private members of the
    // class, not of the interface, whose members would be public.
    private sealed interface Rule {}

    private record Refuse() implements Rule {}

    private record GrantAtMost(Qos maximum) implements Rule {}
}
