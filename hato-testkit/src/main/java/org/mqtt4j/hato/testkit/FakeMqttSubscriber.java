package org.mqtt4j.hato.testkit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

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
/// [MqttSubscriber]. It records each subscription with its handler and grants it at once at the
/// requested QoS. Rules like those of [BrokerControls] refuse later subscriptions to a filter or
/// grant them a lower QoS. A test delivers a message to a subscription with
/// [FakeSubscription#deliver(MqttMessage)], which runs the handler on the test's thread, and then
/// checks what the handler did with the delivery: whether it called [MqttDelivery#ack()], and
/// what it threw.
///
/// The fake records these facts without applying a client's acknowledgement rules: what the
/// [org.mqtt4j.hato.AckMode] and the [org.mqtt4j.hato.FailurePolicy] of the recorded [Subscribe]
/// make of them is for the test to check. It supports only subscriptions with a handler; pull
/// subscriptions, [org.mqtt4j.hato.DeliveryOptions], and hand-off subscriptions throw
/// [UnsupportedOperationException], as the role's default methods do. It is not a client: apart
/// from refusing a second subscription to the filter of an active one, it checks nothing that a
/// client checks. All methods may be called from any thread.
public final class FakeMqttSubscriber implements MqttSubscriber {

    private final List<FakeSubscription> subscriptions = new ArrayList<>();
    private final Map<TopicFilter, Rule> rules = new HashMap<>();

    public FakeMqttSubscriber() {
    }

    /// Records the subscription and returns its handle, which [#subscriptions()] also lists. The
    /// subscription is granted the requested QoS at once, or the lower QoS of a rule. A refused
    /// subscription, or a second one to the filter of an active subscription, ends at once:
    /// [Subscription#established()] fails with [MqttSubscriptionRefusedException] or
    /// [IllegalStateException].
    ///
    /// @throws NullPointerException if `request` or `handler` is null
    @Override
    public synchronized FakeSubscription subscribe(
            Subscribe request,
            Consumer<MqttDelivery> handler
    ) {
        requireNonNull(request, "request must not be null");
        requireNonNull(handler, "handler must not be null");
        TopicFilter filter = request.filter();
        // A client refuses a second subscription to a filter itself, before a broker could.
        boolean duplicate = subscriptions.stream()
                .anyMatch(active -> !active.ended() && active.filter().equals(filter));
        FakeSubscription subscription;
        if (duplicate) {
            subscription = FakeSubscription.refused(
                    request,
                    handler,
                    new IllegalStateException("Already subscribed to " + filter.pattern())
            );
        } else {
            subscription = switch (rules.get(filter)) {
                case null -> FakeSubscription.granted(request, handler, request.qos());
                case Refuse _ -> FakeSubscription.refused(
                        request,
                        handler,
                        new MqttSubscriptionRefusedException(ReasonCode.UNSPECIFIED_ERROR)
                );
                case GrantAtMost(var maximum) -> FakeSubscription.granted(
                        request,
                        handler,
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
    /// @throws NullPointerException if `filter` is null
    public synchronized void refuseSubscriptions(TopicFilter filter) {
        rules.put(requireNonNull(filter, "filter must not be null"), new Refuse());
    }

    /// Grants later subscriptions to exactly `filter` the lower of `maximum` and the requested
    /// QoS.
    ///
    /// @throws NullPointerException if `filter` or `maximum` is null
    public synchronized void grantAtMost(TopicFilter filter, Qos maximum) {
        requireNonNull(filter, "filter must not be null");
        requireNonNull(maximum, "maximum must not be null");
        rules.put(filter, new GrantAtMost(maximum));
    }

    /// Removes the rule for `filter`, if there is one, so that later subscriptions to it are
    /// granted as requested.
    ///
    /// @throws NullPointerException if `filter` is null
    public synchronized void clearSubscriptionRule(TopicFilter filter) {
        rules.remove(requireNonNull(filter, "filter must not be null"));
    }

    /// The subscriptions made so far, ended ones included, in the order made, as an unmodifiable
    /// list that later subscriptions do not affect.
    public synchronized List<FakeSubscription> subscriptions() {
        return List.copyOf(subscriptions);
    }

    /// The most recent subscription made to exactly `filter`, ended or not, or empty if none was
    /// made.
    ///
    /// @throws NullPointerException if `filter` is null
    public synchronized Optional<FakeSubscription> subscription(TopicFilter filter) {
        requireNonNull(filter, "filter must not be null");
        return subscriptions.reversed().stream()
                .filter(subscription -> subscription.filter().equals(filter))
                .findFirst();
    }

    /// A subscription that the fake recorded. Besides the [Subscription] methods that the code
    /// under test calls, it lets the test see the request, deliver messages to the handler, and
    /// see whether the subscription has ended.
    public static final class FakeSubscription implements Subscription {

        private final Subscribe request;
        private final Consumer<MqttDelivery> handler;
        // Null when the fake refused the subscription, which then ended at once.
        private final @Nullable Qos granted;
        private final CompletableFuture<Void> terminated = new CompletableFuture<>();
        // What establishment fails with once the subscription has ended; the first ending
        // decides it. Null while the subscription is active.
        private @Nullable RuntimeException ending;
        // Deliveries whose handler is running now.
        private int running;

        private FakeSubscription(
                Subscribe request,
                Consumer<MqttDelivery> handler,
                @Nullable Qos granted,
                @Nullable RuntimeException ending
        ) {
            this.request = request;
            this.handler = handler;
            this.granted = granted;
            this.ending = ending;
            if (ending != null) {
                terminated.complete(null);
            }
        }

        private static FakeSubscription granted(
                Subscribe request,
                Consumer<MqttDelivery> handler,
                Qos granted
        ) {
            return new FakeSubscription(request, handler, granted, null);
        }

        private static FakeSubscription refused(
                Subscribe request,
                Consumer<MqttDelivery> handler,
                RuntimeException refusal
        ) {
            return new FakeSubscription(request, handler, null, refusal);
        }

        /// The request that the code under test subscribed with.
        public Subscribe request() {
            return request;
        }

        /// Delivers `message` to the handler on the calling thread, and returns the delivery once
        /// the handler returns. The delivery has no DUP flag, connection attempt 1, and the
        /// filter of this subscription. What the handler throws is recorded in the delivery
        /// instead of being thrown here. Calls from several threads run the handler at the same
        /// time, which a client never does.
        ///
        /// @throws NullPointerException if `message` is null
        /// @throws IllegalStateException if this subscription has ended
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
                handler.accept(delivery);
            } catch (Throwable failure) {
                // A client catches whatever escapes the handler, too.
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

        /// Whether this subscription has ended: the code under test closed or unsubscribed it,
        /// or the fake refused it.
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

        /// Returns a future completed with the granted QoS while this subscription is active.
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
        @Override
        public CompletableFuture<Void> unsubscribe() {
            end(new IllegalStateException("The subscription was removed"));
            return CompletableFuture.completedFuture(null);
        }

        /// Returns a future that completes once this subscription has ended and no handler of
        /// it is running.
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
    /// [MqttDelivery] methods that the handler calls, it shows what the handler did with it.
    public static final class FakeMqttDelivery implements MqttDelivery {

        private final MqttMessage message;
        private final TopicFilter filter;
        private volatile boolean acked;
        private volatile @Nullable Throwable failure;

        private FakeMqttDelivery(MqttMessage message, TopicFilter filter) {
            this.message = message;
            this.filter = filter;
        }

        @Override
        public MqttMessage message() {
            return message;
        }

        /// Always false: the fake delivers no duplicates.
        @Override
        public boolean dup() {
            return false;
        }

        /// Always 1.
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

        /// Whether [#ack()] has been called, by the handler or later by any thread.
        public boolean acked() {
            return acked;
        }

        /// What the handler threw for this delivery, or empty if it returned normally.
        public Optional<Throwable> failure() {
            return Optional.ofNullable(failure);
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
