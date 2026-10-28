package org.mqtt4j.hato;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// Receives timings that only the client can measure: how long each publication takes from its
/// admission to the completion of its future, and how long each delivery waits from its
/// reception until application code receives it. Register one with
/// [MqttClientBuilder#metricsListener(MetricsListener)]; without one, the client reads no time
/// and creates no event for these timings.
///
/// Callbacks run serially, on a thread of their own, never on a thread that the client needs for
/// I/O, publications, or deliveries. Events wait in a bounded queue: when a slow callback lets
/// it fill, the oldest event not yet delivered is dropped and counted in
/// [ClientStats#droppedMetricEvents()], so the listener never delays the client. Durations are
/// measured with the client's [TimerScheduler]; an event whose times the scheduler fails to read
/// is not reported. Listener exceptions are logged and do not affect the client, and interrupt
/// status left by a callback is cleared before the next one. [MqttClient#terminated()]
/// completes only after the queued events have been delivered. The default methods do nothing.
public interface MetricsListener {

    /// Called once for each publication that the client admitted, when its future completes,
    /// whether it succeeded or failed. A publication refused at once, whose future has already
    /// failed when [MqttPublisher#publish(MqttMessage)] returns, produces none.
    ///
    /// @param completion
    ///     how the publication completed
    default void onPublishCompleted(PublishCompletion completion) {
    }

    /// Called once for each delivery when application code receives it: when its handler
    /// starts, when its hand-off acceptor is called with it, or when a pull receive returns it.
    /// A delivery that the client discards before then produces none.
    ///
    /// @param start
    ///     when application code received the delivery
    default void onDeliveryStarted(DeliveryStart start) {
    }

    /// How one admitted publication completed. Equality compares the failure by identity,
    /// because exceptions have no value equality.
    final class PublishCompletion {

        private final Qos qos;

        private final @Nullable Throwable failure;

        private final Duration duration;

        private PublishCompletion(Qos qos, @Nullable Throwable failure, Duration duration) {
            this.qos = requireNonNull(qos, "qos");
            if (requireNonNull(duration, "duration").isNegative()) {
                throw new IllegalArgumentException("Duration must not be negative");
            }
            this.failure = failure;
            this.duration = duration;
        }

        /// {@return the completion of a publication at `qos` that succeeded `duration` after its
        /// admission}
        ///
        /// @param qos
        ///     the QoS that the publication was sent with
        /// @param duration
        ///     the time from the publication's admission to the completion of its future
        /// @throws IllegalArgumentException
        ///     if `duration` is negative
        public static PublishCompletion of(Qos qos, Duration duration) {
            return new PublishCompletion(qos, null, duration);
        }

        /// {@return the completion of a publication at `qos` whose future failed with `failure`,
        /// `duration` after its admission}
        ///
        /// @param qos
        ///     the QoS that the publication was sent with
        /// @param failure
        ///     the exception with which the publication's future failed
        /// @param duration
        ///     the time from the publication's admission to the completion of its future
        /// @throws IllegalArgumentException
        ///     if `duration` is negative
        public static PublishCompletion of(Qos qos, Throwable failure, Duration duration) {
            return new PublishCompletion(qos, requireNonNull(failure, "failure"), duration);
        }

        /// {@return the QoS that the publication was sent with}
        public Qos qos() {
            return qos;
        }

        /// {@return the exception with which the publication's future failed, or empty if it
        /// succeeded}
        public Optional<Throwable> failure() {
            return Optional.ofNullable(failure);
        }

        /// {@return the time from when the client admitted the publication to when its future
        /// completed}
        public Duration duration() {
            return duration;
        }

        @Override
        @SuppressWarnings("exports") // Nullness annotations are compile-time only.
        public boolean equals(@Nullable Object other) {
            return other instanceof PublishCompletion that
                    && qos == that.qos
                    && Objects.equals(failure, that.failure)
                    && duration.equals(that.duration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(qos, failure, duration);
        }

        @Override
        public String toString() {
            return "PublishCompletion[qos=" + qos
                    + ", failure=" + failure
                    + ", duration=" + duration + "]";
        }
    }

    /// When application code received one delivery.
    final class DeliveryStart {

        private final Subscription subscription;

        private final Duration waited;

        private DeliveryStart(Subscription subscription, Duration waited) {
            this.subscription = requireNonNull(subscription, "subscription");
            if (requireNonNull(waited, "waited").isNegative()) {
                throw new IllegalArgumentException("Wait must not be negative");
            }
            this.waited = waited;
        }

        /// {@return the start of a delivery of `subscription` that application code received
        /// `waited` after the client received its PUBLISH}
        ///
        /// @param subscription
        ///     the subscription whose delivery this is
        /// @param waited
        ///     how long the delivery waited after the client received its PUBLISH
        /// @throws IllegalArgumentException
        ///     if `waited` is negative
        public static DeliveryStart of(Subscription subscription, Duration waited) {
            return new DeliveryStart(subscription, waited);
        }

        /// {@return the subscription whose delivery this is}
        public Subscription subscription() {
            return subscription;
        }

        /// {@return the time from when the client received the PUBLISH from its transport to
        /// when application code received the delivery}
        public Duration waited() {
            return waited;
        }

        @Override
        @SuppressWarnings("exports") // Nullness annotations are compile-time only.
        public boolean equals(@Nullable Object other) {
            return other instanceof DeliveryStart that
                    && subscription.equals(that.subscription)
                    && waited.equals(that.waited);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subscription, waited);
        }

        @Override
        public String toString() {
            return "DeliveryStart[subscription=" + subscription.filter().pattern()
                    + ", waited=" + waited + "]";
        }
    }
}
