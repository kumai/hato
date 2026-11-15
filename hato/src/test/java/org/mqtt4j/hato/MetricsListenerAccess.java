package org.mqtt4j.hato;

import java.time.Duration;

import org.jspecify.annotations.Nullable;

/// Makes metrics events as the builder lets the client do, for tests that construct the client
/// directly or compare the events that a listener received.
public final class MetricsListenerAccess {

    private MetricsListenerAccess() {
    }

    public static MetricsListener.PublishCompletion completion(Qos qos, Duration duration) {
        return MetricsListener.PublishCompletion.of(qos, null, duration);
    }

    public static MetricsListener.PublishCompletion completion(
            Qos qos,
            @Nullable Throwable failure,
            Duration duration
    ) {
        return MetricsListener.PublishCompletion.of(qos, failure, duration);
    }

    public static MetricsListener.DeliveryStart start(Subscription subscription, Duration waited) {
        return MetricsListener.DeliveryStart.of(subscription, waited);
    }
}
