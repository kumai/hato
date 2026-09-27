package org.mqtt4j.mqtj;

import org.mqtt4j.mqtj.client.internal.common.protocol.SharedTopicFilter;

import static java.util.Objects.requireNonNull;

/// A request for one subscription. Shared filters use `$share/{group}/{filter}` syntax.
/// The original filter, including a shared group, identifies duplicate subscriptions.
///
/// All QoS values can be represented; the current client accepts only QoS 0 subscriptions.
///
/// @param filter the filter sent to the broker
/// @param qos the maximum quality of service requested
/// @param bufferCapacity the maximum number of queued and running deliveries for this
///         subscription; a running handler retains its capacity until it returns
public record Subscribe(TopicFilter filter, Qos qos, int bufferCapacity) {

    /// Uses capacity for 256 queued and running deliveries.
    public Subscribe(TopicFilter filter, Qos qos) {
        this(filter, qos, 256);
    }

    /// @throws IllegalArgumentException if capacity is not positive or shared syntax is invalid
    public Subscribe {
        requireNonNull(filter, "Filter must not be null");
        requireNonNull(qos, "QoS must not be null");
        if (bufferCapacity < 1) {
            throw new IllegalArgumentException("Buffer capacity must be positive");
        }
        SharedTopicFilter.parse(filter);
    }
}
