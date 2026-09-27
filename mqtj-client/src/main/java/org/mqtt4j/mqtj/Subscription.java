package org.mqtt4j.mqtj;

import java.util.concurrent.CompletableFuture;

/// One subscription established by a successful SUBACK.
public interface Subscription extends AutoCloseable {

    /// The original filter sent to the broker, including any shared group name.
    TopicFilter filter();

    /// The maximum QoS granted by the broker, which can be lower than requested.
    Qos grantedQos();

    /// Stops new local delivery when called and completes after UNSUBACK.
    /// Already queued deliveries may run while acknowledgement is pending; any still queued
    /// when it completes are discarded. Handlers already running can finish.
    ///
    /// If capacity prevents admitting the removal, new delivery resumes and the future fails with
    /// [MqttCapacityException]; the removal can be retried. Messages arriving during
    /// the attempted removal are not replayed to this subscription.
    /// While a disconnect is pending, the removal fails with [MqttNotConnectedException].
    /// Each call returns an independent future; changing it does not alter the wire request or
    /// later results. Calls during one attempt observe the same wire exchange and use the
    /// operation budget until it ends, even if their futures are completed or cancelled first.
    /// Retrying after a timeout therefore cannot speed up removal and may exhaust the budget
    /// for other operations. Rejection of an additional call leaves the pending removal
    /// unchanged.
    /// A call admitted before permanent client closure can be rejected if it has not started
    /// observing that exchange.
    /// A repeated request after successful removal is harmless.
    /// The same filter can be registered again after removal completes.
    ///
    /// Connection termination retires this handle. Subsequent calls complete locally without
    /// sending an UNSUBSCRIBE, even after reconnecting and registering the same filter again.
    /// Calls after client shutdown has been requested also complete locally and discard queued
    /// deliveries. This does not change the outcome of a removal already pending when the
    /// connection ends.
    CompletableFuture<Void> unsubscribe();

    /// Initiates [#unsubscribe()] without waiting for the acknowledgement or application code.
    /// While a removal is pending, this has no further effect.
    /// Use [#unsubscribe()] instead when completion or failure must be observed.
    @Override
    void close();
}
