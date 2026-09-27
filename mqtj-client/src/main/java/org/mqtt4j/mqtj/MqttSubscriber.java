package org.mqtt4j.mqtj;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/// The subscribing role of a client, without access to its connection lifecycle.
public interface MqttSubscriber {

    /// Registers one subscription and completes after a successful SUBACK.
    /// The current client supports only QoS 0; other QoS values fail with
    /// [IllegalArgumentException] through the returned future.
    ///
    /// Deliveries can reach the handler before the future completes. Each subscription's
    /// handler runs serially, independently of I/O and operation completion. A handler failure
    /// is logged and ends its connection if still active, notifying [ConnectionListener].
    /// A failure after that connection ends is logged without another disconnection notification
    /// or any effect on a later connection. Exhausted receive capacity also ends the connection.
    /// On connection termination, queued deliveries are discarded; a running handler can finish.
    /// Interrupt status left by a normally returning handler is cleared before the next delivery.
    ///
    /// Every matching subscription receives each incoming publication. MQTT 3.1.1 cannot
    /// distinguish broker copies for overlapping subscriptions, so each copy is routed again.
    /// Shared subscriptions are matched against the filter after their shared group name.
    /// A shared subscription cannot overlap any other subscription on the same client, including
    /// another shared subscription. Such combinations fail with [IllegalStateException] in
    /// either registration order, including subscriptions awaiting SUBACK or UNSUBACK.
    /// Use separate clients when those filters are needed together. Ordinary subscriptions may
    /// overlap each other.
    ///
    /// The same original filter fails with [IllegalStateException] while it is pending, active,
    /// or awaiting unsubscribe acknowledgement. A broker refusal fails with
    /// [MqttSubscribeException]. An unconnected client fails with [MqttNotConnectedException];
    /// admission failures use [MqttCapacityException].
    CompletableFuture<Subscription> subscribe(Subscribe request, Consumer<MqttDelivery> handler);
}
