package org.mqtt4j.mqtj;

/// An application delivery. QoS 0 needs no acknowledgement; its receive capacity remains
/// reserved until the handler returns.
public interface MqttDelivery {

    /// The received message, whose payload accessor returns an independent copy.
    MqttMessage message();

    /// Whether the broker marked the publication as a duplicate. Always false for QoS 0.
    boolean dup();
}
