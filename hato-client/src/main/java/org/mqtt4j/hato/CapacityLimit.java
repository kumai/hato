package org.mqtt4j.hato;

/// A limit that refuses operations with [MqttCapacityException] when it is full.
public enum CapacityLimit {

    /// The operation capacity that publications share with concurrent [MqttClient#connect()]
    /// and [MqttClient#disconnect()] calls beyond the first of each; see
    /// [ClientStats.Sending#operations()].
    OPERATIONS,

    /// The operations waiting for the client to process them.
    OPERATION_QUEUE,

    /// [MqttClientBuilder#maxInflightPublishes(int)].
    INFLIGHT_PUBLISHES,

    /// The bytes that unfinished publications keep.
    PUBLICATION_BYTES,

    /// The packet identifiers that publications and subscription requests share.
    PACKET_IDENTIFIERS,

    /// The send queue, or a backlog in it that waits for send capacity.
    SEND_QUEUE,

    /// [MqttClientBuilder#maxSubscriptions(int)], for registrations and delivery lanes.
    SUBSCRIPTIONS,

    /// The registrations waiting for the client to process them.
    REGISTRATION_QUEUE,

    /// The pending observations, such as [MqttClient#whenConnected()],
    /// [Subscription#established()], [MqttClient#terminated()], and
    /// [MqttClient#stats()].
    OBSERVATIONS
}
