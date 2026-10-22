package org.mqtt4j.hato.testkit;

/// A kind of response that [BrokerControls] can hold back: the acknowledgements that a client
/// waits for after its requests. Experimental, like [BrokerControls].
public enum ResponseKind {
    /// The answer to SUBSCRIBE.
    SUBACK,
    /// The answer to UNSUBSCRIBE.
    UNSUBACK,
    /// The answer to a QoS 1 PUBLISH.
    PUBACK,
    /// The first answer to a QoS 2 PUBLISH.
    PUBREC,
    /// The answer to PUBREL, which completes a QoS 2 publication.
    PUBCOMP
}
