package org.mqtt4j.mqtj;

/// Successful completion of a publication: the transport write for QoS 0, or receipt of PUBACK
/// for QoS 1. Neither confirms completion of any subscriber's application processing.
public record PublishResult() {
}
