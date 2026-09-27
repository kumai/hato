package org.mqtt4j.mqtj;

/// Successful completion of a publication. For the currently supported QoS 0, this means
/// the transport finished writing the packet. It does not confirm reception by the broker
/// or completion of any subscriber's application processing.
public record PublishResult() {
}
