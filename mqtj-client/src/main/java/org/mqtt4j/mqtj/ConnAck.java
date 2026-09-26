package org.mqtt4j.mqtj;

/// The broker's successful connection acknowledgement.
public record ConnAck(boolean sessionPresent) {
}
