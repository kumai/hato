package org.mqtt4j.mqtj;

import org.jspecify.annotations.Nullable;

/// The end of an established connection. A null cause means no failure is reported;
/// it does not indicate a graceful MQTT disconnect.
/// `userRequested` identifies disconnect or close, including a failed DISCONNECT write.
/// These fields do not distinguish a successful disconnect from an immediate close.
/// Locally detected packet failures use [MqttProtocolException] as the cause.
@SuppressWarnings("exports") // Nullness annotations are compile-time only.
public record Disconnection(@Nullable Throwable cause, boolean userRequested) {
}
