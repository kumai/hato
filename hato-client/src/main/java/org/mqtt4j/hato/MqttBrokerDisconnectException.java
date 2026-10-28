package org.mqtt4j.hato;

import java.io.Serial;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// An MQTT 5.0 broker ended the connection with DISCONNECT. This is the cause in the
/// [Disconnection] of that connection, and pending operations fail with
/// [MqttConnectionLostException] that carries it as its cause. The reason code decides whether
/// the client reconnects; see [ReconnectPolicy]. With [ReasonCode#USE_ANOTHER_SERVER] or
/// [ReasonCode#SERVER_MOVED], the broker can name another server in [#serverReference()]. The
/// client does not follow it; an application that does creates a client for that server. MQTT
/// 3.1.1 brokers close connections without DISCONNECT, so this exception appears only with
/// MQTT 5.0.
@SuppressWarnings("exports") // Nullness annotations are compile-time only.
public final class MqttBrokerDisconnectException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ReasonCode reasonCode;

    private final @Nullable String reasonString;

    private final @Nullable String serverReference;

    /// A DISCONNECT from the broker, with the explanation and the server reference that it
    /// carried, if any.
    ///
    /// @param reasonCode
    ///     the reason code of the broker's DISCONNECT
    /// @param reasonString
    ///     the broker's explanation, or null when it sent none
    /// @param serverReference
    ///     the server that the broker named, or null when it named none
    public MqttBrokerDisconnectException(
            ReasonCode reasonCode,
            @Nullable String reasonString,
            @Nullable String serverReference
    ) {
        super(message(reasonCode, reasonString));
        this.reasonCode = reasonCode;
        this.reasonString = reasonString;
        this.serverReference = serverReference;
    }

    /// {@return the reason code of the broker's DISCONNECT}
    public ReasonCode reasonCode() {
        return reasonCode;
    }

    /// {@return the broker's explanation for people diagnosing the end, or empty when it sent
    /// none}
    public Optional<String> reasonString() {
        return Optional.ofNullable(reasonString);
    }

    /// {@return the server that the broker named for the client to use instead, or empty when
    /// it named none}
    public Optional<String> serverReference() {
        return Optional.ofNullable(serverReference);
    }

    private static String message(ReasonCode reasonCode, @Nullable String reasonString) {
        String message = "Broker ended the connection: "
                + requireNonNull(reasonCode, "reasonCode must not be null");
        return reasonString == null ? message : message + " (" + reasonString + ")";
    }
}
