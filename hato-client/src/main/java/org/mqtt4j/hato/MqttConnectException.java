package org.mqtt4j.hato;

import java.io.Serial;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// A connection attempt failed. A broker refusal carries its reason code; transport failures
/// and deadline expiry carry a cause instead. An MQTT 5.0 broker can also explain its refusal in
/// [#reasonString()] and, with [ReasonCode#USE_ANOTHER_SERVER] or [ReasonCode#SERVER_MOVED],
/// name another server in [#serverReference()], which the client does not follow. Locally
/// detected packet failures carry [MqttProtocolException] as their cause, leaving [#reasonCode()]
/// empty.
/// An attempt that discarded a broker session without local state and then finds a session
/// again, as [MqttClient#connect()] describes, fails with neither a reason code nor a cause,
/// as does an attempt that [MqttClient#disconnect()] or [MqttClient#close()] withdraws. A
/// failure while closing the connection that found the session is attached as a suppressed
/// exception. If the client's [TimerScheduler] fails meanwhile,
/// that failure is the cause instead, and the refusal is attached as a suppressed exception.
@SuppressWarnings("exports") // Nullness annotations are compile-time only.
public final class MqttConnectException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final @Nullable ReasonCode reasonCode;

    private final @Nullable String reasonString;

    private final @Nullable String serverReference;

    /// A failure with neither a reason code nor a cause.
    ///
    /// @param message
    ///     the detail message
    public MqttConnectException(String message) {
        super(message);
        this.reasonCode = null;
        this.reasonString = null;
        this.serverReference = null;
    }

    /// A broker refusal without an explanation or a server reference.
    ///
    /// @param message
    ///     the detail message
    /// @param reasonCode
    ///     the broker's refusal reason
    public MqttConnectException(String message, ReasonCode reasonCode) {
        this(message, reasonCode, null, null);
    }

    /// A broker refusal with the explanation and the server reference that the broker sent, if
    /// any.
    ///
    /// @param message
    ///     the detail message
    /// @param reasonCode
    ///     the broker's refusal reason
    /// @param reasonString
    ///     the broker's explanation, or null when it sent none
    /// @param serverReference
    ///     the server that the broker named, or null when it named none
    public MqttConnectException(
            String message,
            ReasonCode reasonCode,
            @Nullable String reasonString,
            @Nullable String serverReference
    ) {
        super(message);
        this.reasonCode = requireNonNull(reasonCode, "reasonCode must not be null");
        this.reasonString = reasonString;
        this.serverReference = serverReference;
    }

    /// A failure with a cause instead of a reason code, such as a transport failure or deadline
    /// expiry.
    ///
    /// @param message
    ///     the detail message
    /// @param cause
    ///     the cause of the failure
    public MqttConnectException(String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = null;
        this.reasonString = null;
        this.serverReference = null;
    }

    /// {@return the broker's refusal reason, or empty when the attempt failed locally}
    public Optional<ReasonCode> reasonCode() {
        return Optional.ofNullable(reasonCode);
    }

    /// {@return the broker's explanation of its refusal for people diagnosing it, or empty when
    /// it sent none, as MQTT 3.1.1 brokers never do}
    public Optional<String> reasonString() {
        return Optional.ofNullable(reasonString);
    }

    /// {@return the server that the broker named for the client to use instead, or empty when
    /// it named none}
    public Optional<String> serverReference() {
        return Optional.ofNullable(serverReference);
    }
}
