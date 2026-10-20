package org.mqtt4j.hato;

import java.io.Serial;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// A connection attempt failed. A broker refusal carries its reason code; transport failures
/// and deadline expiry carry a cause instead. An MQTT 5.0 broker can also explain its refusal in
/// [#reasonString()] and, with [ReasonCode#USE_ANOTHER_SERVER] or [ReasonCode#SERVER_MOVED],
/// name another server in [#serverReference()], which the client does not follow. Locally
/// detected packet failures carry [MqttProtocolException] as their cause, leaving [#reasonCode]
/// empty.
/// An attempt that discarded a broker session without local state and then finds a session
/// again, as [MqttClient#connect()] describes, fails with neither a reason code nor a cause;
/// its message tells this case apart. A failure while closing the connection that found the
/// session is attached as a suppressed exception. If the client's
/// [org.mqtt4j.hato.spi.TimerScheduler] fails meanwhile, that failure is the cause instead,
/// and the refusal is attached as a suppressed exception.
@SuppressWarnings("exports") // Nullness annotations are compile-time only.
public final class MqttConnectException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final @Nullable ReasonCode reasonCode;

    private final @Nullable String reasonString;

    private final @Nullable String serverReference;

    public MqttConnectException(String message) {
        super(message);
        this.reasonCode = null;
        this.reasonString = null;
        this.serverReference = null;
    }

    public MqttConnectException(String message, ReasonCode reasonCode) {
        this(message, reasonCode, null, null);
    }

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

    public MqttConnectException(String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = null;
        this.reasonString = null;
        this.serverReference = null;
    }

    /// Returns the broker's refusal reason, or empty when the attempt failed locally.
    public Optional<ReasonCode> reasonCode() {
        return Optional.ofNullable(reasonCode);
    }

    /// The broker's explanation of its refusal for people diagnosing it, or empty when it sent
    /// none, as MQTT 3.1.1 brokers never do.
    public Optional<String> reasonString() {
        return Optional.ofNullable(reasonString);
    }

    /// The server that the broker named for the client to use instead, or empty when it named
    /// none.
    public Optional<String> serverReference() {
        return Optional.ofNullable(serverReference);
    }
}
