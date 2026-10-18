package org.mqtt4j.hato;

import java.io.Serial;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// A connection attempt failed. A broker refusal carries its reason code; transport failures
/// and deadline expiry carry a cause instead. Locally detected packet failures carry
/// [MqttProtocolException] as their cause, leaving [#reasonCode] empty.
/// An attempt that discarded a broker session without local state and then finds a session
/// again, as [MqttClient#connect()] describes, fails with neither a reason code nor a cause;
/// its message tells this case apart. A failure while closing the connection that found the
/// session is attached as a suppressed exception. If the client's
/// [org.mqtt4j.hato.spi.TimerScheduler] fails meanwhile, that failure is the cause instead,
/// and the refusal is attached as a suppressed exception.
public final class MqttConnectException extends MqttException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final @Nullable ReasonCode reasonCode;

    public MqttConnectException(String message) {
        super(message);
        this.reasonCode = null;
    }

    public MqttConnectException(String message, ReasonCode reasonCode) {
        super(message);
        this.reasonCode = requireNonNull(reasonCode, "reasonCode must not be null");
    }

    public MqttConnectException(String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = null;
    }

    /// Returns the broker's refusal reason, or empty when the attempt failed locally.
    public Optional<ReasonCode> reasonCode() {
        return Optional.ofNullable(reasonCode);
    }
}
