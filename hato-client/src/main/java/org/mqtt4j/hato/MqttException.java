package org.mqtt4j.hato;

import java.io.Serial;

import static java.util.Objects.requireNonNull;

/// Base class for failures of MQTT operations.
public sealed class MqttException extends RuntimeException permits
        MqttBrokerDisconnectException,
        MqttCapacityException,
        MqttConnectException,
        MqttConnectionLostException,
        MqttNotConnectedException,
        MqttProtocolException,
        MqttPublicationRefusedException,
        MqttSubscriptionRefusedException {

    // The class stays concrete although the client never throws it directly. A pattern switch over
    // an abstract sealed class may leave out its default once it names every permitted subclass,
    // and an application written that way would stop compiling, or fail with MatchException, when
    // a later version adds a subclass. A concrete class keeps a case for MqttException itself
    // required.

    @Serial
    private static final long serialVersionUID = 1L;

    /// A failure with a detail message.
    ///
    /// @param message
    ///   the detail message
    MqttException(String message) {
        super(message);
    }

    /// A failure with a detail message and a cause.
    ///
    /// @param message
    ///   the detail message
    /// @param cause
    ///   the cause of the failure
    MqttException(String message, Throwable cause) {
        super(message, requireNonNull(cause, "cause must not be null"));
    }
}
