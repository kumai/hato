package org.mqtt4j.hato.testkit;

import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.ReasonCode;

import static java.util.Objects.requireNonNull;

/// The reason that an embedded broker gives a client when a test makes it refuse a request or
/// end a connection with [BrokerControls]: a reason code, and the Reason String and the Server
/// Reference that MQTT 5.0 carries with it, if any. Start from [#of(ReasonCode)] and add the
/// strings with the `with` methods, which return copies.
///
/// Each operation of [BrokerControls] that takes a reason checks it against what it answers,
/// and tells which reason codes it accepts. A Server Reference goes only with
/// [ReasonCode#USE_ANOTHER_SERVER] and [ReasonCode#SERVER_MOVED], which only a refused
/// connection and an ended one can carry. Both strings must be MQTT UTF-8 strings: no U+0000,
/// no unpaired surrogate, and at most 65,535 bytes in UTF-8.
///
/// Experimental, like [BrokerControls].
public final class BrokerReason {

    private final ReasonCode reasonCode;

    private final @Nullable String reasonString;

    private final @Nullable String serverReference;

    private BrokerReason(
            ReasonCode reasonCode,
            @Nullable String reasonString,
            @Nullable String serverReference
    ) {
        this.reasonCode = reasonCode;
        this.reasonString = reasonString;
        this.serverReference = serverReference;
    }

    /// A reason with `reasonCode` alone.
    ///
    /// @throws NullPointerException if `reasonCode` is null
    public static BrokerReason of(ReasonCode reasonCode) {
        return new BrokerReason(
                requireNonNull(reasonCode, "reasonCode must not be null"),
                null,
                null
        );
    }

    /// Returns a copy with `reasonString`, which the broker sends to MQTT 5.0 clients for
    /// diagnostics.
    ///
    /// @throws NullPointerException if `reasonString` is null
    public BrokerReason withReasonString(String reasonString) {
        return new BrokerReason(
                reasonCode,
                requireNonNull(reasonString, "reasonString must not be null"),
                serverReference
        );
    }

    /// Returns a copy with `serverReference`, which tells MQTT 5.0 clients the server or servers
    /// to use instead, as a space-separated list of references whose form MQTT leaves open.
    ///
    /// @throws NullPointerException if `serverReference` is null
    public BrokerReason withServerReference(String serverReference) {
        return new BrokerReason(
                reasonCode,
                reasonString,
                requireNonNull(serverReference, "serverReference must not be null")
        );
    }

    /// The reason code.
    public ReasonCode reasonCode() {
        return reasonCode;
    }

    /// The Reason String, if there is one.
    public Optional<String> reasonString() {
        return Optional.ofNullable(reasonString);
    }

    /// The Server Reference, if there is one.
    public Optional<String> serverReference() {
        return Optional.ofNullable(serverReference);
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof BrokerReason that
                && reasonCode == that.reasonCode
                && Objects.equals(reasonString, that.reasonString)
                && Objects.equals(serverReference, that.serverReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reasonCode, reasonString, serverReference);
    }

    @Override
    public String toString() {
        return "BrokerReason[reasonCode=" + reasonCode
                + ", reasonString=" + reasonString
                + ", serverReference=" + serverReference + "]";
    }
}
