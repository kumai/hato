package org.mqtt4j.hato;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.client.internal.common.protocol.MqttString;

import static java.util.Objects.requireNonNull;

/// Immutable MQTT credentials: a username and an optional binary password.
/// The username is an MQTT UTF-8 string of at most 65535 bytes and may be empty.
/// A password contains at most 65535 bytes and is copied on construction and access.
/// An absent password and an empty password are distinct values.
/// Equality compares password contents. [#toString()] reveals neither credential.
public final class MqttCredentials {

    private final String username;

    private final byte @Nullable [] password;

    private MqttCredentials(String username, byte @Nullable [] password) {
        requireNonNull(username, "username");
        MqttString.validate(username);
        if (password != null && password.length > 65_535) {
            throw new IllegalArgumentException("Password exceeds 65535 bytes");
        }
        this.username = username;
        this.password = password == null ? null : password.clone();
    }

    /// Creates credentials containing only a username.
    ///
    /// @param username
    ///     the username, which may be empty
    /// @return
    ///     the credentials
    /// @throws IllegalArgumentException
    ///     if the username contains U+0000, an unpaired surrogate, or exceeds 65535 bytes in
    ///     UTF-8
    public static MqttCredentials of(String username) {
        return new MqttCredentials(username, null);
    }

    /// Creates credentials with a copy of the binary password, which may be empty.
    ///
    /// @param username
    ///     the username, which may be empty
    /// @param password
    ///     the password, which may be empty
    /// @return
    ///     the credentials
    /// @throws IllegalArgumentException
    ///     if the username contains U+0000, an unpaired surrogate, or exceeds 65535 bytes in
    ///     UTF-8, or the password exceeds 65535 bytes
    public static MqttCredentials of(String username, byte[] password) {
        return new MqttCredentials(username, requireNonNull(password, "password"));
    }

    /// {@return the username, which may be empty}
    public String username() {
        return username;
    }

    /// {@return a copy of the password, or empty when no password was supplied}
    /// A supplied zero-length password produces a present result containing an empty array.
    public Optional<byte[]> password() {
        return password == null ? Optional.empty() : Optional.of(password.clone());
    }

    @Override
    @SuppressWarnings("exports") // Nullness annotations are compile-time only.
    public boolean equals(@Nullable Object other) {
        return other instanceof MqttCredentials that
                && username.equals(that.username)
                && Arrays.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, Arrays.hashCode(password));
    }

    @Override
    public String toString() {
        return "MqttCredentials[redacted]";
    }
}
