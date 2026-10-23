package org.mqtt4j.hato;

import org.mqtt4j.hato.client.internal.common.protocol.Property;

/// Wraps a received User Property, as the builder lets the client do, for tests that construct
/// the client directly.
public final class UserPropertyAccess {

    private UserPropertyAccess() {
    }

    public static UserProperty received(Property.UserProperty property) {
        return UserProperty.received(property);
    }
}
