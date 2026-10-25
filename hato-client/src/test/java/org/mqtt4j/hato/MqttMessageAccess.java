package org.mqtt4j.hato;

import org.mqtt4j.hato.client.internal.common.protocol.Packet;

/// Makes and reads messages without copying their arrays, as the builder lets the client do, for
/// tests that construct the client directly.
public final class MqttMessageAccess {

    private MqttMessageAccess() {
    }

    public static MqttMessage received(Packet.Publish publish) {
        return MqttMessage.received(publish);
    }

    public static byte[] payloadArray(MqttMessage message) {
        return message.payloadArray();
    }
}
