package org.mqtt4j.hato.broker.internal;

import java.util.ArrayList;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.mqtt4j.hato.broker.internal.common.protocol.MqttString;
import org.mqtt4j.hato.broker.internal.common.protocol.Packet;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketCodec;
import org.mqtt4j.hato.broker.internal.common.protocol.PacketType;
import org.mqtt4j.hato.broker.internal.common.protocol.Properties;
import org.mqtt4j.hato.broker.internal.common.protocol.Property;
import org.mqtt4j.hato.broker.internal.common.protocol.PropertyId;
import org.mqtt4j.hato.broker.internal.common.value.MqttVersion;
import org.mqtt4j.hato.broker.internal.common.value.ReasonCode;

// A reason that a test makes the broker give a client: a reason code, and the MQTT 5.0 Reason
// String and Server Reference to send with it, if any.
record Reason(ReasonCode code, @Nullable String reasonString, @Nullable String serverReference) {

    // The reason that a test chose for a packet of `type`, checked as the test sets it. A
    // refusal must be a failure that MQTT 5.0 allows in the packet, and DISCONNECT any reason
    // that a server may send, which leaves out Disconnect with Will Message (MQTT 5.0 section
    // 3.14.2.1). A Server Reference goes only with Use another server or Server moved, which
    // only CONNACK and DISCONNECT carry (MQTT 5.0 section 4.11). Both strings must be MQTT
    // UTF-8 strings.
    static Reason checked(
            PacketType type,
            int value,
            @Nullable String reasonString,
            @Nullable String serverReference
    ) {
        ReasonCode code = ReasonCode.of(value);
        boolean given = (type == PacketType.DISCONNECT)
                ? code != ReasonCode.DISCONNECT_WITH_WILL_MESSAGE
                : code.value() >= 0x80;
        if (!given || !PacketCodec.allowsReason(type, code)) {
            throw new IllegalArgumentException(
                    code + " is not a reason that the broker gives in " + type
            );
        }
        boolean redirects = code == ReasonCode.USE_ANOTHER_SERVER
                || code == ReasonCode.SERVER_MOVED;
        if (serverReference != null && !redirects) {
            throw new IllegalArgumentException(
                    "A Server Reference goes only with Use another server or Server moved"
            );
        }
        if (reasonString != null) {
            MqttString.validate(reasonString);
        }
        if (serverReference != null) {
            MqttString.validate(serverReference);
        }
        return new Reason(code, reasonString, serverReference);
    }

    // The MQTT 5.0 packet that `build` makes with the properties of this reason. While the
    // packet would be larger than `limit`, the client's Maximum Packet Size, the Reason String
    // and then the Server Reference are left out, the Reason String first as each packet's
    // rules require (for example [MQTT-3.4.2-2]), and the Server Reference because no packet
    // may exceed the limit [MQTT-3.1.2-24].
    <P extends Packet> P fitted(Function<Properties, P> build, int limit) {
        var properties = new ArrayList<Property>();
        if (reasonString != null) {
            properties.add(new Property.StringValue(PropertyId.REASON_STRING, reasonString));
        }
        if (serverReference != null) {
            properties.add(new Property.StringValue(PropertyId.SERVER_REFERENCE, serverReference));
        }
        while (true) {
            P packet = build.apply(new Properties(properties));
            if (properties.isEmpty()
                    || PacketCodec.of(MqttVersion.V5).encode(packet).length <= limit) {
                return packet;
            }
            properties.removeFirst();
        }
    }
}
