package org.mqtt4j.mqtj.nativesmoke;

import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.client.internal.common.log.Log;

/// Entry point of the native-image smoke test. At milestone M2 this will start the embedded
/// broker and run a publish/subscribe round trip. Until then it exercises the embedded wire model
/// and the logging facade, so that System.Logger inside a native image is covered early.
public final class NativeSmoke {

    private NativeSmoke() {
    }

    public static void main(String[] args) {
        // White-box probe until the broker has a public API: exercise both embedded copies.
        var brokerVersion = org.mqtt4j.mqtj.broker.internal.common.value.MqttVersion.of(4);
        if (MqttVersion.of(5).protocolLevel() != 5 || brokerVersion.protocolLevel() != 4) {
            throw new AssertionError("Embedded protocol implementations are not usable");
        }
        Log.of(NativeSmoke.class).info("embedded implementations verified for {}", Qos.of(1));
        System.out.println("mqtj native smoke: " + Qos.of(1) + " OK");
    }
}
