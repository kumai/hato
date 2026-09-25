package org.mqtt4j.mqtj.nativesmoke;

import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.client.internal.common.log.Log;

/// Verifies embedded protocol values and the System.Logger facade in a native image.
public final class NativeSmoke {

    private NativeSmoke() {
    }

    public static void main(String[] args) {
        // Exercise the independent protocol copies embedded in the client and broker jars.
        var brokerVersion = org.mqtt4j.mqtj.broker.internal.common.value.MqttVersion.of(4);
        if (MqttVersion.of(5).protocolLevel() != 5 || brokerVersion.protocolLevel() != 4) {
            throw new AssertionError("Embedded protocol implementations are not usable");
        }
        Log.of(NativeSmoke.class).info("embedded implementations verified for {}", Qos.of(1));
        System.out.println("mqtj native smoke: " + Qos.of(1) + " OK");
    }
}
