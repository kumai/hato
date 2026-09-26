package org.mqtt4j.mqtj.nativesmoke;

import java.net.URI;

import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.client.internal.common.log.Log;

/// Verifies packaged values, client lifecycle setup, and logging in a native image.
public final class NativeSmoke {

    private NativeSmoke() {
    }

    public static void main(String[] args) {
        // Exercise the independent protocol copies embedded in the client and broker jars.
        var brokerVersion = org.mqtt4j.mqtj.broker.internal.common.value.MqttVersion.of(4);
        if (MqttVersion.of(5).protocolLevel() != 5 || brokerVersion.protocolLevel() != 4) {
            throw new AssertionError("Embedded protocol implementations are not usable");
        }
        MqttClient client = MqttClient.builder()
                .server(URI.create("mqtt://localhost"))
                .version(MqttVersion.V3_1_1)
                .build();
        client.close();
        if (!client.connect().isCompletedExceptionally()) {
            throw new AssertionError("Closed client accepted a connection");
        }
        Log.of(NativeSmoke.class).info("embedded implementations verified for {}", Qos.of(1));
        System.out.println("mqtj native smoke: " + Qos.of(1) + " OK");
    }
}
