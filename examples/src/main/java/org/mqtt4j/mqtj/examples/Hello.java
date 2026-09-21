package org.mqtt4j.mqtj.examples;

import org.mqtt4j.mqtj.MqttVersion;

/// Placeholder until the client exists: prints the protocol versions the library will speak.
public final class Hello {

    private Hello() {
    }

    public static void main(String[] args) {
        for (MqttVersion version : MqttVersion.values()) {
            System.out.println(version + " (protocol level " + version.protocolLevel() + ")");
        }
    }
}
