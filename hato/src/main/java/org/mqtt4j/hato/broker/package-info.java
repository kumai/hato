/// An MQTT 3.1.1 and 5.0 broker that runs inside the application; [MqttBroker] starts one. It
/// keeps clean and persistent sessions in memory, with QoS 0/1/2, retained messages, and Wills,
/// and an [Authenticator] can check each client and limit its topics. Nothing is saved to disk.
@NullMarked
package org.mqtt4j.hato.broker;

import org.jspecify.annotations.NullMarked;
