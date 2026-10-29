# Hato embedded broker

Run an MQTT broker inside your application for tests, device simulators, and demos.
The broker is a standalone jar; use it with Hato or another MQTT client.

## Getting it

Gradle:

```kotlin
dependencies {
    implementation("org.mqtt4j:hato-broker:0.9.0")
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>hato-broker</artifactId>
    <version>0.9.0</version>
</dependency>
```

Use `testImplementation` or Maven's `test` scope if you only need it in tests.
For JPMS, the module name is `org.mqtt4j.hato.broker`.

## Starting and stopping

`MqttBroker.start()` starts a broker on an available local port. This example
connects a Hato client to it; add [hato-client](../README.md#getting-it) to run it.

```java
import java.io.IOException;
import java.net.URI;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.broker.MqttBroker;

void main() throws IOException {
    try (
            MqttBroker broker = MqttBroker.start();
            MqttClient client = MqttClient.builder()
                    .server(URI.create("mqtt://localhost:" + broker.port()))
                    .build()
    ) {
        client.connect().join();
        System.out.println("Connected to the embedded broker");
        client.disconnect().join();
    }
}
```

Each broker instance is independent, so tests can run with their own brokers.
Leaving the block stops the broker and disconnects its clients.

Use `MqttBroker.start(InetSocketAddress)` when you need a particular address and
port. The default listens only on loopback. Starting the broker throws
`IOException` if it cannot open the listening socket.

## Choosing where to use it

Use the broker in local or trusted environments. It does not check credentials. It
supports MQTT 3.1.1, MQTT 5.0 as far as client tests need it, QoS 0/1/2, retained
messages, and Wills. An MQTT 5.0 Will waits for its delay unless the session ends
first, and the broker drops it if the client resumes the session within the delay. A
message's Message Expiry Interval counts down while it waits in the broker, which
drops it, retained messages included, once its lifetime has passed. MQTT 5.0
subscriptions honor No Local, Retain As Published, Retain Handling, and Subscription
Identifiers, and MQTT 5.0 clients may set up to 16 Topic Aliases. The broker does
not support shared subscriptions, and tells MQTT 5.0 clients so when they connect;
it refuses `$share/` filters in MQTT 3.1.1 too. With the test kit, tests can make it
announce smaller limits to MQTT 5.0 clients, and refuse publications, subscriptions,
and connections, or end connections, with reason codes of their choice. In turn, it
keeps to the Receive Maximum and Maximum Packet Size that MQTT 5.0 clients announce,
and drops a message too large for a client instead of sending it. An MQTT 5.0
persistent session ends once its Session Expiry Interval has passed after its
connection; MQTT 3.1.1 persistent sessions do not expire.

With Clean Session false, or a positive session expiry in MQTT 5.0, subscriptions
and QoS 1/2 deliveries survive a disconnect, and reconnecting with the same client
identifier resumes them. Delivery storage is bounded, so a subscriber that falls too
far behind loses its session.

State lives in memory and is lost when the broker stops; use test fixtures or your
application to recreate it. MQTT delivery acknowledgements do not mean that data
has been saved to disk. The broker API is experimental and outside the
compatibility promise.

See the [MqttBroker Javadoc](src/main/java/org/mqtt4j/hato/broker/MqttBroker.java)
for the API contract, or the [test kit](../hato-testkit/README.md) to control the
broker from tests, simulate connection failures, and control time in client tests.
