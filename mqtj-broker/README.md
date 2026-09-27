# MQTJ embedded broker

Run an MQTT broker inside your application for tests, device simulators, and demos.
The broker is a standalone jar; use it with MQTJ or another MQTT client.

## Getting it

Gradle:

```kotlin
dependencies {
    implementation("org.mqtt4j:mqtj-broker:x.y.z")
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>mqtj-broker</artifactId>
    <version>x.y.z</version>
</dependency>
```

Use `testImplementation` or Maven's `test` scope if you only need it in tests.
For JPMS, the module name is `org.mqtt4j.mqtj.broker`.

## Starting and stopping

`MqttBroker.start()` starts a broker on an available local port. This example
connects an MQTJ client to it; add [mqtj-client](../README.md#getting-it) to run it.

```java
import java.io.IOException;
import java.net.URI;

import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.broker.MqttBroker;

void main() throws IOException {
    try (
            MqttBroker broker = MqttBroker.start();
            MqttClient client = MqttClient.builder()
                    .server(URI.create("mqtt://localhost:" + broker.port()))
                    .version(MqttVersion.V3_1_1)
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

Use the broker in local or trusted environments. It does not check credentials.
State lives in memory and is lost when the broker stops; use test fixtures or your
application to recreate it. MQTT delivery acknowledgements do not mean that data
has been saved to disk.

See the [MqttBroker Javadoc](src/main/java/org/mqtt4j/mqtj/broker/MqttBroker.java)
for the API contract, or the [test kit](../mqtj-testkit/README.md) to simulate
connection failures and control time in client tests.
