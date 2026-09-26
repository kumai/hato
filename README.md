# MQTJ

An MQTT client for Java. One jar, a plain API, and the things you need. It just works.

- **Small.** One standalone jar, no runtime dependencies.
  Add it to your application and get on with your work.
- **Plain.** Standard Java APIs, no reflection, no bytecode generation.
  Built on virtual threads, with no event loop to manage or reactive pipeline to learn.
  Your application controls where handlers run.
- **Enough.** MQTT 3.1.1 and 5.0, TLS, and WebSocket support.
  An embeddable in-memory broker and a test kit are available as separate
  modules. The things you need, without a messaging framework.

Requires Java 25 or later.

## Getting it

Gradle:

```kotlin
dependencies {
    implementation("org.mqtt4j:mqtj-client:x.y.z")
    testImplementation("org.mqtt4j:mqtj-testkit:x.y.z")
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>mqtj-client</artifactId>
    <version>x.y.z</version>
</dependency>
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>mqtj-testkit</artifactId>
    <version>x.y.z</version>
    <scope>test</scope>
</dependency>
```

The embedded broker is `org.mqtt4j:mqtj-broker`, a separate and equally
standalone jar. Client and broker do not depend on each other, so use either
alone or both together. On the module path the modules are
`org.mqtt4j.mqtj.client`, `org.mqtt4j.mqtj.broker`, and `org.mqtt4j.mqtj.testkit`.

## Connecting

```java
import java.net.URI;

import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttVersion;

try (MqttClient client = MqttClient.builder()
        .server(URI.create("mqtt://localhost:1883"))
        .version(MqttVersion.V3_1_1)
        .build()) {
    client.connect().join();
    client.disconnect().join();
}
```

`connect()` completes when the broker accepts the connection. `disconnect()` sends
DISCONNECT and closes the connection; the client can connect again afterward.
Leaving the block calls `close()`, which requests immediate, permanent shutdown
without sending DISCONNECT. For a graceful disconnect, wait for `disconnect()` to
complete before leaving the block, as above. Configure a `connectionListener` on
the builder to receive disconnection notifications. A null notification cause
means no failure is reported; it does not confirm that DISCONNECT was sent.

## Embedded broker

The experimental broker starts and stops with an application. Use `mqtj-broker`
directly; the test kit is optional. This example connects through the public APIs:

```java
import java.net.URI;

import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.broker.MqttBroker;

try (
        MqttBroker broker = MqttBroker.start();
        MqttClient client = MqttClient.builder()
                .server(URI.create("mqtt://localhost:" + broker.port()))
                .version(MqttVersion.V3_1_1)
                .build()
) {
    client.connect().join();
    client.disconnect().join();
}
```

`start()` binds a system-assigned port on loopback; `port()` returns that port.
Use `start(InetSocketAddress)` to select a bind address and port. Starting can
throw `IOException`. Leaving the block calls `close()`, which stops the listener,
closes all connections, and discards their state. Repeated close calls are harmless.

The current broker supports MQTT 3.1.1 over TCP, clean sessions, subscriptions,
and QoS 0/1 routing. Clean Session 0 and Will requests are refused with CONNACK
0x03; an empty client identifier with Clean Session 0 is refused with 0x02.
Retained publications reach current subscribers but are not stored. Credentials
are not checked, and `$share/` filters are matched literally without shared routing.
The client publish and subscribe APIs are still on the roadmap.

A connection is disconnected when it exceeds its provisional send budget of 128
packets or 16 MiB, or its 128 unacknowledged QoS 1 deliveries. The budget counts
every outgoing packet, so a single publication whose packet exceeds 16 MiB
disconnects each matching subscriber, even an idle one, while the publisher
still receives its PUBACK. These are internal limits, not configurable settings
or stable contracts. The broker API is experimental and outside the
compatibility promise.

The intended scope is local or trusted environments: tests, device simulators,
demos, and temporary routing where state can be recreated. There is no disk
persistence, clustering, replication, or high availability. QoS does not imply
durability across restarts. Persistent sessions, Wills, retained storage, and
configurable operating limits are future work. Server-side TLS, authentication/ACLs,
and WebSocket listeners are later candidates, independent of the client's support.
Limited production use remains a future goal; supported features and measured
operating limits will be documented before a stable broker release.

## Roadmap

The first evaluation release will exercise TCP/TLS, serial and pull-based
delivery, reconnection, and explicit outcomes for handler failure, overload, and
session loss. MQTT 5 and JDK-based WS/WSS follow. Interoperability with an external
broker and native-image smoke tests start early in development.

The broker has its own acceptance criteria, including an independent client,
bounded memory, restart behavior, and resource cleanup. Advanced dispatch modes,
authentication helpers, and broader broker features are later candidates rather
than prerequisites for the first evaluation release.

## Building

```
./gradlew build
```

The first run downloads Gradle and the Java 25 toolchain. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the repository layout, the checks to
run before a pull request, and the code conventions.

## License

MIT. See [LICENSE](LICENSE).
