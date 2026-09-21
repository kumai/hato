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

## Embedded broker

The broker is intended to start and stop with an application, without installing
a separate service. Suitable uses include local tool integration, device
simulators, demos, and temporary message routing where state can be recreated
after a restart. Applications use `mqtj-broker` directly; the test kit is optional.

All retained messages, subscriptions, sessions, and pending deliveries live in
memory and are lost when the broker instance stops or restarts. There is no disk
persistence, clustering, replication, or high availability. QoS does not imply
durability across broker restarts.

The initial scope is TCP in local or trusted environments, with loopback binding
by default and configurable limits on connections and stored data. Server-side
TLS, authentication/ACLs, and WebSocket listeners are later candidates. The
client's TLS and WebSocket support is independent of these broker features.
Limited production use is an intended use case when these constraints fit;
supported features and measured operating limits will be documented before a
stable broker release.

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
