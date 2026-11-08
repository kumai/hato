<p align="center">
  <img src="site/src/assets/seal-hakubun.png" alt="" width="96" height="96">
</p>

<h1 align="center">Hato</h1>

<p align="center">
  An MQTT client for Java.<br>
  <a href="https://hato.mqtt4j.org">Website</a> ·
  <a href="hato-client/README.md">User Guide</a> ·
  <a href="https://hato.mqtt4j.org/api/client/">API</a> ·
  <a href="examples/README.md">Examples</a>
</p>

## Features

- MQTT 5.0 and 3.1.1, over TCP, TLS, and WebSocket.
- Java 25 or later, with no runtime dependencies and no reflection.
- Message handlers may block, and run one message at a time unless you
  [choose otherwise](hato-client/README.md#choosing-how-messages-run).
- Operations return `CompletableFuture`s: join one, or chain the next step.
- The client reconnects and subscribes again by itself.
- [Stats and timings](hato-client/README.md#watching-the-client) for monitoring are built in.
- A [test kit](hato-testkit/README.md) tests your code without a broker, including lost
  connections and timeouts.
- An [embedded broker](hato-broker/README.md) for local development, tests, and device
  simulators.

## Getting it

Gradle:

```kotlin
dependencies {
    implementation("org.mqtt4j:hato-client:1.0.2")
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>hato-client</artifactId>
    <version>1.0.2</version>
</dependency>
```

## A first program

This program needs an MQTT broker listening at `mqtt://localhost:1883`. It subscribes to
temperature readings, publishes one, and prints it as it comes back:
`sensors/kitchen/temperature: 21.5`. Only the broker address is required; the other settings
use their defaults.

```java
import java.net.URI;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;

void main() {
    try (
            MqttClient client = MqttClient.builder()
                    .server(URI.create("mqtt://localhost:1883"))
                    .build()
    ) {
        client.connect().join();
        // The message handler runs for each message that the subscription receives.
        client.subscribe(
                Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_LEAST_ONCE),
                delivery -> show(delivery.message())
        ).established().join();

        client.publish(
                MqttMessage.of(
                        new Topic("sensors/kitchen/temperature"), "21.5".getBytes(UTF_8),
                        Qos.AT_LEAST_ONCE,
                        false
                )
        ).join();

        IO.println("Press Enter to stop.");
        IO.readln();
        client.disconnect().join();
    }
}

void show(MqttMessage message) {
    IO.println(message.topic() + ": " + new String(message.payload(), UTF_8));
}
```

To try Hato without setting up a broker, clone the repository and run
`./gradlew :examples:runHello`, which starts its own embedded broker.

## Next steps

The [client guide](hato-client/README.md) is the user guide: it takes you from your first
messages to tuning, and points out the places where the client may surprise you. Read
[How the client works](hato-client/README.md#how-the-client-works) first: the few behaviors
that shape everything else. The guide also shows how to:

- [connect over TLS or WebSocket](hato-client/README.md#connecting-over-tls-or-websocket),
  and [authenticate](hato-client/README.md#authenticating)
- [choose when messages are acknowledged](hato-client/README.md#choosing-when-to-acknowledge)
- [keep messages flowing when handlers fall behind](hato-client/README.md#keeping-messages-flowing)
- [reconnect, keep a session, and shut down](hato-client/README.md#staying-connected)
- [work without blocking](hato-client/README.md#working-without-blocking), and see
  [where your code runs](hato-client/README.md#where-your-code-runs)
- [troubleshoot](hato-client/README.md#troubleshooting) and
  [tune settings](hato-client/README.md#tuning-the-client)

## Other guides

- [Embedded broker](hato-broker/README.md): A broker for local or trusted environments, including its supported features and limitations
- [Test kit](hato-testkit/README.md): Test doubles, connection failures, controlled time, and broker behavior in tests
- [Examples](examples/README.md): Complete programs compiled and checked by the build

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for building, testing, and contributing.

## License

MIT. See [LICENSE](LICENSE).
