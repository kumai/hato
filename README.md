# Hato

Hato is an MQTT client for Java 25 and later. It supports MQTT 3.1.1 and 5.0 over TCP, TLS,
and WebSocket, ships as one jar with no runtime dependencies, and uses no reflection or
bytecode generation.

- Operations return [`CompletableFuture`s](hato-client/README.md#working-without-blocking).
  The handlers that process received messages
  [run on virtual threads by default, or on your own executors](hato-client/README.md#where-your-code-runs),
  and may block.
- The client [reconnects by itself](hato-client/README.md#staying-connected) and subscribes
  again after each reconnection.
- A message handler processes one message at a time by default.
  [Parallel processing, your own executors, hand-off to your own queue, and pull delivery](hato-client/README.md#choosing-how-messages-run)
  are there when you need them.

An embedded in-memory broker and a test kit are separate modules, for local development,
tests, and device simulators.

The [client guide](hato-client/README.md) is the user guide: it takes you from your first
messages to tuning, and points out the places where the client may surprise you.

## Getting it

Gradle:

```kotlin
dependencies {
    implementation("org.mqtt4j:hato-client:0.7.0")
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>hato-client</artifactId>
    <version>0.7.0</version>
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

Read [How the client works](hato-client/README.md#how-the-client-works) next: the few
behaviors that shape everything else. The client guide also shows how to:

- [connect over TLS or WebSocket](hato-client/README.md#connecting-over-tls-or-websocket),
  and [authenticate](hato-client/README.md#authenticating)
- [choose when messages are acknowledged](hato-client/README.md#choosing-when-to-acknowledge)
- [process messages in parallel or on your own threads](hato-client/README.md#choosing-how-messages-run)
- [keep messages flowing when handlers fall behind](hato-client/README.md#keeping-messages-flowing)
- [reconnect, keep a session, and shut down](hato-client/README.md#staying-connected)
- [work without blocking](hato-client/README.md#working-without-blocking), and see
  [where your code runs](hato-client/README.md#where-your-code-runs)
- [watch the client](hato-client/README.md#watching-the-client) through logs, stats, and
  Micrometer
- [test your code](hato-client/README.md#testing-your-code) without a broker
- [troubleshoot](hato-client/README.md#troubleshooting) and
  [tune settings](hato-client/README.md#tuning-the-client)

## Other guides

| Guide | What you will find |
| --- | --- |
| [Embedded broker](hato-broker/README.md) | A broker for local or trusted environments, including its supported features and limitations |
| [Test kit](hato-testkit/README.md) | Test doubles, connection failures, controlled time, and broker behavior in tests |
| [Examples](examples/README.md) | Complete programs compiled and checked by the build |

## Compatibility

Hato follows semantic versioning; before 1.0, a minor release can change the API.
The client and the test kit are covered. The embedded broker, `FaultyTransport`, and
the broker controls in the test kit are experimental and can change in any release.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for building, testing, and contributing.

## License

MIT. See [LICENSE](LICENSE).
