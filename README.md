# Hato

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
    implementation("org.mqtt4j:hato-client:x.y.z")
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>hato-client</artifactId>
    <version>x.y.z</version>
</dependency>
```

## Publishing

Send a temperature reading:

```java
import java.net.URI;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;

void main() {
    try (MqttClient client = MqttClient.builder()
            .server(URI.create("mqtt://localhost:1883"))
            .build()) {
        client.connect().join();
        client.publish(MqttMessage.of(
                new Topic("sensors/kitchen/temperature"),
                "21.5".getBytes(UTF_8),
                Qos.AT_MOST_ONCE,
                false
        )).join();
        client.disconnect().join();
    }
}
```

## Subscribing

Print readings from every sensor as they arrive. Start it, then run the publishing
example: it prints `sensors/kitchen/temperature: 21.5`.

```java
import java.net.URI;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;

void main() {
    try (MqttClient client = MqttClient.builder()
            .server(URI.create("mqtt://localhost:1883"))
            .build()) {
        client.connect().join();
        client.subscribe(
                Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_MOST_ONCE),
                delivery -> show(delivery.message())
        ).established().join();

        IO.println("Waiting for readings. Press Enter to stop.");
        IO.readln();
        client.disconnect().join();
    }
}

void show(MqttMessage message) {
    IO.println(message.topic() + ": " + new String(message.payload(), UTF_8));
}
```

## Guides

- [Client](hato-client/README.md): connect, publish, receive messages, handle
  failures, and fit the client into your application.
- [Embedded broker](hato-broker/README.md): run a broker inside your application
  for tests, device simulators, and demos.
- [Test kit](hato-testkit/README.md): simulate connection failures and control time
  in your tests.
- [Examples](examples/README.md): compile and run the examples above, or try a
  complete round trip with the embedded broker.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for building, testing, and contributing.

## License

MIT. See [LICENSE](LICENSE).
