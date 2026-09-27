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
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>mqtj-client</artifactId>
    <version>x.y.z</version>
</dependency>
```

## Publishing

Send a temperature reading:

```java
import java.net.URI;

import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;

void main() {
    try (MqttClient client = MqttClient.builder()
            .server(URI.create("mqtt://localhost:1883"))
            .version(MqttVersion.V3_1_1)
            .build()) {
        client.connect().join();
        client.publish(new MqttMessage(
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

import org.mqtt4j.mqtj.MqttClient;
import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.MqttVersion;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;

void main() {
    try (MqttClient client = MqttClient.builder()
            .server(URI.create("mqtt://localhost:1883"))
            .version(MqttVersion.V3_1_1)
            .build()) {
        client.connect().join();
        client.subscribe(
                new Subscribe(new TopicFilter("sensors/+/temperature"), Qos.AT_MOST_ONCE),
                delivery -> show(delivery.message())
        ).join();

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

- [Client](mqtj-client/README.md): configure connections, manage subscriptions,
  and handle failures.
- [Embedded broker](mqtj-broker/README.md): run a broker inside your application
  for tests, device simulators, and demos.
- [Test kit](mqtj-testkit/README.md): simulate connection failures and control time
  in your tests.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for building, testing, and contributing.

## License

MIT. See [LICENSE](LICENSE).
