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

## Publishing and subscribing

After connecting, register a handler and publish a message:

```java
import java.util.concurrent.CompletableFuture;

import org.mqtt4j.mqtj.MqttMessage;
import org.mqtt4j.mqtj.Qos;
import org.mqtt4j.mqtj.Subscribe;
import org.mqtt4j.mqtj.Subscription;
import org.mqtt4j.mqtj.Topic;
import org.mqtt4j.mqtj.TopicFilter;

var received = new CompletableFuture<MqttMessage>();
Subscription subscription = client.subscribe(
        new Subscribe(new TopicFilter("sensors/#"), Qos.AT_MOST_ONCE),
        delivery -> received.complete(delivery.message())
).join();
client.publish(new MqttMessage(
        new Topic("sensors/temperature"),
        new byte[] {21},
        Qos.AT_MOST_ONCE,
        false
)).join();
received.join();
subscription.unsubscribe().join();
```

QoS 0 publication completes after the transport finishes writing; it does not
confirm broker receipt. Subscription completes on SUBACK, but handlers can receive
messages before that acknowledgement. Each subscription runs its handler serially
on its own virtual thread. A handler can publish and wait for completion.
Client and subscription methods are safe to call concurrently from any thread.
`MqttPublisher` and `MqttSubscriber` expose these roles without connection ownership.
`MqttMessage` copies its payload on construction and access.

An identical filter is rejected with `IllegalStateException` while its
subscription is pending, active, or being removed. Each incoming PUBLISH goes to
every matching subscription. MQTT 3.1.1 cannot identify broker copies for
overlapping subscriptions, so each copy also reaches every match.
`$share/group/filter` is routed using the trailing filter on clients; the broker
must support shared subscriptions to use it.
If either filter is shared, overlapping subscriptions on the same client are
rejected with `IllegalStateException`. This includes different shared groups and
subscriptions awaiting SUBACK or UNSUBACK. Without a subscription identifier on
received packets, an ordinary subscription's copy could otherwise reach a shared
handler and defeat the broker's distribution. Use separate clients for such
combinations. Ordinary subscriptions can still overlap each other.

The third `Subscribe` argument sets its queued-plus-running delivery limit
(default 256). Shared internal limits currently allow 256 subscription lanes,
4096 recipient deliveries, and 32 MiB of received packet bytes (raised with
`maxPacketSize` when necessary). Shared payloads count once. Up to 256 received
packets can wait for the client's protocol thread; a longer back-to-back burst
closes the connection even when handlers are fast. A full receive budget
or a handler failure closes the connection and notifies `connectionListener`.
Handler failures are logged even when their connection has already ended. Such
late failures neither issue another disconnection notification nor affect a new
connection. Interrupt status left by a normally returning handler is cleared
before the next delivery.
Queued deliveries are discarded and counted as abandoned on connection ending or
UNSUBACK; an executing handler can finish and retains its capacity until returning.
Lane capacity is released when its worker exits, so subscribing immediately after
removal at the 256-lane limit can still be temporarily rejected.
These limits are provisional and may change after load measurements.

`unsubscribe()` stops new local admissions immediately. If its normal send budget
is full, only that operation fails and the subscription becomes active again; it
can be retried. While `disconnect()` is pending, it fails with
`MqttNotConnectedException`. `Subscription.close()` starts unsubscribe without
waiting and has no further effect while removal is pending.
Each `unsubscribe()` call returns an independent future. Changing or timing out
that future does not affect removal or later calls. Calls while removal is pending
share the wire request, but each holds an operation slot until the exchange ends,
even after its own future times out. Retrying after a timeout therefore cannot
speed up removal and can make other operations, including `disconnect()`, fail
with `MqttCapacityException`. Rejecting an additional call leaves the pending
removal unchanged.
When the connection ends, its subscription handles are retired. Later calls to
`unsubscribe()` complete locally, including after reconnecting or requesting client
shutdown; after a shutdown request they also discard queued deliveries. They do not
remove a new subscription for the same filter or change the failure of a removal
that was pending when the old connection ended.

The client admits at most 64 operations at a time. Each holds its slot until its
future completes and dependent callbacks return, so wait for results or bound the
number of outstanding operations. Queued publications can hold up to 32 MiB of
payload in total. A publication whose encoded packet exceeds the 16 MiB send
budget always fails with `MqttCapacityException`; `maxPacketSize` limits only
incoming packets, so retrying cannot succeed. A temporarily full send budget
fails the affected operation the same way and preserves the connection.
API commands and incoming packets have separate mailbox budgets, so queued
publications do not consume receive capacity. Payload copying and PUBLISH encoding
run on a separate sender, allowing the protocol thread to process incoming packets
and timers while a large publication is prepared. Send capacity includes queued,
preparing, and active writes across connection changes; capacity held by an old
connection returns once both the sender and the transport have released the data.
Disconnected publish/subscribe calls fail immediately. QoS 1/2 operations are
currently rejected.

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
The client currently supports QoS 0 publishing and push subscriptions; QoS 1
and pull subscriptions are planned next.

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
