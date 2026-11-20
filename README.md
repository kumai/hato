<p align="center">
  <img src="site/src/assets/seal-hakubun.png" alt="" width="96" height="96">
</p>

<h1 align="center">Hato</h1>

<p align="center">
  An MQTT client and broker for Java.<br>
  <a href="https://hato.mqtt4j.org">Website</a> ·
  <a href="https://hato.mqtt4j.org/api/hato/">API</a> ·
  <a href="examples/README.md">Examples</a>
</p>

## Features

Hato is an MQTT 5.0 and 3.1.1 client and broker for Java 25 or later. Both come in one jar,
with no runtime dependencies.

- **Client.** It connects to any MQTT broker over TCP, TLS, or WebSocket. Message handlers
  are plain code that may block: each subscription hands its handler one message at a time
  and acknowledges it when the handler returns, and you can [run handlers in parallel, on
  your own executors, or on your own threads](#choosing-how-messages-run), or
  [acknowledge later](#choosing-when-to-acknowledge).
- **Broker.** It [runs inside your application](#running-a-broker-in-your-application),
  serves MQTT clients over TCP and TLS, checks them with your own code, and keeps its memory
  bounded. Hato clients in the same JVM reach it without a socket.
- **Metrics.** The client's [stats and timings](#watching-the-client) and the broker's
  [stats and notifications](#watching-and-managing-clients) are built in, ready to feed to
  Micrometer or another metrics library.
- **Tests.** The [test kit](hato-testkit/README.md) tests your code without a broker,
  including lost connections and timeouts, and controls the broker in tests that run one.

## Contents

- [Getting started](#getting-started)
  - [Getting it](#getting-it)
  - [A first program](#a-first-program)
- [Using the client](#using-the-client)
  - [How the client works](#how-the-client-works)
  - [Connecting to a broker](#connecting-to-a-broker)
  - [Publishing](#publishing)
  - [Subscribing](#subscribing)
  - [Choosing how messages run](#choosing-how-messages-run)
  - [Keeping messages flowing](#keeping-messages-flowing)
  - [Staying connected](#staying-connected)
  - [Handling failures](#handling-failures)
  - [Working without blocking](#working-without-blocking)
  - [Where your code runs](#where-your-code-runs)
  - [Passing the client to your code](#passing-the-client-to-your-code)
  - [Watching the client](#watching-the-client)
  - [Troubleshooting](#troubleshooting)
  - [Tuning the client](#tuning-the-client)
- [Running a broker in your application](#running-a-broker-in-your-application)
  - [Starting and stopping a broker](#starting-and-stopping-a-broker)
  - [Connecting clients in the same JVM](#connecting-clients-in-the-same-jvm)
  - [Opening the broker to a network](#opening-the-broker-to-a-network)
  - [Checking clients and their topics](#checking-clients-and-their-topics)
  - [Listening with TLS](#listening-with-tls)
  - [What the broker supports](#what-the-broker-supports)
  - [Watching and managing clients](#watching-and-managing-clients)
  - [Stopping the broker](#stopping-the-broker)
  - [Broker settings](#broker-settings)
- [Testing your application](#testing-your-application)
  - [Testing without a broker](#testing-without-a-broker)
  - [Testing with a broker](#testing-with-a-broker)
- [Other guides](#other-guides)

## Getting started

### Getting it

Gradle:

```kotlin
dependencies {
    implementation("org.mqtt4j:hato:1.1.0")
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>hato</artifactId>
    <version>1.1.0</version>
</dependency>
```

Requires Java 25 or later. For JPMS, the module name is `org.mqtt4j.hato`.
The Javadoc of each type gives its exact behavior, starting with
[MqttClient](hato/src/main/java/org/mqtt4j/hato/MqttClient.java).

### A first program

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

The lambda passed to `subscribe()` is the subscription's message handler: your code, which
the client calls with each message that the subscription receives. It gets an `MqttDelivery`,
whose `message()` is the received message.

To try Hato without setting up a broker, clone the repository and run
`./gradlew :examples:runHello`, which starts its own embedded broker;
[Examples](examples/README.md) has the complete sources.

The examples below omit imports and application objects such as `repository`. Most of them
wait with `join()` to stay short; [Working without blocking](#working-without-blocking) shows
how to chain the next step instead.

## Using the client

The client connects to any MQTT 3.1.1 or 5.0 broker,
[Hato's own](#running-a-broker-in-your-application) included. Give it a broker address,
connect, and publish or subscribe; the defaults suit most applications. When yours needs
more, you choose where and how messages are processed, when they are acknowledged, and how
much the client holds.

### How the client works

A few behaviors shape everything below, and most surprises come from one of them:

- **It stays connected.** `build()` does not connect. Once `connect()` succeeds, the client
  reconnects by itself whenever the connection drops, until you call `disconnect()` or
  `close()`. A failed first attempt fails `connect()` unless you
  [ask the client to retry](#choosing-when-to-reconnect).
- **Subscriptions outlast connections.** `subscribe()` registers a subscription, even before
  `connect()`, and the client subscribes again after each reconnection. Subscribe once and
  keep the handle.
- **Your handler may block.** The client calls a subscription's message handler for one
  message at a time, apart from the network, by default on a thread of the client or
  on an executor that you choose, and other subscriptions' handlers run meanwhile. The handler
  can write to a database, or publish and wait for the result. Only a subscription that you
  mark as [inline](#running-handlers-as-part-of-receiving) promises the opposite: its handler
  returns at once, and the connection waits for it.
  [Where your code runs](#where-your-code-runs) covers every callback.
- **Acknowledgements keep arrival order.** The client acknowledges QoS 1 and 2 messages in the
  order they arrived, across all subscriptions of a connection, so one slow handler or one
  missing `ack()` holds up the rest; see [Keeping messages flowing](#keeping-messages-flowing).
- **Nothing waits for a connection.** `publish()` fails with `MqttNotConnectedException`
  while the client is not connected, including while it reconnects. The client does not queue
  publications for later.
- **Results arrive as futures.** Operations return a `CompletableFuture` and report failures
  through it. Wait for it with `join()`, or [chain your next step](#working-without-blocking)
  without blocking. Cancelling or timing out a future does not cancel the operation. The
  client and its subscriptions are safe to use from any thread; the builder is not.

### Connecting to a broker

```java
MqttClient client = MqttClient.builder()
        .server(URI.create("mqtt://broker.example.com:1883"))
        .clientId("sensor-17")
        .keepAlive(Duration.ofSeconds(30))
        .build();
client.connect().join();
```

`server` is required, and the other settings have defaults; see
[Tuning the client](#tuning-the-client). Omitting `clientId` generates a random one. Give every
running client its own `clientId`: a broker closes the older connection of two with the same
identifier, and with a persistent session, the one that connects later also discards the
other's session.

#### Connecting over TLS or WebSocket

Choose the transport with the URI scheme:

| URI | Connects over | Default port |
| --- | --- | ---: |
| `mqtt://broker.example.com` | TCP | 1883 |
| `mqtts://broker.example.com` | TLS | 8883 |
| `ws://broker.example.com:8080/mqtt` | WebSocket | 80 |
| `wss://broker.example.com/mqtt` | WebSocket over TLS | 443 |
| `inproc://fleet` | Memory, to an embedded broker in the same JVM | |

- TLS uses the JDK's default trust and verifies the broker hostname. Supply
  `sslContext(context)` for your own CA or a client certificate, and
  `sslParameters(parameters -> ...)` for SNI, ALPN, or protocol settings. The client uses the
  context that you give it, so initialize it before `build()` and leave it unchanged while the
  client runs.
- For WebSocket, use the port and path of the broker's WebSocket listener. The client uses the
  JDK's WebSocket client and connects directly, without the JVM's HTTP proxy settings. `wss`
  takes the same `sslContext(context)`, but `build()` rejects TLS parameters that it cannot
  apply, such as ALPN.
- Over WebSocket, the JDK can miss a connection that ends abruptly. The client then finds out
  through keep-alive, within about twice the `keepAlive` interval; shorten the interval if
  that is too long for you.
- `inproc` reaches an [embedded broker](#connecting-clients-in-the-same-jvm) by its name,
  without a socket. The client works as over TCP, subscriptions and their
  handlers included, and takes no TLS settings or transport factory.

#### Authenticating

For username/password authentication, add
`credentials(() -> MqttCredentials.of(username, passwordBytes))`, or
`MqttCredentials.of(username)` for a username alone. The client calls the supplier before
every connection attempt, so it can fetch a fresh token each time. Hato authenticates with
these credentials and TLS client certificates; it does not support MQTT 5.0 enhanced
authentication, such as SCRAM.

#### Connecting to an MQTT 3.1.1 broker

The client speaks MQTT 5.0. For a broker without it, choose `version(MqttVersion.V3_1_1)`.
The features marked MQTT 5.0 in this guide are then unavailable: the client refuses messages
with properties and subscriptions with options, and `build()` rejects a Will with properties
or a delay. A positive `sessionExpiry` still keeps the session, but the broker decides for how
long.

### Publishing

The [first program](#a-first-program) publishes an immutable `MqttMessage`. Its payload is
bytes, in the encoding that your subscribers expect. A `Topic` names one destination, while a
subscription's `TopicFilter` can contain wildcards. The QoS decides what a completed
`publish()` future tells you:

| QoS | Once the future completes |
| --- | --- |
| `AT_MOST_ONCE` (0) | The client has sent the message; the broker may not have received it |
| `AT_LEAST_ONCE` (1) | The broker has the message; subscribers may receive it more than once |
| `EXACTLY_ONCE` (2) | The broker has received the message once |

None of them tells you that a subscriber has processed the message, and QoS 2 does not make
your application's own effects, such as database writes, happen exactly once.

`whenConnected()` tells you that a connection is up, but it can end before your next
`publish()`. If the connection ends while a publication is in progress, the broker may or may
not have it, so publishing it again can deliver it twice. With a
[persistent session](#keeping-a-session-between-connections), unfinished QoS 1 and 2
publications continue after the client reconnects.

Set `retain` to `true` to have the broker keep the latest message on a topic for future
subscribers. A retained publication with an empty payload clears it.

#### Publishing many messages at once

A client keeps up to 64 QoS 1 and 2 publications unfinished at once. One more fails at once
with `MqttCapacityException` instead of waiting, so a fast publisher should let earlier
futures complete before publishing more. This loop waits only while 64 are unfinished, and
each publication frees its place when it completes:

```java
// As many places as the client's default limit.
var unfinished = new Semaphore(64);
for (MqttMessage reading : readings) {
    unfinished.acquire();
    // Runs when the publication completes.
    client.publish(reading).whenComplete((_, failure) -> {
        unfinished.release();
        if (failure != null) {
            System.err.println("Not published: " + failure);
        }
    });
}
```

Set `maxInflightPublishes(n)` on the builder to keep more or fewer. An MQTT 5.0 broker can
take fewer at a time with its Receive Maximum; the client holds the rest back, and they count
toward the limit.

#### Adding message properties (MQTT 5.0)

A message can carry properties, such as a content type, a lifetime, and name-value pairs of
your own:

```java
MqttMessage reading = MqttMessage.of(topic, payload, Qos.AT_LEAST_ONCE, false)
        .withPayloadUtf8(true)
        .withContentType("application/json")
        .withMessageExpiry(Duration.ofMinutes(5))
        .withUserProperties(List.of(new UserProperty("site", "plant-3")));
```

Subscribers read them from the received message, for example with `contentType()`. For a
request that expects a reply, set `withResponseTopic(...)` and `withCorrelationData(...)`.

Keep control characters, such as a newline, out of topics and properties: MQTT lets brokers
treat them as malformed, and many end the connection over them. The client sends none, so a
publication with one fails with `IllegalArgumentException` instead, while the connection goes
on.

### Subscribing

#### Handling each message

```java
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_LEAST_ONCE),
        delivery -> repository.save(delivery.message())
);
subscription.established().join();
```

The second argument is the subscription's message handler, a `Consumer<MqttDelivery>`. The
client calls it with each message that the subscription receives: `delivery.message()`
returns the message, and `delivery.ack()` acknowledges it when you
[acknowledge it yourself](#choosing-when-to-acknowledge).

- `subscribe()` returns a handle at once, even before `connect()`. `established()` completes
  with the granted QoS when the broker accepts the subscription, and fails if the broker
  refuses it. It does not connect: call `connect()` before you wait for it.
- The handler runs one message at a time, in arrival order.
  [Choosing how messages run](#choosing-how-messages-run) shows the other ways.
- A client holds up to 4,096 subscriptions by default; `maxSubscriptions(n)` raises or lowers
  that. Subscribing to the same filter again, even with other options, fails through
  `established()`: keep and reuse the first handle.

#### Stopping a subscription

`unsubscribe()` stops new messages and lets the queued ones finish; its future completes when
the broker has removed the subscription. `subscription.close()` instead stops delivery at once
and drops the queued messages. Either way, `subscription.terminated()` completes once the
handler has returned for the last message. Once removal completes, the same filter can be
subscribed again.

To replace a handler without giving up the subscription, have the handler call a delegate that
your application swaps through an atomic reference.

#### Choosing when to acknowledge

The client acknowledges a QoS 1 or 2 message to the broker once the subscription's `AckMode`
allows it:

| `AckMode` | Allows the acknowledgement | Default for |
| --- | --- | --- |
| `AFTER_HANDLER` | When the handler returns normally | Subscriptions with a handler |
| `ON_RECEIVE` | When your code receives the message, before processing it | Hand-off and pull subscriptions |
| `MANUAL` | When you call `delivery.ack()` | |

Set it on the request: `Subscribe.of(filter, Qos.AT_LEAST_ONCE).withAckMode(AckMode.MANUAL)`.

Start with `AFTER_HANDLER` when the handler does all the work. If the handler only puts the
work in a queue or starts asynchronous work, returning is not the end of it: use `MANUAL` and
acknowledge once the work is done, as in
[Acknowledging after asynchronous work](#acknowledging-after-asynchronous-work). Hand-off and
pull subscriptions cannot use `AFTER_HANDLER`.

Choose `ON_RECEIVE` when you can afford to lose a message whose processing fails or is cut
short: a later failure cannot take the acknowledgement back. For a hand-off subscription, the
message is acknowledged once your queue takes it, before your worker has processed it.

With `MANUAL`, call `ack()` for every delivery. It is safe from any thread, and repeated calls
and calls for QoS 0 do nothing. A message that you never acknowledge holds up the
acknowledgements of every later one. [MqttDelivery](hato/src/main/java/org/mqtt4j/hato/MqttDelivery.java)
describes acknowledgement across reconnections.

A message that is not yet acknowledged when the connection ends comes back only when the
client resumes a [persistent session](#keeping-a-session-between-connections).

#### Setting subscription options (MQTT 5.0)

`withNoLocal(true)` leaves out your own publications, `withRetainAsPublished(true)` keeps the
retain flag that the publisher set, and `withRetainHandling(RetainHandling.DO_NOT_SEND)` skips
the retained messages when you subscribe. `RetainHandling.SEND_IF_NEW_SUBSCRIPTION` sends them
only for a subscription that the broker does not have yet.

#### Overlapping and shared subscriptions

Filters may overlap, and each matching subscription receives its own delivery of a message. A
broker can also send a message once for each overlapping subscription. With MQTT 5.0
Subscription Identifiers, each such copy reaches only the subscription it was meant for.
Without them, on MQTT 3.1.1 or with a broker that does not support them, every copy reaches
every matching subscription, so your handler can receive the same message twice.

Shared subscriptions use `$share/{group}/{filter}`. A shared filter cannot overlap any other
filter on the same client, shared or not: `established()` fails for the subscription that
would overlap. Use separate clients when you need those filters together.

### Choosing how messages run

With a message handler alone, as in [Handling each message](#handling-each-message), a
subscription runs its messages one at a time, in arrival order. The other ways change where
your code runs, how many messages run at once, which order is kept, and what waits while your
code blocks:

| When you need | Use | Order | At once | While it blocks |
| --- | --- | --- | --- | --- |
| The simplest code | [`client.subscribe(request, handler)`](#handling-each-message) | arrival | one | its subscription waits |
| Your own thread or pool | [`DeliveryOptions.serial().withExecutor(pool)`](#running-handlers-on-your-executor) | arrival | one | its subscription waits |
| The threads that own each device's state | [`DeliveryOptions.lanes(selector)`](#running-each-device-on-its-own-thread) | per lane | one per lane | its lane waits |
| Each device in order, devices in parallel | [`DeliveryOptions.striped(n, key)`](#running-devices-in-parallel) | per key | up to `n` | its stripe waits |
| Independent messages, as fast as possible | [`DeliveryOptions.parallel(n)`](#running-independent-messages-in-parallel) | none | up to `n` | the others go on |
| The least overhead, for a handler that returns at once | [`DeliveryOptions.inline()`](#running-handlers-as-part-of-receiving) | arrival | one | the whole connection waits, even its keep-alive |
| A queue that your application owns | [`client.handOff(request, mailbox::offer)`](#handing-messages-to-your-own-mailbox) | arrival, one order across hand-offs | your choice | in the acceptor, every hand-off waits |
| Your own threads taking messages | [`client.subscribe(request)` and `receive()`](#receiving-on-your-own-threads) | arrival, as taken | one per thread | the other threads go on |

- Start with a handler alone. When it is slower than messages arrive, such as a wildcard
  subscription whose handler writes each reading to a database, messages queue up and their
  acknowledgements wait behind them. Striping by device lets devices go on independently
  while each keeps its order, and with enough stripes the processing can keep up.
- Choose by the order that you need, not by overhead: running messages on stripes or in
  parallel costs little next to a handler that does real work. Inline delivery saves the
  hand-off to another thread, which matters only for a handler that takes microseconds.
- Use lanes when the work must run on threads that your application already owns. Hand off
  when your application already has a queue, such as a mailbox that also takes work of its
  own. Hand off rather than offer to your queue from a handler: an acceptor can refuse a
  message when your queue is full, which the client then treats as
  [a full buffer](#when-a-buffer-is-full), while a handler can only drop it, fail, or wait.
  Receive on your own threads when your loop should decide when to take each next message,
  for example with a timeout.
- Whichever you choose, a message that waits behind a blocked handler can hold back the
  acknowledgements of later QoS 1 and 2 messages from every subscription; see
  [Keeping messages flowing](#keeping-messages-flowing).

#### Running handlers on your executor

Pass `DeliveryOptions` to run a subscription's handler on an executor that your application
owns, such as a shared thread pool or the thread of your own event loop:

```java
// A pool that your application owns; the client never shuts it down.
ExecutorService pool = Executors.newFixedThreadPool(4);

Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.EXACTLY_ONCE),
        // One message at a time, as without options, but on a thread of your pool.
        DeliveryOptions.serial().withExecutor(pool),
        delivery -> repository.save(delivery.message())
);
```

- The handler still runs one message at a time, in arrival order, even on a pool of many
  threads.
- Waiting messages stay in the subscription's queue, and the subscription holds none of the
  executor's threads while no message waits, so a busy subscription cannot flood the executor.
- An executor that rejects a task fails that message as a throwing handler would under
  `FailurePolicy.STOP`, whatever the subscription's policy, since your handler has not run.
- The executor stays yours: the client never shuts it down. Shut it down once
  `client.terminated()` completes.

#### Running each device on its own thread

To choose the executor for each message yourself, such as the single thread that owns a
device's state, pick a lane, an executor, for every message:

```java
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("devices/+/events"), Qos.AT_LEAST_ONCE),
        // Each message runs on the executor, its lane, that laneFor picks for its device.
        DeliveryOptions.lanes(message -> laneFor(message.topic())),
        delivery -> devices.apply(delivery.message())
);
```

- Messages on the same lane run one at a time in arrival order, and different lanes run in
  parallel. Return the same executor object for the same lane, such as one from a fixed set of
  single-thread executors.
- A lane function that throws or returns null fails that message as a throwing handler would.
- [MailboxLanes](examples/src/main/java/org/mqtt4j/hato/examples/MailboxLanes.java) runs
  each device on its own single-thread executor in a complete program.

#### Running devices in parallel

To keep each device's messages in order while different devices run at the same time, stripe
the subscription by a key:

```java
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("devices/+/events"), Qos.AT_LEAST_ONCE),
        // Up to 16 messages at once; messages with the same device ID run in order.
        DeliveryOptions.striped(16, message -> deviceId(message.topic())),
        delivery -> devices.apply(delivery.message())
);
```

- Messages with equal keys run one at a time in arrival order. The handler processes at most
  16 messages of this subscription at once, by default each on a thread of the client.
- `DeliveryOptions.striped(16)` uses each message's topic as its key.
- The key's hash picks one of the 16 stripes, so two devices can share a stripe and then wait
  for each other.
- Add `.withExecutor(pool)` to run the stripes on your own executor.

#### Running independent messages in parallel

When messages do not depend on each other, run them in any order:

```java
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("jobs/+/requests"), Qos.AT_LEAST_ONCE),
        // Up to 16 messages at once, in any order.
        DeliveryOptions.parallel(16),
        delivery -> jobs.run(delivery.message())
);
```

- Up to 16 messages run at once, in no particular order: a later message can finish before an
  earlier one, even one from the same device.
- `.withExecutor(pool)` works here too.

#### Running handlers as part of receiving

A handler that only updates memory and returns at once can run as part of receiving its
message, which spares the client handing each message to another thread:

```java
Map<Topic, MqttMessage> latest = new ConcurrentHashMap<>();

Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_LEAST_ONCE),
        // The handler only stores the reading, so it may run as part of receiving.
        DeliveryOptions.inline(),
        delivery -> latest.put(delivery.message().topic(), delivery.message())
);
```

- Messages run one at a time, in arrival order, as with a handler alone.
- While the handler runs, the connection may read nothing else: no messages for your other
  subscriptions, and no answers to your publications or to the keep-alive. A handler that
  blocks holds up the whole connection, and one that runs for about the keep-alive interval
  can lose it.
- Never wait for this client inside the handler. `join()` and the `get` methods of the futures
  that it returns, and `receive()` of its pull subscriptions, throw `IllegalStateException`
  there, even when the result is already in. Read a completed result with `getNow()`, or
  chain the next step instead; a publication that you start without waiting goes out as usual.
  Other waits are not detected.
- The buffer, the acknowledgement mode, and the failure policy work as with a handler alone.
  `.withExecutor(pool)` does not apply.

#### Handing messages to your own mailbox

When your application already runs its work through its own queue, such as one mailbox per
device, hand each message to that queue directly. In place of a message handler, give an
acceptor, here `mailbox::offer`: the client offers it each message and lets go of the message
once the acceptor takes it:

```java
// A queue that your application owns and its workers take from.
var mailbox = new ArrayBlockingQueue<MqttDelivery>(10_000);

// handOff() in place of subscribe(), with an acceptor in place of a message handler.
Subscription subscription = client.handOff(
        Subscribe.of(new TopicFilter("devices/+/telemetry"), Qos.AT_LEAST_ONCE),
        // Returns true if the mailbox took the message, and false if it is full.
        mailbox::offer
);
```

- Return `true` to take a message; from then on it is yours to process or drop.
- Return `false` when your mailbox is full. The client then drops a QoS 0 message for this
  subscription and ends the connection for a QoS 1 or 2 message, as
  [When a buffer is full](#when-a-buffer-is-full) describes.
- The client offers the messages of all its hand-off subscriptions one at a time, in the order
  they arrive, so your mailbox sees one order across them, and `delivery.filter()` tells which
  subscription each message came through. Never wait in the acceptor: use `offer`, not `put`,
  or the other hand-off subscriptions wait too.
- To stop, close the subscription, wait for `subscription.terminated()`, and then drain or
  discard your mailbox: nothing is offered after that. Each message reports
  `delivery.connectionAttempt()`, so messages kept across a reconnection can be told apart
  from new ones.
- [HandOffToMailbox](examples/src/main/java/org/mqtt4j/hato/examples/HandOffToMailbox.java)
  acknowledges each message after processing it and shuts down in the order a mailbox needs.

#### Receiving on your own threads

Omit the message handler to create a pull subscription, whose messages your own threads take.
Call `receive()` from one or more application threads:

```java
// No message handler: subscribe() returns a PullSubscription.
PullSubscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_LEAST_ONCE)
                // Acknowledge each message yourself once it is saved.
                .withAckMode(AckMode.MANUAL)
);
subscription.established().join();

// Your thread takes each message until the subscription ends.
while (subscription.receive() instanceof ReceiveResult.Received(var delivery)) {
    repository.save(delivery.message());
    delivery.ack();
}
```

- Each message goes to one receiving thread.
- `receive()` returns `ReceiveResult.Received` for each message and `ReceiveResult.Ended` once
  the subscription ends. `receive(Duration)` can also return `ReceiveResult.TimedOut`.
- A waiting `receive()` keeps waiting across reconnections, but messages not yet taken are
  dropped when the connection ends.

### Keeping messages flowing

Each subscription holds up to 1,024 deliveries by default. A delivery keeps its place until
your code is done with it:

| Delivery style | A delivery keeps its place until |
| --- | --- |
| Message handler | The handler returns and, for QoS 1 and 2, the message's acknowledgement has its turn |
| Pull | `receive()` returns it; with `MANUAL`, a QoS 1 or 2 delivery until `ack()` |
| Hand-off | Your acceptor takes it; from then on it takes room in your queue instead |

The client also limits the deliveries and bytes that it holds across all subscriptions,
whatever their buffer sizes.

#### Acknowledgements keep arrival order

The client acknowledges QoS 1 and 2 messages in the order they arrived, across all
subscriptions of a connection. It acknowledges a message that several subscriptions receive
once each of them allows it. A message whose acknowledgement waits, in a queue, in a slow
handler, or for an `ack()` that never comes, therefore holds back the acknowledgements of all
later ones, from every subscription. Those later messages keep their places in their buffers
until their turn, even when the handler has finished with them. Striped and parallel delivery
do not change this order.

#### When a buffer is full

| Received QoS | What the client does |
| --- | --- |
| 0 | Drops messages to stay within the buffers, possibly before one is full, and goes on. Which messages it drops, and when, may change between versions. |
| 1 or 2 | Ends the connection rather than hold more, which affects every subscription on it. |

What counts is the QoS of the received message, which can be higher than a subscription
requested when another subscription of the client also matches. A hand-off acceptor that
returns `false` refuses the message: the client drops a QoS 0 message for that subscription,
ends the connection for a QoS 1 or 2 message, and does not offer it again.
`FailurePolicy.SKIP` does not change any of this. If you cannot lose a message, receive it
with QoS 1 or 2.

When the connection ends, queued deliveries are discarded; a handler that is already
processing a message can finish. The broker sends unacknowledged messages again only when the
client resumes a [persistent session](#keeping-a-session-between-connections).
[Stats](#checking-what-the-client-holds) count drops by reason.

#### Keeping up

- Process messages as fast as they arrive. [Striped or parallel delivery](#choosing-how-messages-run)
  spreads the work while keeping the order you need.
- Size the buffer for bursts, such as retained messages arriving when you subscribe:
  `Subscribe.of(filter, qos).withBufferCapacity(4096)`. A larger buffer absorbs a burst; it
  cannot keep up with processing that is always slower than arrivals.
- Acknowledge before processing with `AckMode.ON_RECEIVE` when you can afford to lose a
  message whose processing fails or is cut short.
- Receive slow topics with a separate client, which has its own connection and its own order.
- Take and acknowledge every message: a message that nobody acknowledges can also keep the
  broker from sending later QoS 1 and 2 messages. If you cannot process one, end the
  connection.
- Watch [what the client holds](#checking-what-the-client-holds), such as how long the oldest
  acknowledgement has waited, before and after you change a setting.

### Staying connected

#### Choosing when to reconnect

The client waits longer after each failed attempt to reconnect. Once it is back, it restores
subscriptions, and a persistent session's unfinished QoS 1 and 2 publications resume.

By default, `connect()` fails if its first attempt fails, for example because the broker
cannot be reached or refuses the credentials, so a configuration error shows up at startup.
To keep trying instead, or to change the delays, set a policy on the builder:

```java
ReconnectPolicy policy = ReconnectPolicy
        .exponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30))
        .withInitialRetry();
MqttClient client = MqttClient.builder()
        .server(URI.create("mqtt://broker.example.com:1883"))
        .reconnectPolicy(policy)
        .build();
```

With initial retry, `connect()` keeps waiting until an attempt succeeds; call `disconnect()`
to stop trying. To wait until the client is connected without starting a
connection, use `whenConnected()`.

The client stays disconnected after an MQTT 5.0 broker ends the connection for an
administrative action, and keeps retrying after an unspecified error. To reverse either, add
`withAdministrativeActionRetry()` or `withoutUnspecifiedErrorRetry()` to the policy.
`ReconnectPolicy.disabled()` reconnects only when you call `connect()` again.

#### Reacting to connection changes

The `connectionListener` is called whenever a connection ends, whether you disconnected or
not. A lambda, such as `connectionListener(end -> System.out.println("Connection ended: " + end))`,
handles just these endings. Override the other listener methods to observe successful
connections, failed connection attempts, and subscriptions that the broker refuses to
re-create:

```java
var listener = new ConnectionListener() {
    @Override
    public void onConnected(ConnAck connAck) {
        System.out.println("Connected: " + connAck);
    }

    @Override
    public void onDisconnected(Disconnection end) {
        System.out.println("Connection ended: " + end);
    }

    @Override
    public void onConnectFailed(Disconnection failure) {
        System.err.println("Connection attempt failed: " + failure);
    }

    @Override
    public void onResubscribeFailed(
            Subscription subscription,
            MqttSubscriptionRefusedException failure
    ) {
        System.err.println("Subscription ended: " + subscription.filter() + ": " + failure);
    }
};
```

- Pass it with `connectionListener(listener)` when building the client. Notifications carry
  the attempt's number, and endings tell when the client reconnects.
- Once connected, or with `withInitialRetry()`, the client keeps retrying after refusals such
  as bad credentials. To stop, call `disconnect()` from `onConnectFailed`.
- An MQTT 5.0 broker that ends the connection says why: the ending's cause is an
  `MqttBrokerDisconnectException` with the broker's reason code. The client stays disconnected
  when the broker bans it, hands its session to another client with the same `clientId`, or
  sends it to another server, named by `serverReference()`.
- A subscription that the broker refuses to re-create ends; the connection and the other
  subscriptions go on.
- A listener that falls far behind loses its oldest notifications instead of slowing the
  client down, so release resources when the futures that own them complete, not only in
  `onDisconnected`. See [ConnectionListener](hato/src/main/java/org/mqtt4j/hato/ConnectionListener.java)
  for callback ordering and threading.

#### Keeping a session between connections

To have the broker keep messages for your subscriptions while the client is disconnected, and
to resume unfinished QoS 1 and 2 exchanges, use an explicit `clientId` and a positive expiry,
such as `sessionExpiry(Duration.ofDays(1))`. By default, each connection starts a fresh
session.

The client keeps its side of a session in memory. A new client object, for example after your
application restarts, therefore starts a new session: messages that the broker stored for the
old one, including those published while your application was stopped, are not received,
unless you [keep the session in a file](#continuing-a-session-after-a-restart).

While the client reconnects, unfinished QoS 1 and 2 publications keep waiting. They complete
if the broker still has the session, and fail if it has lost the session or the client gives
up reconnecting; see [MqttPublisher](hato/src/main/java/org/mqtt4j/hato/MqttPublisher.java).
Some brokers keep no session for a client without subscriptions, so a client that only
publishes sees these publications fail whenever its connection ends.

#### Continuing a session after a restart

To let a new client object, for example after your application restarts, continue the session
and receive the messages that the broker stored meanwhile, give the client a file for its side
of the session:

```java
MqttClient client = MqttClient.builder()
        .server(URI.create("mqtt://broker.example.com:1883"))
        .clientId("orders-worker-1")
        .sessionExpiry(Duration.ofDays(1))
        .sessionFile(Path.of("/var/lib/orders-worker/mqtt-session"))
        .build();
// Stored messages arrive as soon as the client connects, so subscribe first.
client.subscribe(
        Subscribe.of(new TopicFilter("orders/#"), Qos.AT_LEAST_ONCE),
        delivery -> repository.save(delivery.message())
);
client.connect().join();
```

- `disconnect()` saves the session to the file before it completes, and the next client object
  with the file continues the session when it connects. Wait for your QoS 1 and 2 publications
  before disconnecting: the client saves nothing while one is unfinished.
- Closing the client without `disconnect()`, or a crash, saves nothing. The next client object
  then starts a new session, as it would without the file.
- Messages that you acknowledge after `disconnect()` completes arrive again.
- Give each `clientId` its own file, on a local disk that stays with it, such as a volume that
  belongs to the instance. A client object holds the file from its connection until its
  connections have ended after `disconnect()` saved the session or after `close()`, and others
  with the file cannot connect meanwhile. With `withInitialRetry()`, the new instance of a
  rolling update keeps trying, and continues the session once the old one disconnects.

The [builder](hato/src/main/java/org/mqtt4j/hato/MqttClientBuilder.java) describes what the file
keeps and how the client uses it.

#### Leaving a last Will

Use `will(MqttMessage.of(topic, payload, qos, retain))` to have the broker publish a message
if the connection ends without MQTT DISCONNECT. With MQTT 5.0, the Will can also carry message
properties, and `willDelay(Duration.ofSeconds(30))` asks the broker to hold it for a while: a
client with a persistent session that reconnects within the delay publishes no Will.

#### Shutting down

`disconnect()` does not wait for pending publications, so first wait for the futures whose
results you need. Then wait for `disconnect()` before closing the client. It sends MQTT
DISCONNECT, which tells the broker to discard the Will:

```java
try {
    client.disconnect().join();
} finally {
    client.close();
}
```

With a `sessionFile`, waiting for `disconnect()` also saves the session for the next client
object; see [Continuing a session after a restart](#continuing-a-session-after-a-restart).

`close()`, including leaving a try-with-resources block, shuts down at once and can cut a
pending DISCONNECT short. It does not wait for message handlers, listeners, or other callbacks
that are already running. Once `client.terminated()` completes, none of them runs or will
start, so you can release what they use, such as a database connection or an executor that you
gave the client. To release them without waiting, chain the release to the future, as in
`client.terminated().thenRun(pool::close)`. Never wait for `terminated()` from a message
handler or listener: the waiting callback keeps the client from terminating. Work that you
handed to your own queue needs its own shutdown.

### Handling failures

Operations report failures through their futures; with `join()`, the exception arrives as the
cause of a `CompletionException`. Null arguments throw at once, as do invalid subscription
requests; a publication that the client cannot send fails its future instead.

| Exception | Meaning |
| --- | --- |
| `MqttConnectException` | `connect()` gave up: the broker could not be reached, refused the connection, or did not answer in time, or `disconnect()`, `close()`, or a failing message handler withdrew the request first. |
| `MqttNotConnectedException` | The client was not connected, for example while it was reconnecting. |
| `MqttConnectionLostException` | The connection ended before the result was known. A publication may or may not have reached the broker. |
| `MqttCapacityException` | The client reached a limit, such as `maxInflightPublishes` or `maxSubscriptions`. The connection stays up; let outstanding work finish before retrying. |
| `MqttProtocolException` | For `publish()`, the broker accepted a QoS 2 publication only at QoS 1, so it may be delivered more than once; the connection stays up. Other protocol violations end the connection and are its cause. |
| `MqttPublicationRefusedException` | An MQTT 5.0 broker refused the publication, or the publication exceeds a limit that the broker announced. The connection stays up. |
| `MqttSubscriptionRefusedException` | The broker refused the subscription or does not support it. |

#### When your handler throws

A message handler that throws for a QoS 1 or 2 message whose acknowledgement it still holds back
(`AFTER_HANDLER`, or `MANUAL` before `delivery.ack()`) ends the connection, and the client
stops reconnecting until you call `connect()` again. If the client then resumes a persistent
session, the broker delivers the message again.

A failure that holds back nothing, such as one for a QoS 0 message, is skipped: the client
logs it and the subscription goes on. To skip failed QoS 1 and 2 messages too, acknowledging
them unprocessed, choose `FailurePolicy.SKIP`:
`Subscribe.of(filter, Qos.AT_LEAST_ONCE).withFailurePolicy(FailurePolicy.SKIP)`.

For a transient error, catch it and retry inside the handler with a bounded retry policy; the
subscription keeps its order meanwhile. To skip a single message, record the error and return
normally with `AFTER_HANDLER`, or call `delivery.ack()` with `MANUAL`.

### Working without blocking

Every operation returns a `CompletableFuture`, so an application that must not block, such as
one built around an event loop or a server's request threads, can chain its next step instead
of waiting with `join()`.

#### Starting up

```java
// Registered before connecting, the subscription goes out with the first connection.
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("commands/sensor-17"), Qos.AT_LEAST_ONCE),
        delivery -> commands.run(delivery.message())
);
client.connect()
        // Runs once the client is connected, and is skipped when connect() fails.
        .thenCompose(_ -> subscription.established())
        .whenComplete((qos, failure) -> {
            if (failure == null) {
                System.out.println("Listening for commands at " + qos);
            } else {
                System.err.println("Could not start: " + failure);
            }
        });
```

In a chained stage, a failure arrives wrapped in a `CompletionException`; its cause is one of
the exceptions under [Handling failures](#handling-failures).

#### Acknowledging after asynchronous work

When the handler starts asynchronous work, such as a save that returns a `CompletableFuture`,
acknowledge the message when that work completes rather than when the handler returns:

```java
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_LEAST_ONCE)
                // The handler's return no longer allows the acknowledgement.
                .withAckMode(AckMode.MANUAL),
        delivery -> repository.saveAsync(delivery.message()).whenComplete((_, failure) -> {
            if (failure != null) {
                // Give the message up: one never acknowledged would hold up all later ones.
                System.err.println("Not saved: " + failure);
            }
            delivery.ack();
        })
);
```

- The handler returns at once, so the saves of later messages start before earlier ones
  finish: the subscription's order holds for starting the work, not for finishing it.
- A QoS 1 or 2 message keeps its place in the subscription's buffer until it is acknowledged,
  so the buffer also bounds how many saves are unfinished at once.

### Where your code runs

Unless you choose [inline delivery](#running-handlers-as-part-of-receiving), none of your code
runs on the threads that read and write the network, so a slow callback never stalls the
connection or its keep-alive.

| Your code | Runs on | Can it block? |
| --- | --- | --- |
| Message handler | By default, a thread of the client for each subscription, or up to `n` with striped or parallel delivery. With `withExecutor(pool)` or `lanes(selector)`, a thread of your executor instead | Yes. Its own subscription waits meanwhile, and acknowledgements keep [arrival order](#acknowledgements-keep-arrival-order). On your executor, it also holds that executor's thread |
| Inline message handler | Possibly as part of receiving, before the connection reads on | No. The whole connection waits meanwhile, and waiting for this client throws |
| Hand-off acceptor | A thread of the client, one acceptor at a time | No. The other hand-off subscriptions wait while it runs |
| `receive()` | Your own threads | Yes |
| Connection listener, metrics listener | A thread of its own for each listener, one notification at a time | Briefly. A listener that falls behind loses its oldest notifications |
| Credentials supplier | A thread of the client, before each connection attempt | Within the connection timeout |
| Continuations of a returned future, such as `thenApply` or `whenComplete` | A thread of the client, whatever executors you chose for handlers, or your own thread when the future has already completed as you attach them | Yes, with the cautions below |

- A continuation does not run on the thread that started the operation. A `publish()` that a
  handler starts completes on another thread, so its continuation runs alongside the
  subscription's later messages, outside their order.
- A continuation that blocks does not keep other operations from completing, but their
  completions can wait a moment behind it, so keep continuations short where you can.
- To run a continuation on a thread of your choice, such as your event loop, use the
  `...Async` variant with your executor, as in `whenCompleteAsync(action, executor)`.
- `terminated()` waits until continuations have returned, so one that never returns keeps the
  client from terminating. Continuations of `established()`, `whenConnected()`, and
  `unsubscribe()` also count toward the client's limit on pending observations until they
  return, so keep them short.

### Passing the client to your code

Code that only publishes can take an `MqttPublisher`, and code that only subscribes an
`MqttSubscriber`. Neither can disconnect the client.

### Watching the client

#### Reading the logs

The client logs through `System.Logger`, so its logs go to `java.util.logging` by default. To
send them to SLF4J or Log4j, add the bridge `org.slf4j:slf4j-jdk-platform-logging` or
`org.apache.logging.log4j:log4j-jpl` next to the client jar, which also works inside an executable
jar. Spring Boot applications should add it as well: Spring Boot stops forwarding
`java.util.logging` before it destroys beans, so without the bridge the logs of a client closing
with its bean are lost. The loggers are named under `org.mqtt4j.hato`:

| Level | What the client logs |
| --- | --- |
| `ERROR` | Internal errors that the client cannot recover from. |
| `WARNING` | Failures of your handlers and listeners, broker protocol violations, subscriptions that end because the broker refuses to re-create them, and notifications that a slow listener starts to lose. |
| `INFO` | Connections, disconnections, failed attempts, reconnection delays, and the start and end of persistent sessions. |
| `DEBUG` | Each packet. |
| `TRACE` | The bytes on the wire. |

Payloads appear only at `TRACE`, and credentials never appear.

The client does not wait for the logging backend, so a backend that is slow or blocked does not
hold the client up. Its lines keep their order and reach the backend a moment after the client
made them, and a level that you change in the backend applies a moment later. When the backend falls far behind, the client drops lines, and a `WARNING` in their
place says how many. `close()` and `terminated()` each give the backend a moment to take the
lines made until then, so close the client before the application exits.

#### Checking what the client holds

`stats()` shows what only the client can see: the messages waiting for each subscription, the
acknowledgements it holds back and how long the oldest has waited, and what it dropped or
refused, and why.

```java
ClientStats stats = client.stats().join();
stats.receiving().oldestAwaitingConditions()
        .filter(age -> age.compareTo(Duration.ofMinutes(1)) > 0)
        .ifPresent(age -> System.err.println("An acknowledgement has waited for " + age));
for (ClientStats.SubscriptionStats subscription : stats.subscriptions()) {
    System.out.println(subscription.filter().pattern() + ": " + subscription.waiting()
            + " waiting, " + subscription.dropped());
}
```

| Question | Values to inspect |
| --- | --- |
| Is it connected? | `connection().state()`, `attempt()`, `establishedAttempt()` |
| Is processing keeping up? | Each subscription's `waiting()`, `running()`, and `awaitingAck()` |
| What holds up acknowledgements? | `receiving().awaitingConditions()`, `oldestAwaitingConditions()`, `awaitingTurn()` |
| How close is the client to its receive limits? | `heldDeliveries()`, `heldBytes()`, `heldConditions()`, and `receptions()`, each with its `used` and `limit` |
| What holds up sending? | `sending().publications()`, `queuedBytes()`, `quota()`, and `oldestResponseWait()` |
| What was lost or refused? | `receiving().dropped()`, `unrouted()`, `refusals()`, `overloads()` |
| Is monitoring falling behind? | `connection().droppedNotifications()` and `droppedMetricEvents()` |

Drops, refusals, and the other counts add up over the life of the client object, so watch how
they change rather than their value. A snapshot takes longer the more subscriptions the client
holds, and calls made while one is pending share it. After `close()`, `stats()` returns the
last values, ready for a final report. [ClientStats](hato/src/main/java/org/mqtt4j/hato/ClientStats.java)
explains every value.

#### Timing publications and deliveries

To time the client, register a `MetricsListener` with `metricsListener(...)` on the builder.
It learns how long each publication took until its future completed, and how long each message
waited between its arrival and your code receiving it, whether through a message handler, a
hand-off acceptor, or `receive()`. Its callbacks run on a thread of their own, so a slow
listener never delays publications or deliveries; without a listener, the client takes no
timings. Feed the timings to the timers of your metrics library:

```java
var timings = new MetricsListener() {
    @Override
    public void onPublishCompleted(MetricsListener.PublishCompletion completion) {
        publishTimer.record(completion.duration());
    }

    @Override
    public void onDeliveryStarted(MetricsListener.DeliveryStart start) {
        deliveryWaitTimer.record(start.waited());
    }
};
```

The wait ends when your code receives the message, so time your own processing separately.

#### Recording metrics with Micrometer

The [MicrometerMonitoring](examples/src/main/java/org/mqtt4j/hato/examples/MicrometerMonitoring.java)
example records these timings in Micrometer timers, times the message handler's own work, and
publishes `stats()` values as gauges and function counters. Run it with
`./gradlew :examples:runMicrometerMonitoring`; it uses a local `SimpleMeterRegistry` where your
application would use its own registry. It takes a snapshot every second on a thread of its
own and lets the meters read the latest one, so that reading the meters never waits for the
client. Since the counts in a snapshot already add up, it registers them as function counters
rather than adding them to a `Counter` on each snapshot.

### Troubleshooting

| Symptom | What to check |
| --- | --- |
| `connect()` fails at startup | The exception's cause tells why: the address and port of the broker's listener, TLS trust and hostname, credentials, or the MQTT version. With a `sessionFile`, another client object may hold the file, as the message says. The first attempt is retried only when you [ask for it](#choosing-when-to-reconnect). |
| `publish()` fails while the client reconnects | Publications need a connection and are not queued. Wait with `whenConnected()`, and still expect the connection to end again. |
| `established()` never completes | A subscription waits for a connection: call `connect()`. It also waits while the client reconnects. |
| The client stops reconnecting | The [connection listener's](#reacting-to-connection-changes) ending gives the cause, such as a message handler that threw while holding back an acknowledgement, a ban by the broker, or another client taking over the `clientId` or the `sessionFile`. |
| Every subscription stalls behind one slow topic | One acknowledgement [holds up the rest](#acknowledgements-keep-arrival-order). Look at `oldestAwaitingConditions()` and at each subscription's `waiting()`, `running()`, and `awaitingAck()`. |
| `MqttCapacityException`, or connections that end under load | `refusals()` names the limit that refused an operation, and `overloads()` why the client ended a connection. Compare the `used` and `limit` values before you change a setting. |
| Messages are missing | `receiving().dropped()` gives drops by reason, and `unrouted()` QoS 0 messages that arrived faster than the client could take them. Also check the acknowledgement mode, the session expiry, application restarts, and whether your own queue finished the work. |
| Shutdown never finishes | `terminated()` completes only after `close()`, once every message handler and listener has returned. Do not wait for it from those callbacks. |

### Tuning the client

The defaults suit most applications. Change a setting for a concrete need, and check the
result with [stats and timings](#watching-the-client). Builder settings take effect at
`build()`. `Subscribe` and `DeliveryOptions` are immutable: their `with...` methods return
changed copies.

#### Capacity and processing

| Setting | Default | Change it to |
| --- | --- | --- |
| `Subscribe.withBufferCapacity(n)` | 1,024 deliveries per subscription | Absorb bursts, or hold less; see [Keeping messages flowing](#keeping-messages-flowing). |
| `maxInflightPublishes(n)` | 64 QoS 1 and 2 publications | Keep more publications outstanding. Beyond the limit, `publish()` fails rather than waits. |
| `maxSubscriptions(n)` | 4,096 | Hold more subscriptions, or catch an application that subscribes without end. Subscriptions that wait for the broker count too. |
| `maxPacketSize(n)` | 16 MiB | Accept larger incoming packets, or only smaller ones. MQTT 5.0 tells the broker the limit; on MQTT 3.1.1, a larger packet ends the connection. |
| `DeliveryOptions.striped(n, key)`, `parallel(n)` | One message at a time | Let the handler process up to 256 messages of a subscription at once; see [Choosing how messages run](#choosing-how-messages-run). |
| `DeliveryOptions.withExecutor(pool)`, `lanes(selector)` | Threads of the client | Run message handlers on executors that your application owns; the client never shuts them down. |
| `DeliveryOptions.inline()` | Handlers apart from the network | Spare the hand-off for a handler that returns at once; see [Running handlers as part of receiving](#running-handlers-as-part-of-receiving). |

The client keeps some limits internal, such as the deliveries and bytes that it holds across
subscriptions. [Stats](#checking-what-the-client-holds) report their use and limit, and when one
refuses work or ends a connection, `refusals()` and `overloads()` name it.

#### Connections and timeouts

| Setting | Default | Change it to |
| --- | --- | --- |
| `version(...)` | MQTT 5.0 | Use `MqttVersion.V3_1_1` for a broker without MQTT 5.0; the client does not fall back by itself. |
| `clientId(...)` | A generated identifier | Give a stable, unique identifier, which a persistent session needs. |
| `sessionExpiry(...)` | Zero: a fresh session for each connection | Keep the session across this client object's connections; see [sessions](#keeping-a-session-between-connections). |
| `sessionFile(...)` | None: a new client object starts a new session | Continue the session in the next client object, for example after a restart; see [Continuing a session after a restart](#continuing-a-session-after-a-restart). |
| `keepAlive(...)` | 60 seconds | Detect a dead connection sooner, or send less traffic. Zero turns keep-alive off. |
| `connectionTimeout(...)` | 30 seconds | Bound how long a connection may take to set up, from fetching credentials to the broker's answer. |
| `responseTimeout(...)` | 60 seconds | Bound how long the broker takes to answer a subscription request or a step of a QoS 1 or 2 publication; expiry ends the connection. It is not a deadline for the whole `publish()`. |
| `reconnectPolicy(...)` | Delays from 1 to 120 seconds; a failed first attempt is not retried | Change the delays, retry the first attempt, or turn reconnection off; see [Choosing when to reconnect](#choosing-when-to-reconnect). |

Credentials, TLS, the Will, and the listeners are described where they are used.
`transportFactory(...)` and `timerScheduler(...)` replace the network transport and the timer,
for special integrations and for [tests](hato-testkit/README.md); the
[builder](hato/src/main/java/org/mqtt4j/hato/MqttClientBuilder.java) describes every setting.

## Running a broker in your application

The same jar holds an MQTT broker that runs inside your application: for devices and services
that connect to your application, for local development, and for tests. It serves MQTT 3.1.1
and 5.0 clients of any library over TCP and TLS, and Hato clients in the same JVM without a
socket. It keeps its state in memory and writes no files.

### Starting and stopping a broker

`MqttBroker.start()` starts a broker on a port of the loopback address that the system
assigns. This example connects a client to it:

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
        IO.println("Connected to the broker");
        client.disconnect().join();
    }
}
```

Each start creates an independent broker. `MqttBroker.builder()` sets the addresses, TLS, the
name, the limits, the authenticator, and the listener before `start()`, which throws
`IOException` if it cannot open a listening socket. Leaving the block closes the broker, which
disconnects its clients at once; [Stopping the broker](#stopping-the-broker) shows how to let
them receive what waits for them first.

### Connecting clients in the same JVM

A Hato client in the same JVM reaches the broker by its URI, `inproc://` and the broker's name,
which `broker.uri()` returns. The client passes its packets to the broker in memory, without a
socket, and otherwise works as over TCP: the broker checks it with the same authenticator,
which sees no remote address, and holds it to the same limits.

```java
try (
        MqttBroker broker = MqttBroker.builder().name("fleet").start();
        MqttClient client = MqttClient.builder()
                .server(broker.uri())
                .build()
) {
    client.connect().join();
    IO.println("Connected to " + broker.uri());
    client.disconnect().join();
}
```

A name is a host name in lower case, such as `fleet` or `site-a.fleet`. Without one, the broker
takes `hato-` and a number. A running broker holds its name until it stops, and no other broker
can start with that name meanwhile. A client reaches a broker only when both use the Hato
classes of the same class loader, as in an ordinary application.

Since the URI is a plain string, a client can take `inproc://fleet` from your configuration and
be built before the broker starts. A connection attempt while no broker of that name runs fails
as one to an unreachable host does, and `withInitialRetry()` on the
[reconnect policy](#choosing-when-to-reconnect) keeps the client trying until the broker is
there. Give brokers in tests that run at the same time their own names, or none, as you give
them their own ports.

### Opening the broker to a network

By default, the broker listens only at the loopback address, so only programs on the same
machine reach it, and it accepts every client. `listen(address)` sets another address and
port; `new InetSocketAddress(1883)` listens on every interface. A broker that listens beyond
the loopback address needs an [authenticator](#checking-clients-and-their-topics) to decide on
its clients, or `allowAnonymous()` to accept every client anyway; without either, `start()`
refuses the address.

```java
MqttBroker broker = MqttBroker.builder()
        .listen(new InetSocketAddress(1883))
        .authenticator(authenticator)
        .start();
```

### Checking clients and their topics

An `Authenticator` decides on each client that connects. It returns a `Decision` that refuses
the client, or accepts it with a `Scope`: the topics that the client may publish to, and the
filters within which it may subscribe.

```java
Authenticator authenticator = request -> {
    // Look the client up in your application's own store; the lookup may block.
    if (!devices.matches(request.clientId(), request.password())) {
        return Authenticator.Decision.refuseBadCredentials();
    }
    String device = request.clientId();
    return Authenticator.Decision.accept(
            Authenticator.Scope.of(
                    List.of(new TopicFilter("devices/" + device + "/#")),
                    List.of(new TopicFilter("commands/" + device + "/#"))
            )
    );
};
```

- The broker asks apart from its I/O and its other clients, so a decision may block, as for a
  database lookup. A client whose decision throws, or takes longer than
  `authenticationTimeout(...)`, is refused.
- The broker holds the client to its scope while it is connected, without asking again. A
  publication outside the scope reaches no subscriber, and an MQTT 5.0 client learns of it
  from the acknowledgement of a QoS 1 or 2 publication; a subscription outside it is refused.
  `Scope.all()` allows everything.
- The broker ties no client identifier to a user: a client that the authenticator accepts takes
  over the session of its identifier, with the messages that wait there within its scope,
  whoever used the identifier before. So check that the client may use its identifier, as the
  example does by matching the password to the device of that identifier.
- The request also shows the user name, the address that the client connects from, whether it
  came through TLS, and its certificates.
- A client that you [disconnect](#watching-and-managing-clients) meets the authenticator again
  when it reconnects, so refusing it then keeps it out.

### Listening with TLS

`listenTls(address, context)` has the broker listen with TLS, alone or next to a plain listener
on another port, and `tlsPort()` gives its port. The `SSLContext` holds the broker's key and
certificate, and the trust in client certificates.

```java
MqttBroker broker = MqttBroker.builder()
        .listenTls(new InetSocketAddress(8883), context)
        .sslParameters(parameters -> parameters.setWantClientAuth(true))
        .authenticator(authenticator)
        .start();
```

- Initialize the context before `start()`. The broker takes a TLS engine from it for each
  connection, so key and trust managers that return renewed certificates serve the connections
  that come afterwards. A key manager of your own extends `X509ExtendedKeyManager` and
  implements `chooseEngineServerAlias`, as the JDK's do; with another, every handshake fails.
- [RenewBrokerCertificate](examples/src/main/java/org/mqtt4j/hato/examples/RenewBrokerCertificate.java)
  renews the broker's certificate with such a key manager while its clients stay connected.
- `sslParameters(...)` adjusts the TLS parameters. With `setNeedClientAuth(true)`, a client
  needs a certificate that the context trusts; with `setWantClientAuth(true)`, it may connect
  without one. The authenticator sees the certificates in `request.clientCertificates()`.
- Give the context the trust that you mean for client certificates: one initialized without
  trust managers, as `SSLContext.getDefault()` is, accepts a certificate from any certificate
  authority that the JDK trusts.
- TLS authenticates the broker, and the clients only through the certificates that your
  authenticator checks. So a TLS listener beyond the loopback address needs an authenticator or
  `allowAnonymous()`, as a plain one does.
- Nothing checks a client's certificate again while its connection lasts. To end the
  connections whose certificates expire or are revoked meanwhile, use `disconnect(connection)`.
- A trust manager that waits, as for a revocation check over the network, holds up the
  handshakes of the clients that connect meanwhile. Checking revocation in the authenticator,
  which the broker asks on every CONNECT and which may take its time, avoids that.

### What the broker supports

The broker supports MQTT 3.1.1 and 5.0 with:

- QoS 0, 1, and 2
- Retained messages and Wills
- Sessions that outlive their connections
- The MQTT 5.0 subscription options, message expiry, and Will delay

It does not support:

- Shared subscriptions, which it tells MQTT 5.0 clients as they connect
- MQTT 5.0 enhanced authentication

Everything lives in memory: sessions, retained messages, and the messages that wait for
clients are lost when the broker stops, and an acknowledgement does not mean that a message has
been saved to disk. Your application recreates what it needs after a restart.

The broker bounds what it holds:

- A subscriber that falls too far behind, or is away, misses messages, whatever their QoS,
  while its connection and session go on. The [listener](#watching-and-managing-clients) hears
  of the QoS 1 and 2 messages that it misses.
- A client that is connected also holds what is on its way to it, up to a bound of its own, so
  the memory that the broker takes grows with the clients that are connected, which
  `maxConnections(n)` limits.
- A client that sends requests faster than it reads the answers is held back rather than
  disconnected.
- Beyond the limit on clients or on persistent sessions, a client is refused as it connects. A
  retained publication beyond the retained limits reaches the current subscribers but is not
  kept. A client that sends a packet larger than the limit loses its connection.
- A client that sends text with a control character, such as a newline in a user property,
  loses its connection, as MQTT lets a receiver treat such a packet as malformed.

[Broker settings](#broker-settings) lists the limits that you can change; the others are
internal.

### Watching and managing clients

A `BrokerListener` hears of each client that the broker accepts and of the end of its
connection with the reason, of the QoS 1 and 2 messages that the limits kept from a client, and
of an internal failure that stops the broker.

```java
BrokerListener listener = new BrokerListener() {

    @Override
    public void onConnected(Connection connection) {
        IO.println(connection.clientId() + " connected");
    }

    @Override
    public void onDisconnected(Connection connection, DisconnectReason reason) {
        IO.println(connection.clientId() + " disconnected: " + reason);
    }
};
MqttBroker broker = MqttBroker.builder().listener(listener).start();
```

- The calls come one at a time, apart from the broker's work, so a listener that takes its time
  delays only its own later notifications. One that falls far behind loses the oldest
  notifications, which `stats()` counts. The notifications of one client come in order, so a
  listener can follow which clients are connected.
- `clients()` lists the connected clients, each with its connection, the subscriptions of its
  session with the QoS that the broker granted, and the messages that wait for it, for a page or
  an API that shows what your devices are doing. It reads them without holding up the broker.
  With `recordClientActivity()`, each client also shows what it has done on its connection: the
  messages that it published, with the topic of the last one and how long ago it came, the
  messages that it received, and what its scope refused. Without it, the broker records none
  of this.
- `disconnect(clientId)` closes a client's connection, as for an administrative action: its
  Will is published, and its session stays. `disconnect(connection)` closes a connection that
  the listener heard of or `clients()` listed, and never a later one of the same client.
- `stats()` counts the connected clients, persistent sessions, retained messages, waiting
  deliveries, and the messages received, delivered, and dropped at each QoS. A monitor that
  reports several values reads them from one call.

### Stopping the broker

`close()` stops the broker at once: it frees its ports, closes every connection, and discards
the sessions and retained messages, without publishing Wills.

`shutdown(timeout)` lets the connected clients receive what waits for them first. It frees the
ports and the name at once and refuses new clients, serves the connected ones until each has
received what waits for it or `timeout` has passed, tells MQTT 5.0 clients that the server is
shutting down, and then closes as `close()` does. It returns whether the clients received
everything in time. Disconnect your application's own clients before the shutdown to have what
they published last delivered.

`terminated()` completes once the broker has ended, also when an internal failure stopped it,
which the listener's `onFailed` hears of.

### Broker settings

| Setting | Default | Change it to |
| --- | --- | --- |
| `listen(address)` | The loopback address, on a port that the system assigns | Listen at a fixed port, or beyond the loopback address; see [Opening the broker to a network](#opening-the-broker-to-a-network). |
| `listenTls(address, context)`, `sslParameters(...)` | No TLS listener | Listen with TLS; see [Listening with TLS](#listening-with-tls). |
| `name(...)` | `hato-` and a number | Give clients in the same JVM a fixed URI, such as one from your configuration. |
| `authenticator(...)` | None: every client is accepted | Check clients and their topics; see [Checking clients and their topics](#checking-clients-and-their-topics). |
| `authenticationTimeout(...)` | 10 seconds | Give the authenticator more or less time for a decision. |
| `listener(...)` | None | Hear of clients and failures; see [Watching and managing clients](#watching-and-managing-clients). |
| `recordClientActivity()` | Off | Show what each client published and received in `clients()`; see [Watching and managing clients](#watching-and-managing-clients). |
| `maxConnections(n)` | 10,000 clients | Serve more clients at once, or fewer. |
| `maxPersistentSessions(n)` | 10,000 | Keep more sessions that outlive their connections, or fewer. |
| `maxSessionExpiry(...)` | 24 hours | Keep the sessions of absent clients longer, or not as long. |
| `maxQueuedDeliveries(n)`, `maxQueuedDeliveryBytes(n)` | 1,000 deliveries and 16 MiB for each session | Hold more QoS 1 and 2 messages for a subscriber that is behind or away, or fewer. |
| `maxRetainedMessages(n)`, `maxRetainedBytes(n)` | 10,000 messages, and 64 MiB or an eighth of the largest heap if that is less | Keep more retained messages, or fewer. |
| `maxPacketSize(n)` | 1 MiB | Take larger packets from clients, or only smaller ones. |

The [builder](hato/src/main/java/org/mqtt4j/hato/broker/MqttBrokerBuilder.java) describes every
setting, and [MqttBroker](hato/src/main/java/org/mqtt4j/hato/broker/MqttBroker.java) how the broker
handles sessions, retained messages, Wills, MQTT 5.0, and its limits.

## Testing your application

The test kit, a separate jar for your tests, serves both sides: it stands in for the client in
unit tests, and it controls the connection and the broker in tests that run one. Its
[guide](hato-testkit/README.md) shows how.

### Testing without a broker

In unit tests, the test kit's `FakeMqttPublisher` and `FakeMqttSubscriber` stand in for the
client: they record what your code publishes and subscribes, and let a test deliver messages to
the handlers that your code registered. They fit code that takes an `MqttPublisher` or an
`MqttSubscriber`, as in [Passing the client to your code](#passing-the-client-to-your-code).

### Testing with a broker

Start a broker for each test with `MqttBroker.start()` and close it afterwards; brokers on ports
that the system assigns do not collide. The test kit simulates interrupted connections and
timeouts between the client and the broker, and its `BrokerControls` makes a broker refuse
publications, subscriptions, and connections, hold back its answers, end connections, or
announce narrower limits, so that you can test how your application copes.

## Other guides

- [Test kit](hato-testkit/README.md): Test doubles, connection failures, controlled time, and broker behavior in tests
- [Examples](examples/README.md): Complete programs compiled and checked by the build

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for building, testing, and contributing.

## License

MIT. See [LICENSE](LICENSE).
