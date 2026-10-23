# Hato Client

Hato connects Java applications to MQTT brokers over TCP, TLS, or WebSocket. It supports
MQTT 3.1.1 and 5.0 and has no runtime dependencies. Give it a broker address, connect, and
publish or subscribe; the defaults suit most applications. When yours needs more, you choose
where and how messages are processed, when they are acknowledged, and how much the client
holds.

This guide covers:

- [Installation](#getting-it), [your first messages](#your-first-messages), and
  [what to know before you start](#how-the-client-works).
- [Connecting](#connecting-to-a-broker), [publishing](#publishing), and
  [subscribing](#subscribing).
- [Processing messages in parallel or on your own threads](#choosing-how-messages-run), and
  [keeping messages flowing](#keeping-messages-flowing).
- [Reconnection, sessions, and shutdown](#staying-connected), [failures](#handling-failures),
  [working without blocking](#working-without-blocking), and
  [where your code runs](#where-your-code-runs).
- [Logs, stats, and metrics](#watching-the-client), and [troubleshooting](#troubleshooting).
- [Testing your code](#fitting-the-client-into-your-application),
  [settings](#tuning-the-client), and [speed](#how-fast-it-is).

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

Requires Java 25 or later. For JPMS, the module name is `org.mqtt4j.hato.client`.
The Javadoc of each type gives its exact behavior, starting with
[MqttClient](src/main/java/org/mqtt4j/hato/MqttClient.java).

## Your first messages

Both programs below need a broker at `mqtt://localhost:1883`. If you do not have one,
`./gradlew :examples:runHello` from a repository checkout runs a complete exchange with
Hato's embedded broker; [Examples](../examples/README.md) has the complete sources.

### Sending a reading

```java
import java.net.URI;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Topic;

import static java.nio.charset.StandardCharsets.UTF_8;

void main() {
    try (
            MqttClient client = MqttClient.builder()
                    .server(URI.create("mqtt://localhost:1883"))
                    .build()
    ) {
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

### Receiving readings

The filter `sensors/+/temperature` matches readings from any one sensor, such as
`sensors/kitchen/temperature`. Start the receiver, wait for its message, and then run the
publisher in another process: the receiver prints `sensors/kitchen/temperature: 21.5`.
Press Enter to stop it.

```java
import java.net.URI;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.TopicFilter;

import static java.nio.charset.StandardCharsets.UTF_8;

void main() {
    try (
            MqttClient client = MqttClient.builder()
                    .server(URI.create("mqtt://localhost:1883"))
                    .build()
    ) {
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

The lambda passed to `subscribe()` is the subscription's message handler: your code, which
the client calls with each message that the subscription receives. It gets an `MqttDelivery`,
whose `message()` is the received message.

The examples below omit imports and application objects such as `repository`. Most of them
wait with `join()` to stay short; [Working without blocking](#working-without-blocking) shows
how to chain the next step instead.

## How the client works

A few behaviors shape everything below, and most surprises come from one of them:

- **It stays connected.** `build()` does not connect. Once `connect()` succeeds, the client
  reconnects by itself whenever the connection drops, until you call `disconnect()` or
  `close()`. A failed first attempt fails `connect()` unless you
  [ask the client to retry](#choosing-when-to-reconnect).
- **Subscriptions outlast connections.** `subscribe()` registers a subscription, even before
  `connect()`, and the client subscribes again after each reconnection. Subscribe once and
  keep the handle.
- **Your handler may block.** The client calls a subscription's message handler for one
  message at a time, apart from the network, by default on a virtual thread of the client or
  on an executor that you choose, and other subscriptions' handlers run meanwhile. The handler
  can write to a database, or publish and wait for the result.
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

## Connecting to a broker

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
running client its own `clientId`: two clients with the same identifier keep taking over each
other's connection, and with a persistent session, the one that connects later also discards
the other's session.

### Connecting over TLS or WebSocket

Choose the transport with the URI scheme:

| URI | Connects over | Default port |
| --- | --- | ---: |
| `mqtt://broker.example.com` | TCP | 1883 |
| `mqtts://broker.example.com` | TLS | 8883 |
| `ws://broker.example.com:8080/mqtt` | WebSocket | 80 |
| `wss://broker.example.com/mqtt` | WebSocket over TLS | 443 |

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

### Authenticating

For username/password authentication, add
`credentials(() -> MqttCredentials.of(username, passwordBytes))`, or
`MqttCredentials.of(username)` for a username alone. The client calls the supplier before
every connection attempt, so it can fetch a fresh token each time. Hato authenticates with
these credentials and TLS client certificates; it does not support MQTT 5.0 enhanced
authentication, such as SCRAM.

### Connecting to an MQTT 3.1.1 broker

The client speaks MQTT 5.0. For a broker without it, choose `version(MqttVersion.V3_1_1)`.
The features marked MQTT 5.0 in this guide are then unavailable: the client refuses messages
with properties and subscriptions with options, and `build()` rejects a Will with properties
or a delay. A positive `sessionExpiry` still keeps the session, but the broker decides for how
long.

## Publishing

The [first example](#sending-a-reading) publishes an immutable `MqttMessage`. Its payload is
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

### Publishing many messages at once

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

### Adding message properties (MQTT 5.0)

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

## Subscribing

### Handling each message

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

### Stopping a subscription

`unsubscribe()` stops new messages and lets the queued ones finish; its future completes when
the broker has removed the subscription. `subscription.close()` instead stops delivery at once
and drops the queued messages. Either way, `subscription.terminated()` completes once the
handler has returned for the last message. Once removal completes, the same filter can be
subscribed again.

To replace a handler without giving up the subscription, have the handler call a delegate that
your application swaps through an atomic reference.

### Choosing when to acknowledge

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
acknowledgements of every later one. [MqttDelivery](src/main/java/org/mqtt4j/hato/MqttDelivery.java)
describes acknowledgement across reconnections.

A message that is not yet acknowledged when the connection ends comes back only when the
client resumes a [persistent session](#keeping-a-session-between-connections).

### Setting subscription options (MQTT 5.0)

`withNoLocal(true)` leaves out your own publications, `withRetainAsPublished(true)` keeps the
retain flag that the publisher set, and `withRetainHandling(RetainHandling.DO_NOT_SEND)` skips
the retained messages when you subscribe. `RetainHandling.SEND_IF_NEW_SUBSCRIPTION` sends them
only for a subscription that the broker does not have yet.

### Overlapping and shared subscriptions

Filters may overlap, and each matching subscription receives its own delivery of a message. A
broker can also send a message once for each overlapping subscription. With MQTT 5.0
Subscription Identifiers, each such copy reaches only the subscription it was meant for.
Without them, on MQTT 3.1.1 or with a broker that does not support them, every copy reaches
every matching subscription, so your handler can receive the same message twice.

Shared subscriptions use `$share/{group}/{filter}`. A shared filter cannot overlap any other
filter on the same client, shared or not: `established()` fails for the subscription that
would overlap. Use separate clients when you need those filters together.

## Choosing how messages run

With a message handler alone, as in [Handling each message](#handling-each-message), a
subscription runs its messages one at a time, in arrival order. The other ways change where
your code runs, how many messages run at once, and which order is kept:

| When you need | Use | Order | At once |
| --- | --- | --- | --- |
| The simplest code | [`client.subscribe(request, handler)`](#handling-each-message) | arrival | one |
| Your own thread or pool | [`DeliveryOptions.serial().withExecutor(pool)`](#running-handlers-on-your-executor) | arrival | one |
| The threads that own each device's state | [`DeliveryOptions.lanes(selector)`](#running-each-device-on-its-own-thread) | per lane | one per lane |
| Each device in order, devices in parallel | [`DeliveryOptions.striped(n, key)`](#running-devices-in-parallel) | per key | up to `n` |
| Independent messages, as fast as possible | [`DeliveryOptions.parallel(n)`](#running-independent-messages-in-parallel) | none | up to `n` |
| A queue that your application owns | [`client.handOff(request, mailbox::offer)`](#handing-messages-to-your-own-mailbox) | arrival, into your queue | your choice |
| Your own threads taking messages | [`client.subscribe(request)` and `receive()`](#receiving-on-your-own-threads) | arrival, as taken | one per thread |

- Start with a handler alone. When it is slower than messages arrive, such as a wildcard
  subscription whose handler writes each reading to a database, messages queue up and their
  acknowledgements wait behind them. Striping by device lets devices go on independently
  while each keeps its order, and with enough stripes the processing can keep up.
- Choose by the order that you need, not by overhead: running messages on stripes or in
  parallel costs little next to a handler that does real work.
- Use lanes when the work must run on threads that your application already owns. Hand off
  when your application already has a queue, such as a mailbox that also takes work of its
  own. Receive on your own threads when your loop should decide when to take each next
  message, for example with a timeout.
- Whichever you choose, acknowledgements keep arrival order across the connection; see
  [Keeping messages flowing](#keeping-messages-flowing).

### Running handlers on your executor

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

### Running each device on its own thread

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
- [MailboxLanes](../examples/src/main/java/org/mqtt4j/hato/examples/MailboxLanes.java) runs
  each device on its own single-thread executor in a complete program.

### Running devices in parallel

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
  16 messages of this subscription at once, by default each on a virtual thread of the client.
- `DeliveryOptions.striped(16)` uses each message's topic as its key.
- The key's hash picks one of the 16 stripes, so two devices can share a stripe and then wait
  for each other.
- Add `.withExecutor(pool)` to run the stripes on your own executor.

### Running independent messages in parallel

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

### Handing messages to your own mailbox

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
- Return `false` when your mailbox is full. The client treats that as a full subscription
  buffer; see [When a buffer is full](#when-a-buffer-is-full).
- One thread of the client offers the messages of all its hand-off subscriptions in the order
  they arrive, so your mailbox sees one order across them, and `delivery.filter()` tells which
  subscription each message came through. Never wait in the acceptor: use `offer`, not `put`,
  or the other hand-off subscriptions wait too.
- To stop, close the subscription, wait for `subscription.terminated()`, and then drain or
  discard your mailbox: nothing is offered after that. Each message reports
  `delivery.connectionAttempt()`, so messages kept across a reconnection can be told apart
  from new ones.
- [HandOffToMailbox](../examples/src/main/java/org/mqtt4j/hato/examples/HandOffToMailbox.java)
  acknowledges each message after processing it and shuts down in the order a mailbox needs.

### Receiving on your own threads

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

## Keeping messages flowing

Each subscription holds up to 1,024 deliveries by default. A delivery keeps its place until
your code is done with it:

| Delivery style | A delivery keeps its place until |
| --- | --- |
| Message handler | The handler returns and, for QoS 1 and 2, the message's acknowledgement has its turn |
| Pull | `receive()` returns it; with `MANUAL`, a QoS 1 or 2 delivery until `ack()` |
| Hand-off | Your acceptor takes it; from then on it takes room in your queue instead |

The client also limits the deliveries and bytes that it holds across all subscriptions,
whatever their buffer sizes.

### Acknowledgements keep arrival order

The client acknowledges QoS 1 and 2 messages in the order they arrived, across all
subscriptions of a connection. It acknowledges a message that several subscriptions receive
once each of them allows it. A message whose acknowledgement waits, in a queue, in a slow
handler, or for an `ack()` that never comes, therefore holds back the acknowledgements of all
later ones, from every subscription. Those later messages keep their places in their buffers
until their turn, even when the handler has finished with them. Striped and parallel delivery
do not change this order.

### When a buffer is full

| Received QoS | What the client does |
| --- | --- |
| 0 | Drops the message for the full subscription, while other matching subscriptions still receive it, and goes on. When the client-wide limit is full, it drops the message for every subscription. |
| 1 or 2 | Ends the connection rather than hold more, which affects every subscription on it. |

What counts is the QoS of the received message, which can be higher than a subscription
requested when another subscription of the client also matches. A hand-off acceptor that
returns `false` counts as a full buffer, and the client does not offer that message again.
`FailurePolicy.SKIP` does not change any of this.

When the connection ends, queued deliveries are discarded; a handler that is already
processing a message can finish. The broker sends unacknowledged messages again only when the
client resumes a [persistent session](#keeping-a-session-between-connections). The client logs
drops, and [stats](#checking-what-the-client-holds) count them by reason.

### Keeping up

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

## Staying connected

### Choosing when to reconnect

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

### Reacting to connection changes

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
  `onDisconnected`. See [ConnectionListener](src/main/java/org/mqtt4j/hato/ConnectionListener.java)
  for callback ordering and threading.

### Keeping a session between connections

To have the broker keep messages for your subscriptions while the client is disconnected, and
to resume unfinished QoS 1 and 2 exchanges, use an explicit `clientId` and a positive expiry,
such as `sessionExpiry(Duration.ofDays(1))`. By default, each connection starts a fresh
session.

The client keeps its side of a session in memory. A new client object, for example after your
application restarts, therefore starts a new session: messages that the broker stored for the
old one, including those published while your application was stopped, are not received.

While the client reconnects, unfinished QoS 1 and 2 publications keep waiting. They complete
if the broker still has the session, and fail if it has lost the session or the client gives
up reconnecting; see [MqttPublisher](src/main/java/org/mqtt4j/hato/MqttPublisher.java).

### Leaving a last Will

Use `will(MqttMessage.of(topic, payload, qos, retain))` to have the broker publish a message
if the connection ends without MQTT DISCONNECT. With MQTT 5.0, the Will can also carry message
properties, and `willDelay(Duration.ofSeconds(30))` asks the broker to hold it for a while: a
client with a persistent session that reconnects within the delay publishes no Will.

### Shutting down

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

`close()`, including leaving a try-with-resources block, shuts down at once and can cut a
pending DISCONNECT short. It does not wait for message handlers, listeners, or other callbacks
that are already running. Once `client.terminated()` completes, none of them runs or will
start, so you can release what they use, such as a database connection or an executor that you
gave the client. To release them without waiting, chain the release to the future, as in
`client.terminated().thenRun(pool::close)`. Never wait for `terminated()` from a message
handler or listener: the waiting callback keeps the client from terminating. Work that you
handed to your own queue needs its own shutdown.

## Handling failures

Operations report failures through their futures; with `join()`, the exception arrives as the
cause of a `CompletionException`. Invalid arguments throw at once.

| Exception | Meaning |
| --- | --- |
| `MqttConnectException` | `connect()` gave up: the broker could not be reached, refused the connection, or did not answer in time, or `disconnect()`, `close()`, or a failing message handler withdrew the request first. |
| `MqttNotConnectedException` | The client was not connected, for example while it was reconnecting. |
| `MqttConnectionLostException` | The connection ended before the result was known. A publication may or may not have reached the broker. |
| `MqttCapacityException` | The client reached a limit, such as `maxInflightPublishes` or `maxSubscriptions`. The connection stays up; let outstanding work finish before retrying. |
| `MqttProtocolException` | For `publish()`, the broker accepted a QoS 2 publication only at QoS 1, so it may be delivered more than once; the connection stays up. Other protocol violations end the connection and are its cause. |
| `MqttPublicationRefusedException` | An MQTT 5.0 broker refused the publication, or the publication exceeds a limit that the broker announced. The connection stays up. |
| `MqttSubscriptionRefusedException` | The broker refused the subscription or does not support it. |

### When your handler throws

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

## Working without blocking

Every operation returns a `CompletableFuture`, so an application that must not block, such as
one built around an event loop or a server's request threads, can chain its next step instead
of waiting with `join()`.

### Starting up

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

### Acknowledging after asynchronous work

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

## Where your code runs

None of your code runs on the threads that read and write the network, so a slow callback
never stalls the connection or its keep-alive.

| Your code | Runs on | Can it block? |
| --- | --- | --- |
| Message handler | By default, a virtual thread of the client for each subscription, or up to `n` with striped or parallel delivery. With `withExecutor(pool)` or `lanes(selector)`, a thread of your executor instead | Yes. Its own subscription waits meanwhile, and acknowledgements keep [arrival order](#acknowledgements-keep-arrival-order). On your executor, it also holds that executor's thread |
| Hand-off acceptor | One thread of the client, shared by all hand-off subscriptions | No. The other hand-off subscriptions wait while it runs |
| `receive()` | Your own threads | Yes |
| Connection listener, metrics listener | A thread of its own for each listener, one notification at a time | Briefly. A listener that falls behind loses its oldest notifications |
| Credentials supplier | A virtual thread of the client, before each connection attempt | Within the connection timeout |
| Continuations of a returned future, such as `thenApply` or `whenComplete` | A virtual thread of the client for each completed operation, whatever executors you chose for handlers, or your own thread when the future has already completed as you attach them | Yes, with the cautions below |

- A continuation does not run on the thread that started the operation. A `publish()` that a
  handler starts completes on another thread, so its continuation runs alongside the
  subscription's later messages, outside their order.
- To run a continuation on a thread of your choice, such as your event loop, use the
  `...Async` variant with your executor, as in `whenCompleteAsync(action, executor)`.
- `terminated()` waits until continuations have returned, so one that never returns keeps the
  client from terminating. Continuations of `established()`, `whenConnected()`, and
  `unsubscribe()` also count toward the client's 1,024 pending observations until they return,
  so keep them short.

## Watching the client

### Reading the logs

The client logs through `System.Logger`, so its logs go to `java.util.logging` by default. To
send them to SLF4J or Log4j, add the bridge `org.slf4j:slf4j-jdk-platform-logging` or
`org.apache.logging.log4j:log4j-jpl`. The loggers are named under `org.mqtt4j.hato`:

| Level | What the client logs |
| --- | --- |
| `ERROR` | Internal errors that the client cannot recover from. |
| `WARNING` | Failures of your handlers and listeners, broker protocol violations, subscriptions that end because the broker refuses to re-create them, and notifications that a slow listener starts to lose. |
| `INFO` | Connections, disconnections, failed attempts, reconnection delays, and the start and end of persistent sessions. |
| `DEBUG` | Each packet. |
| `TRACE` | The bytes on the wire. |

Payloads appear only at `TRACE`, and credentials never appear.

### Checking what the client holds

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
last values, ready for a final report. [ClientStats](src/main/java/org/mqtt4j/hato/ClientStats.java)
explains every value.

### Timing publications and deliveries

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

### Recording metrics with Micrometer

The [MicrometerMonitoring](../examples/src/main/java/org/mqtt4j/hato/examples/MicrometerMonitoring.java)
example records these timings in Micrometer timers, times the message handler's own work, and
publishes `stats()` values as gauges and function counters. Run it with
`./gradlew :examples:runMicrometerMonitoring`; it uses a local `SimpleMeterRegistry` where your
application would use its own registry. It takes a snapshot every second on a thread of its
own and lets the meters read the latest one, so that reading the meters never waits for the
client. Since the counts in a snapshot already add up, it registers them as function counters
rather than adding them to a `Counter` on each snapshot.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| `connect()` fails at startup | The exception's cause tells why: the address and port of the broker's listener, TLS trust and hostname, credentials, or the MQTT version. The first attempt is retried only when you [ask for it](#choosing-when-to-reconnect). |
| `publish()` fails while the client reconnects | Publications need a connection and are not queued. Wait with `whenConnected()`, and still expect the connection to end again. |
| `established()` never completes | A subscription waits for a connection: call `connect()`. It also waits while the client reconnects. |
| The client stops reconnecting | The [connection listener's](#reacting-to-connection-changes) ending gives the cause, such as a message handler that threw while holding back an acknowledgement, a ban by the broker, or another client taking over the `clientId`. |
| Every subscription stalls behind one slow topic | One acknowledgement [holds up the rest](#acknowledgements-keep-arrival-order). Look at `oldestAwaitingConditions()` and at each subscription's `waiting()`, `running()`, and `awaitingAck()`. |
| `MqttCapacityException`, or connections that end under load | `refusals()` names the limit that refused an operation, and `overloads()` why the client ended a connection. Compare the `used` and `limit` values before you change a setting. |
| Messages are missing | `receiving().dropped()` gives drops by reason, and `unrouted()` QoS 0 messages that arrived faster than the client could take them. Also check the acknowledgement mode, the session expiry, application restarts, and whether your own queue finished the work. |
| Shutdown never finishes | `terminated()` completes only after `close()`, once every message handler and listener has returned. Do not wait for it from those callbacks. |

## Fitting the client into your application

### Passing the client to your code

Code that only publishes can take an `MqttPublisher`, and code that only subscribes an
`MqttSubscriber`. Neither can disconnect the client.

### Testing your code

In unit tests, the [test kit](../hato-testkit/README.md)'s `FakeMqttPublisher` and
`FakeMqttSubscriber` stand in for the client: they record what your code publishes and
subscribes, and let a test deliver messages to the handlers that your code registered. For
tests that need a broker, the
[embedded broker](../hato-broker/README.md) runs one inside your test, and the test kit
simulates interrupted connections, timeouts, and a broker that refuses or holds back its
answers.

### Seeing it all together

The [examples](../examples/README.md) combine these pieces into complete programs, which
`./gradlew build` compiles and runs.

## Tuning the client

The defaults suit most applications. Change a setting for a concrete need, and check the
result with [stats and timings](#watching-the-client). Builder settings take effect at
`build()`. `Subscribe` and `DeliveryOptions` are immutable: their `with...` methods return
changed copies.

### Capacity and processing

| Setting | Default | Change it to |
| --- | --- | --- |
| `Subscribe.withBufferCapacity(n)` | 1,024 deliveries per subscription | Absorb bursts, or hold less; see [Keeping messages flowing](#keeping-messages-flowing). |
| `maxInflightPublishes(n)` | 64 QoS 1 and 2 publications | Keep more publications outstanding. Beyond the limit, `publish()` fails rather than waits. |
| `maxSubscriptions(n)` | 4,096 | Hold more subscriptions, or catch an application that subscribes without end. Subscriptions that wait for the broker count too. |
| `maxPacketSize(n)` | 16 MiB | Accept larger incoming packets, or only smaller ones. MQTT 5.0 tells the broker the limit; on MQTT 3.1.1, a larger packet ends the connection. |
| `DeliveryOptions.striped(n, key)`, `parallel(n)` | One message at a time | Let the handler process up to 256 messages of a subscription at once; see [Choosing how messages run](#choosing-how-messages-run). |
| `DeliveryOptions.withExecutor(pool)`, `lanes(selector)` | Virtual threads of the client | Run message handlers on executors that your application owns; the client never shuts them down. |

The client keeps some limits internal, such as the deliveries and bytes that it holds across
subscriptions. [Stats](#checking-what-the-client-holds) report their use and limit, and when one
refuses work or ends a connection, `refusals()` and `overloads()` name it.

### Connections and timeouts

| Setting | Default | Change it to |
| --- | --- | --- |
| `version(...)` | MQTT 5.0 | Use `MqttVersion.V3_1_1` for a broker without MQTT 5.0; the client does not fall back by itself. |
| `clientId(...)` | A generated identifier | Give a stable, unique identifier, which a persistent session needs. |
| `sessionExpiry(...)` | Zero: a fresh session for each connection | Keep the session across this client object's connections; see [sessions](#keeping-a-session-between-connections). |
| `keepAlive(...)` | 60 seconds | Detect a dead connection sooner, or send less traffic. Zero turns keep-alive off. |
| `connectionTimeout(...)` | 30 seconds | Bound how long a connection may take to set up, from fetching credentials to the broker's answer. |
| `responseTimeout(...)` | 60 seconds | Bound how long the broker takes to answer a subscription request or a step of a QoS 1 or 2 publication; expiry ends the connection. It is not a deadline for the whole `publish()`. |
| `reconnectPolicy(...)` | Delays from 1 to 120 seconds; a failed first attempt is not retried | Change the delays, retry the first attempt, or turn reconnection off; see [Choosing when to reconnect](#choosing-when-to-reconnect). |

Credentials, TLS, the Will, and the listeners are described where they are used.
`transportFactory(...)` and `timerScheduler(...)` replace the network transport and the timer,
for special integrations and for [tests](../hato-testkit/README.md); the
[builder](src/main/java/org/mqtt4j/hato/MqttClientBuilder.java) describes every setting.

## How fast it is

On a laptop, against a broker stand-in on the same machine that answers at once, with small
messages and message handlers that do nothing, the client spends under a microsecond on each
QoS 0 message that it receives, and a few microseconds on each message that it publishes and
each QoS 1 or 2 message that it receives. Your broker, network, and message handlers set the
pace that you will see.
