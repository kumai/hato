# Hato client

Connect to a broker, publish, receive messages, handle failures, and fit the client into your
application. Add the client as shown in [Getting it](../README.md#getting-it). For JPMS, the
module name is `org.mqtt4j.hato.client`. The Javadoc of each type gives its exact behavior,
starting with [MqttClient](src/main/java/org/mqtt4j/hato/MqttClient.java).

## How the client works

A few behaviors shape everything below:

- **It stays connected.** `connect()` asks the client to stay connected. When the connection
  drops, the client reconnects by itself until you call `disconnect()` or `close()`.
- **Subscriptions outlast connections.** `subscribe()` registers a subscription, even before
  `connect()`, and the client subscribes again after each reconnection. Subscribe once.
- **Your code may block.** A subscription's handler runs one message at a time on a virtual
  thread of the client, apart from the network. It can write to a database, or publish and
  wait for the result.
- **Acknowledgements keep arrival order.** The client acknowledges QoS 1 and 2 messages in the
  order they arrived, across all subscriptions of a connection, so one slow message can hold
  up the rest; see [Keeping messages flowing](#keeping-messages-flowing).
- **Results arrive as futures.** Operations return a `CompletableFuture` and report failures
  through it. The client and its subscriptions are safe to use from any thread.

## Connecting to a broker

```java
MqttClient client = MqttClient.builder()
        .server(URI.create("mqtt://broker.example.com:1883"))
        .clientId("sensor-17")
        .keepAlive(Duration.ofSeconds(30))
        .build();
client.connect().join();
```

`server` is required, and the other settings have defaults; see the
[builder Javadoc](src/main/java/org/mqtt4j/hato/MqttClientBuilder.java). Omitting `clientId`
generates a random one. Give every running client its own `clientId`: two clients with the
same identifier keep taking over each other's connection, and with a persistent session, the
one that connects later also discards the other's session.

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
  `sslParameters(parameters -> ...)` for SNI, ALPN, or protocol settings.
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

### Keeping a session between connections

To have the broker keep messages for your subscriptions while the client is disconnected, and
to resume unfinished QoS 1 and 2 exchanges, use an explicit `clientId` and a positive expiry,
such as `sessionExpiry(Duration.ofDays(1))`. By default, each connection starts a fresh
session.

The client keeps its side of a session in memory. A new client object, for example after your
application restarts, therefore starts a new session: messages that the broker stored for the
old one, including those published while your application was stopped, are not received.

### Connecting to an MQTT 3.1.1 broker

The client speaks MQTT 5.0. For a broker without it, choose `version(MqttVersion.V3_1_1)`.
The features marked MQTT 5.0 in this guide are then unavailable: the client refuses messages
with properties and subscriptions with options, and `build()` rejects a Will with properties
or a delay. A positive `sessionExpiry` still keeps the session, but the broker decides for how
long.

## Staying connected

### Choosing when to reconnect

The client waits longer after each failed attempt to reconnect. Once it is back, subscriptions
and a persistent session's unfinished QoS 1 and 2 publications resume.

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

The client stays disconnected after an MQTT 5.0 broker ends the connection for an
administrative action, and keeps retrying after an unspecified error. To reverse either, add
`withAdministrativeActionRetry()` or `withoutUnspecifiedErrorRetry()` to the policy.
`ReconnectPolicy.disabled()` reconnects only when you call `connect()` again.

To wait until the client is connected without starting a connection, use `whenConnected()`.

### Leaving a last Will

Use `will(MqttMessage.of(topic, payload, qos, retain))` to have the broker publish a message
if the connection ends without MQTT DISCONNECT. With MQTT 5.0, the Will can also carry message
properties, and `willDelay(Duration.ofSeconds(30))` asks the broker to hold it for a while: a
client with a persistent session that reconnects within the delay publishes no Will.

### Shutting down

Wait for `disconnect()` before closing the client. It sends MQTT DISCONNECT, which tells the
broker to discard the Will:

```java
try {
    client.disconnect().join();
} finally {
    client.close();
}
```

`close()`, including leaving a try-with-resources block, shuts down at once and can cut a
pending DISCONNECT short. It does not wait for handlers or other callbacks that are already
running. Once `client.terminated()` completes, none of them runs or will start, so you can
release what they use, such as a database connection or an executor that you gave the client.

## Publishing

Publish as in the [publishing example](../README.md#publishing). For messages that must
arrive, use `Qos.AT_LEAST_ONCE` or `Qos.EXACTLY_ONCE`; the future then completes once the
broker has taken the message.

### Publishing many messages at once

A client keeps up to 64 QoS 1 and 2 publications unfinished at once. A publication beyond
that fails with `MqttCapacityException` instead of waiting, so a fast publisher should let
earlier futures complete before publishing more. Set `maxInflightPublishes(n)` on the builder
to keep more or fewer.

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
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.EXACTLY_ONCE),
        delivery -> repository.save(delivery.message())
);
subscription.established().join();
```

- `subscribe()` returns a handle at once, even before `connect()`. `established()` completes
  with the granted QoS when the broker accepts the subscription, and fails if the broker
  refuses it.
- The handler runs one message at a time, in arrival order.
  [Choosing how messages run](#choosing-how-messages-run) shows the other ways.
- A client holds up to 4,096 subscriptions by default; `maxSubscriptions(n)` raises or lowers
  that. Routing a message takes no longer as the client holds more.

### Stopping a subscription

`unsubscribe()` stops new messages and lets the queued ones finish; its future completes when
the broker has removed the subscription. `subscription.close()` instead stops delivery at once
and drops the queued messages. Either way, `subscription.terminated()` completes once the last
handler has returned. Once removal completes, the same filter can be subscribed again.

To replace a handler without giving up the subscription, have the handler call a delegate that
your application swaps through an atomic reference.

### Choosing when to acknowledge

| `AckMode` | Acknowledges a QoS 1 or 2 message | Default for |
| --- | --- | --- |
| `AFTER_HANDLER` | When the handler returns normally | Subscriptions with a handler |
| `ON_RECEIVE` | When your code receives it, before processing | Hand-off and pull subscriptions |
| `MANUAL` | When you call `delivery.ack()`, which you do for every message | |

Set it on the request: `Subscribe.of(filter, Qos.AT_LEAST_ONCE).withAckMode(AckMode.MANUAL)`.
Choose `ON_RECEIVE` when you can afford to lose a message whose processing fails or is cut
short, and `MANUAL` when processing ends after your code returns, for example in your own
queue.

### Setting subscription options (MQTT 5.0)

`withNoLocal(true)` leaves out your own publications, `withRetainAsPublished(true)` keeps the
retain flag that the publisher set, and `withRetainHandling(RetainHandling.DO_NOT_SEND)` skips
the retained messages when you subscribe. `RetainHandling.SEND_IF_NEW_SUBSCRIPTION` sends them
only for a subscription that the broker does not have yet.

## Choosing how messages run

A subscription with a plain handler runs its messages one at a time, in arrival order. The
other ways change where your code runs, how many messages run at once, and which order is
kept:

| When you need | Use | Order | At once |
| --- | --- | --- | --- |
| The simplest code | [`client.subscribe(request, handler)`](#handling-each-message) | arrival | one |
| Your own thread or pool | [`DeliveryOptions.serial().withExecutor(pool)`](#running-handlers-on-your-executor) | arrival | one |
| The threads that own each device's state | [`DeliveryOptions.lanes(selector)`](#running-each-device-on-its-own-thread) | per lane | one per lane |
| Each device in order, devices in parallel | [`DeliveryOptions.striped(n, key)`](#running-devices-in-parallel) | per key | up to `n` |
| Independent messages, as fast as possible | [`DeliveryOptions.parallel(n)`](#running-independent-messages-in-parallel) | none | up to `n` |
| A queue that your application owns | [`client.handOff(request, mailbox::offer)`](#handing-messages-to-your-own-mailbox) | arrival, into your queue | your choice |
| Your own threads taking messages | [`client.subscribe(request)` and `receive()`](#receiving-on-your-own-threads) | arrival, as taken | one per thread |

- Start with a plain handler. When it is slower than messages arrive, such as a wildcard
  subscription whose handler writes each reading to a database, messages queue up and their
  acknowledgements wait behind them. Striping by device lets devices go on independently
  while each keeps its order, and with enough stripes the handlers can keep up.
- Choose by the order that you need, not by overhead: passing messages to stripes or parallel
  handlers costs little next to a handler that does real work.
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
ExecutorService pool = Executors.newFixedThreadPool(4);

Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.EXACTLY_ONCE),
        DeliveryOptions.serial().withExecutor(pool),
        delivery -> repository.save(delivery.message())
);
```

- The handler still runs one message at a time, in arrival order, even on a pool of many
  threads.
- Waiting messages stay in the subscription's queue, and the subscription holds none of the
  executor's threads while no message waits, so a busy subscription cannot flood the executor.
- An executor that refuses a task fails that message as a throwing handler would.
- The executor stays yours: the client never shuts it down. Shut it down once
  `client.terminated()` completes.

### Running each device on its own thread

To choose the executor for each message yourself, such as the single thread that owns a
device's state, pick a lane, an executor, for every message:

```java
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("devices/+/events"), Qos.AT_LEAST_ONCE),
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
        DeliveryOptions.striped(16, message -> deviceId(message.topic())),
        delivery -> devices.apply(delivery.message())
);
```

- Messages with equal keys run one at a time in arrival order. At most 16 handlers of this
  subscription run at once, each on a virtual thread of the client.
- `DeliveryOptions.striped(16)` uses each message's topic as its key.
- The key's hash picks one of the 16 stripes, so two devices can share a stripe and then wait
  for each other.
- Add `.withExecutor(pool)` to run the stripes on your own executor.

### Running independent messages in parallel

When messages do not depend on each other, run them in any order:

```java
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("jobs/+/requests"), Qos.AT_LEAST_ONCE),
        DeliveryOptions.parallel(16),
        delivery -> jobs.run(delivery.message())
);
```

- Up to 16 messages run at once, in no particular order: a later message can finish before an
  earlier one, even one from the same device.
- `.withExecutor(pool)` works here too.

### Handing messages to your own mailbox

When your application already runs its work through its own queue, such as one mailbox per
device, hand each message to that queue directly. The client offers each message to your
acceptor and lets go of it once the acceptor takes it:

```java
BlockingQueue<MqttDelivery> mailbox = new ArrayBlockingQueue<>(10_000);

Subscription subscription = client.handOff(
        Subscribe.of(new TopicFilter("devices/+/telemetry"), Qos.AT_LEAST_ONCE),
        mailbox::offer
);
```

- Return `true` to take a message; from then on it is yours to process or drop.
- Return `false` when your mailbox is full. The client treats that as a full subscription
  queue; see [Keeping messages flowing](#keeping-messages-flowing).
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

Omit the handler to create a pull subscription. Call `receive()` from one or more
application threads:

```java
PullSubscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_LEAST_ONCE)
                .withAckMode(AckMode.MANUAL)
);
subscription.established().join();

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

Each subscription has a queue of 1,024 messages. It holds messages while your code is busy,
and QoS 1 and 2 messages until they are acknowledged. Messages can arrive all at once, such as
retained messages when you subscribe, so size the queue for them:
`Subscribe.of(filter, qos).withBufferCapacity(4096)`. When the queue is full, a QoS 0 message
is dropped and logged, and the subscription goes on. A QoS 1 or 2 message ends the connection
instead of growing the queue without limit.

Because acknowledgements keep arrival order across the connection, a message whose
acknowledgement waits, in a queue or for slow processing, holds back the acknowledgements of
all later ones, from every subscription. When too many wait behind it, the client also ends
the connection. To keep messages flowing:

- Process messages as fast as they arrive.
  [Choosing how messages run](#choosing-how-messages-run) shows how to spread the work.
- Acknowledge before processing with `AckMode.ON_RECEIVE` when you can afford to lose a
  message whose processing fails or is cut short.
- Receive slow topics with a separate client, which has its own connection and its own order.
- Take and acknowledge every message. A message that nobody takes or acknowledges can stop
  the broker's later QoS 1 and 2 messages; if you cannot process one, end the connection.

[Checking what the client holds](#checking-what-the-client-holds) shows how to spot messages
that wait too long.

## Handling failures

Operations report failures through their futures. With `join()`, the exception arrives as the
cause of a `CompletionException`.

| Exception | Meaning |
| --- | --- |
| `MqttConnectException` | `connect()` gave up: the broker could not be reached, refused the connection, or did not answer in time, or `disconnect()`, `close()`, or a failing handler withdrew the request first. |
| `MqttNotConnectedException` | The client was not connected, for example while it was reconnecting. |
| `MqttConnectionLostException` | The connection ended before the result was known. A publication may or may not have reached the broker. |
| `MqttCapacityException` | The client reached an admission limit, such as `maxInflightPublishes` or `maxSubscriptions`. The connection stays up; let outstanding work drain before retrying. |
| `MqttProtocolException` | The broker accepted a QoS 2 publication only at QoS 1, so it may be delivered more than once. The connection stays up. |
| `MqttPublicationRefusedException` | An MQTT 5.0 broker refused the publication, or the publication exceeds a limit that the broker announced. The connection stays up. |
| `MqttSubscriptionRefusedException` | The broker refused the subscription or does not support it. |

### Reacting to connection changes

The `connectionListener` is called whenever a connection ends, whether you disconnected or
not. A lambda, such as `connectionListener(end -> System.out.println("Connection ended: " + end))`,
handles just these endings. Override the other listener methods to observe successful
connections, failed connection attempts, and subscriptions that the broker refuses to
re-create:

```java
ConnectionListener listener = new ConnectionListener() {
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

### Handling failures in your handler

A handler that throws for a QoS 1 or 2 message whose acknowledgement it still holds back
(`AFTER_HANDLER`, or `MANUAL` before `delivery.ack()`) ends the connection, and the client
stops reconnecting until you call `connect()` again. A persistent session then gets the
message again from the broker.

A failure that holds back nothing, such as one for a QoS 0 message, is skipped: the client
logs it and the subscription goes on. To skip failed QoS 1 and 2 messages too, acknowledging
them unprocessed, choose `FailurePolicy.SKIP`:
`Subscribe.of(filter, Qos.AT_LEAST_ONCE).withFailurePolicy(FailurePolicy.SKIP)`.

For a transient error, catch it and retry inside the handler with a bounded retry policy; the
subscription keeps its order meanwhile. To skip a single message, record the error and return
normally with `AFTER_HANDLER`, or call `delivery.ack()` with `MANUAL`.

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
        .ifPresent(age -> System.err.println("A message has waited for its handler for " + age));
for (ClientStats.SubscriptionStats subscription : stats.subscriptions()) {
    System.out.println(subscription.filter().pattern() + ": " + subscription.waiting()
            + " waiting, " + subscription.dropped());
}
```

It also counts operations refused by each limit, connections that the client ended because it
was overloaded, and notifications and timings that a slow listener lost. A snapshot takes
longer the more subscriptions the client holds, and calls made while one is pending share it.
After `close()`, `stats()` returns the last values, ready for a final report.

### Timing publications and deliveries

To time the client, register a `MetricsListener`. It learns how long each publication took,
from `publish()` until its future completed, and how long each message waited between its
arrival and your code receiving it, whether through a handler, a hand-off, or `receive()`.
Feed the timings to the timers of your metrics library:

```java
MetricsListener timings = new MetricsListener() {
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

Pass it with `metricsListener(timings)` when building the client. Its callbacks run on a
thread of their own, so a slow listener never delays publications or deliveries. Without a
listener, the client takes no timings.

## Fitting the client into your application

### Passing the client to your code

Code that only publishes can take an `MqttPublisher`, and code that only subscribes an
`MqttSubscriber`. Neither can disconnect the client.

### Testing your code

In unit tests, the [test kit](../hato-testkit/README.md)'s `FakeMqttPublisher` and
`FakeMqttSubscriber` stand in for the client: they record what your code publishes and
subscribes, and let a test deliver messages to its handlers. For tests that need a broker, the
[embedded broker](../hato-broker/README.md) runs one inside your test, and the test kit
simulates interrupted connections, timeouts, and a broker that refuses or holds back its
answers.

### Seeing it all together

The [examples](../examples/README.md) combine these pieces into complete programs, which
`./gradlew build` compiles and runs.

## How fast it is

On a MacBook Pro with an Apple M1 Max and Java 25, against a broker stand-in on the same
machine that answers at once, with messages of a few bytes and handlers that do nothing, a
client takes per message:

| | QoS 0 | QoS 1 | QoS 2 |
| --- | ---: | ---: | ---: |
| Receiving | 0.9 µs | 6 µs | 11 µs |
| Publishing, 64 at a time | 2.3 µs | 3.7 µs | 7 µs |

That is over a million QoS 0 messages a second received, or about 170,000 at QoS 1. Striped
and parallel handlers add about a microsecond to run messages on other threads, and a
`MetricsListener` adds under a microsecond to a publication. Routing a message takes as long
with 100,000 subscriptions as with one.

These figures are a baseline to compare against, not a promise: your network, your broker,
and your handlers set the pace that you will see.
