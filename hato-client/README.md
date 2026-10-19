# Hato client

Configure connections, manage subscriptions, and handle failures. Add the client as
shown in [Getting it](../README.md#getting-it). For JPMS, the module name is
`org.mqtt4j.hato.client`.

## Configuring a connection

```java
MqttClient client = MqttClient.builder()
        .server(URI.create("mqtt://broker.example.com:1883"))
        .version(MqttVersion.V3_1_1)
        .clientId("sensor-17")
        .keepAlive(Duration.ofSeconds(30))
        .connectionListener(end -> System.out.println("Connection ended: " + end))
        .build();
```

`server` and `version` are required. By default, each connection starts a fresh
session, and omitting `clientId` generates a random one. The other settings have
defaults; see the
[builder Javadoc](src/main/java/org/mqtt4j/hato/MqttClientBuilder.java).

For TLS, use `mqtts://broker.example.com` (default port 8883). The client uses the
JDK's default trust and verifies the broker hostname. Supply `sslContext(context)`
for your own CA or client certificate, and `sslParameters(parameters -> ...)` for
SNI, ALPN, or protocol settings.

For username/password authentication, add
`credentials(() -> MqttCredentials.of(username, passwordBytes))`. The password is
binary and is copied when the credentials are created. For a username without a
password, use `MqttCredentials.of(username)`. The supplier runs once per connection
attempt on a virtual thread before opening the transport, so it can fetch a fresh token;
all connections of one attempt use the same token.
Evaluation, transport establishment, and CONNACK share one connection timeout.
Its exception, a null result, or exceeding the connection timeout fails that attempt with
`MqttConnectException`. Only one supplier invocation runs at a time per client;
respond to interruption so an abandoned invocation does not delay the next attempt.

Use `will(MqttMessage.of(topic, payload, qos, retain))` to ask the broker to publish
a last Will if the connection ends without receiving MQTT DISCONNECT. For normal
shutdown, wait for `disconnect()` to complete successfully before calling `close()`,
as shown below. Any QoS and retention flag are supported. A Will payload must fit in
65,535 bytes; `build()` rejects larger payloads.

To retain a session between connections, use an explicit `clientId` and set
`cleanStart(false).sessionExpiry(Duration.ofDays(1))`. For MQTT 3.1.1, a positive
expiry selects a persistent session; the broker determines how long it retains
the session. The client keeps its own session state in memory. When a new client object
connects, for example after your application restarts, it therefore replaces the session that
the broker kept with a new one: messages that the broker stored for the old session, including
those published while your application was stopped, are not received.

## Staying connected

`connect()` asks the client to stay connected. When the connection drops, the client
reconnects by itself, waiting longer after each failed attempt, and established
subscriptions and a persistent session's unfinished QoS 1 and 2 publications resume.
`disconnect()` and `close()` stop it.

By default, `connect()` fails if its first attempt fails, for example because the broker
cannot be reached or refuses the credentials, so a configuration error shows up at startup.
To keep trying instead, or to change the delays, set a policy on the builder:

```java
ReconnectPolicy policy = ReconnectPolicy
        .exponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30))
        .withInitialRetry();
MqttClient client = MqttClient.builder()
        .server(URI.create("mqtt://broker.example.com:1883"))
        .version(MqttVersion.V3_1_1)
        .reconnectPolicy(policy)
        .build();
```

`ReconnectPolicy.disabled()` reconnects only when you call `connect()` again.
`whenConnected()` completes when the client is connected, without starting a connection.
To check repeatedly, keep the returned future and call `whenConnected()` again only after it
completes.

Give every running client its own `clientId`: two clients with the same identifier keep
taking over each other's connection, and with a persistent session, one that connects later
also discards the other's session.

To shut down a connected client, wait for the asynchronous DISCONNECT write before closing:

```java
try {
    client.disconnect().join();
} finally {
    client.close();
}
```

`close()`, including leaving a try-with-resources block, shuts down immediately and can
discard a pending DISCONNECT. Successful completion of `disconnect()` confirms the write,
but the broker does not acknowledge receipt. If disconnection fails, the Will may still
be published.

`close()` does not wait for handlers or other callbacks that are already running. Once
`client.terminated()` completes, none of them runs or will start, so you can release what
they use, such as a database connection or an injected `TimerScheduler`.

## Publishing

Use `Qos.EXACTLY_ONCE` in the [publishing example](../README.md#publishing)
to publish at QoS 2.

## Managing subscriptions

```java
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.EXACTLY_ONCE),
        delivery -> repository.save(delivery.message())
);
subscription.established().join();

subscription.unsubscribe().join();
```

- Each subscription's handler runs one message at a time, independently of I/O
  and operation completion. The handler can block, for example to write to a
  database or to publish and wait for the result.
- QoS 1 and 2 messages are acknowledged after the handler returns. Choose `AckMode.ON_RECEIVE`
  to acknowledge before the handler runs, or `AckMode.MANUAL` to call `delivery.ack()`
  yourself for every message: `Subscribe.of(filter, Qos.AT_LEAST_ONCE).withAckMode(AckMode.MANUAL)`.
- While the handler is busy, new messages for the subscription wait in a queue of
  1,024, which also holds QoS 1 and 2 messages until they are acknowledged. Messages can arrive
  all at once, such as retained messages when you subscribe, so size the queue for them:
  `Subscribe.of(filter, qos).withBufferCapacity(4096)`. A QoS 0 message that finds the queue
  full is dropped and logged, and the subscription goes on; a QoS 1 or 2 message ends the
  connection instead, as described below.
- `unsubscribe()` stops new messages and lets the queued ones finish; its future tells you
  when the broker has removed the subscription. `subscription.close()` instead stops delivery
  at once and drops queued messages without waiting for the broker. Either way,
  `subscription.terminated()` completes once the subscription has ended and its last handler
  has returned. The same filter can be registered again after removal completes.
- `subscribe()` returns a handle immediately, even before `connect()`. Use
  `subscription.established()` to wait for broker acceptance and obtain the granted QoS; it
  also tells you when the broker refuses the subscription. Registrations survive connection
  failures, `disconnect()`, failed connection attempts, and handler failures. After the next
  connection, including an automatic reconnection, messages arrive without another
  `subscribe()`.
- `unsubscribe()` also works while disconnected. It completes locally for a nonpersistent
  session; a persistent session waits for the next connection to confirm removal. Connection
  loss does not fail a pending removal.
- To replace a handler without waiting to reuse its filter, keep the subscription and switch
  its delegate through an application-owned atomic reference.
- The client and its subscriptions are safe to use from any thread.

## Running handlers on your executor

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
- Acknowledgement and failures work as on the client's own threads. An executor that refuses a
  task fails that message as a throwing handler would.
- The executor stays yours: the client never shuts it down. Shut it down once
  `client.terminated()` completes; tasks it still holds then do nothing.

To keep each device's messages in order while different devices run in parallel, pick a lane,
an executor, for every message:

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
- Acknowledgements still leave in arrival order, so a slow lane holds back the acknowledgement
  of later QoS 1 and 2 messages on other lanes.

## Receiving on your own threads

Omit the handler to create a pull subscription. Call `receive()` from one or more
application threads:

```java
Subscription subscription = client.subscribe(
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
- `receive()` returns `ReceiveResult.Received` for each message and
  `ReceiveResult.Ended` once the subscription ends normally. After `unsubscribe()`, the
  messages already queued are received first. `receive(Duration)` can also return
  `ReceiveResult.TimedOut`.
- A waiting `receive()` keeps waiting across reconnections.
- QoS 1 and 2 messages are acknowledged when `receive()` hands them over.
  Choose `AckMode.MANUAL`, as above, to acknowledge after your processing.
  `AckMode.AFTER_HANDLER` needs a handler.
- The queue of 1,024 holds messages until a thread takes them, and QoS 1/2 `MANUAL`
  messages until `delivery.ack()`.
- QoS 1 and 2 acknowledgements leave in arrival order across all subscriptions, so a message
  that nobody takes or acknowledges can stop the broker's later QoS 1 and 2 messages. If you
  cannot process one, end the connection.
- Messages not yet taken are dropped when the connection ends.

## Handing messages to your own mailbox

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

- Return `true` to take a message; from then on it is yours to process or drop. QoS 1 and 2
  messages are acknowledged when you take them.
- Return `false` when your mailbox is full. The client treats that as a full subscription
  queue: a QoS 0 message is dropped, and a QoS 1 or 2 message ends the connection.
- One thread of the client offers the messages of all its hand-off subscriptions in the order
  they arrive, so your mailbox sees one order across them. Never wait in the acceptor: use
  `offer`, not `put`, or the other hand-off subscriptions wait too.
- To stop, close the subscription, wait for `subscription.terminated()`, and then drain or
  discard your mailbox: nothing is offered after that. Each message reports
  `delivery.connectionAttempt()`, so messages kept across a reconnection can be told apart
  from new ones.

## Handling failures

Operations and observations report failures through their futures. With `join()`, the exception
arrives as the cause of a `CompletionException`. Invalid subscription arguments and calls to
`subscribe()` after client closure throw synchronously. Duplicate filters, shared-filter overlap,
and registration capacity fail `established()` and end only the returned handle.

| Exception | Meaning |
| --- | --- |
| `MqttConnectException` | `connect()` gave up: the broker could not be reached, refused the connection, did not answer in time, or still reported a session after the client discarded it, or `disconnect()`, `close()`, or a failing handler withdrew the request first. |
| `MqttNotConnectedException` | The client was not connected, for example while it was reconnecting. |
| `MqttConnectionLostException` | The connection ended before the result was known. A publication may or may not have reached the broker. |
| `MqttCapacityException` | The client reached an admission limit. The connection stays up; let outstanding work drain before retrying. |
| `MqttProtocolException` | The broker accepted a QoS 2 publication only at QoS 1, so it may be delivered more than once. The connection stays up. |
| `MqttSubscribeException` | The broker refused the subscription. |

The `connectionListener` is called whenever a connection ends, whether you
disconnected or not. A lambda, as in the configuration example, handles just
these endings; override the other listener methods to observe successful
connections, failed connection attempts, and subscriptions that the broker refuses
to re-create:

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
    public void onResubscribeFailed(Subscription subscription, MqttSubscribeException failure) {
        System.err.println("Subscription ended: " + subscription.filter() + ": " + failure);
    }
};
```

Pass it with `connectionListener(listener)` when building the client. Connection
notifications carry the attempt's number, and endings tell when the client reconnects. Once
connected, or with `withInitialRetry()`, the client keeps retrying after refusals such as bad
credentials; to stop, call `disconnect()` from `onConnectFailed`. A refused subscription
ends; the connection and its other subscriptions continue. A listener that falls far behind
loses its oldest notifications instead of slowing the client down, so release resources when
the futures that own them complete, not only in `onDisconnected`. See
[ConnectionListener](src/main/java/org/mqtt4j/hato/ConnectionListener.java) for callback
ordering and threading.

Besides broker and network failures, the client itself ends the connection when:

- a handler throws for a QoS 1 or 2 message whose acknowledgement it still holds back
  (`AFTER_HANDLER`, or `MANUAL` before `delivery.ack()`), including one still running from
  before a reconnection in a persistent session, or the executor that runs it refuses its
  task. The client then stops reconnecting until you call `connect()` again, and a
  persistent session gets the message again from the broker.
- a QoS 1 or 2 message arrives for a subscription whose queue is full, for example with
  messages waiting for `delivery.ack()`, or too many QoS 1 and 2 messages wait behind one that
  is not yet acknowledged, rather than buffering without limit.

A handler failure that holds back nothing, such as one for a QoS 0 message, is skipped: the
client logs it and the subscription goes on. To skip failed QoS 1 and 2 messages too,
acknowledging them unprocessed, choose `FailurePolicy.SKIP`:
`Subscribe.of(filter, Qos.AT_LEAST_ONCE).withFailurePolicy(FailurePolicy.SKIP)`.

For a transient error, catch it and retry inside the handler with a bounded retry
policy. To skip a single message, record the error and return normally with
`AFTER_HANDLER`, or call `delivery.ack()` with `MANUAL`. Returning normally or
acknowledging records completion even when the application chose not to process
the message.

## Passing the client to your code

Code that only publishes can take an `MqttPublisher`, and code that only subscribes
an `MqttSubscriber`. Neither can disconnect the client. `MqttPublisher` has a single
method, so a lambda can replace it in tests.
