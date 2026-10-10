# MQTJ client

Configure connections, manage subscriptions, and handle failures. Add the client as
shown in [Getting it](../README.md#getting-it). For JPMS, the module name is
`org.mqtt4j.mqtj.client`.

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

`server` and `version` are required. Without `clientId`, the client generates a
random one. The other settings have defaults; see the
[builder Javadoc](src/main/java/org/mqtt4j/mqtj/MqttClientBuilder.java).

Connect with `connect()`. After `disconnect()` or a connection failure, the client can
connect again, and established subscriptions resume on their existing handles. End with
`disconnect()` rather than just closing the client: `close()`, including leaving a
try-with-resources block, drops the connection without telling the broker.

## Managing subscriptions

```java
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_MOST_ONCE),
        delivery -> repository.save(delivery.message())
).join();

subscription.unsubscribe().join();
```

- By default, each handler runs one message at a time on its own virtual
  thread. The handler can block, for example to write to a database or to publish
  and wait for the result.
- QoS 1 messages are acknowledged after the handler returns. Choose `AckMode.ON_RECEIVE`
  to acknowledge before the handler runs, or `AckMode.MANUAL` to call `delivery.ack()`
  yourself for every message: `Subscribe.of(filter, Qos.AT_LEAST_ONCE).withAckMode(AckMode.MANUAL)`.
- While the handler is busy, new messages for the subscription wait in a queue of
  1,024, which also holds QoS 1 messages until they are acknowledged. Messages can arrive
  all at once, such as retained messages when you subscribe, so size the queue for them:
  `Subscribe.of(filter, qos).withBufferCapacity(4096)`.
- `subscription.close()` stops delivery at once and drops queued messages without
  waiting for the broker. Use `unsubscribe()` when you need to know that the broker
  has removed the subscription. The same filter can be registered again after removal
  completes.
- Subscriptions survive connection failures, `disconnect()`, and handler failures.
  After the next `connect()`, messages arrive again without another `subscribe()`.
- The client and its subscriptions are safe to use from any thread.

## Receiving on your own threads

Omit the handler to create a pull subscription. Call `receive()` from one or more
application threads:

```java
Subscription subscription = client.subscribe(
        Subscribe.of(new TopicFilter("sensors/+/temperature"), Qos.AT_LEAST_ONCE)
                .withAckMode(AckMode.MANUAL)
).join();

while (subscription.receive() instanceof ReceiveResult.Received(var delivery)) {
    repository.save(delivery.message());
    delivery.ack();
}
```

- Each message goes to one receiving thread.
- `receive()` returns `ReceiveResult.Received` for each message and
  `ReceiveResult.Ended` once the subscription ends normally. `receive(Duration)` can
  also return `ReceiveResult.TimedOut`.
- A waiting `receive()` keeps waiting across reconnections.
- QoS 1 messages are acknowledged when `receive()` hands them over.
  Choose `AckMode.MANUAL`, as above, to acknowledge after your processing.
  `AckMode.AFTER_HANDLER` needs a handler.
- The queue of 1,024 holds messages until a thread takes them, and QoS 1 `MANUAL`
  messages until `delivery.ack()`.
- QoS 1 acknowledgements leave in arrival order across all subscriptions, so a message
  that nobody takes or acknowledges can stop the broker's later QoS 1 messages. If you
  cannot process one, end the connection.
- Messages not yet taken are dropped when the connection ends.

## Handling failures

Operations report failures through their futures. With `join()`, the exception
arrives as the cause of a `CompletionException`.

| Exception | Meaning |
| --- | --- |
| `MqttConnectException` | The broker could not be reached, refused the connection, or did not answer in time. |
| `MqttNotConnectedException` | The client was not connected. |
| `MqttConnectionLostException` | The connection ended before the result was known. A publication may or may not have reached the broker. |
| `MqttCapacityException` | The client had too much work in progress. The connection stays up; wait for pending results, then try again. |
| `MqttSubscribeException` | The broker refused the subscription. |

The `connectionListener` is called whenever a connection ends, whether you
disconnected or not. Besides broker and network failures, the client itself ends
the connection when:

- a handler throws, by default. Catch exceptions inside the handler to keep the
  connection.
- a subscription's queue fills up, for example with messages waiting for
  `delivery.ack()`, or too many QoS 1 messages wait behind one that is not yet
  acknowledged, rather than buffering without limit.

## Passing the client to your code

Code that only publishes can take an `MqttPublisher`, and code that only subscribes
an `MqttSubscriber`. Neither can disconnect the client. `MqttPublisher` has a single
method, so a lambda can replace it in tests.
