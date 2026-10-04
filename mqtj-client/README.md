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

Connect with `connect()`. After `disconnect()`, the client can connect again. End with
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

- By default, each subscription handles one message at a time on its own virtual
  thread. The handler can block, for example to write to a database or to publish
  and wait for the result.
- QoS 1 messages are acknowledged after the handler returns. Choose `AckMode.ON_RECEIVE`
  to acknowledge before the handler runs, or `AckMode.MANUAL` to call `delivery.ack()`
  yourself for every message: `Subscribe.of(filter, Qos.AT_LEAST_ONCE).withAckMode(AckMode.MANUAL)`.
- While the handler is busy, new messages for the subscription wait in a queue of
  256, which also holds QoS 1 messages until they are acknowledged. Change its size with
  `Subscribe.of(filter, qos).withBufferCapacity(1024)`.
- The client and its subscriptions are safe to use from any thread.

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
  `delivery.ack()`, rather than buffering without limit.

## Passing the client to your code

Code that only publishes can take an `MqttPublisher`, and code that only subscribes
an `MqttSubscriber`. Neither can disconnect the client. `MqttPublisher` has a single
method, so a lambda can replace it in tests.
