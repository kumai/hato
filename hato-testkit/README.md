# Hato test kit

Test how your application handles interrupted connections, timeouts, and a broker
that refuses or holds back its answers. The test kit includes the
[client](../hato-client/README.md) and [embedded broker](../hato-broker/README.md) as
dependencies.

## Getting it

Gradle:

```kotlin
dependencies {
    testImplementation("org.mqtt4j:hato-testkit:x.y.z")
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>hato-testkit</artifactId>
    <version>x.y.z</version>
    <scope>test</scope>
</dependency>
```

For JPMS, the module name is `org.mqtt4j.hato.testkit`.

## Testing with a broker

Start an embedded broker for each test and close it afterward with
try-with-resources. See the [broker example](../hato-broker/README.md#starting-and-stopping).
If you only need a broker, you can depend on `hato-broker` directly.

## Controlling the broker

[BrokerControls](src/main/java/org/mqtt4j/hato/testkit/BrokerControls.java) lets a
test make the embedded broker refuse, hold back, or disconnect:

```java
try (MqttBroker broker = MqttBroker.start()) {
    BrokerControls controls = BrokerControls.of(broker);
    controls.refuseSubscriptions(new TopicFilter("alarms/#"));
    controls.holdResponses(ResponseKind.PUBACK, "device-1");
    // Run the code under test, and check what it does while it waits.
    controls.releaseResponses(ResponseKind.PUBACK, "device-1");
}
```

- `refuseSubscriptions(filter)` refuses subscriptions to that exact filter, and
  `grantAtMost(filter, qos)` grants them a lower QoS. `clearSubscriptionRule(filter)`
  lets them through again, so a test can refuse a subscription for a while.
- `holdResponses(kind, clientId)` holds back SUBACK, UNSUBACK, PUBACK, PUBREC, or
  PUBCOMP for one client until `releaseResponses(kind, clientId)` sends them in
  order, for example to see the application time out. `heldResponses()` lists what
  the broker holds, so wait for an entry before you act.
- `connectedClients()` lists the clients with an open connection, and
  `disconnect(clientId)` closes one from the broker's side, which publishes the
  client's Will.

## Simulating failures and timeouts

- [FaultyTransport](src/main/java/org/mqtt4j/hato/testkit/FaultyTransport.java)
  wraps a transport such as `Transport.tcp(clientId)`. Supply a fresh wrapper through
  the client's `transportFactory` for each connection. Call `disconnect(cause)`
  to simulate a connection failure, `receiveDelay(Duration)` to delay incoming
  data, or `sendDelay(Duration)` to delay outgoing writes. Use a long delay in
  either direction to simulate stopped traffic. Disconnect while a send is still
  delayed to fail it before writing. After a simulated failure, the client
  reconnects once the delay that `Disconnection.reconnectDelay()` reports has
  passed. On manual time, advance the client's clock by that delay, or build the
  client with `ReconnectPolicy.disabled()` to keep it disconnected.
- [ManualTimerScheduler](src/main/java/org/mqtt4j/hato/testkit/ManualTimerScheduler.java)
  lets a test advance time with `advance(Duration)` instead of sleeping. Supply
  it to the client's `timerScheduler` or to `FaultyTransport` to control their
  timers. Advancing time runs due timers; wait separately for the client's
  response before checking the result.

`FaultyTransport` is experimental: its API may change as it gains kinds of
faults. `BrokerControls`, `ResponseKind`, and `HeldResponse` are experimental,
like the broker that they control.
