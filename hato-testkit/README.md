# Hato test kit

Test the code that publishes and subscribes without a broker, and how your
application handles interrupted connections, timeouts, and a broker that refuses,
holds back its answers, or announces narrower limits. The test kit includes the
[client](../hato-client/README.md) and [embedded broker](../hato-broker/README.md) as
dependencies.

## Getting it

Gradle:

```kotlin
dependencies {
    testImplementation("org.mqtt4j:hato-testkit:0.8.1")
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>hato-testkit</artifactId>
    <version>0.8.1</version>
    <scope>test</scope>
</dependency>
```

For JPMS, the module name is `org.mqtt4j.hato.testkit`.

## Testing code that publishes or subscribes

Code that takes an `MqttPublisher` or an `MqttSubscriber` can be tested without a
client or a broker.
[FakeMqttPublisher](src/main/java/org/mqtt4j/hato/testkit/FakeMqttPublisher.java)
records what your code publishes, and
[FakeMqttSubscriber](src/main/java/org/mqtt4j/hato/testkit/FakeMqttSubscriber.java)
records its subscriptions and lets the test deliver messages to them:

```java
var publisher = new FakeMqttPublisher();
var subscriber = new FakeMqttSubscriber();
// The code under test subscribes to alarms and forwards each one.
new AlarmForwarder(subscriber, publisher).start();

FakeMqttDelivery delivery = subscriber.subscription(new TopicFilter("sensors/+/alarm"))
        .orElseThrow()
        .deliver(MqttMessage.of(
                new Topic("sensors/kitchen/alarm"),
                "smoke".getBytes(UTF_8),
                Qos.AT_LEAST_ONCE,
                false
        ));

assertThat(publisher.published())
        .extracting(MqttMessage::topic)
        .containsExactly(new Topic("alarms/kitchen"));
assertThat(delivery.failure()).isEmpty();
```

- `FakeMqttPublisher` completes each publication at once. `failPublications(failure)`
  fails later ones, for example with `MqttNotConnectedException`, and
  `holdPublications()` holds them until the test completes or fails each of
  `heldPublications()`, so a test can see what your code does while it waits.
- `FakeMqttSubscriber` grants each subscription the QoS it asks for, and
  `refuseSubscriptions(filter)` and `grantAtMost(filter, qos)` work as they do on
  the broker controls below. `deliver(message)` runs the handler on the test's
  thread and returns the delivery, which shows whether your code called `ack()`
  and what the handler threw.
- The fakes stand in for the roles, not for a client: they neither connect nor run
  handlers on other threads, and they do not support pull subscriptions, delivery
  options, or hand-off subscriptions. To see how your code works with a real client,
  test it with a broker.

## Testing with a broker

Start an embedded broker for each test and close it afterward with
try-with-resources. See the [broker example](../hato-broker/README.md#starting-and-stopping).
If you only need a broker, you can depend on `hato-broker` directly.

## Controlling the broker

[BrokerControls](src/main/java/org/mqtt4j/hato/testkit/BrokerControls.java) lets a
test make the embedded broker refuse, hold back, disconnect, or announce narrower
limits:

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
- `refusePublications(filter, reason)` refuses publications to the topics that the
  filter matches: they reach no subscriber, and an MQTT 5.0 client's QoS 1 or 2
  publication fails with the reason. `refuseConnections(clientId, reason)` refuses
  the client's connections, for example with bad credentials or a redirection, and
  `refuseSubscriptions(filter, reason)` gives a subscription refusal a reason.
  `clearPublicationRule(filter)` and `clearConnectionRule(clientId)` let them through
  again. Build a reason with
  [BrokerReason](src/main/java/org/mqtt4j/hato/testkit/BrokerReason.java), for
  example `BrokerReason.of(ReasonCode.NOT_AUTHORIZED).withReasonString("no access")`;
  each operation refuses a reason code that its MQTT packet cannot carry.
- `holdResponses(kind, clientId)` holds back SUBACK, UNSUBACK, PUBACK, PUBREC, or
  PUBCOMP for one client until `releaseResponses(kind, clientId)` sends them in
  order, for example to see the application time out. `heldResponses()` lists what
  the broker holds, so wait for an entry before you act.
- `connectedClients()` lists the clients with an open connection, and
  `disconnect(clientId)` closes one from the broker's side, which publishes the
  client's Will as for a lost connection, after its delay if it has one.
  `disconnect(clientId, reason)` tells an MQTT 5.0 client a reason of the test's
  choice instead of Administrative action.
- `announce(capabilities)` changes the limits that the broker announces to MQTT 5.0
  clients as they connect: the highest QoS, whether retained messages, wildcard
  subscriptions, and Subscription Identifiers are available, the Receive Maximum, the
  Maximum Packet Size, and the Topic Alias Maximum. Build them with
  [BrokerCapabilities](src/main/java/org/mqtt4j/hato/testkit/BrokerCapabilities.java),
  for example `BrokerCapabilities.defaults().withMaximumQos(Qos.AT_LEAST_ONCE)`, and
  announce `BrokerCapabilities.defaults()` again to go back. Only clients that connect
  afterward get the new limits:
  open connections keep theirs, and the broker's sessions and retained messages stay
  as they are, so announce before the code under test connects. A Hato client refuses
  what the limits forbid with `MqttPublicationRefusedException` or
  `MqttSubscriptionRefusedException` and stays connected. MQTT 3.1.1 connections learn
  of no limits.

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
faults. `BrokerControls`, `BrokerCapabilities`, `BrokerReason`, `ResponseKind`, and
`HeldResponse` are experimental, like the broker that they control.
