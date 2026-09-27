# MQTJ test kit

Test how your application handles interrupted connections and timeouts. The test
kit includes the [client](../mqtj-client/README.md) and
[embedded broker](../mqtj-broker/README.md) as dependencies.

## Getting it

Gradle:

```kotlin
dependencies {
    testImplementation("org.mqtt4j:mqtj-testkit:x.y.z")
}
```

Maven:

```xml
<dependency>
    <groupId>org.mqtt4j</groupId>
    <artifactId>mqtj-testkit</artifactId>
    <version>x.y.z</version>
    <scope>test</scope>
</dependency>
```

For JPMS, the module name is `org.mqtt4j.mqtj.testkit`.

## Testing with a broker

Start an embedded broker for each test and close it afterward with
try-with-resources. See the [broker example](../mqtj-broker/README.md#starting-and-stopping).
If you only need a broker, you can depend on `mqtj-broker` directly.

## Simulating failures and timeouts

- [FaultyTransport](src/main/java/org/mqtt4j/mqtj/testkit/FaultyTransport.java)
  wraps a transport such as `Transport.tcp(clientId)`. Supply a fresh wrapper through
  the client's `transportFactory` for each connection. Call `disconnect(cause)`
  to simulate a connection failure or `receiveDelay(Duration)` to delay incoming
  data.
- [ManualTimerScheduler](src/main/java/org/mqtt4j/mqtj/testkit/ManualTimerScheduler.java)
  lets a test advance time with `advance(Duration)` instead of sleeping. Supply
  it to the client's `timerScheduler` or to `FaultyTransport` to control their
  timers. Advancing time runs due timers; wait separately for the client's
  response before checking the result.
