# MQTJ examples

The root README's [publishing example](src/main/java/org/mqtt4j/mqtj/examples/PublishReading.java)
and [subscribing example](src/main/java/org/mqtt4j/mqtj/examples/SubscribeReadings.java)
are compiled with the public client API by `./gradlew build`. Both expect a broker
at `mqtt://localhost:1883`. Run `SubscribeReadings` from your IDE, then
`PublishReading`; press Enter in the subscriber to stop it.

For a complete example that starts its own embedded broker, run:

```shell
./gradlew :examples:run
```

[Hello](src/main/java/org/mqtt4j/mqtj/examples/Hello.java) publishes messages and
receives them through both a handler and the application's own thread.
