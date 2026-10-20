# Hato examples

The root README's [publishing example](src/main/java/org/mqtt4j/hato/examples/PublishReading.java)
and [subscribing example](src/main/java/org/mqtt4j/hato/examples/SubscribeReadings.java)
are compiled with the public client API by `./gradlew build`. Both expect a broker
at `mqtt://localhost:1883`. Run `SubscribeReadings` from your IDE, then
`PublishReading`; press Enter in the subscriber to stop it.

For a complete example that starts its own embedded broker, run:

```shell
./gradlew :examples:run
```

[Hello](src/main/java/org/mqtt4j/hato/examples/Hello.java) publishes messages and
receives them through both a handler and the application's own thread.

[HandOffToMailbox](src/main/java/org/mqtt4j/hato/examples/HandOffToMailbox.java) also
starts its own broker. It hands messages to a mailbox that the application owns,
acknowledges each one after processing it, and shuts down in the order a mailbox needs.
Run it from your IDE.
