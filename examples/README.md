# Hato examples

The root README's [first program](src/main/java/org/mqtt4j/hato/examples/ReadingRoundTrip.java),
and the client guide's [publishing example](src/main/java/org/mqtt4j/hato/examples/PublishReading.java)
and [subscribing example](src/main/java/org/mqtt4j/hato/examples/SubscribeReadings.java), are
compiled with the public client API by `./gradlew build`. They expect a broker at
`mqtt://localhost:1883`. Run `ReadingRoundTrip` from your IDE on its own, or run
`SubscribeReadings`, then `PublishReading`. Press Enter in the program that waits to stop it.

The examples below start their own embedded broker, and `./gradlew build` runs each of
them. To run one yourself, use its task, such as `./gradlew :examples:runHello`, or run it
from your IDE.

- [Hello](src/main/java/org/mqtt4j/hato/examples/Hello.java) publishes messages and receives
  them through both a handler and the application's own thread.
- [HandOffToMailbox](src/main/java/org/mqtt4j/hato/examples/HandOffToMailbox.java) hands
  messages to a mailbox that the application owns, acknowledges each one after processing it,
  and shuts down in the order a mailbox needs.
- [MailboxLanes](src/main/java/org/mqtt4j/hato/examples/MailboxLanes.java) runs each device's
  messages on that device's own single-thread executor, and stops the executors only after the
  client has terminated.
- [MicrometerMonitoring](src/main/java/org/mqtt4j/hato/examples/MicrometerMonitoring.java)
  records the client's timings and stats in Micrometer, and times the handler's own work, with
  a local registry and no monitoring server. Micrometer is a dependency of the examples, not
  of the client.
