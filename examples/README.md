# Hato examples

The root README's [publishing example](src/main/java/org/mqtt4j/hato/examples/PublishReading.java)
and [subscribing example](src/main/java/org/mqtt4j/hato/examples/SubscribeReadings.java)
are compiled with the public client API by `./gradlew build`. Both expect a broker
at `mqtt://localhost:1883`. Run `SubscribeReadings` from your IDE, then
`PublishReading`; press Enter in the subscriber to stop it.

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
