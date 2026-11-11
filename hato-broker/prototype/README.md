# Broker thread-isolation prototype

This branch puts the final thread-isolation experiment directly in the broker's sources for
review. Its base is `391973c16028a0ec799bff640841f09f87d35d9e`. It is a design-comparison
prototype, with product work still outstanding. The public broker API is unchanged.

## Configuration and source map

The default is four platform-thread selectors and two platform-thread actor workers.
Socket I/O posts work to the session actors rather than running them inline. Each worker turn
processes at most 256 events before reserving another turn at the back of the executor queue.

| Source | Change |
|---|---|
| [BrokerServer](../src/main/java/org/mqtt4j/hato/broker/internal/BrokerServer.java) | Platform-thread accept loop; closes the actor executor and selectors |
| [Routing](../src/main/java/org/mqtt4j/hato/broker/internal/Routing.java) | Owns the selectors and actor executor; assigns connections round-robin |
| [Reactor](../src/main/java/org/mqtt4j/hato/broker/internal/Reactor.java) | Owns key and channel cleanup, coalesces wakeups, and closes the broker on fatal failure |
| [Connection](../src/main/java/org/mqtt4j/hato/broker/internal/Connection.java) | Incremental framing, nonblocking reads and partial writes, pause/resume for bounded queues |
| [Client](../src/main/java/org/mqtt4j/hato/broker/internal/Client.java) | Reports CONNECT acceptance through an internal callback instead of waiting on a future |
| [Session](../src/main/java/org/mqtt4j/hato/broker/internal/Session.java) | Uses the actor executor for queued turns; wakes a reader when inbox space returns |

The queue quotas, codec, delivery state, retained messages, and subscription matching are reused.
EOF and socket errors follow the packets already posted to the session. A queued DISCONNECT
can apply its session expiry and Will before the actor ends the connection. A peer that
half-closes can still receive a final MQTT packet. The selector releases the send batch
and the socket after the connection ends.

The original experiment applied `selector.patch`, `selector-bulk.patch`, `selector-ready.patch`,
and `selector-fair.patch` and ran with `s0.inPlace=0`. The current experiment defaults to
`hato.broker.inPlace=0`. The internal `hato.broker.selectors`, `hato.broker.actors`,
and `hato.broker.inPlace` properties are experiment controls.
The normal Gradle build also regenerates the broker class index and processes nullness metadata.

## Verification

Run from the repository root with the existing JDK 25 toolchain and Gradle cache:

```sh
./gradlew :hato-broker:jar :hato-broker:checkstyleMain :hato-broker:checkstyleIntegrationTest --offline
./gradlew :hato-broker:integrationTest :hato-broker:integrationTestSingleCarrier \
  --offline -Phato.interop=none --continue
```

The suite now tests selector survival through repeated disconnects, broker shutdown on an
injected selector failure, and the final response after a peer half-closes. The full suite
also exercises race tests whose blocking hooks need per-connection pause and resume.

The review measurements and their commands are in
[`notes/bench/broker-threads.md`](../../notes/bench/broker-threads.md) and
[`notes/bench/broker/s0/review/`](../../notes/bench/broker/s0/review/).

The 2026-11-10 checkout passed compilation with NullAway, Checkstyle, and all 145 selected
cases in the normal and single-carrier configurations. The probe was run once per carrier
count against that day's jar:

| Carriers | Maximum PINGREQ | Maximum CONNECT | Maximum delivery | Keep-alive peer |
|---|---|---|---|---|
| 1 | 2.2 ms | 3.4 ms | 2.3 ms | Stayed connected |
| 2 | 3.1 ms | 5.2 ms | 4.2 ms | Stayed connected |

These outputs are in [the earlier branch measurements](../../notes/bench/broker/s0/review/previous/).

## Earlier measurements

The [original measurements](../../notes/bench/broker/s0/results/) date from 2026-11-10,
before this branch was created. The broker used
four Linux CPUs, 4 GiB container memory, and a 2 GiB heap; the raw load driver used six separate
CPUs and a separate JVM. The image was `eclipse-temurin:25-jre`, Temurin 25.0.4.1+1, on an
Apple M1 Max Docker Desktop VM (Linux 7.0.14-linuxkit, aarch64). Three rounds rotated broker order.

In the table, each cell is median deliveries/second, p99 milliseconds, and broker CPU cores.

| Fan-out | Base | Final candidate |
|---|---|---|
| 10 subscribers, 100,000 publications/s | 999,865 / 0.885 / 3.39 | 999,772 / 0.950 / 2.81 |
| 100 subscribers, 10,000 publications/s | 999,826 / 46.137 / 3.72 | 999,799 / 1.901 / 3.06 |
| 1,000 subscribers, 500 publications/s | 499,986 / 4.194 / 3.27 | 499,914 / 4.719 / 2.57 |

The ten-subscriber candidate missed 935, 335, and zero of five million expected deliveries in
the three rounds. The other two conditions received every delivery. Base p99 also varied
substantially between rounds and experiments; the table does not establish a general latency
speedup. The load driver's percentage rounds to two decimal places, so `lostPercent=0.00`
does not always mean zero missing deliveries.

With four full-speed publishers, one comparison measured 1.20 million deliveries/s for the
candidate versus 1.87 million for base. With 10,000 idle connections, the candidate and base
each peaked at 14 Java platform threads, and cgroup memory was 275.0 versus 324.5 MiB before GC.
Those are individual observations; 100,000 connections were not tested with this candidate.

## Product work left for the next stage

- Replace the queued-turn Thread sentinel with an explicit ownership state.
- Share framing with the blocking reader and replace blocking race-test hooks with connection
  pause/resume operations. A blocking hook currently holds its entire selector or actor worker.
- Meet the throughput targets and verify the remaining stop races. Subscription matching still
  scans sessions.
- Decide how to isolate the application's logging backend from the broker's execution threads.
- Run the full broker/client/testkit, distribution, and native-image checks. TLS and application
  authentication are not implemented by this prototype.
