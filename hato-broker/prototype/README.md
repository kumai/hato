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
| [BrokerServer](../src/main/java/org/mqtt4j/hato/broker/internal/BrokerServer.java) | Platform-thread accept loop that waits in a selector of its own; closes the actor executor and selectors |
| [Routing](../src/main/java/org/mqtt4j/hato/broker/internal/Routing.java) | Owns the selectors and actor executor; assigns connections round-robin |
| [Reactor](../src/main/java/org/mqtt4j/hato/broker/internal/Reactor.java) | Owns key and channel cleanup, coalesces wakeups, ends a connection whose work fails, and closes the broker when the selector itself fails or its thread is interrupted |
| [Connection](../src/main/java/org/mqtt4j/hato/broker/internal/Connection.java) | Incremental framing, nonblocking reads and partial writes, pause/resume for bounded queues |
| [Client](../src/main/java/org/mqtt4j/hato/broker/internal/Client.java) | Reports CONNECT acceptance through an internal callback instead of waiting on a future |
| [Session](../src/main/java/org/mqtt4j/hato/broker/internal/Session.java) | Uses the actor executor for queued turns; wakes a reader when inbox space returns |
| [Worker](../src/main/java/org/mqtt4j/hato/broker/internal/Worker.java) | Actor pool thread; keeps the connections that a turn queued output for until the turn ends |

The queue quotas, codec, delivery state, retained messages, and subscription matching are reused.
EOF and socket errors follow the packets already posted to the session. A queued DISCONNECT
can apply its session expiry and Will before the actor ends the connection. When a write fails,
the selector first hands over the input that has already arrived, so a DISCONNECT sent just
before a reset still applies. A peer that half-closes can still receive a final MQTT packet.
The selector releases the send batch and the socket after the connection ends.

The accepting thread waits for connections in a selector and never inside `accept`. On macOS,
closing a listener at the moment a thread enters a blocking `accept` can leave `close()` waiting
in the kernel until another file descriptor operation in the process ends, which in a quiet
process is never. `close()` closes that selector too, which frees the listening port before it
returns. It then waits for the selectors and the accepting thread, which close the sockets of
the connections as they end, so every connection is closed when `close()` returns. A thread of
the broker that closes it after a failure does not wait for the others.

An interrupt ends the accepting thread or a selector thread, and the broker closes with it: an
interrupted thread no longer waits in a selector, and no other thread serves its sockets. A
thread of the actor pool takes no notice of an interrupt.

The selectors and the pool hand work to each other in batches. A session's inbox takes 128
received packets, so that a selector seldom stops within the packets of one read. A QoS 0
publication finds it full at 16: nothing else holds such publications back, and a publisher
that gets far ahead of the actor overruns the queues of its subscribers. A worker keeps the
connections that a turn queued output for, and has their selectors look at them when the turn
ends, when it holds 64 of them, or at once when the turn fills a connection's send queue; a
selector then writes a turn's output with one call.

A failure in the work of one connection, an `Error` included, ends that connection alone, as it
does on a session's actor. Only a failure of the selector loop itself closes the broker. The
application has no way yet to learn that the broker closed itself, other than the log.

No thread of the broker enters the application's logging backend. The shared `Log` facade queues
each statement without a lock, and one platform thread gives the statements to the backend.
Where the application logs from virtual threads, a selector or a worker that called the backend
itself waited for its lock for seconds, behind a virtual thread that found no carrier. The
measurement is in [`notes/bench/broker-threads.md`](../../notes/bench/broker-threads.md),
chapter 9.

The original experiment applied `selector.patch`, `selector-bulk.patch`, `selector-ready.patch`,
and `selector-fair.patch` and ran with `s0.inPlace=0`, which left every actor event to the pool.
That is the only form now, and the setting is gone. The internal `hato.broker.selectors` and
`hato.broker.actors` properties are experiment controls.
The normal Gradle build also regenerates the broker class index and processes nullness metadata.

## Verification

Run from the repository root with the existing JDK 25 toolchain and Gradle cache:

```sh
./gradlew :hato-broker:jar :hato-broker:checkstyleMain :hato-broker:checkstyleIntegrationTest --offline
./gradlew :hato-broker:integrationTest :hato-broker:integrationTestSingleCarrier \
  --offline -Phato.interop=none --continue
```

The suite tests selector survival through repeated disconnects, that a connection ended by
another thread stays open until its selector closes it, that an error in one connection leaves
the others served, broker shutdown on an injected selector failure and on an interrupt of a
selector thread, the final response after a peer half-closes, a DISCONNECT that arrived before
a write failed, a burst that overfills a session's inbox, a publication to more subscribers
than a worker keeps for the end of its turn, a session with a long backlog, which lets a
waiting session take its turn, and that `close()` waits for a selector that still has a
connection open.

The race tests stop the broker at a chosen point in two ways. A hook that waits holds its whole
selector or actor worker, which is what a thread that loses the processor there does; such a
test counts on its other connections having selectors of their own, which take connections in
turn, and waits for nothing that the held thread does. A test that needs a connection to go on
reading while its output waits, as with a peer that takes no more bytes, keeps the write of a
delivery back on that connection alone, and no thread waits.

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

## Hand-off measurements

On 2026-11-11 the two changes to the hand-off were compared with the prototype before them and
with base `391973c`, in the environment of the earlier measurements. Each cell is the median of
three rounds, and of five rounds for the fan-out loads.

| Load | Base | Before | Larger inbox | And a turn's output together |
|---|---|---|---|---|
| QoS 1, 8 pairs, window 100 | 1,393,471 PUBACK/s | 811,153 | 1,210,555 | 1,382,437 |
| QoS 1, 1 pair, window 100 | 431,467 PUBACK/s | 256,911 | 560,347 | 532,392 |
| 4 full-speed publishers | 2,043,050 deliveries/s, 5.09 % lost | 1,249,354, 0.46 % | 1,193,738, 0.68 % | 1,304,641, 0.27 % |
| 8 full-speed publishers | 2,373,201 deliveries/s, 4.78 % lost | 1,439,037, 2.04 % | 1,459,590, 1.02 % | 1,576,407, 0.96 % |
| 10 subscribers, 100,000 publications/s | p99 0.885 ms | 2.621 ms | 1.311 ms | 0.983 ms |
| 100 subscribers, 10,000 publications/s | p99 1.704 ms | 1.704 ms | 1.704 ms | 1.769 ms |
| 1,000 subscribers, 500 publications/s | p99 4.063 ms | 4.719 ms | 4.719 ms | 4.456 ms |

Before, a reader stopped after every 16 packets of a QoS 1 publisher, and each stop cost a wake
of the selector and a turn of the actor. An inbox of 128 for every packet lets full-speed QoS 0
publishers overrun their subscribers, which then lost a tenth of the deliveries or more, so
QoS 0 publications keep the limit of 16. The variants and their measurements are in
[`notes/bench/broker-threads.md`](../../notes/bench/broker-threads.md), chapter 7.

## Product work left for the next stage

- Share framing with the blocking reader.
- Verify the remaining stop races. Subscription matching still scans sessions.
- Keep the 99th percentile of a fan-out to 1,000 subscribers under its limit of 5 ms at busier
  times too. It stays close to the limit, as before these changes.
- Give the application a way to learn that the broker closed itself after a selector failed.
- After a failed write, read on until the actor has taken a packet that waits for it. Today the
  input behind such a packet ends with the connection.
- Count the received packets that wait in all inboxes against a budget for the whole broker.
  Each session's inbox takes 128 of them, eight times what it took.
- Hand over the output that a worker holds when one of its events blocks. Today it waits for the
  end of the turn, with the output of the events before.
- Choose the numbers of selectors and workers from the processors at hand. Two workers are the
  limit for eight QoS 1 pairs now.
- Remove the `hato.broker.selectors` and `hato.broker.actors` experiment properties.
- Run the full broker/client/testkit, distribution, and native-image checks. TLS and application
  authentication are not implemented by this prototype.
