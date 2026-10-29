# Contributing to Hato

Hato is a compact MQTT 3.1.1 and 5.0 client library for Java 25 and later, with
an in-memory broker and a test kit alongside it. Client and broker each ship as one
plain jar with no runtime dependencies; the test kit depends on those two jars. The
libraries use no runtime reflection or bytecode generation and are meant to work
unchanged under GraalVM native-image.

This guide covers how to report a problem, how to propose a change, and the
requirements a change must meet before it can be merged. For an overview of the
project, start with the [README](README.md).

Read the [Java Code Conventions](docs/conventions/java.md) and the relevant
source files and tests before making changes. Follow the written conventions,
and use the surrounding code as a guide where they leave room for choice.

## Reporting a problem

Open a GitHub issue. A report is most useful when it includes:

- the Hato version, the Java version and vendor, and whether the application
  runs on a JVM or as a native image;
- the broker product and version on the other side of the connection, and the
  MQTT protocol version in use (3.1.1 or 5.0);
- what you did, what you expected, and what happened instead;
- the relevant log excerpt. Hato logs through `System.Logger`; the `DEBUG`
  level shows the packets exchanged, which is usually what is needed;
- when you can, a failing test written against the embedded broker from
  `hato-testkit`.

A reproducible report is at least as valuable as a fix. If you are not sure
whether something is a bug, ask in an issue anyway.

Do not report security vulnerabilities in public issues. Use GitHub's private
vulnerability reporting for this repository instead.

## Proposing a change

- Open an issue before changing the public API (the packages exported by the
  module descriptors of `hato-client`, `hato-broker`, and `hato-testkit`), the
  protocol state machine, a module boundary, or anything a broker can observe
  on the wire. Agreeing on the direction up front is far less work for everyone
  than reworking it once the code is already written.
- Keep a pull request to one concern. Fix the problem you describe; leave
  unrelated fixes and improvements for a separate pull request.

## Repository layout

| Project | Contents | Published |
| --- | --- | --- |
| `hato-common` | Shared values and implementation for client, broker, and testkit: protocol, logging, concurrency, I/O | no; embedded at build time |
| `hato-client` | Client API/SPI, session, transport, and delivery | `org.mqtt4j:hato-client`, one standalone jar |
| `hato-broker` | In-memory, single-node broker | `org.mqtt4j:hato-broker`, one standalone jar with no client dependency |
| `hato-testkit` | Broker test wrapper, fake client, fault injection, manual scheduler | `org.mqtt4j:hato-testkit`; depends on client and broker |
| `distribution-tests` | Final jar, JPMS, sources, and publication metadata checks | no |
| `examples` | Usage examples | no |
| `native-smoke` | GraalVM native-image smoke test | no |
| `release-tools` | Release version and bundle checks, and the upload to Maven Central, run by the release workflow | no |
| `build-logic` | Convention plugins and the source-embedding tasks | no |

The client's public API package is `org.mqtt4j.hato`; the JPMS modules are
`org.mqtt4j.hato.client`, `org.mqtt4j.hato.broker`, and `org.mqtt4j.hato.testkit`.
Client, broker, and testkit each embed the shared sources of `hato-common` in
their own internal packages, so client and broker ship as standalone jars that
work separately or together, on the classpath or the module path. There is no
additional jar and no special classloader; `hato-testkit` deliberately uses
ordinary dependencies on both.

`./gradlew build` produces the complete runtime jars at
`hato-client/build/libs/hato-client-<version>.jar` and
`hato-broker/build/libs/hato-broker-<version>.jar`; the sources and Javadoc
jars beside them are for development tools. The build also runs
`distribution-tests`: fresh Java processes consume only the final jars,
individually and together, and verify JPMS encapsulation and the publication
metadata. Run `./gradlew :distribution-tests:test` for just these checks.

The shared sources are maintained only in `hato-common`. The
`embedCommonSources` task copies them to each consumer's `build/generated/`
under `org.mqtt4j.hato.<distribution>.internal.common` before compilation; in
the client, `common.value` maps onto the public `org.mqtt4j.hato` package. The
rules for shared sources are in the code conventions below.

## Before you open a pull request

Gradle needs some JDK to start; the Java 25 JDK it runs on and compiles with is
provisioned automatically on the first run, as is the Gradle distribution
itself. Both need network access once.

Docker is needed for the integration tests that run against real brokers. Without
it, `./gradlew build` fails and explains how to skip those tests: add
`-Phato.docker.required=false` to the command, or put `hato.docker.required=false`
in `~/.gradle/gradle.properties` to skip them in every build. Skipped tests are
listed in the build output, and CI always runs them. A GraalVM JDK is needed only
for the native-image smoke test, which CI runs on every pull request.

`./gradlew build` also runs the tests against EMQX, HiveMQ CE, NanoMQ, and
RabbitMQ, the classes named after each broker. CI runs them in a job of their own,
one broker at a time. To do the same, add `-Phato.interop=emqx` (or `hivemq`,
`nanomq`, `rabbitmq`) to the integration test tasks; `-Phato.interop=none` leaves
them out, as the main CI job does.

CI also runs the tests on the newest JDK release, with the classes still compiled
for Java 25. To do the same, add `-Phato.testJava=27` to the test tasks; Gradle
provisions that JDK as it does Java 25.

CI runs the checks below on every pull request, and all of them must pass
before merge. Run the ones that match what you changed.

| You changed | Run |
| --- | --- |
| Anything | `./gradlew build` |
| One module | `./gradlew :hato-client:check` (substitute the module name) |
| `build-logic/`, `settings.gradle.kts`, or `gradle/libs.versions.toml` | `./gradlew build :build-logic:check` |
| Anything that could affect native-image compatibility | `./gradlew :native-smoke:nativeRun` (needs a GraalVM JDK 25) |

`./gradlew build` compiles with every javac lint on, `-Werror`, and NullAway,
checks the sources with Checkstyle, and runs the unit tests, the integration
tests, and the check that the published modules have no external runtime
dependencies. Client and broker also reject dependencies on other Hato jars.
`distribution-tests` compiles and runs probes using the final jars alone and
together, checks JPMS encapsulation, and verifies sources and POM/Gradle
metadata. The separate
`:build-logic:check` task is needed because `build-logic` is an included build;
its checks do not run as part of `./gradlew build`.

Maintainers can find bundle inspection, trial uploads, and publication steps in
the [release guide](docs/releasing.md).

## Code conventions

The [Java Code Conventions](docs/conventions/java.md) explain both the rules and
their underlying principles. Apply the same principles to cases not explicitly
covered. Checkstyle enforces the layout and import rules
(`config/checkstyle/checkstyle.xml`); a finding fails the build. The
`.editorconfig` at the root carries the same layout settings for editors.

A few points sit outside code style and are checked in review:

- Client and broker must each work with only their own jar and the JDK. Testkit
  may depend on client and broker. External runtime dependencies are forbidden;
  test-only and build-time dependencies are fine.
- Nullness is documented with JSpecify: packages are `@NullMarked` and only what
  may be null carries `@Nullable`; the [Java Code Conventions](docs/conventions/java.md)
  say how to write it. JSpecify is a build-time dependency only. It is
  `compileOnly` in Gradle and `requires static org.jspecify;` in the module
  descriptors, never `transitive`, so the published jars declare no dependency
  and consumers compile against them without JSpecify on their classpath or
  module path. `distribution-tests` prove that by compiling consumer probes with
  `-Xlint:all -Werror` and no JSpecify jar.
- NullAway checks every `@NullMarked` package inside javac, with JSpecify
  semantics including generics, and a finding fails the compilation. Error Prone
  is only the vehicle: its own bug patterns are switched off. When a finding is
  wrong, add `@SuppressWarnings("NullAway")` to the smallest enclosing
  declaration with a comment that says why; do not weaken the configuration.
- `hato-common` is an internal development project and is not published.
  Shared sources live under `org.mqtt4j.hato.common` in the layers `value`, `util`, `log`,
  `protocol`, `concurrent`, and `io`; the build embeds them into
  `org.mqtt4j.hato.<distribution>.internal.common` in client, broker, and testkit.
  The client maps `common.value` to its public `org.mqtt4j.hato` package instead:
  `MqttVersion` and `Qos` have one source definition and no internal wrapper enum.
  Edit only the originals, never generated copies. Each layer imports only
  itself and the layers below it, which the `hato-common` tests check. Resources, module
  descriptors, or other languages in the source archive require explicit build
  support; unsupported files fail the build.
- Keep internal shared types out of exported signatures. Reusable immutable
  values belong in `common.value` and may be mapped into a distribution's public
  API at build time. Protocol code imports these values from common; it never
  depends on the client project. Keep APIs/SPI that own behavior and lifetimes in
  their distributions. Shared code must not depend on actors or broker routing
  state, and `protocol` must not depend on sockets or threads.
- Publication group is `org.mqtt4j`; Java packages and JPMS names are rooted at
  `org.mqtt4j.hato`. Include the artifact name only in module-specific packages
  and module names, not in the Maven group.
- No reflection, `ServiceLoader`, dynamic proxies, `Unsafe`, Java
  serialization, or runtime class generation. Everything is wired explicitly
  through builders. This is what keeps native-image builds working without
  reachability metadata. Build-time source generation, package relocation, and
  bytecode processing are allowed; they must preserve the distribution contracts.
- The public API is exactly what the module descriptors export. Everything else
  is internal and may change at any time. Adding a public type or method is a
  design decision; open an issue first.
- Logging goes through `System.Logger` only, via the internal `Log` facade in
  `common.log` with SLF4J-style `{}` placeholders. Each class owns a static
  `Log.of(Class)` logger. Package names identify the distribution; individual
  class names are implementation details. Client worker names include the
  client id; connection lifecycle messages include the id and endpoint. Never log at
  `INFO` on a hot path, and never log payloads below `TRACE`.
- Java 25 is the baseline. Records, sealed types, pattern matching, and virtual
  threads are expected. Preview features are not used.
- Unit tests must not need a network, a broker, or Docker. Use `hato-testkit`
  for that: the embedded broker, the fake client, the fault-injecting
  transport, and the manual scheduler. Tests that need a real broker belong in
  the `integrationTest` suite.

## Commits and pull requests

A commit title starts with the area of the change, a colon, and a summary that
begins with a lowercase verb and has no final period. The verb says what kind of
change it is, such as add, fix, or remove:

```text
client: fix a lost PUBACK after a reconnect that resumes the session
```

Use one of the areas below. When a change spans several, choose the one a
reader should look at first; a change to both client and broker is usually
better split.

| Area | Covers |
| --- | --- |
| `client` | `hato-client`: the public API, sessions, reconnection, and delivery |
| `broker` | `hato-broker` |
| `testkit` | `hato-testkit` |
| `protocol` | `protocol` and `value` in `hato-common`: packets, the codec, topics, and reason codes |
| `io` | `io` in `hato-common`: TCP, TLS, WebSocket, and framing |
| `common` | the other layers of `hato-common`, and changes that span several of its layers |
| `build` | the Gradle build, `build-logic`, dependency versions, `distribution-tests`, and `native-smoke` |
| `ci` | GitHub Actions, the `release-tools` they run, and Dependabot |
| `docs` | the README, this guide, the code conventions, and `examples` |

- The body explains what the change makes possible and why it is needed: the
  new capability or the corrected behavior, and the decisions behind it. The
  code shows what was written, so do not list the files or classes that
  changed.
- Pull request titles take the same form, because a squashed pull request uses
  its title as the commit title.
- A pull request should contain one coherent change that can be reverted as a
  whole. Pull requests are merged either as one squashed commit or with a merge
  commit. Rebase merges are disabled so that the boundary of a change stays
  visible in `main`. When each commit is a meaningful step in its own right
  that builds and passes the checks, a merge commit keeps those steps;
  otherwise, the pull request is squashed into one commit. Shape your commits
  accordingly before asking for review.
- Personal editor settings and agent-specific configuration files (`AGENTS.md`,
  `CLAUDE.md`, `.claude/`, and similar) are deliberately ignored by Git. Keep
  them local; do not add them to a pull request.
- By contributing you agree that your contribution is licensed under the
  [MIT License](LICENSE), like the rest of the project.
