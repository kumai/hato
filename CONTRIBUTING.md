# Contributing to MQTJ

MQTJ is a compact MQTT 3.1.1 and 5.0 client library for Java 25 and later, with
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

- the MQTJ version, the Java version and vendor, and whether the application
  runs on a JVM or as a native image;
- the broker product and version on the other side of the connection, and the
  MQTT protocol version in use (3.1.1 or 5.0);
- what you did, what you expected, and what happened instead;
- the relevant log excerpt. MQTJ logs through `System.Logger`; the `DEBUG`
  level shows the packets exchanged, which is usually what is needed;
- when you can, a failing test written against the embedded broker from
  `mqtj-testkit`.

A reproducible report is at least as valuable as a fix. If you are not sure
whether something is a bug, ask in an issue anyway.

Do not report security vulnerabilities in public issues. Use GitHub's private
vulnerability reporting for this repository instead.

## Proposing a change

- Open an issue before changing the public API (the packages exported by the
  module descriptors of `mqtj-client`, `mqtj-broker`, and `mqtj-testkit`), the
  protocol state machine, a module boundary, or anything a broker can observe
  on the wire. Agreeing on the direction up front is far less work for everyone
  than reworking it once the code is already written.
- Pull requests from forks are welcome.
- Keep a pull request to one concern. Fix the problem you describe; leave
  unrelated fixes and improvements for a separate pull request.

## Repository layout

| Project | Contents | Published |
| --- | --- | --- |
| `mqtj-common` | Shared values and implementation for client, broker, and testkit: protocol, logging, concurrency, I/O | no; embedded at build time |
| `mqtj-client` | Client API/SPI, session, transport, and delivery | `org.mqtt4j:mqtj-client`, one standalone jar |
| `mqtj-broker` | In-memory, single-node broker | `org.mqtt4j:mqtj-broker`, one standalone jar with no client dependency |
| `mqtj-testkit` | Broker test wrapper, fake client, fault injection, manual scheduler | `org.mqtt4j:mqtj-testkit`; depends on client and broker |
| `distribution-tests` | Final jar, JPMS, sources, and publication metadata checks | no |
| `examples` | Usage examples | no |
| `native-smoke` | GraalVM native-image smoke test | no |
| `build-logic` | Convention plugins and the source-embedding tasks | no |

The client's public API package is `org.mqtt4j.mqtj`; the JPMS modules are
`org.mqtt4j.mqtj.client`, `org.mqtt4j.mqtj.broker`, and `org.mqtt4j.mqtj.testkit`.
Client, broker, and testkit each embed the shared sources of `mqtj-common` in
their own internal packages, so client and broker ship as standalone jars that
work separately or together, on the classpath or the module path. There is no
additional jar and no special classloader; `mqtj-testkit` deliberately uses
ordinary dependencies on both.

`./gradlew build` produces the complete runtime jars at
`mqtj-client/build/libs/mqtj-client-<version>.jar` and
`mqtj-broker/build/libs/mqtj-broker-<version>.jar`; the sources and Javadoc
jars beside them are for development tools. The build also runs
`distribution-tests`: fresh Java processes consume only the final jars,
individually and together, and verify JPMS encapsulation and the publication
metadata. Run `./gradlew :distribution-tests:test` for just these checks.

The shared sources are maintained only in `mqtj-common`. The
`embedCommonSources` task copies them to each consumer's `build/generated/`
under `org.mqtt4j.mqtj.<distribution>.internal.common` before compilation; in
the client, `common.value` maps onto the public `org.mqtt4j.mqtj` package. The
rules for shared sources are in the code conventions below.

## Before you open a pull request

Gradle needs some JDK to start; the Java 25 JDK it runs on and compiles with is
provisioned automatically on the first run, as is the Gradle distribution
itself. Both need network access once.

Docker is needed only for the integration tests that run against real brokers;
they are skipped when Docker is not available. A GraalVM JDK is needed only for
the native-image smoke test, which CI runs on every pull request.

CI runs the checks below on every pull request, and all of them must pass
before merge. Run the ones that match what you changed.

| You changed | Run |
| --- | --- |
| Anything | `./gradlew build` |
| One module | `./gradlew :mqtj-client:check` (substitute the module name) |
| `build-logic/`, `settings.gradle.kts`, or `gradle/libs.versions.toml` | `./gradlew build :build-logic:check` |
| Anything that could affect native-image compatibility | `./gradlew :native-smoke:nativeCompile` (needs a GraalVM JDK 25) |

`./gradlew build` compiles with every javac lint on, `-Werror`, and NullAway,
checks the sources with Checkstyle, and runs the unit tests, the integration
tests, and the check that the published modules have no external runtime
dependencies. Client and broker also reject dependencies on other MQTJ jars.
`distribution-tests` compiles and runs probes using the final jars alone and
together, checks JPMS encapsulation, and verifies sources and POM/Gradle
metadata. The separate
`:build-logic:check` task is needed because `build-logic` is an included build;
its checks do not run as part of `./gradlew build`.

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
- `mqtj-common` is an internal development project and is not published.
  Shared sources live under `org.mqtt4j.mqtj.common` in the layers `value`, `util`, `log`,
  `protocol`, `concurrent`, and `io`; the build embeds them into
  `org.mqtt4j.mqtj.<distribution>.internal.common` in client, broker, and testkit.
  The client maps `common.value` to its public `org.mqtt4j.mqtj` package instead:
  `MqttVersion` and `Qos` have one source definition and no internal wrapper enum.
  Edit only the originals, never generated copies. Each layer imports only
  itself and the layers below it, which the `mqtj-common` tests check. Resources, module
  descriptors, or other languages in the source archive require explicit build
  support; unsupported files fail the build.
- Keep internal shared types out of exported signatures. Reusable immutable
  values belong in `common.value` and may be mapped into a distribution's public
  API at build time. Protocol code imports these values from common; it never
  depends on the client project. Keep APIs/SPI that own behavior and lifetimes in
  their distributions. Shared code must not depend on actors or broker routing
  state, and `protocol` must not depend on sockets or threads.
- Publication group is `org.mqtt4j`; Java packages and JPMS names are rooted at
  `org.mqtt4j.mqtj`. Include the artifact name only in module-specific packages
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
  `common.log` with SLF4J-style `{}` placeholders. Logger names that users
  configure belong to the distribution (`org.mqtt4j.mqtj.client.<component>`,
  `org.mqtt4j.mqtj.broker.<component>`); shared code receives its `Log` from the
  owner, and class-based names are for internal diagnostics. Never log at
  `INFO` on a hot path, and never log payloads below `TRACE`.
- Java 25 is the baseline. Records, sealed types, pattern matching, and virtual
  threads are expected. Preview features are not used.
- Unit tests must not need a network, a broker, or Docker. Use `mqtj-testkit`
  for that: the embedded broker, the fake client, the fault-injecting
  transport, and the manual scheduler. Tests that need a real broker belong in
  the `integrationTest` suite.

## Commits and pull requests

- Read `git log` and match what you see: the shape of the title and the level of
  detail in the body. A message explains what the change makes possible and why
  it is needed, not which files were edited.
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
