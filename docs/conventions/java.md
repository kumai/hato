

# Java Code Conventions

This document sets the shared standard for writing, reading, and reviewing MQTJ's Java code.
Its aim is to make types, the absence of values, and the boundaries of processing easy to read,
and to remove notation that repeats without adding meaning.

## 1. Scope and how to read this document

### 1.1 Scope

These conventions apply to the hand-written Java code in this repository: the client, the
broker, the test kit, the shared sources in `mqtj-common`, and the tests. Tests follow the same
rules; test-specific treatment is noted in the relevant sections. The copies of the shared
sources that the build generates are out of scope; edit the originals.
Code examples are shortened for explanation and omit imports and surrounding declarations.

### 1.2 Rule levels

| Level | Meaning |
| --- | --- |
| **MUST** | Follow within the stated scope. Any permitted exception is defined in the rule itself |
| **SHOULD** | The usual choice. Another form is acceptable when you understand the concrete reason and its consequences |
| **GUIDANCE** | Decide by context. No single form is required |

When the reason for an exception cannot be read from the code, leave it in a nearby comment or in
the change description. Not every departure from a SHOULD needs a boilerplate comment.

This document defines only the level of each rule. Whether a rule is checked mechanically, or can
be fixed automatically, is not defined here.

### 1.3 Guiding principle

The rules in this document rest on one value. Code is read far more often than it is written, so
the reader comes before the writer. Being easy to write never outranks being easy to understand
when read.

This value takes a different shape in production code and in test code.

**Production code is written so that its behavior can be read from the source.**

State in the code which objects are used and which operations are called. MQTJ uses no
framework, reflection, or runtime code generation: collaborators are wired explicitly through
constructors and builders, so the reader never has to guess what happens (see CONTRIBUTING.md).

**Test code is written so that what it verifies is easy to see.**

Use the test kit and the test libraries freely to cut the code that is not part of the
verification. The restraint on dependencies expected of production code does not apply to
tests.

### 1.4 Where this document is silent, follow common Java practice

**SHOULD**

For naming and notation not covered here, follow the conventions in wide use in the Java
community. For example, the case of identifiers follows these defaults.

| Element | Style | Examples |
| --- | --- | --- |
| Types (class, interface, record, enum, and so on) | UpperCamelCase | `ClientId`, `SessionStore` |
| Methods, ordinary fields, parameters, local variables | lowerCamelCase | `findSubscription`, `clientId` |
| Packages | lowercase | `org.mqtt4j.mqtj` |

Rules stated explicitly in this document take precedence over these defaults; for example,
constants and shared fields follow section 2.4 and test method names follow section 8.1. Where
several conventions coexist, prefer consistency with the surrounding code that plays the same
role.

## 2. Baseline and basic notation

### 2.1 Java 25

**MUST**

The Java 25 toolchain is the baseline. Records, sealed types, pattern-matching `switch`, text
blocks, `var`, virtual threads, `_` for unused variables, and Markdown Javadoc are all available.
Do not rewrite them into older forms on the grounds that an older Java could not compile them.
Name unused catch parameters `_`; `_` may also be used for lambda parameters and other places
where a value is deliberately ignored.

### 2.2 Whitespace, line length, and blocks

**MUST**

| Item | Rule |
| --- | --- |
| Encoding | UTF-8 |
| Tabs and line ends | No tabs, no trailing whitespace, and a newline at the end of the file |
| Line length | At most 100 characters (exceptions below) |
| Block indentation | 4 spaces per level |
| Continuation lines | 8 spaces deeper than the line being continued |
| Control statements | Braces around the body of every `if`, `else`, and loop |
| Brace placement | The opening brace stays on the line of the declaration or control statement |

`package` and `import` lines, and lines containing a URL, are exempt from the line length. This
does not mean that a URL licenses an arbitrarily long line of code: distinguish a URL that cannot
be split from surrounding code that can be wrapped.

Blank lines mark units of processing and the boundaries between members. An empty constructor,
or the `{}` of a record with no body, may stay on one line.

For example, even a short conditional gets braces, and a blank line separates it from what
follows.

```java
if (candidates.isEmpty()) {
    return Optional.empty();
}

return selectBestCandidate(candidates);
```

### 2.3 Import order and usage

**MUST**

Group imports in the following order, with a blank line between groups and alphabetical order
within each group.

1. Java standard library
2. Third-party libraries
3. `org.mqtt4j.mqtj.*`
4. Static imports

Leave no unused or redundant imports. Static imports such as `requireNonNull` and AssertJ's
`assertThat` are used as in the existing code.

In test code, `*` may be used for both regular and static imports. When names collide, import the
needed ones individually or qualify the type at the point of use.

```java
import java.util.*;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;
```

### 2.4 Name constants and static shared fields by their role

**SHOULD**

Name a static field by whether it holds a value or a collaborator. A fixed value that the
processing relies on (a duration, a count, a name, a lookup table) gets an `UPPER_SNAKE_CASE`
constant name. A collaborator that does work (a logger, a resolver, a mapper, a cache) gets a
`lowerCamelCase` name even when it is `static final`. The modifier only says that the field is
never reassigned; it says nothing about whether the field is a value or a collaborator, so decide
by role, not by modifier.

For example, a fixed timeout and a limit are written differently from a shared logger or
parser.

```java
private static final Duration PING_RESPONSE_TIMEOUT = Duration.ofSeconds(10);
private static final int MAX_PACKET_ID = 65_535;

private static final Log log = Log.of(SessionActor.class);
private static final TopicFilterParser topicFilterParser = new TopicFilterParser();
```

The first two name fixed values that the processing takes as given. The last two share
collaborators that do logging and parsing.

A reference type can still be a value: a fixed `Duration` or an immutable lookup table reads as a
value and gets a constant name. Conversely, the fact that an object is immutable is not by itself
a reason for a constant name.

## 3. Line breaks and expression layout

### 3.1 The standard form for calls and declarations

**SHOULD**

Arguments and parameters laid out vertically are the standard form. A call that reads well on
one line may stay on one line. When it grows long, break after the opening parenthesis and put
one argument per line as a rule. Method and constructor parameters and record components are
laid out the same way.

```java
send(clientId, packet);

return dispatch(
        new Delivery(
                message,
                subscription,
                ack
        ),
        deadline);
```

You may break a line that fits within 100 characters when doing so makes the correspondence of
arguments or the structure of an expression easier to read. Do not collapse an existing,
readable line break merely because the result would fit.

### 3.2 A vertically laid-out argument list closes on a line of its own

**MUST**

The closing parenthesis returns to the indentation of the line where the call or declaration
started. A following `;`, `,`, or the `{` of a declaration may share that line. For lambdas and
text blocks, the accepted variants in section 3.4 also apply.

```java
private SubAck subscribe(
        Subscribe request,
        List<TopicFilter> filters) {
    return session.subscribe(request, filters);
}
```

### 3.3 Method chains

**SHOULD**

When a chain grows long, break before each call so that the stages of processing can be read,
and align the leading `.` of the continued calls.

```java
return subscriptions.stream()
        .filter(subscription -> subscription.matches(topic))
        .map(subscription -> deliveryFor(subscription))
        .toList();
```

The first call normally stays on the line of the receiver; with a long receiver or declaration,
breaking right after the receiver is also acceptable. A short sequence such as
`subscription.filter().matches(topic)` may stay on one line. Breaking mechanically before
every `.` is not required.

### 3.4 Accepted variants for lambdas, logging, and text blocks

**GUIDANCE**

Place a lambda's parameters and the start of its body where their correspondence is easy to
read. The `)` that closes an inner construction and the `)` that closes the outer call may share
a line.

```java
return subscriptions.stream()
        .map(subscription -> new Delivery(
                messageFor(subscription),
                ackFor(subscription)
        ))
        .toList();
```

Short arguments followed by a final block lambda may start on the same line and close with `});`.

```java
subscribe(topic, message -> {
    handle(message);
});
```

In log calls, the message may stay on the line of the call, with short placeholder arguments
gathered on the next line. Return to the standard form of section 3.1 once the call grows long.

```java
log.warn("Failed to write {} to {}",
        packet, address, e);
```

The content of a text block follows the structure of the JSON or other embedded text itself. The opening and
closing `"""` and the call's arguments need not be split by the rules for ordinary argument
lists.

### 3.5 Keep readable existing layout

**GUIDANCE**

Where existing code satisfies the line length and the mandatory layout rules and falls within
the accepted variants, its layout may stay as it is. Do not change a readable line break only
because it differs from the standard form. For example, a call whose short arguments were laid
out vertically to make them easy to compare may stay that way.

## 4. Local variable declarations

### 4.1 When the initializer shows the type

**MUST**

Declare a local variable with `var` when it is initialized by one of the following. Put any type
arguments the variable needs in the initializer.

- a constructor call
- an array creation expression
- an explicit cast

The basic forms:

```java
var list = new ArrayList<String>();
var clientId = new ClientId(value);
var buffer = new byte[1024];
var publish = (Publish) packet;
```

Do not type a local variable as an interface or a supertype merely by convention. The following
declaration is normally rewritten to the `var list = new ArrayList<String>();` above.

```java
List<String> list = new ArrayList<>();
```

The diamond may be used when the type arguments can be inferred properly from the constructor
arguments.

```java
var names = new ArrayList<>(List.of("Alice", "Bob"));
```

An empty construction written as `var list = new ArrayList<>();` has the type
`ArrayList<Object>`. Replacing the declared type with `var` does not always preserve the original
type arguments.

### 4.2 When naming a supertype carries meaning

As an exception to section 4.1, the needed type may be written out when the same variable is
later reassigned a different implementation, or when the declared type matters for something
such as overload selection. "It is common to hold it as the interface" is not by itself a reason
for this exception.

Moving the supertype from the left-hand side into a cast on the right of `var` is not a standard
form either; a cast is for cases where the conversion itself has meaning.

For anonymous classes, type annotations, wildcards, and the like, check individually what
omitting the type would mean.

For example, a local variable that later receives a different implementation is declared with a
type that admits that assignment.

```java
List<String> names = new ArrayList<>();
if (useDefaults) {
    names = defaultNames();
}
```

When converting existing code to `var`, also check where the variable is used. Compiling is not
proof of unchanged behavior; the declared type can change which overload is selected.

### 4.3 Other local variables are declared with an explicit type

**SHOULD**

Apart from the cases in section 4.1 and the exceptions listed here, write the type of a local
variable.

Results of helpers, getters, `resolve`, `build`, `Files` operations, and the like are declared
as follows. A descriptive variable name, or the writer knowing the return type, is not a reason
to use `var`.

```java
Subscription subscription = registry.find(filter);
Path configurationFile = directory.resolve("config.json");
Matcher matcher = pattern.matcher(value);
```

The following places do not require an explicit type; choose by the readability of the
expression and its surroundings.

- Initialization with a literal or a constant.
- A factory call qualified by a type name that returns a value of that type.
- The variable of an enhanced `for` loop, and lambda parameters.

For example, forms where the type information appears in the literal or in the type name are
acceptable.

```java
var retryCount = 3;
var timeout = Duration.ofSeconds(5);
```

Do not extend this to the results of ordinary helpers and getters. `Files.readString(path)` is
not a factory that produces a `Files`, so it is written `String contents = Files.readString(path);`.

### 4.4 Do not mark local variables `final`

**MUST**

Local variable declarations carry no explicit `final`, whether the type is written or `var` is
used. A preference for immutable values is not a reason to add `final` to locals across the
board.

Written normally:

```java
List<Subscription> subscriptions = registry.matching(topic);
var delivery = new Delivery(message, ack);
executor.execute(() -> dispatch(delivery));
```

Not written:

```java
final List<Subscription> subscriptions = registry.matching(topic);
final var delivery = new Delivery(message, ack);
```

`final` on a local only forbids reassignment; it has nothing to do with the immutability of the
referenced object. Nor is it needed for variables captured by lambdas: the compiler checks that
they are effectively final.

## 5. Absence of values and type design

### 5.1 Do not use `Optional` for state or input

**MUST**

Do not use `java.util.Optional` for fields, record components, DTO or persistence members,
constructor parameters, or method parameters. State or input that may be absent is expressed as
a nullable value plus the handling of its absence.

```java
record SessionState(
        ClientId clientId,
        @Nullable Will will) {
    SessionState {
        requireNonNull(clientId, "clientId must not be null");
    }
}
```

Local variables that receive an `Optional` return value, and processing through `map`, `orElse`,
and the like, are fine.

### 5.2 Express an absent result with `Optional` or an empty collection

**SHOULD**

When a method returns a single value that may be absent, return `Optional<T>`. Choose a nullable
return only for a concrete reason, such as a record component accessor or an external contract
that cannot be changed.

For example, return `Optional.empty()` when no topic has been registered for an alias.

```java
public Optional<Topic> findAlias(int topicAlias) {
    return Optional.ofNullable(aliases.get(topicAlias));
}
```

Never make the `Optional` itself null; absence is `Optional.empty()`.

When returning a `List`, `Set`, `Map`, or similar, express "no matching elements" as an empty
collection, not as null. For example, return an empty list when no subscription matches.

```java
public List<Subscription> matching(Topic topic) {
    return subscriptions.stream()
            .filter(subscription -> subscription.filter().matches(topic))
            .toList();
}
```

When empty and absent mean different things, as in "not yet fetched" versus "fetched, zero
results", make the distinction explicit in the result type or the contract.

A record's component accessor returns the component's own type, so the accessor of a nullable
component stays nullable. There is no need to turn the record into a class for that, or to add
an alias accessor that returns `Optional`. Methods defined on the record beyond its accessors
follow this recommendation.

Where the return type or the meaning of null cannot be changed, as when implementing an external
interface, follow that contract. When null is returned, mark it `@Nullable` as the next section
describes.

### 5.3 Non-null is the default; annotate only what may be null with `@Nullable`

**MUST**

Non-null is the default contract in MQTJ, and every package says so: its `package-info.java`
carries JSpecify's `@NullMarked`. Callers pass non-null values, and callees are written on that
assumption. Mark nullable declarations with `@Nullable`; the annotation expresses the departure
from the default, that is, that null is allowed.

`@NonNull` is not used to restate the default. It is written only where JSpecify's rules for
generics need it, typically to narrow a type variable that admits null:

```java
public static <T extends @Nullable Object> @NonNull T requireValue(T value, String name) {
    if (value == null) {
        throw new IllegalArgumentException(name + " must not be null");
    }
    return value;
}
```

The rule applies to every visibility. NullAway checks all `@NullMarked` code during compilation
and rejects null flowing into an unannotated parameter, field, or return, whether the declaration
is public or private. Record components are treated as the contract of the constructor and
accessors that the record exposes.

```java
public void disconnect(ReasonCode reason, @Nullable String reasonString) {
    session.disconnect(reason, reasonString);
}

private String describe(@Nullable String reasonString) {
    return reasonString == null ? "no reason given" : reasonString;
}
```

In `disconnect`, `reason` is non-null and `reasonString` may be null. Nothing is added to
`reason`.

Keep `@Nullable` out of exported signatures. Return `Optional` for an absent result and offer an
overload instead of accepting null (section 5.2). Where a nullable parameter or return is the
honest contract, annotate it and put `@SuppressWarnings("exports")` on the class: javac's
`exports` lint objects to the annotation type, which is only `requires static`, appearing in an
exported signature. Do not disable the lint category; it also catches real leaks such as a
`java.net.http` type in a public signature. Internal packages have no such restriction.

When NullAway reports something that is not a bug, add `@SuppressWarnings("NullAway")` to the
smallest enclosing declaration together with a comment that says why. JSpecify is a compile-time
dependency only; CONTRIBUTING.md describes the build and distribution rules that keep the
annotations out of the published jars.

### 5.4 Prefer immutable values and choose the type that fits the representation

**SHOULD**

Prefer to represent domain values immutably. For a value that exposes its components as they
are, a record is the concise form. For example, a topic filter paired with its granted QoS:

```java
public record Granted(TopicFilter filter, Qos qos) {}
```

Choose a class when you need to adjust equality, member visibility, or auxiliary state. A record
can override `equals` and `hashCode`, but the accessors of its components are always public. Do
not force a design into the shape of a record.

For example, an immutable class that hides its internal number and offers only a decision:

```java
public final class RetryPolicy {

    private final int maxAttempts;
    public RetryPolicy(int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        this.maxAttempts = maxAttempts;
    }
    public boolean allowsRetry(int attempts) {
        return attempts < maxAttempts;
    }
}
```

Enforce a value's invariants in its constructor so that callers do not have to check them again.
Examples include requiring non-null or non-empty values, restricting a value to a range, and
rejecting duplicate elements.

A runtime that manages mutable state owns its state changes; this is not a rule to turn every
domain-related class into an immutable record.

Collections held by a value type are shielded from outside modification where needed with
`List.copyOf`, `Set.copyOf`, `Map.copyOf`, and the like. Making a type a record does not make
what it references immutable.

```java
public record SubscribeRequest(List<TopicFilter> filters) {

    public SubscribeRequest {
        filters = List.copyOf(filters);
    }
}
```

With arrays or mutable elements, also consider element ownership and modification through
accessors. Where an immutable collection is already held, there is no need for ceremonial copies
in the accessors.

### 5.5 Express per-state invariants with sealed interfaces and records

**SHOULD**

When each alternative needs different data, consider a sealed interface with records. Compared
with one type that carries a kind field plus nullable fields, it makes invalid combinations hard
to express.

For example, serial dispatch carries no data, while dispatch on an application executor always
names the executor.

```java
public sealed interface Dispatch {

    record Serial() implements Dispatch {}
    record OnExecutor(Executor executor) implements Dispatch {
        public OnExecutor {
            requireNonNull(executor, "executor must not be null");
        }
    }
}
```

`OnExecutor`'s constructor rejects null, and `Serial` has nowhere to hold an executor. A
combination such as "serial dispatch, but with an executor set" cannot be expressed.

When fixed values suffice, an enum is the natural representation.

```java
public enum AckMode {
    ON_RECEIVE,
    AFTER_HANDLER,
    MANUAL
}
```

When the implementations are nested inside the sealed type and the permitted types are obvious,
prefer omitting `permits`. Write it when the types live elsewhere, or when spelling them out
makes the structure clearer.

## 6. Javadoc, comments, and strings

### 6.1 Write Javadoc in Markdown form

**MUST**

Javadoc, when written, uses the `///` Markdown form. On a public API, describe the contract its
users need and leave the internal implementation out. A public API is a type or method that
offers a contract to other modules or to users.

```java
/// Finds the subscription whose filter matches `topic`.
///
/// Returns an empty result when no subscription matches.
///
/// @param topic the topic of a received message
/// @return the subscription, or empty when none matches
public Optional<Subscription> find(Topic topic) {
    // ...
}
```

Write what the declaration alone does not say: the meaning of absence, ordering, side effects,
and threading or callback contracts. Missing Javadoc on internal or package-private code is not
by itself a review finding.

The Markdown form still allows tags such as `@param`, `@return`, and `{@inheritDoc}`, and HTML
where needed; using `///` does not mean removing all Javadoc tags. Ordinary `//` and `/* ... */`
comments need not be converted into Javadoc.

### 6.2 Use comments to convey what the code means and how to read it

**SHOULD**

Write comments freely, as part of the explanation a reader needs to understand the code. Say
what the processing does, what the data represents, and by what reasoning the result is reached,
so that readers need not reconstruct purpose and mechanism from the details of the
implementation.

Explaining "what is happening" is welcome too. Stating up front the role of a block of code, or
the approach of an algorithm, makes the code that follows easier to understand. Even where the
content could be derived by tracing the code, a comment that summarizes the point and makes the
code easier to understand is worth having.

For example, explain:

- What a method offers and under what conditions: the meaning of parameters and results,
  ordering, side effects, and so on, written as Javadoc so that the method can be used without
  reading the implementation.
- The approach of an algorithm, the role of each stage, and what each loop iteration checks.
- What a data structure or a variable represents: how elements correspond, valid ranges, units,
  and the intermediate state being carried.
- Why a step or an ordering is necessary: conditions to uphold, why an optimization holds,
  compatibility constraints, and deliberate simplifications with the conditions for revisiting
  them.

The following simplified example reduces the matches of one message against overlapping
subscriptions. The comments say what intermediate state the map holds and what the loop as a
whole is after.

```java
record Match(Subscription subscription, Qos grantedQos) {}

// A message may match several overlapping subscriptions of the same client.
// This index holds the highest QoS granted per subscription among the matches seen so far.
var bestBySubscription = new HashMap<Subscription, Match>();
// Reduce filter-level matches to one delivery per subscription.
for (Match match : matches) {
    Match current = bestBySubscription.get(match.subscription());
    if (current == null || match.grantedQos().level() > current.grantedQos().level()) {
        bestBySubscription.put(match.subscription(), match);
    }
}
```

Processing that is easier to understand when read as a whole is written as one flow, with the
needed explanation placed where it applies. There is no need to cram explanations into very long
method names, or to split out helper methods merely to replace a comment.

The length of an explanation is set by what is needed to understand, not by the amount of code.
One line of code may warrant ten lines of comment if that is what conveying its reason or
meaning takes. Comments that merely restate each line need not be repeated, but do not delete a
summary of the processing or an explanation of the data on the grounds that it duplicates the
code.

When changing the processing, also check that the intent and constraints written in the comments
still hold.

### 6.3 Write multi-line strings as text blocks

**SHOULD**

Strings that span several lines, or that would need many escaped quotes, are written as text
blocks. The content is easier to read without string concatenation or escaped quotes. A string
that reads well on one line may stay an ordinary literal.

Test data such as JSON is the usual case: when escaping or concatenation would make it hard to
read, use a text block.

```java
String payload = """
        {
          "clientId": "sensor-1",
          "status": "online"
        }
        """;
```

There is no need to turn single words or short values into text blocks; the empty JSON object may
be written `"{}"`.

## 7. Control flow and exception handling

### 7.1 Branch over a closed set of alternatives with a switch expression that lists them all

**SHOULD**

When branching on a value whose alternatives form a closed set, such as a sealed type or an enum,
use a switch expression that lists every alternative. Adding an alternative then fails to
compile, which exposes the branch that was not added. Pattern matching extracts the value each
alternative carries right there. For example, describing the dispatch policy from section 5.5:

```java
String describe(Dispatch dispatch) {
    return switch (dispatch) {
        case Dispatch.Serial _ -> "one virtual thread per subscription";
        case Dispatch.OnExecutor(var executor) -> "the application's " + executor;
    };
}
```

For values whose alternatives are not closed, such as unknown values from an external protocol,
state explicitly what `default` does. Rewriting an existing chain of `if`s merely because newer
syntax exists is not required.

### 7.2 Streams and loops: choose whichever reads better

**GUIDANCE**

Processing that reads naturally as a data transformation can be written with streams.
Processing dominated by side effects, branching, or early exit is written as a loop rather than
forced into a chain. Uniformity in either direction is not required.

### 7.3 Exception handling and logging

**GUIDANCE**

When catching an exception, make clear whether to propagate it, retry the operation, or stop the
operation. Logging alone does not count as the required error handling. Include in the log the
context that identifies the situation, such as the client id and the packet or operation. Use
the `{}` placeholders of the `Log` facade, and pass the exception object as the last argument
when the cause is needed.

The log level, how broadly to catch, and whether to rethrow are decided by the responsibility of
that boundary. They are not turned into a rule that checks the shape of a `catch` clause alone.

## 8. Tests

Test code gives priority to making what it verifies easy to see (section 1.3).

### 8.1 Name tests after behavior, in plain English

**SHOULD**

A test class is named after its main subject or contract plus `Test`, such as
`TopicFilterTest` or `SessionActorTest`. When the role of a test needs distinguishing, be
specific, as in `MosquittoInteropTest`.

A test method name states the expected behavior as a plain English phrase, as if explaining it
to a user or a domain expert. Include the condition when it is needed to tell the behavior apart.
snake_case is recommended because it reads as a sentence; no fixed word order or prefix is
imposed.

Fill-in-the-blanks schemes such as `[MethodUnderTest]_[Scenario]_[ExpectedResult]` are not used.
Mechanically including the method name or the exception class tilts the name toward describing
the implementation and obscures what the test guarantees. The name should tell what happens for
the user under that condition.

| Hard to read the intent | Names the behavior |
| --- | --- |
| `subscribe_ClosedClient_ThrowsIllegalStateException` | `a_closed_client_rejects_subscribe` |
| `testUnsubscribe` | `unsubscribing_twice_has_no_effect` |
| `matches_MultiLevelWildcard_ReturnsTrue` | `a_multi_level_wildcard_matches_deeper_topics` |

Do not stop at "normal case" or "error case"; name the expected behavior. As a rule of thumb,
renaming an internal method or splitting the implementation differently should not require
renaming a test whose behavior is unchanged.

### 8.2 Reduce the code that is not part of the verification

**SHOULD**

Use the test kit freely to cut the code that is not part of the verification. The embedded
broker stands in for a real one, the fake client for a connection, and the fault-injecting
transport and the manual scheduler for the network and the clock. Star imports (section 2.3)
shorten the import list.

For example, a session test replaces the network and the clock with the test kit and drives time
explicitly.

```java
class SessionActorTest {

    ManualScheduler scheduler = new ManualScheduler();
    FaultyTransport transport = new FaultyTransport();
    SessionActor sut = new SessionActor(transport, scheduler);

    @Test
    void sends_a_ping_when_the_keep_alive_interval_elapses() {
        // Arrange: connected with a 30 second keep-alive.
        sut.connected(connAck(), Duration.ofSeconds(30));
        // Act
        scheduler.advance(Duration.ofSeconds(30));
        // Assert
        assertThat(transport.sent()).extracting(Packet::type).contains(PacketType.PINGREQ);
    }
}
```

For the same reason, when the operation and the property under test are the same and only the
input varies, combine the cases into a parameterized test. For example, one parameterized test
can verify that publishing is rejected in every state other than connected.

```java
@ParameterizedTest(name = "rejects publish while {0}")
@EnumSource(
        value = ConnectionState.class,
        names = {"DISCONNECTED", "RECONNECT_WAIT", "CLOSED"})
void rejects_publish_unless_connected(ConnectionState state) {
    // Arrange
    SessionActor sut = actorIn(state);
    // Act & Assert
    assertThatThrownBy(() -> sut.publish(message()).join())
            .hasCauseInstanceOf(NotConnectedException.class);
}
```

Passing the expected value alongside each input also works. For complex inputs, use
`@MethodSource` or similar, with names and data that make the meaning of each case readable.
When display names are given, make a failing case distinguishable by its input. For example, a
record of the source and target states carries its meaning into the display name.

```java
record Transition(ConnectionState from, ConnectionState to) {}

static Stream<Transition> rejectedTransitions() {
    return Stream.of(
            new Transition(CLOSED, CONNECTING),
            new Transition(CONNECTED, CONNECTING)
    );
}

@ParameterizedTest(name = "{0}")
@MethodSource("rejectedTransitions")
void rejects_transition_between_incompatible_states(Transition transition) {
    // ...
}
```

See [JUnit's Parameterized Tests](https://docs.junit.org/6.0.2/writing-tests/parameterized-classes-and-tests.html)
for the notation.

When inputs call for different operations, or the assertions switch on `if` or `switch` to check
different properties, separate tests read better. Do not merge different scenarios just because
lines repeat.

### 8.3 Show the flow with Arrange / Act / Assert, and the subject with `sut`

**SHOULD**

Separate a test with the three comments Arrange, Act, and Assert, so that preparation, the action
on the subject, and the check of the result are easy to tell apart. Name the instance under test
`sut` (system under test), so that the Act section shows by name which component it exercises.
It is usually created as a local variable in the Arrange section; when many tests repeat the
same construction, a field is also fine.

```java
@Test
void delivers_to_the_only_matching_subscription() {
    // Arrange
    var subscriptions = List.of(matchingSubscription, otherSubscription);
    var sut = new Dispatcher(subscriptions);
    // Act
    List<Delivery> deliveries = sut.dispatch(message);
    // Assert
    assertThat(deliveries)
            .extracting(Delivery::subscription)
            .containsExactly(matchingSubscription);
}
```

Where the subject only comes into being in the Act section, as when the construction of a value
is itself what is verified, `sut` is not needed.

```java
@Test
void preserves_filters_when_the_input_list_changes() {
    // Arrange
    var filters = new ArrayList<>(List.of(filter));

    // Act
    var request = new SubscribeRequest(filters);
    filters.clear();
    // Assert
    assertThat(request.filters()).containsExactly(filter);
}
```

When execution and verification are written together, as with `assertThatThrownBy`,
`// Act & Assert` or `// Act / Assert` may be used. A test with no preparation needs no empty
Arrange section. When a field or a helper does the preparation, say so, as in
`// Arrange: connected with a 30 second keep-alive.`. Shared helpers need not carry these markers.

Short tests whose flow is obvious may omit the comments.

```java
@Test
void a_multi_level_wildcard_matches_deeper_topics() {
    assertThat(TopicFilter.of("sensors/#").matches(Topic.of("sensors/1/temp"))).isTrue();
}
```

### 8.4 Make the contract under verification clear

**SHOULD**

Verify the contract that must not change, and avoid adding tests that depend only on how a fake
was called or on how the implementation is split. Where the interaction itself is the contract,
verify its count or order. Use an existing test that would catch the breakage first, rather than
adding a duplicate guarantee.

For example, if the contract is that a graceful disconnect sends exactly one DISCONNECT packet
carrying the given reason, verify both the count and the reason.

```java
// Act
sut.disconnect(ReasonCode.NORMAL_DISCONNECTION).join();

// Assert
assertThat(transport.sent(PacketType.DISCONNECT))
        .singleElement()
        .extracting(Disconnect::reasonCode)
        .isEqualTo(ReasonCode.NORMAL_DISCONNECTION);
```
