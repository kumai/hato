package org.mqtt4j.hato.common.log;

import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/// Logging facade over `System.Logger` with SLF4J-style `{}` placeholders.
///
/// `System.Logger` formats parameterised messages with `java.text.MessageFormat`, whose `{0}`
/// placeholders and apostrophe handling are easy to get wrong. This facade substitutes `{}`
/// itself, only when the level is enabled, and hands the finished text to `System.Logger`.
/// Applications keep configuring levels and backends through `System.Logger` names.
///
/// Arguments are rendered only when the level is enabled, so pass values as they are instead of
/// preformatting them or guarding the call. Arrays render their elements; byte arrays render as
/// hexadecimal, as in wire dumps.
///
/// Each class owns a static log named after that class. Package names identify the distribution;
/// individual class names are implementation details.
///
/// A statement never waits for the logging backend. The calling thread renders the text when the
/// level is enabled; a thread of its own then gives the statements of the whole distribution to
/// `System.Logger`, in the order in which they were made. Nor does the calling thread ask the
/// backend whether a level is enabled: a log keeps the backend's answer, which it gets as it is
/// made and which that thread asks for again every second. A backend can hold its caller for
/// seconds, in a level check as well as in a write, and not only when it writes to a slow device:
/// an application's filter can wait as long as its handler can. Where the application logs from
/// virtual threads, a platform thread that waits for the backend's lock behind one of them sleeps
/// until that virtual thread has run, although the lock is free, and that takes as long as the
/// application keeps every carrier busy (JDK 25). The threads that call this facade keep
/// connections alive, so they must not be the ones that wait.
///
/// The price is this. The backend stamps a statement with the time at which it is written.
/// Statements that find no room because the backend has fallen behind are dropped, and a warning
/// in their place says how many. Statements that still wait when the JVM exits are lost, so an
/// entry point of the distribution waits for them in [#awaitWritten(Duration)] when the
/// application closes it. A level that the application changes in the backend applies to the
/// statements within a second. A filter of the backend that decides by the thread that calls it,
/// such as by that thread's diagnostic context, sees the thread that writes.
///
/// A failing backend never reaches the caller. Logging happens in cleanup, notification, and
/// failure-reporting paths, so a throwing handler must not interrupt them or replace the
/// failure being reported.
///
/// The JDK looks for a `System.LoggerFinder` on the system class loader only. A launcher of nested
/// jars or an application server loads this class and the logging bridge from a child of that
/// loader, so the JDK sees no bridge and settles on `java.util.logging`. In that case this facade
/// uses the finder that its own class loader sees, and applications add the bridge the same way
/// wherever the jar runs. This is the one use of `ServiceLoader` in the project: no explicit wiring
/// can replace it without a public API that every application would have to call before any class
/// of the distribution initializes. It hinders neither native images, which need no metadata for
/// it, nor the AOT cache.
public final class Log {

    // Null when the JDK found a provider itself or none exists, where System.getLogger is right.
    private static final System.@Nullable LoggerFinder ownLoaderFinder =
            findOwnLoaderFinder().orElse(null);

    private static final Level[] LEVELS = Level.values();

    private final System.Logger logger;

    // The levels that the backend enables, one bit for each by ordinal, as it last answered: as
    // the log was made, and since then each time the thread that writes asks again. Statements
    // read these instead of asking the backend.
    private volatile int enabled;

    private Log(System.Logger logger) {
        this.logger = logger;
        enabled = enabledLevels(logger);
        try {
            Writer.instance.watch(this);
        } catch (Throwable _) {
            // The thread that writes could not start, so statements are lost anyway; the levels
            // stay as the backend first answered.
        }
    }

    /// Returns the log with the given `System.Logger` name.
    public static Log of(String name) {
        requireNonNull(name, "name must not be null");
        return of(systemLogger(name));
    }

    private static System.Logger systemLogger(String name) {
        if (ownLoaderFinder != null) {
            try {
                return ownLoaderFinder.getLogger(name, Log.class.getModule());
            } catch (Throwable _) {
                // A broken bridge must not keep the class asking for its log from initializing.
            }
        }
        return System.getLogger(name);
    }

    /// Returns the log named after `type`. After embedding, the name carries the distribution's
    /// package, so client and broker diagnostics never share a name.
    public static Log of(Class<?> type) {
        requireNonNull(type, "type must not be null");
        return of(type.getName());
    }

    static Log of(System.Logger logger) {
        return new Log(requireNonNull(logger, "logger must not be null"));
    }

    /// Waits until the backend has taken the statements that the logs of the distribution made
    /// before this call, or until `limit` has passed. An interrupted thread does not wait, and
    /// keeps its interrupt status. Like a statement, the wait never fails its caller.
    ///
    /// @return whether the backend has taken them
    public static boolean awaitWritten(Duration limit) {
        try {
            return Writer.instance.awaitWritten(limit.toNanos());
        } catch (Throwable _) {
            // The thread that writes could not start, so nothing waits to be written.
            return false;
        }
    }

    /// Has every log of the distribution ask the backend again which levels it enables, and waits
    /// until they all have, or until `limit` has passed. Otherwise a level that the application
    /// changes in the backend applies within a second; a test that changes one waits here. An
    /// interrupted thread does not wait, and keeps its interrupt status. Like a statement, the
    /// wait never fails its caller.
    ///
    /// @return whether the logs have asked
    public static boolean awaitLevels(Duration limit) {
        try {
            return Writer.instance.awaitLevels(limit.toNanos());
        } catch (Throwable _) {
            // The thread that writes could not start, so no statement is written anyway.
            return false;
        }
    }

    private static Optional<System.LoggerFinder> findOwnLoaderFinder() {
        try {
            // The JDK's own finders are boot classes. Any other finder is a provider that the JDK
            // found, and looking again would only create a second instance of it.
            if (System.LoggerFinder.getLoggerFinder().getClass().getClassLoader() != null) {
                return Optional.empty();
            }
            return ServiceLoader.load(System.LoggerFinder.class, Log.class.getClassLoader())
                    .findFirst();
        } catch (Throwable _) {
            // As the JDK does with a broken provider, carry on without it.
            return Optional.empty();
        }
    }

    /// The `System.Logger` name.
    public String getName() {
        return logger.getName();
    }

    public boolean isLoggable(Level level) {
        return (enabled & (1 << level.ordinal())) != 0;
    }

    // Asks the backend which levels it enables. A level whose check fails counts as disabled,
    // as its statements would be lost.
    private static int enabledLevels(System.Logger logger) {
        int bits = 0;
        for (Level level : LEVELS) {
            try {
                if (logger.isLoggable(level)) {
                    bits |= 1 << level.ordinal();
                }
            } catch (Throwable _) {
                // The backend failed to answer for this level.
            }
        }
        return bits;
    }

    public void error(String message, @Nullable Object... args) {
        log(Level.ERROR, message, args);
    }

    public void warn(String message, @Nullable Object... args) {
        log(Level.WARNING, message, args);
    }

    public void info(String message, @Nullable Object... args) {
        log(Level.INFO, message, args);
    }

    public void debug(String message, @Nullable Object... args) {
        log(Level.DEBUG, message, args);
    }

    public void trace(String message, @Nullable Object... args) {
        log(Level.TRACE, message, args);
    }

    private void log(Level level, String message, @Nullable Object[] args) {
        try {
            if (!isLoggable(level)) {
                return;
            }
            // As in SLF4J, a trailing Throwable that no placeholder consumes is the cause.
            int used = args.length;
            Throwable thrown = null;
            if (used > countPlaceholders(message) && args[used - 1] instanceof Throwable last) {
                thrown = last;
                used--;
            }
            String text = substitute(message, args, used);
            Writer.instance.take(Statement.of(
                    logger,
                    level,
                    text,
                    thrown,
                    Thread.currentThread().getName()
            ));
        } catch (Throwable _) {
            // The statement is lost; the caller continues.
        }
    }

    private static int countPlaceholders(String message) {
        int count = 0;
        int i = 0;
        while (i < message.length()) {
            if (message.charAt(i) == '\\' && message.startsWith("{}", i + 1)) {
                i += 3;
            } else if (message.startsWith("{}", i)) {
                count++;
                i += 2;
            } else {
                i++;
            }
        }
        return count;
    }

    /// Replaces each unescaped `{}` with the next argument. `\{}` stays a literal `{}`, and
    /// placeholders without an argument stay as they are.
    private static String substitute(String message, @Nullable Object[] args, int argCount) {
        var text = new StringBuilder(message.length() + 32);
        int next = 0;
        int i = 0;
        while (i < message.length()) {
            if (message.charAt(i) == '\\' && message.startsWith("{}", i + 1)) {
                text.append("{}");
                i += 3;
            } else if (message.startsWith("{}", i) && next < argCount) {
                text.append(render(args[next++]));
                i += 2;
            } else {
                text.append(message.charAt(i));
                i++;
            }
        }
        return text.toString();
    }

    private static String render(@Nullable Object value) {
        try {
            return switch (value) {
                case null -> "null";
                case Object[] array -> Arrays.deepToString(array);
                case byte[] array -> HexFormat.of().formatHex(array);
                case int[] array -> Arrays.toString(array);
                case long[] array -> Arrays.toString(array);
                case char[] array -> Arrays.toString(array);
                case boolean[] array -> Arrays.toString(array);
                case short[] array -> Arrays.toString(array);
                case double[] array -> Arrays.toString(array);
                case float[] array -> Arrays.toString(array);
                default -> String.valueOf(value);
            };
        } catch (RuntimeException e) {
            // A broken toString() must not take the log statement, or the caller, down with it.
            return "[toString failed: " + e + "]";
        }
    }

    /// What one call leaves for the backend, with the name of the thread that made it and the
    /// room that it takes while it waits. Like the thread that takes it, it is open to the
    /// package for the test that queues a statement which no call makes.
    record Statement(
            System.Logger logger,
            Level level,
            String text,
            @Nullable Throwable thrown,
            String thread,
            long size
    ) {

        // A statement takes its text and the messages of its exception, the causes and the
        // suppressed exceptions included, in characters, and STATEMENT_SIZE and THROWABLE_SIZE
        // more for itself and for each exception. A statement whose exceptions cannot all be
        // counted, because there are more than MAX_EXCEPTIONS or one of them fails to tell,
        // keeps what it cannot count all the same. It takes the whole room, so it waits only
        // when nothing else does, as a statement of any size may.
        // Ceiling: an exception's stack trace, and the fields of an exception class of the
        // application, count as THROWABLE_SIZE whatever they hold. Rendering the exception here
        // would count them, at the price of its whole text for every statement with one.
        static Statement of(
                System.Logger logger,
                Level level,
                String text,
                @Nullable Throwable thrown,
                String thread
        ) {
            long size = Writer.STATEMENT_SIZE + text.length();
            var exceptions = new ArrayDeque<Throwable>();
            if (thrown != null) {
                exceptions.add(thrown);
            }
            // The exceptions found so far, counted or not yet. A cause chain can come back to
            // itself, so the count also bounds the walk.
            int found = exceptions.size();
            Throwable exception;
            while ((exception = exceptions.poll()) != null) {
                size += Writer.THROWABLE_SIZE;
                try {
                    String message = exception.getMessage();
                    if (message != null) {
                        size += message.length();
                    }
                    Throwable cause = exception.getCause();
                    Throwable[] suppressed = exception.getSuppressed();
                    found += (cause == null ? 0 : 1) + suppressed.length;
                    if (found > Writer.MAX_EXCEPTIONS) {
                        return new Statement(logger, level, text, thrown, thread, Writer.ROOM);
                    }
                    if (cause != null) {
                        exceptions.add(cause);
                    }
                    exceptions.addAll(Arrays.asList(suppressed));
                } catch (RuntimeException _) {
                    return new Statement(logger, level, text, thrown, thread, Writer.ROOM);
                }
            }
            return new Statement(logger, level, text, thrown, thread, size);
        }
    }

    /// The thread that gives the statements of the distribution to the backend. Nothing ends it:
    /// a pass of its loop that fails costs at most the statement at hand, and unless the backend
    /// failed it, a warning tells of it.
    ///
    /// While it writes a statement, its name ends with the name of the thread that made the
    /// statement. A backend prints the name of the thread that calls it, and that name is how an
    /// application tells the statements of its clients apart: each thread of a client carries
    /// the client identifier in its name, and many statements name the client nowhere else. The
    /// thread keeps its own name in front, so that no thread dump and no count of threads by name
    /// shows a second selector or a second actor.
    ///
    /// It is a `System.Logger` in name only, for the sake of `java.util.logging`. Its default
    /// format prints the caller that it infers for a statement: the nearest frame under the
    /// backend that does not belong to a `System.Logger`. Every frame of this thread belongs to
    /// this class, so `java.util.logging` finds no caller and prints the name of the log instead,
    /// which is the class that made the statement. Otherwise it would print this class for
    /// every statement.
    ///
    /// It also asks the backend which levels the logs enable, every second, so that a level that
    /// the application changes reaches the statements, while the threads that make them never
    /// ask.
    static final class Writer extends Thread implements System.Logger {

        // Each distribution embeds a copy under org.mqtt4j.hato.<distribution>; naming the
        // thread after it tells the copies apart in one JVM.
        private static final String NAME =
                "hato-" + Log.class.getPackageName().split("\\.")[3] + "-log";

        // The room for waiting statements, in characters of their text and of the messages of
        // their exceptions, with STATEMENT_SIZE more for each statement and THROWABLE_SIZE for
        // each exception (see Statement.of). It bounds the memory that they hold while the
        // backend stands still. Counting statements would not: a trace statement holds a whole
        // packet as text, and an exception can carry a message of any length.
        private static final long ROOM = 4L << 20;

        private static final long STATEMENT_SIZE = 64;

        // What an exception holds beside its message: about what its stack trace takes before a
        // backend prints it.
        private static final long THROWABLE_SIZE = 1024;

        // The exceptions of one statement that the walk counts. A statement with more takes the
        // whole room (see Statement.of).
        private static final int MAX_EXCEPTIONS = 64;

        private static final long POLL_NANOS = 1_000_000;

        // How often the thread asks the backend about the levels again, unless awaitLevels asks
        // it to now.
        // Ceiling: a statement made within a second of a change in the backend follows the old
        // level. A backend that told of its changes would lift this; System.Logger has no way to.
        private static final long LEVELS_NANOS = 1_000_000_000;

        // Starting it here starts it with the first statement, or with the distribution's class
        // preload. If it cannot start, the statements of the distribution are lost.
        static final Writer instance = new Writer();

        static {
            instance.start();
        }

        private final ConcurrentLinkedQueue<Statement> statements = new ConcurrentLinkedQueue<>();

        // The room that the statements in the queue take, the one being written included.
        private final AtomicLong waiting = new AtomicLong();

        // How many statements the queue has taken, and how many of them the backend has had.
        private final AtomicLong taken = new AtomicLong();

        private volatile long written;

        // Statements that found no room, until a warning in the queue tells their number.
        private final AtomicLong dropped = new AtomicLong();

        // Passes of the thread that failed, though not by the backend's doing, until a warning
        // in the queue tells their number, and what failed the first of them.
        private final AtomicLong failed = new AtomicLong();

        private volatile @Nullable Throwable failure;

        // Whether the thread may be parked, so that a new statement has to wake it.
        private volatile boolean idle;

        // The name of another thread that this thread's name ends with, or empty. Only this
        // thread reads and writes it.
        private String carried = "";

        // Tells of dropped statements under the name of the facade. It is no log: a log would
        // ask this thread, which is still being made, to keep its levels.
        private final System.Logger own = systemLogger(Log.class.getName());

        // Every log of the distribution, whose levels this thread asks the backend about. There
        // are as many as classes that log, each held by a static field for good.
        private final ConcurrentLinkedQueue<Log> logs = new ConcurrentLinkedQueue<>();

        // How many times callers have asked for the levels now, and how many of those requests
        // the thread had seen when it last began to ask the backend.
        private final AtomicLong levelRequests = new AtomicLong();

        private volatile long levelRequestsServed;

        private Writer() {
            // The values that the first thread to log happens to hold in inheritable
            // thread-locals, such as a logging context, are not this thread's.
            super(null, null, NAME, 0, false);
            setDaemon(true);
        }

        /// Queues `statement`, or drops it when there is no room. Never waits and takes no lock:
        /// a lock here could hold a platform thread behind a virtual thread just as the
        /// backend's lock can.
        void take(Statement statement) {
            long size = statement.size();
            long ahead = waiting.get();
            // A statement of any size fits an empty queue, so a large one is lost only while
            // the backend is behind. Callers that pass this check together overshoot the room
            // by no more than their own statements.
            if (ahead != 0 && ahead + size > ROOM) {
                dropped.incrementAndGet();
                return;
            }
            // The warning stands where the dropped statements would have stood.
            long lost = dropped.get() == 0 ? 0 : dropped.getAndSet(0);
            if (lost != 0) {
                queue(Statement.of(
                        own,
                        Level.WARNING,
                        "Logging fell behind; dropped " + lost + " log statements",
                        null,
                        ""
                ));
            }
            // A warning of failed passes comes with the first statement made after them. Their
            // number is taken only once the warning is made: where memory failed the thread, it
            // can fail this caller too, and then a later statement tells of them.
            long failures = failed.get();
            if (failures != 0) {
                Statement warning = Statement.of(
                        own,
                        Level.WARNING,
                        "Logging failed " + failures
                                + " times; lost at most one log statement each time",
                        failure,
                        ""
                );
                if (failed.compareAndSet(failures, 0)) {
                    queue(warning);
                }
            }
            queue(statement);
            // The thread looks at the queue once more after it has set the flag, so either it
            // sees this statement or this caller sees the flag.
            if (idle) {
                LockSupport.unpark(this);
            }
        }

        private void queue(Statement statement) {
            waiting.addAndGet(statement.size());
            taken.incrementAndGet();
            statements.add(statement);
        }

        // Has this thread keep the levels of `log` up to date from now on.
        void watch(Log log) {
            logs.add(log);
        }

        /// Has the thread ask the backend about the levels of every log now, and waits until it
        /// has done so from start to end, for at most `nanos`.
        boolean awaitLevels(long nanos) {
            if (Thread.currentThread() == this) {
                return false;
            }
            long request = levelRequests.incrementAndGet();
            LockSupport.unpark(this);
            long deadline = System.nanoTime() + nanos;
            while (levelRequestsServed < request) {
                long left = deadline - System.nanoTime();
                if (left <= 0 || Thread.currentThread().isInterrupted()) {
                    return false;
                }
                LockSupport.parkNanos(this, Math.min(left, POLL_NANOS));
            }
            return true;
        }

        /// Waits until the backend has had every statement queued before this call, for at most
        /// `nanos`.
        boolean awaitWritten(long nanos) {
            // A backend that closes a client or a broker while it writes would wait for itself.
            if (Thread.currentThread() == this) {
                return false;
            }
            long target = taken.get();
            long deadline = System.nanoTime() + nanos;
            while (written < target) {
                long left = deadline - System.nanoTime();
                if (left <= 0 || Thread.currentThread().isInterrupted()) {
                    return false;
                }
                // Looks again every millisecond, which can return that much later than the
                // backend allows. Closing is rare; have the thread wake those who wait if this
                // wait ever shows.
                LockSupport.parkNanos(this, Math.min(left, POLL_NANOS));
            }
            return true;
        }

        @Override
        public void run() {
            long count = 0;
            long levelsAsked = System.nanoTime();
            // The requests for the levels that the thread has begun to serve. A pass that fails
            // while it asks leaves them unserved, and they wait for the next time that the
            // levels are due: asking again at once would fail pass after pass, ahead of the
            // statements, for as long as the cause lasts.
            long levelRequestsTried = 0;
            while (true) {
                try {
                    // A request seen here is served by asking every log after it was made.
                    long requested = levelRequests.get();
                    if (requested != levelRequestsTried
                            || System.nanoTime() - levelsAsked >= LEVELS_NANOS) {
                        levelsAsked = System.nanoTime();
                        levelRequestsTried = requested;
                        for (Log log : logs) {
                            log.enabled = enabledLevels(log.logger);
                        }
                        levelRequestsServed = requested;
                    }
                    Statement next = statements.poll();
                    if (next == null) {
                        carry("");
                        idle = true;
                        if (statements.isEmpty()) {
                            LockSupport.parkNanos(
                                    this,
                                    LEVELS_NANOS - (System.nanoTime() - levelsAsked)
                            );
                        }
                        idle = false;
                        // An interrupted thread no longer parks, and this one has nothing to stop
                        // for.
                        boolean _ = Thread.interrupted();
                        continue;
                    }
                    try {
                        carry(next.thread());
                        try {
                            next.logger().log(next.level(), null, next.text(), next.thrown());
                        } catch (Throwable _) {
                            // The statement is lost. A backend that fails is not this thread's
                            // failure, and no warning tells of it.
                        }
                    } finally {
                        // Written or lost, the statement no longer waits.
                        waiting.addAndGet(-next.size());
                        written = ++count;
                    }
                } catch (Throwable e) {
                    // Nothing ends the thread: whatever fails a pass, it goes on with the next
                    // one. Memory can run out wherever this thread asks for some, as it does to
                    // walk through the logs and to make a name. A failed pass loses the
                    // statement that it had taken, if it had one, and nothing else. Logs that it
                    // had not asked yet keep their levels, and a request for the levels stays
                    // unserved, until a later pass asks every log. The next statement that is
                    // made brings a warning of the failed passes with it.
                    if (failed.get() == 0) {
                        failure = e;
                    }
                    failed.incrementAndGet();
                }
            }
        }

        // Puts the name of the thread that made a statement behind this thread's own, or takes
        // it off for an empty one. The name stays until the next statement of another thread, or
        // until nothing waits.
        private void carry(String thread) {
            if (!thread.equals(carried)) {
                setName(thread.isEmpty() ? NAME : NAME + " for " + thread);
                // Noted once the thread has the name: a name that could not be made is made
                // again for the next statement, which would otherwise go out under the old one.
                carried = thread;
            }
        }

        // Nothing logs through this thread.
        @Override
        public boolean isLoggable(Level level) {
            return false;
        }

        @Override
        public void log(
                Level level,
                @Nullable ResourceBundle bundle,
                @Nullable String message,
                @Nullable Throwable thrown
        ) {
        }

        @Override
        public void log(
                Level level,
                @Nullable ResourceBundle bundle,
                @Nullable String format,
                @Nullable Object @Nullable ... params
        ) {
        }
    }
}
