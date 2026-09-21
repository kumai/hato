package org.mqtt4j.mqtj.common.log;

import java.lang.System.Logger.Level;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;

/// Logging facade over `System.Logger` with SLF4J-style `{}` placeholders.
///
/// `System.Logger` formats parameterised messages with `java.text.MessageFormat`, whose `{0}`
/// placeholders and apostrophe handling are easy to get wrong. This facade substitutes `{}`
/// itself, only when the level is enabled, and hands the finished text to `System.Logger`.
/// Applications keep configuring levels and backends through `System.Logger` names.
///
/// Names matter to users. A component whose messages users configure gets a stable name owned by
/// the distribution, such as `org.mqtt4j.mqtj.client.transport`. The shared implementation receives
/// its `Log` from that owner. Class-based names are for internal diagnostics.
public final class Log {

    private final System.Logger logger;

    private final String prefix;

    private final Set<String> warnedKeys;

    private Log(System.Logger logger, String prefix, Set<String> warnedKeys) {
        this.logger = logger;
        this.prefix = prefix;
        this.warnedKeys = warnedKeys;
    }

    /// Returns the log with the given `System.Logger` name.
    public static Log of(String name) {
        return of(System.getLogger(Objects.requireNonNull(name, "name must not be null")));
    }

    /// Returns the log named after `type`. After embedding, the name carries the distribution's
    /// package, so client and broker diagnostics never share a name.
    public static Log of(Class<?> type) {
        Objects.requireNonNull(type, "type must not be null");
        return of(System.getLogger(type.getName()));
    }

    static Log of(System.Logger logger) {
        return new Log(Objects.requireNonNull(logger, "logger must not be null"), "",
                ConcurrentHashMap.newKeySet());
    }

    /// Returns a log that puts `[prefix]` in front of every message, typically the client id.
    /// The returned log has its own `warnOnce` history.
    public Log withPrefix(String prefix) {
        return new Log(logger, "[" + prefix + "] ", ConcurrentHashMap.newKeySet());
    }

    /// The `System.Logger` name.
    public String name() {
        return logger.getName();
    }

    public boolean isDebug() {
        return logger.isLoggable(Level.DEBUG);
    }

    public boolean isTrace() {
        return logger.isLoggable(Level.TRACE);
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

    /// Logs a warning the first time `key` is seen by this log and ignores later calls with the
    /// same key. Use it for tolerated protocol violations that would otherwise repeat per packet.
    /// Keys must come from a bounded set, such as constants.
    public void warnOnce(String key, String message, @Nullable Object... args) {
        if (warnedKeys.add(key)) {
            log(Level.WARNING, message, args);
        }
    }

    private void log(Level level, String message, @Nullable Object[] args) {
        if (!logger.isLoggable(level)) {
            return;
        }
        // As in SLF4J, a trailing Throwable that no placeholder consumes is the cause.
        int used = args.length;
        Throwable thrown = null;
        if (used > countPlaceholders(message) && args[used - 1] instanceof Throwable last) {
            thrown = last;
            used--;
        }
        String text = prefix + substitute(message, args, used);
        if (thrown == null) {
            logger.log(level, text);
        } else {
            logger.log(level, text, thrown);
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
                case byte[] array -> Arrays.toString(array);
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
}
