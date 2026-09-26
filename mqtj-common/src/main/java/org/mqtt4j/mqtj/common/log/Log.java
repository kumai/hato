package org.mqtt4j.mqtj.common.log;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.ResourceBundle;

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
/// individual class names are implementation details. Implementing `System.Logger` lets JUL infer
/// the actual caller through this facade. Parameterized overloads all use `{}`, not MessageFormat.
public final class Log implements System.Logger {

    private final System.Logger logger;

    private Log(System.Logger logger) {
        this.logger = logger;
    }

    /// Returns the log with the given `System.Logger` name.
    public static Log of(String name) {
        return of(System.getLogger(requireNonNull(name, "name must not be null")));
    }

    /// Returns the log named after `type`. After embedding, the name carries the distribution's
    /// package, so client and broker diagnostics never share a name.
    public static Log of(Class<?> type) {
        requireNonNull(type, "type must not be null");
        return of(System.getLogger(type.getName()));
    }

    static Log of(System.Logger logger) {
        return new Log(requireNonNull(logger, "logger must not be null"));
    }

    /// The `System.Logger` name.
    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public boolean isLoggable(Level level) {
        return logger.isLoggable(level);
    }

    @Override
    public void log(
            Level level,
            @Nullable ResourceBundle bundle,
            String message,
            @Nullable Throwable thrown
    ) {
        logger.log(level, bundle, message, thrown);
    }

    @Override
    public void log(
            Level level,
            @Nullable ResourceBundle bundle,
            String message,
            @Nullable Object @Nullable ... args
    ) {
        formatAndLog(level, bundle, message, args == null ? new Object[0] : args);
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

    @Override
    public void log(Level level, String message, @Nullable Object... args) {
        formatAndLog(level, null, message, args);
    }

    private void formatAndLog(
            Level level,
            @Nullable ResourceBundle bundle,
            String message,
            @Nullable Object[] args
    ) {
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
        String text = substitute(message, args, used);
        logger.log(level, bundle, text, thrown);
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
}
