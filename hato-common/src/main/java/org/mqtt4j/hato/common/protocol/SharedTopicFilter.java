package org.mqtt4j.hato.common.protocol;

import java.util.Optional;

import org.mqtt4j.hato.common.value.TopicFilter;

import static java.util.Objects.requireNonNull;

/// A shared subscription's topic filter, `$share/{ShareName}/{filter}`, split into the share
/// name and the topic filter that selects messages.
///
/// Shared subscriptions are an MQTT 5.0 feature. MQTT 3.1.1 gives `$share/` no meaning, but it
/// reserves names starting with `$` for server-specific use, and widely used brokers such as
/// Mosquitto, EMQX, HiveMQ, and AWS IoT Core accept shared subscriptions from 3.1.1 clients as
/// an extension. They deliver messages under the published topic name, not the `$share/`
/// filter, so a subscriber can route a delivery only by the filter after the share name.
///
/// The distributions apply this rule as follows:
///
/// - The client interprets `$share/` in MQTT 3.1.1 and 5.0 alike. When subscribing, it rejects
///   a filter that [#parse] rejects, and it routes deliveries by [#filter()].
/// - The broker treats `$share/` filters as ordinary topic filters in MQTT 3.1.1 until it
///   supports shared subscriptions. [TopicFilter] and the MQTT 3.1.1 codec therefore keep the
///   filter exactly as written.
/// - The MQTT 5.0 codec uses this type to reject No Local on a shared subscription
///   (`MQTT-3.8.3-4`).
///
/// See [MQTT 5.0, section 4.8.2: Shared Subscriptions](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
///
/// @param shareName the share name: at least one character, without `/`, `+`, or `#`
/// @param filter the topic filter after the share name, used for matching deliveries
public record SharedTopicFilter(String shareName, TopicFilter filter) {

    private static final String PREFIX = "$share/";

    /// Creates a shared filter from its parts.
    ///
    /// @throws IllegalArgumentException if `shareName` is empty or contains `/`, `+`, or `#`
    public SharedTopicFilter {
        requireNonNull(shareName, "shareName must not be null");
        requireNonNull(filter, "filter must not be null");
        // MQTT 5.0 [MQTT-4.8.2-1] and [MQTT-4.8.2-2].
        if (shareName.isEmpty()) {
            throw new IllegalArgumentException("Share name must not be empty");
        }
        if (shareName.indexOf('/') >= 0
            || shareName.indexOf('+') >= 0
            || shareName.indexOf('#') >= 0
        ) {
            throw new IllegalArgumentException("Share name must not contain '/', '+', or '#'");
        }
    }

    /// Interprets `topicFilter` as a shared subscription if it starts with `$share/`.
    ///
    /// Any other filter, including `$share` alone and `$SHARE/...`, is an ordinary filter and
    /// yields an empty result. The prefix is case-sensitive, like every topic filter.
    ///
    /// @param topicFilter the filter as the application or the peer wrote it
    /// @return the share name and the filter after it, or empty for an ordinary filter
    /// @throws IllegalArgumentException if `topicFilter` starts with `$share/` but does not
    ///         continue with a valid share name, `/`, and a non-empty topic filter
    public static Optional<SharedTopicFilter> parse(TopicFilter topicFilter) {
        requireNonNull(topicFilter, "topicFilter must not be null");
        String pattern = topicFilter.pattern();
        if (!pattern.startsWith(PREFIX)) {
            return Optional.empty();
        }

        // The share name runs to the next '/', and the rest is the topic filter [MQTT-4.8.2-2].
        int separator = pattern.indexOf('/', PREFIX.length());
        if (separator < 0 || separator == pattern.length() - 1) {
            throw new IllegalArgumentException(
                    "Shared subscription needs a topic filter after the share name"
            );
        }
        // TopicFilter has already validated the whole pattern, so the rest keeps valid wildcard
        // positions and fits the byte limit; the constructor only checks those rules again.
        return Optional.of(new SharedTopicFilter(
                pattern.substring(PREFIX.length(), separator),
                new TopicFilter(pattern.substring(separator + 1))
        ));
    }
}
