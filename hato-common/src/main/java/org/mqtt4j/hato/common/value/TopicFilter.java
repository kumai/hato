package org.mqtt4j.hato.common.value;

import java.util.Objects;

/// A validated MQTT topic filter using the common topic and wildcard rules.
///
/// The complete pattern must be non-empty, contain no NUL or unpaired surrogates, and encode
/// to at most 65535 bytes in UTF-8. A `+` occupies an entire level; a `#` occupies the entire
/// final level. Whitespace, empty levels, and the original spelling are kept.
///
/// This type gives `$share/` no special meaning, as MQTT 3.1.1 does: validation and
/// [#matches] treat the whole pattern as an ordinary filter.
///
/// - [MQTT 3.1.1 Plus Errata 01, section 1.5.3: UTF-8 encoded strings](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180829)
/// - [MQTT 3.1.1 Plus Errata 01, section 4.7: Topic Names and Topic Filters](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180919)
/// - [MQTT 5.0, section 4.8.2: Shared Subscriptions](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
///
/// @param pattern
///     the complete filter to validate and match
public record TopicFilter(String pattern) {

    /// Creates a filter after validating its pattern.
    ///
    /// @param pattern
    ///     the complete filter to validate and match
    /// @throws NullPointerException
    ///     if `pattern` is null
    /// @throws IllegalArgumentException
    ///     if `pattern` is empty, contains NUL or an unpaired surrogate, exceeds 65535 UTF-8
    ///     bytes, or misplaces a wildcard
    public TopicFilter {
        // $share/{ShareName}/{filter} gets no special treatment here: SharedTopicFilter in
        // common.protocol interprets shared subscriptions. This keeps the public value type
        // version-neutral, so the MQTT 3.1.1 broker can keep such filters as written.
        TopicText.validate(pattern, "Topic filter");

        // Keep trailing empty levels: "a/" is valid, but "a/#/" is not.
        String[] levels = pattern.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];
            // [MQTT-4.7.1-3]
            if (level.indexOf('+') >= 0 && !level.equals("+")) {
                throw new IllegalArgumentException("Wildcard + must occupy an entire level");
            }
            // [MQTT-4.7.1-2]
            if (level.indexOf('#') >= 0 && (!level.equals("#") || i != levels.length - 1)) {
                throw new IllegalArgumentException("Wildcard # must occupy the entire final level");
            }
        }
    }

    /// Tests whether this filter selects `topic`.
    ///
    /// A `+` matches exactly one level, including an empty level. A trailing `#` matches
    /// zero or more levels, so `sport/#` also matches `sport`. Matching is case-sensitive
    /// and does not normalize either string.
    ///
    /// The entire pattern participates in matching, including any `$share/` prefix. A leading
    /// wildcard does not match a topic whose name starts with `$`.
    ///
    /// @param topic
    ///     the topic to match
    /// @return
    ///     whether the topic matches this filter
    /// @throws NullPointerException
    ///     if `topic` is null
    public boolean matches(Topic topic) {
        Objects.requireNonNull(topic, "Topic must not be null");

        String name = topic.name();
        // [MQTT-4.7.2-1]. Validation guarantees that a leading wildcard is the whole first level.
        if (name.startsWith("$") && (pattern.startsWith("+") || pattern.startsWith("#"))) {
            return false;
        }

        // Compare one level at a time in place: routing checks every subscription for each
        // received message, so this must not allocate. A start past the end means that no
        // level remains, while a start equal to the length is a final empty level.
        int filterStart = 0;
        int nameStart = 0;
        while (true) {
            int filterEnd = levelEnd(pattern, filterStart);
            int length = filterEnd - filterStart;
            // [MQTT-4.7.1-2]: # also matches when the topic has no remaining levels.
            if (length == 1 && pattern.charAt(filterStart) == '#') {
                return true;
            }
            if (nameStart > name.length()) {
                return false;
            }
            int nameEnd = levelEnd(name, nameStart);
            // [MQTT-4.7.1-3]: + consumes one level, including an empty one.
            boolean wildcard = length == 1 && pattern.charAt(filterStart) == '+';
            if (!wildcard && (length != nameEnd - nameStart
                    || !pattern.regionMatches(filterStart, name, nameStart, length))) {
                return false;
            }
            if (filterEnd == pattern.length()) {
                return nameEnd == name.length();
            }
            filterStart = filterEnd + 1;
            nameStart = nameEnd + 1;
        }
    }

    /// Returns the pattern unchanged.
    @Override
    public String toString() {
        return pattern;
    }

    private static int levelEnd(String text, int start) {
        int end = text.indexOf('/', start);
        return end < 0 ? text.length() : end;
    }
}
