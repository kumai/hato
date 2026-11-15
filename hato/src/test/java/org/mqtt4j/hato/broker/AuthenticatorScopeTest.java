package org.mqtt4j.hato.broker;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;

class AuthenticatorScopeTest {

    @ParameterizedTest(name = "{0} holds {1}: {2}")
    @CsvSource({
            "a/b/#,   a/b,      true",
            "a/b/#,   a/b/c/d,  true",
            "a/b/#,   a/+/c,    false",
            "a/+,     a/b,      true",
            "a/+,     a/+,      true",
            "a/+,     a/#,      false",
            "a/+,     a,        false",
            "a/b,     a/+,      false",
            "a/b,     a/b/c,    false",
            "a/b/c,   a/b,      false",
            "#,       a/#,      true",
            "#,       $SYS/x,   false",
            "+/x,     $SYS/x,   false",
            "$SYS/#,  $SYS/b/x, true",
            "a//b,    a//b,     true",
            "a//b,    a/x/b,    false"
    })
    void a_subscription_is_permitted_only_within_a_filter_of_the_scope(
            String scopeFilter,
            String requested,
            boolean permitted
    ) {
        // Arrange
        var sut = Authenticator.Scope.of(List.of(), List.of(new TopicFilter(scopeFilter)));
        // Act & Assert
        assertThat(sut.permitsSubscribe(new TopicFilter(requested))).isEqualTo(permitted);
    }

    @ParameterizedTest(name = "{0} holds {1}: {2}")
    @CsvSource({
            "a/b/#, a/b,    true",
            "a/+,   a/b,    true",
            "a/+,   a/b/c,  false",
            "#,     a/b,    true",
            "#,     $SYS/x, false"
    })
    void a_publication_is_permitted_to_the_topics_that_a_filter_of_the_scope_matches(
            String scopeFilter,
            String topic,
            boolean permitted
    ) {
        // Arrange
        var sut = Authenticator.Scope.of(List.of(new TopicFilter(scopeFilter)), List.of());
        // Act & Assert
        assertThat(sut.permitsPublish(new Topic(topic))).isEqualTo(permitted);
    }

    @Test
    void the_scope_without_limits_permits_even_the_topics_that_start_with_a_dollar() {
        Authenticator.Scope sut = Authenticator.Scope.all();

        assertThat(sut.permitsPublish(new Topic("$SYS/x"))).isTrue();
        assertThat(sut.permitsSubscribe(new TopicFilter("$SYS/#"))).isTrue();
        assertThat(sut.permitsSubscribe(new TopicFilter("#"))).isTrue();
    }

    @Test
    void an_empty_scope_permits_nothing() {
        var sut = Authenticator.Scope.of(List.of(), List.of());

        assertThat(sut.permitsPublish(new Topic("a"))).isFalse();
        assertThat(sut.permitsSubscribe(new TopicFilter("a"))).isFalse();
    }
}
