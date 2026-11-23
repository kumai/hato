package org.mqtt4j.hato.broker;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            "a//b,    a/x/b,    false",
            // A '#' also matches the topic of the levels above it, where they make one.
            "+/#,     #,        true",
            "/+/#,    /#,       true",
            "+/+/#,   /#,       true",
            "a/+/#,   a/#,      false",
            "+/+/#,   #,        false",
            "+,       #,        false"
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

    @ParameterizedTest(name = "{0} holds {1}: {2}")
    @CsvSource({
            "$share/g/a/#,    $share/g/a/b,     true",
            "$share/g/a/#,    $share/h/a/b,     false",
            "$share/+/a/#,    $share/h/a/b,     true",
            "$share/+/a/#,    $share/h/x,       false",
            "$share/+/+,      $share/h/a,       true",
            "$share/+/+,      $share/h/a/b,     false",
            // The filter after the group follows the rules of an ordinary filter, so a leading
            // wildcard there holds no topic that starts with '$'.
            "$share/+/#,      $share/g/$SYS/#,  false",
            "$share/+/$SYS/#, $share/g/$SYS/x,  true",
            // A filter that starts with $share/ but is no shared subscription.
            "$share/+/#,      $share/g,         false",
            // An ordinary filter is not a shared subscription that a shared one could hold.
            "$share/+/#,      a/b,              false"
    })
    void a_shared_subscription_is_permitted_only_within_a_shared_one_of_its_group(
            String shareFilter,
            String requested,
            boolean permitted
    ) {
        // Arrange
        var sut = Authenticator.Scope.of(
                List.of(),
                List.of(),
                List.of(new TopicFilter(shareFilter))
        );
        // Act & Assert
        assertThat(sut.permitsSubscribe(new TopicFilter(requested))).isEqualTo(permitted);
    }

    @Test
    void the_subscribe_filters_of_a_scope_permit_no_shared_subscription() {
        // Arrange: a filter of every topic, and one written as a shared subscription.
        var sut = Authenticator.Scope.of(
                List.of(),
                List.of(new TopicFilter("#"), new TopicFilter("$share/g/a/#"))
        );
        // Act & Assert
        assertThat(sut.permitsSubscribe(new TopicFilter("$share/g/a/b"))).isFalse();
        assertThat(sut.permitsSubscribe(new TopicFilter("a/b"))).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"$share/#", "$share/g", "$share//a", "$share/g/", "jobs/#"})
    void a_shared_subscription_that_a_scope_allows_must_name_a_group_and_a_filter(String filter) {
        assertThatThrownBy(() -> Authenticator.Scope.of(
                List.of(),
                List.of(),
                List.of(new TopicFilter(filter))
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void the_scope_without_limits_permits_even_the_topics_that_start_with_a_dollar() {
        Authenticator.Scope sut = Authenticator.Scope.all();

        assertThat(sut.permitsPublish(new Topic("$SYS/x"))).isTrue();
        assertThat(sut.permitsSubscribe(new TopicFilter("$SYS/#"))).isTrue();
        assertThat(sut.permitsSubscribe(new TopicFilter("#"))).isTrue();
        assertThat(sut.permitsSubscribe(new TopicFilter("$share/g/$SYS/#"))).isTrue();
    }

    @Test
    void an_empty_scope_permits_nothing() {
        var sut = Authenticator.Scope.of(List.of(), List.of());

        assertThat(sut.permitsPublish(new Topic("a"))).isFalse();
        assertThat(sut.permitsSubscribe(new TopicFilter("a"))).isFalse();
    }
}
