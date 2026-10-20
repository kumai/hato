package org.mqtt4j.hato;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublishResultTest {

    @Test
    void successful_publications_have_equal_results() {
        // Act
        var sut = PublishResult.of();
        var same = PublishResult.of(ReasonCode.SUCCESS);

        // Assert
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(same).isEqualTo(sut);
        assertThat(sut.reasonCode()).isEqualTo(ReasonCode.SUCCESS);
        assertThat(sut.toString()).isEqualTo("PublishResult[reasonCode=SUCCESS]");
    }

    @Test
    void a_publication_without_subscribers_has_its_own_result() {
        // Act
        var sut = PublishResult.of(ReasonCode.NO_MATCHING_SUBSCRIBERS);

        // Assert
        assertThat(sut.reasonCode()).isEqualTo(ReasonCode.NO_MATCHING_SUBSCRIBERS);
        assertThat(sut).isEqualTo(PublishResult.of(ReasonCode.NO_MATCHING_SUBSCRIBERS))
                .isNotEqualTo(PublishResult.of());
        assertThat(sut.toString()).contains("NO_MATCHING_SUBSCRIBERS");
    }

    @ParameterizedTest
    @EnumSource(
            value = ReasonCode.class,
            names = {"SUCCESS", "NO_MATCHING_SUBSCRIBERS"},
            mode = EnumSource.Mode.EXCLUDE
    )
    void only_the_reason_codes_of_a_successful_acknowledgement_are_results(ReasonCode reason) {
        // Act & Assert
        assertThatThrownBy(() -> PublishResult.of(reason))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
