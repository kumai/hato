package org.mqtt4j.mqtj;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PublishResultTest {

    @Test
    void successful_publications_have_equal_results() {
        // Act
        var sut = PublishResult.of();
        var same = PublishResult.of();

        // Assert
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(same).isEqualTo(sut);
        assertThat(sut.toString()).isEqualTo("PublishResult[]");
    }
}
