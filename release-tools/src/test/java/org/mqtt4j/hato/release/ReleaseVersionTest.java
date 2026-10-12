package org.mqtt4j.hato.release;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReleaseVersionTest {

    @Test
    void a_tag_push_publishes_the_committed_version_that_the_tag_names() {
        var ref = new ReleaseVersion.Ref("push", "tag", "v0.3.0", "");

        assertThat(ReleaseVersion.of(ref, "0.3.0")).isEqualTo("0.3.0");
        assertThatThrownBy(() -> ReleaseVersion.of(ref, "0.3.0-SNAPSHOT"))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("gradle.properties");
    }

    @Test
    void a_manual_trial_publishes_the_pre_release_version_that_its_tag_names() {
        var trial = new ReleaseVersion.Ref("workflow_dispatch", "tag", "v0.3.0-rc.1", "0.3.0-rc.1");
        var other = new ReleaseVersion.Ref("workflow_dispatch", "tag", "v0.3.0-rc.1", "0.3.0-rc.2");

        assertThat(ReleaseVersion.of(trial, "0.3.0-SNAPSHOT")).isEqualTo("0.3.0-rc.1");
        assertThatThrownBy(() -> ReleaseVersion.of(other, "0.3.0-SNAPSHOT"))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("selected tag");
    }

    @Test
    void a_manual_trial_cannot_take_a_final_version_number() {
        var trial = new ReleaseVersion.Ref("workflow_dispatch", "tag", "v0.3.0", "0.3.0");

        assertThatThrownBy(() -> ReleaseVersion.of(trial, "0.3.0-SNAPSHOT"))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("pre-release");
    }

    @ParameterizedTest(name = "{0} {1}")
    @CsvSource({
            "branch, v0.3.0",
            "tag, 0.3.0",
            "tag, v0.3.0-SNAPSHOT",
            "tag, v0.3.0-snapshot",
            "tag, v0.3.0/../../other"
    })
    void only_a_v_prefixed_tag_of_a_release_version_can_publish(String type, String name) {
        var ref = new ReleaseVersion.Ref("push", type, name, "");

        assertThatThrownBy(() -> ReleaseVersion.of(ref, name.substring(1)))
                .isInstanceOf(ReleaseException.class);
    }

    @Test
    void runs_started_by_other_events_cannot_publish() {
        var ref = new ReleaseVersion.Ref("schedule", "tag", "v0.3.0", "");

        assertThatThrownBy(() -> ReleaseVersion.of(ref, "0.3.0"))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("schedule");
    }
}
