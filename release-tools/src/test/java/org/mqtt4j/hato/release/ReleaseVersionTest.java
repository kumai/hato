package org.mqtt4j.hato.release;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReleaseVersionTest {

    @ParameterizedTest(name = "{1} from {0}")
    @CsvSource({
            "0.3.0-SNAPSHOT, v0.3.0",
            "0.8.0-SNAPSHOT, v0.7.1",
            "0.8.0-SNAPSHOT, v0.8.0-rc.1",
            "0.10.0-SNAPSHOT, v0.9.0"
    })
    void a_published_release_publishes_the_tag_up_to_the_committed_version(
            String committed,
            String tag
    ) {
        var ref = new ReleaseVersion.Ref("release", "tag", tag);

        assertThat(ReleaseVersion.of(ref, committed)).isEqualTo(tag.substring(1));
    }

    @ParameterizedTest(name = "{1} from {0}")
    @CsvSource({
            "0.8.0-SNAPSHOT, v0.8.1",
            "0.8.0-SNAPSHOT, v0.8.1-rc.1",
            "0.8.0-SNAPSHOT, v1.0.0",
            "0.9.0-SNAPSHOT, v0.10.0"
    })
    void a_tag_above_the_committed_version_cannot_publish(String committed, String tag) {
        var ref = new ReleaseVersion.Ref("release", "tag", tag);

        assertThatThrownBy(() -> ReleaseVersion.of(ref, committed))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("raise the version there");
    }

    @Test
    void a_committed_version_without_three_numbers_stops_the_release() {
        var ref = new ReleaseVersion.Ref("release", "tag", "v0.3.0");

        assertThatThrownBy(() -> ReleaseVersion.of(ref, "unspecified"))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining("gradle.properties");
    }

    @ParameterizedTest(name = "{0} from {1}")
    @CsvSource({
            "0.8.0, 0.8.0-SNAPSHOT, 0.9.0-SNAPSHOT",
            "0.9.0, 0.9.0-SNAPSHOT, 0.10.0-SNAPSHOT",
            "1.0.0, 1.0.0-SNAPSHOT, 1.1.0-SNAPSHOT"
    })
    void a_release_of_the_committed_version_moves_it_to_the_next_minor_snapshot(
            String released,
            String committed,
            String next
    ) {
        assertThat(ReleaseVersion.next(released, committed)).contains(next);
    }

    @ParameterizedTest(name = "{0} from {1}")
    @CsvSource({
            "0.7.1, 0.8.0-SNAPSHOT",
            "0.8.0-rc.1, 0.8.0-SNAPSHOT",
            "0.8.0, 0.9.0-SNAPSHOT"
    })
    void a_pre_release_or_a_release_of_an_earlier_version_keeps_the_committed_one(
            String released,
            String committed
    ) {
        assertThat(ReleaseVersion.next(released, committed)).isEmpty();
    }

    @Test
    void a_manual_trial_publishes_the_pre_release_version_that_its_tag_names() {
        var trial = new ReleaseVersion.Ref("workflow_dispatch", "tag", "v0.3.0-rc.1");

        assertThat(ReleaseVersion.of(trial, "0.3.0-SNAPSHOT")).isEqualTo("0.3.0-rc.1");
    }

    @Test
    void a_manual_trial_cannot_take_a_final_version_number() {
        var trial = new ReleaseVersion.Ref("workflow_dispatch", "tag", "v0.3.0");

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
        var ref = new ReleaseVersion.Ref("release", type, name);

        assertThatThrownBy(() -> ReleaseVersion.of(ref, "0.3.0-SNAPSHOT"))
                .isInstanceOf(ReleaseException.class);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"push", "schedule"})
    void runs_started_by_other_events_cannot_publish(String event) {
        var ref = new ReleaseVersion.Ref(event, "tag", "v0.3.0");

        assertThatThrownBy(() -> ReleaseVersion.of(ref, "0.3.0-SNAPSHOT"))
                .isInstanceOf(ReleaseException.class)
                .hasMessageContaining(event);
    }
}
