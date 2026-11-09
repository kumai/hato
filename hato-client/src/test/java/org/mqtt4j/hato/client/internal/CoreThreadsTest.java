package org.mqtt4j.hato.client.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CoreThreadsTest {

    @Test
    void clients_run_their_core_on_platform_threads_while_a_place_is_free() {
        // Arrange
        var sut = new CoreThreads.Places(1);

        // Act
        CoreThreads first = sut.take();
        CoreThreads second = sut.take();
        first.giveBack();
        CoreThreads third = sut.take();

        // Assert: the second finds no place and runs on virtual threads; the third takes the
        // place that the first gave back.
        assertThat(first.platform()).isTrue();
        assertThat(second.platform()).isFalse();
        assertThat(third.platform()).isTrue();
    }

    @Test
    void a_place_goes_back_once_however_often_it_is_given_back() {
        // Arrange
        var sut = new CoreThreads.Places(2);
        CoreThreads core = sut.take();

        // Act
        core.giveBack();
        core.giveBack();

        // Assert: the two places are free again, and no third one has appeared.
        assertThat(sut.take().platform()).isTrue();
        assertThat(sut.take().platform()).isTrue();
        assertThat(sut.take().platform()).isFalse();
    }

    @Test
    void the_platform_threads_of_a_core_never_keep_the_jvm_running() {
        // Arrange
        CoreThreads platform = new CoreThreads.Places(1).take();
        CoreThreads virtual = CoreThreads.virtual();

        // Act
        Thread onPlatform = platform.builder().unstarted(() -> { });
        Thread onVirtual = virtual.builder().unstarted(() -> { });

        // Assert
        assertThat(onPlatform.isVirtual()).isFalse();
        assertThat(onPlatform.isDaemon()).isTrue();
        assertThat(onVirtual.isVirtual()).isTrue();
    }
}
