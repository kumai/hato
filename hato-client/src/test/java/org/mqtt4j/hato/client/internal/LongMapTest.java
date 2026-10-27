package org.mqtt4j.hato.client.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class LongMapTest {

    // With this multiplier, every small non-negative key starts its probe at the first slot and
    // every small negative one at the last, so the keys collide into runs, the latter wrapping.
    private static final long COLLIDING = 1;

    @Test
    void a_map_puts_finds_replaces_and_removes_by_key() {
        // Arrange
        var sut = new LongMap<String>();

        // Act
        String first = sut.put(0, "zero");
        sut.put(-1, "minus one");
        sut.put(Long.MAX_VALUE, "max");
        sut.put(Long.MIN_VALUE, "min");
        String replaced = sut.put(0, "nothing");
        String removed = sut.remove(-1);

        // Assert
        assertThat(first).isNull();
        assertThat(replaced).isEqualTo("zero");
        assertThat(removed).isEqualTo("minus one");
        assertThat(sut.get(0)).isEqualTo("nothing");
        assertThat(sut.get(Long.MAX_VALUE)).isEqualTo("max");
        assertThat(sut.get(Long.MIN_VALUE)).isEqualTo("min");
        assertThat(sut.containsKey(-1)).isFalse();
        assertThat(sut.get(-1)).isNull();
        assertThat(sut.remove(-1)).isNull();
        assertThat(sut.size()).isEqualTo(3);
    }

    @Test
    void removing_keys_from_colliding_runs_keeps_the_others_reachable() {
        // Arrange
        var sut = new LongMap<Long>(COLLIDING);
        for (long key = 1; key <= 4; key++) {
            sut.put(key, key);
            sut.put(-key, -key);
        }

        // Act
        sut.remove(-1);
        sut.remove(2);
        sut.remove(-3);
        sut.remove(1);

        // Assert
        assertThat(sut.size()).isEqualTo(4);
        for (long key : List.of(3L, 4L, -2L, -4L)) {
            assertThat(sut.get(key)).isEqualTo(key);
        }
        for (long key : List.of(1L, 2L, -1L, -3L)) {
            assertThat(sut.containsKey(key)).isFalse();
        }
    }

    @Test
    void a_value_is_removed_only_while_its_key_maps_to_it() {
        // Arrange
        var sut = new LongMap<String>();
        sut.put(7, "current");

        // Act
        boolean other = sut.remove(7, "earlier");
        boolean current = sut.remove(7, "current");

        // Assert
        assertThat(other).isFalse();
        assertThat(current).isTrue();
        assertThat(sut.isEmpty()).isTrue();
    }

    @Test
    void values_are_a_copy_that_later_changes_leave_as_it_is() {
        // Arrange
        var sut = new LongMap<String>();
        sut.put(1, "one");
        sut.put(2, "two");

        // Act
        List<String> values = sut.values();
        sut.remove(1);
        sut.clear();

        // Assert
        assertThat(values).containsExactlyInAnyOrder("one", "two");
        assertThat(sut.values()).isEmpty();
        assertThat(sut.get(2)).isNull();
    }

    @ParameterizedTest
    @ValueSource(longs = {COLLIDING, 0x9E3779B97F4A7C15L})
    void random_operations_leave_what_a_hash_map_would(long multiplier) {
        // Arrange
        var sut = new LongMap<Integer>(multiplier);
        var model = new HashMap<Long, Integer>();
        // A fixed seed repeats the same operations on every run.
        var random = new Random(20261027);

        // Act and assert, one operation at a time
        for (int step = 0; step < 20_000; step++) {
            // Few keys, some negative, so that the map grows, collides, and empties again.
            long key = random.nextInt(81) - 40;
            int value = random.nextInt(1_000);
            switch (random.nextInt(10)) {
                case 0, 1, 2, 3 -> assertThat(sut.put(key, value))
                        .isEqualTo(model.put(key, value));
                case 4, 5, 6 -> assertThat(sut.remove(key)).isEqualTo(model.remove(key));
                case 7 -> {
                    Integer current = model.get(key);
                    int expected = current != null && random.nextBoolean() ? current : value;
                    assertThat(sut.remove(key, expected)).isEqualTo(model.remove(key, expected));
                }
                case 8 -> assertThat(sut.get(key)).isEqualTo(model.get(key));
                default -> {
                    if (random.nextInt(200) == 0) {
                        sut.clear();
                        model.clear();
                    }
                }
            }
            assertThat(sut.size()).isEqualTo(model.size());
        }

        // Assert
        for (Map.Entry<Long, Integer> entry : model.entrySet()) {
            assertThat(sut.get(entry.getKey())).isEqualTo(entry.getValue());
        }
        assertThat(sut.values()).containsExactlyInAnyOrderElementsOf(model.values());
    }
}
