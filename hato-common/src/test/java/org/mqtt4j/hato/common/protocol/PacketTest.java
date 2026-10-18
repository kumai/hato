package org.mqtt4j.hato.common.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.mqtt4j.hato.common.value.MqttVersion;
import org.mqtt4j.hato.common.value.Qos;
import org.mqtt4j.hato.common.value.ReasonCode;
import org.mqtt4j.hato.common.value.Topic;
import org.mqtt4j.hato.common.value.TopicFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PacketTest {

    private static final Topic TOPIC = new Topic("a/b");
    private static final TopicFilter FILTER = new TopicFilter("a/#");
    private static final Packet.Subscription SUBSCRIPTION = new Packet.Subscription(
            FILTER, Qos.AT_LEAST_ONCE, false, false, 0
    );

    @ParameterizedTest
    @EnumSource(MqttVersion.class)
    void connect_can_omit_will_and_credentials(MqttVersion version) {
        var sut = new Packet.Connect(
                "client", version, true, 60, null, null, null, Properties.EMPTY
        );

        assertThat(sut.version()).isSameAs(version);
        assertThat(sut.will()).isNull();
        assertThat(sut.username()).isNull();
        assertThat(sut.password()).isNull();
    }

    @Test
    void connect_does_not_include_credentials_in_its_string_representation() {
        var password = new byte[] {1, 2, 3};
        var sut = new Packet.Connect(
                "client",
                MqttVersion.V3_1_1,
                true,
                60,
                null,
                "private-user",
                password,
                Properties.EMPTY
        );

        assertThat(sut.toString()).contains("clientId=client", "keepAliveSeconds=60")
                .doesNotContain("private-user", "username=", "password=", "[B@");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 65535})
    void keep_alive_accepts_the_unsigned_two_byte_boundaries(int seconds) {
        var sut = new Packet.Connect(
                "client", MqttVersion.V3_1_1, true, seconds, null, null, null, Properties.EMPTY
        );

        assertThat(sut.keepAliveSeconds()).isEqualTo(seconds);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 65536, Integer.MIN_VALUE, Integer.MAX_VALUE})
    void keep_alive_outside_the_wire_range_is_rejected(int seconds) {
        assertThatThrownBy(() -> new Packet.Connect(
                "client", MqttVersion.V3_1_1, true, seconds, null, null, null, Properties.EMPTY
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void mqtt_5_can_represent_an_empty_client_id_and_password_without_username() {
        var sut = new Packet.Connect(
                "", MqttVersion.V5, false, 0, null, null, new byte[0], Properties.EMPTY
        );

        assertThat(sut.clientId()).isEmpty();
        assertThat(sut.password()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 65535})
    void will_and_password_accept_the_binary_data_length_boundaries(int length) {
        var bytes = new byte[length];
        var will = new Packet.Will(TOPIC, bytes, Qos.AT_MOST_ONCE, false, Properties.EMPTY);
        var sut = new Packet.Connect(
                "client", MqttVersion.V5, true, 0, will, "user", bytes, Properties.EMPTY
        );

        assertThat(will.payload()).isSameAs(bytes);
        assertThat(sut.password()).isSameAs(bytes);
    }

    @Test
    void will_and_password_cannot_exceed_the_binary_data_length_limit() {
        // MQTT 5.0 section 1.5.6: these fields have a two-byte length prefix.
        var bytes = new byte[65536];

        assertThatThrownBy(() -> new Packet.Will(
                TOPIC, bytes, Qos.AT_MOST_ONCE, false, Properties.EMPTY
        )).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Packet.Connect(
                "client", MqttVersion.V5, true, 0, null, "user", bytes, Properties.EMPTY
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(value = ReasonCode.class, names = "SUCCESS", mode = EnumSource.Mode.EXCLUDE)
    void a_refused_connection_cannot_report_an_existing_session(ReasonCode reasonCode) {
        // MQTT 3.1.1 [MQTT-3.2.2-4] and MQTT 5.0 [MQTT-3.2.2-6].
        assertThatThrownBy(() -> new Packet.ConnAck(true, reasonCode, Properties.EMPTY))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void an_accepted_connection_can_resume_a_session() {
        var sut = new Packet.ConnAck(true, ReasonCode.SUCCESS, Properties.EMPTY);

        assertThat(sut.sessionPresent()).isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("packetsWithIdentifiers")
    void packet_identifiers_must_be_nonzero_unsigned_two_byte_values(
            String name,
            IntFunction<Packet> factory
    ) {
        // MQTT 3.1.1 [MQTT-2.3.1-1].
        for (int id : new int[] {Integer.MIN_VALUE, -1, 0, 65536, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> factory.apply(id)).as("%s id=%s", name, id)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (int id : new int[] {1, 65535}) {
            assertThatCode(() -> factory.apply(id)).as("%s id=%s", name, id)
                    .doesNotThrowAnyException();
        }
    }

    static Stream<Arguments> packetsWithIdentifiers() {
        return Stream.of(
                identified("Publish QoS 1", id -> publish(
                        new byte[0], Qos.AT_LEAST_ONCE, false, id
                )),
                identified("Publish QoS 2", id -> publish(new byte[0], Qos.EXACTLY_ONCE, true, id)),
                identified("AliasPublish QoS 1", id -> aliasPublish(
                        new byte[0],
                        Qos.AT_LEAST_ONCE,
                        false,
                        id
                )),
                identified("AliasPublish QoS 2", id -> aliasPublish(
                        new byte[0],
                        Qos.EXACTLY_ONCE,
                        true,
                        id
                )),
                identified("PubAck", id -> new Packet.PubAck(
                        id, ReasonCode.SUCCESS, Properties.EMPTY
                )),
                identified("PubRec", id -> new Packet.PubRec(
                        id, ReasonCode.SUCCESS, Properties.EMPTY
                )),
                identified("PubRel", id -> new Packet.PubRel(
                        id, ReasonCode.SUCCESS, Properties.EMPTY
                )),
                identified("PubComp", id -> new Packet.PubComp(
                        id, ReasonCode.SUCCESS, Properties.EMPTY
                )),
                identified("Subscribe", id -> new Packet.Subscribe(
                        id, List.of(SUBSCRIPTION), Properties.EMPTY
                )),
                identified("SubAck", id -> new Packet.SubAck(
                        id, List.of(ReasonCode.SUCCESS), Properties.EMPTY
                )),
                identified("Unsubscribe", id -> new Packet.Unsubscribe(
                        id, List.of(FILTER), Properties.EMPTY
                )),
                identified("UnsubAck", id -> new Packet.UnsubAck(id, List.of(), Properties.EMPTY))
        );
    }

    private static Arguments identified(String name, IntFunction<Packet> factory) {
        return Arguments.of(name, factory);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 1, 65535, 65536})
    void qos_zero_does_not_have_a_packet_identifier(int id) {
        // MQTT 3.1.1 [MQTT-2.3.1-5]. Zero represents the absent field in the model.
        assertThatThrownBy(() -> publish(new byte[0], Qos.AT_MOST_ONCE, false, id))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void qos_zero_cannot_set_dup() {
        // [MQTT-3.3.1-2] in both MQTT 3.1.1 and 5.0.
        assertThatThrownBy(() -> publish(new byte[0], Qos.AT_MOST_ONCE, true, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publish_payload_has_no_two_byte_length_limit() {
        var payload = new byte[65536];
        var sut = publish(payload, Qos.AT_MOST_ONCE, false, 0);

        assertThat(sut.payload()).isSameAs(payload);
    }

    @Test
    void publish_equality_uses_payload_contents_and_other_packet_fields() {
        var sut = publish(new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false, 7);
        var same = publish(new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false, 7);

        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(sut).isNotEqualTo(publish(new byte[] {1, 3}, Qos.AT_LEAST_ONCE, false, 7));
        assertThat(sut).isNotEqualTo(publish(new byte[] {1, 2}, Qos.EXACTLY_ONCE, false, 7));
        assertThat(sut).isNotEqualTo(publish(new byte[] {1, 2}, Qos.AT_LEAST_ONCE, true, 7));
        assertThat(sut).isNotEqualTo(publish(new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false, 8));
        assertThat(sut).isNotEqualTo(new Packet.Publish(
                new Topic("other"), new byte[] {1, 2}, Qos.AT_LEAST_ONCE,
                false, false, 7, Properties.EMPTY
        ));
        assertThat(sut).isNotEqualTo(new Packet.Publish(
                TOPIC, new byte[] {1, 2}, Qos.AT_LEAST_ONCE, true, false, 7, Properties.EMPTY
        ));
        assertThat(sut).isNotEqualTo(new Packet.Publish(
                TOPIC, new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false, false, 7,
                new Properties(List.of(new Property.UserProperty("key", "value")))
        ));
        assertThat(sut).isNotEqualTo(null).isNotEqualTo(new Packet.PingReq());
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 1, 65535, 65536})
    void alias_only_qos_zero_does_not_have_a_packet_identifier(int id) {
        assertThatThrownBy(() -> aliasPublish(new byte[0], Qos.AT_MOST_ONCE, false, id))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void alias_only_qos_zero_cannot_set_dup() {
        assertThatThrownBy(() -> aliasPublish(new byte[0], Qos.AT_MOST_ONCE, true, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void alias_only_payload_ownership_transfers_without_a_two_byte_length_limit() {
        // Arrange
        var payload = new byte[65536];
        // Act
        Packet.AliasPublish sut = aliasPublish(payload, Qos.AT_MOST_ONCE, false, 0);
        // Assert
        assertThat(sut.payload()).isSameAs(payload);
    }

    @Test
    void alias_only_equality_uses_payload_contents_and_other_packet_fields() {
        // Arrange
        Packet.AliasPublish sut = aliasPublish(new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false, 7);
        Packet.AliasPublish same = aliasPublish(new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false, 7);

        // Act & Assert
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(sut).isNotEqualTo(aliasPublish(new byte[] {1, 3}, Qos.AT_LEAST_ONCE, false, 7));
        assertThat(sut).isNotEqualTo(aliasPublish(new byte[] {1, 2}, Qos.EXACTLY_ONCE, false, 7));
        assertThat(sut).isNotEqualTo(aliasPublish(new byte[] {1, 2}, Qos.AT_LEAST_ONCE, true, 7));
        assertThat(sut).isNotEqualTo(aliasPublish(new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false, 8));
        assertThat(sut).isNotEqualTo(
                new Packet.AliasPublish(
                        new byte[] {1, 2},
                        Qos.AT_LEAST_ONCE,
                        true,
                        false,
                        7,
                        Properties.EMPTY
                )
        );
        assertThat(sut).isNotEqualTo(
                new Packet.AliasPublish(
                        new byte[] {1, 2},
                        Qos.AT_LEAST_ONCE,
                        false,
                        false,
                        7,
                        new Properties(List.of(new Property.UserProperty("key", "value")))
                )
        );
        assertThat(sut).isNotEqualTo(null).isNotEqualTo(
                publish(new byte[] {1, 2}, Qos.AT_LEAST_ONCE, false, 7)
        );
    }

    @Test
    void mqtt_3_1_1_cannot_encode_an_alias_only_publication() {
        // Arrange
        PacketCodec sut = PacketCodec.of(MqttVersion.V3_1_1);
        Packet.AliasPublish publication = aliasPublish(new byte[0], Qos.AT_MOST_ONCE, false, 0);

        // Act & Assert
        assertThatThrownBy(() -> sut.encode(publication))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic name");
    }

    private static Packet.AliasPublish aliasPublish(byte[] payload, Qos qos, boolean dup, int id) {
        return new Packet.AliasPublish(payload, qos, false, dup, id, Properties.EMPTY);
    }

    @Test
    void connect_equality_includes_password_and_will_payload_contents() {
        var firstWill = new Packet.Will(
                TOPIC, new byte[] {3, 4}, Qos.AT_LEAST_ONCE, true, Properties.EMPTY
        );
        var secondWill = new Packet.Will(
                TOPIC, new byte[] {3, 4}, Qos.AT_LEAST_ONCE, true, Properties.EMPTY
        );
        var sut = new Packet.Connect(
                "client", MqttVersion.V5, true, 60, firstWill, "user",
                new byte[] {1}, Properties.EMPTY
        );
        var same = new Packet.Connect(
                "client", MqttVersion.V5, true, 60, secondWill, "user",
                new byte[] {1}, Properties.EMPTY
        );

        assertThat(firstWill).isEqualTo(secondWill).hasSameHashCodeAs(secondWill);
        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(firstWill).isNotEqualTo(new Packet.Will(
                TOPIC, new byte[] {3, 5}, Qos.AT_LEAST_ONCE, true, Properties.EMPTY
        ));
        assertThat(sut).isNotEqualTo(new Packet.Connect(
                "client", MqttVersion.V5, true, 60, secondWill, "user",
                new byte[] {2}, Properties.EMPTY
        ));
    }

    @Test
    void absent_credentials_compare_equal_but_differ_from_empty_credentials() {
        var sut = new Packet.Connect(
                "client", MqttVersion.V5, true, 0, null, null, null, Properties.EMPTY
        );
        var same = new Packet.Connect(
                "client", MqttVersion.V5, true, 0, null, null, null, Properties.EMPTY
        );

        assertThat(sut).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(sut).isNotEqualTo(new Packet.Connect(
                "client", MqttVersion.V5, true, 0, null, null, new byte[0], Properties.EMPTY
        ));
    }

    @Test
    void request_and_suback_lists_cannot_be_empty() {
        assertThatThrownBy(() -> new Packet.Subscribe(1, List.of(), Properties.EMPTY))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Packet.Unsubscribe(1, List.of(), Properties.EMPTY))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Packet.SubAck(1, List.of(), Properties.EMPTY))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unsuback_can_represent_the_absent_mqtt_3_reason_codes() {
        var sut = new Packet.UnsubAck(1, List.of(), Properties.EMPTY);

        assertThat(sut.reasonCodes()).isEmpty();
    }

    @Test
    void packet_lists_preserve_order_and_duplicates_without_sharing_mutable_storage() {
        var subscriptions = new ArrayList<>(List.of(SUBSCRIPTION, SUBSCRIPTION));
        var filters = new ArrayList<>(List.of(FILTER, FILTER));
        var reasons = new ArrayList<>(List.of(ReasonCode.SUCCESS, ReasonCode.UNSPECIFIED_ERROR));
        var subscribe = new Packet.Subscribe(1, subscriptions, Properties.EMPTY);
        var unsubscribe = new Packet.Unsubscribe(2, filters, Properties.EMPTY);
        var subAck = new Packet.SubAck(1, reasons, Properties.EMPTY);
        var unsubAck = new Packet.UnsubAck(2, reasons, Properties.EMPTY);

        subscriptions.clear();
        filters.clear();
        reasons.clear();

        assertThat(subscribe.subscriptions()).containsExactly(SUBSCRIPTION, SUBSCRIPTION);
        assertThat(unsubscribe.filters()).containsExactly(FILTER, FILTER);
        assertThat(subAck.reasonCodes())
                .containsExactly(ReasonCode.SUCCESS, ReasonCode.UNSPECIFIED_ERROR);
        assertThat(unsubAck.reasonCodes())
                .containsExactly(ReasonCode.SUCCESS, ReasonCode.UNSPECIFIED_ERROR);
        assertThatThrownBy(() -> subscribe.subscriptions().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> unsubscribe.filters().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> subAck.reasonCodes().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> unsubAck.reasonCodes().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void subscription_options_are_preserved(int retainHandling) {
        var sut = new Packet.Subscription(FILTER, Qos.EXACTLY_ONCE, true, true, retainHandling);

        assertThat(sut.noLocal()).isTrue();
        assertThat(sut.retainAsPublished()).isTrue();
        assertThat(sut.retainHandling()).isEqualTo(retainHandling);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 3, 255})
    void undefined_retain_handling_is_rejected(int retainHandling) {
        // MQTT 5.0 section 3.8.3.1.
        assertThatThrownBy(() -> new Packet.Subscription(
                FILTER, Qos.AT_MOST_ONCE, false, false, retainHandling
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"$share/+/a", "$share/g", "$share/group/a/#"})
    void subscription_options_do_not_interpret_the_filter_prefix(String pattern) {
        var filter = new TopicFilter(pattern);
        var sut = new Packet.Subscription(filter, Qos.AT_MOST_ONCE, true, false, 0);

        assertThat(sut.filter()).isEqualTo(filter);
        assertThat(sut.noLocal()).isTrue();
    }

    @Test
    void disconnect_auth_and_pings_represent_their_packet_contents() {
        var disconnect = new Packet.Disconnect(ReasonCode.SUCCESS, Properties.EMPTY);
        var auth = new Packet.Auth(ReasonCode.CONTINUE_AUTHENTICATION, Properties.EMPTY);

        assertThat(disconnect.reasonCode()).isSameAs(ReasonCode.SUCCESS);
        assertThat(auth.reasonCode()).isSameAs(ReasonCode.CONTINUE_AUTHENTICATION);
        assertThat(new Packet.PingReq()).isEqualTo(new Packet.PingReq());
        assertThat(new Packet.PingResp()).isEqualTo(new Packet.PingResp());
    }

    @Test
    @SuppressWarnings("NullAway") // Deliberately violate the non-null contract at runtime.
    void required_fields_and_list_elements_cannot_be_null() {
        assertThatThrownBy(() -> new Packet.Connect(
                null, MqttVersion.V5, true, 0, null, null, null, Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.Connect(
                "client", null, true, 0, null, null, null, Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.Will(
                TOPIC, null, Qos.AT_MOST_ONCE, false, Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.Publish(
                null, new byte[0], Qos.AT_MOST_ONCE, false, false, 0, Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.Publish(
                TOPIC, new byte[0], null, false, false, 0, Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.AliasPublish(
                null,
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.AliasPublish(
                new byte[0],
                null,
                false,
                false,
                0,
                Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.AliasPublish(
                new byte[0],
                Qos.AT_MOST_ONCE,
                false,
                false,
                0,
                null
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.PubAck(1, null, Properties.EMPTY))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.Disconnect(ReasonCode.SUCCESS, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.Subscribe(1, null, Properties.EMPTY))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.Subscribe(
                1, Arrays.asList(SUBSCRIPTION, null), Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.SubAck(
                1, Arrays.asList(ReasonCode.SUCCESS, null), Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.Unsubscribe(
                1, Arrays.asList(FILTER, null), Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Packet.UnsubAck(
                1, Arrays.asList(ReasonCode.SUCCESS, null), Properties.EMPTY
        )).isInstanceOf(NullPointerException.class);
    }

    private static Packet.Publish publish(byte[] payload, Qos qos, boolean dup, int id) {
        return new Packet.Publish(TOPIC, payload, qos, false, dup, id, Properties.EMPTY);
    }
}
