/// Experimental broker API for tests, outside the API compatibility promise until productization.
/// Supports MQTT 3.1.1, and MQTT 5.0 as far as client tests need it, with clean and persistent
/// in-memory sessions, QoS 0/1/2, and retained messages; no disk persistence or Will.
@NullMarked
package org.mqtt4j.hato.broker;

import org.jspecify.annotations.NullMarked;
