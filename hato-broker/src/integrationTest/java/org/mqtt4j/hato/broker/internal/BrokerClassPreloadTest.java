package org.mqtt4j.hato.broker.internal;

import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.MqttClient;
import org.mqtt4j.hato.MqttMessage;
import org.mqtt4j.hato.Qos;
import org.mqtt4j.hato.Subscribe;
import org.mqtt4j.hato.Topic;
import org.mqtt4j.hato.TopicFilter;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/// The broker loads its classes before it runs on virtual threads.
class BrokerClassPreloadTest {

    @Test
    void start_on_a_virtual_thread_loads_the_broker_classes_on_a_platform_thread()
            throws Exception {
        // Arrange: the broker's classes in a class loader of their own, which has loaded none yet.
        try (var loader = new RecordingLoader()) {
            Method scenario = loader.loadClass(Scenario.class.getName())
                    .getMethod("run", Runnable.class);
            var run = new FutureTask<>(() -> scenario.invoke(
                    null,
                    (Runnable) loader::startReturned
            ));
            // Act: start and use a broker on a virtual thread, as an application can.
            Thread.ofVirtual().start(run);
            run.get(30, SECONDS);
            // Assert: once start() returned, only classes of the test itself were loaded, such as
            // the nest host of the scenario's lambdas; the broker's classes, such as its server,
            // were loaded before that on a platform thread.
            assertThat(loader.definitions())
                    .filteredOn(Definition::afterStart)
                    .extracting(Definition::name)
                    .allMatch(name -> name.startsWith(BrokerClassPreloadTest.class.getName()));
            String serverClass = BrokerServer.class.getName();
            assertThat(loader.definitions())
                    .filteredOn(definition -> definition.name().equals(serverClass))
                    .singleElement()
                    .extracting(Definition::virtualThread)
                    .isEqualTo(false);
        }
    }

    /// Starts a broker and uses it in the common ways through a client. It runs in the class
    /// loader under test, so the broker's types are that loader's and the client's are the test's.
    public static final class Scenario {

        public static void run(Runnable startReturned) throws Exception {
            try (MqttBroker broker = MqttBroker.start()) {
                startReturned.run();
                URI server = URI.create("mqtt://localhost:" + broker.port());
                try (MqttClient client = MqttClient.builder().server(server).build()) {
                    client.connect().join();
                    var received = new LinkedBlockingQueue<MqttMessage>();
                    client.subscribe(
                            Subscribe.of(new TopicFilter("preload/+"), Qos.EXACTLY_ONCE),
                            delivery -> received.add(delivery.message())
                    ).established().join();
                    client.publish(message("preload/0", Qos.AT_MOST_ONCE, false)).join();
                    client.publish(message("preload/1", Qos.AT_LEAST_ONCE, false)).join();
                    client.publish(message("preload/2", Qos.EXACTLY_ONCE, true)).join();
                    for (int i = 0; i < 3; i++) {
                        received.take();
                    }
                    // A new subscription receives the retained message.
                    client.subscribe(
                            Subscribe.of(new TopicFilter("preload/2"), Qos.AT_LEAST_ONCE),
                            delivery -> received.add(delivery.message())
                    ).established().join();
                    received.take();
                    client.disconnect().join();
                }
            }
        }

        private static MqttMessage message(String topic, Qos qos, boolean retain) {
            return MqttMessage.of(new Topic(topic), new byte[] {1}, qos, retain);
        }
    }

    record Definition(String name, boolean virtualThread, boolean afterStart) {}

    /// Defines the classes of the broker, and the scenario that uses them, itself instead of
    /// asking its parent. Records each class that it defines, whether a virtual thread asked for
    /// it, and whether `MqttBroker.start()` had returned.
    private static final class RecordingLoader extends URLClassLoader {

        private final List<Definition> definitions = new CopyOnWriteArrayList<>();

        private volatile boolean startReturned;

        RecordingLoader() {
            super(
                    new URL[] {
                            codeSource(MqttBroker.class),
                            codeSource(BrokerClassPreloadTest.class)
                    },
                    BrokerClassPreloadTest.class.getClassLoader()
            );
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            // The broker's public and internal packages, where the scenario is too.
            if (!name.startsWith("org.mqtt4j.hato.broker.")) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> type = findLoadedClass(name);
                if (type == null) {
                    type = findClass(name);
                    definitions.add(new Definition(
                            name,
                            Thread.currentThread().isVirtual(),
                            startReturned
                    ));
                }
                return type;
            }
        }

        void startReturned() {
            startReturned = true;
        }

        List<Definition> definitions() {
            return definitions;
        }

        private static URL codeSource(Class<?> type) {
            return type.getProtectionDomain().getCodeSource().getLocation();
        }
    }
}
