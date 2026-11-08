package org.mqtt4j.hato.client.internal;

import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.*;

import org.junit.jupiter.api.Test;

import org.mqtt4j.hato.*;
import org.mqtt4j.hato.broker.MqttBroker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.*;

class ClassPreloadTest {

    @Test
    void builder_on_a_virtual_thread_loads_the_client_classes_on_a_platform_thread()
            throws Exception {
        // Arrange: the client's classes in a class loader of their own, which has loaded none yet.
        try (
                MqttBroker broker = MqttBroker.start();
                var loader = new RecordingLoader()
        ) {
            Method scenario = loader.loadClass(Scenario.class.getName())
                    .getMethod("run", int.class, Runnable.class);
            var run = new FutureTask<>(() -> scenario.invoke(
                    null,
                    broker.port(),
                    (Runnable) loader::builderReturned
            ));
            // Act: build and use a client on a virtual thread, as an application can.
            Thread.ofVirtual().start(run);
            run.get(30, SECONDS);
            // Assert: once builder() returned, only classes of the test itself were loaded, such as
            // the nest host of the scenario's lambdas; the client's classes, such as its actor,
            // were loaded before that on a platform thread.
            assertThat(loader.definitions())
                    .filteredOn(Definition::afterBuilder)
                    .extracting(Definition::name)
                    .allMatch(name -> name.startsWith(ClassPreloadTest.class.getName()));
            assertThat(loader.definitions())
                    .filteredOn(definition -> definition.name().equals(ClientActor.class.getName()))
                    .singleElement()
                    .extracting(Definition::virtualThread)
                    .isEqualTo(false);
        }
    }

    /// Builds a client and uses it in the common ways. It runs in the class loader under test, so
    /// it shares no types with the test other than those of the JDK.
    public static final class Scenario {

        public static void run(int port, Runnable builderReturned) throws InterruptedException {
            MqttClientBuilder builder = MqttClient.builder();
            builderReturned.run();
            URI server = URI.create("mqtt://localhost:" + port);
            try (MqttClient client = builder.server(server).build()) {
                client.connect().join();
                var handled = new CompletableFuture<MqttMessage>();
                client.subscribe(
                        Subscribe.of(new TopicFilter("preload/handled"), Qos.AT_LEAST_ONCE),
                        delivery -> handled.complete(delivery.message())
                ).established().join();
                var mailbox = new ArrayBlockingQueue<MqttDelivery>(1);
                client.handOff(
                        Subscribe.of(new TopicFilter("preload/handed-off"), Qos.EXACTLY_ONCE),
                        mailbox::offer
                ).established().join();
                PullSubscription pulled = client.subscribe(
                        Subscribe.of(new TopicFilter("preload/pulled"), Qos.AT_MOST_ONCE)
                );
                pulled.established().join();
                client.publish(message("preload/handled", Qos.AT_LEAST_ONCE)).join();
                client.publish(message("preload/handed-off", Qos.EXACTLY_ONCE)).join();
                client.publish(message("preload/pulled", Qos.AT_MOST_ONCE)).join();
                handled.join();
                mailbox.take();
                pulled.receive();
                client.stats().join();
                client.disconnect().join();
            }
        }

        private static MqttMessage message(String topic, Qos qos) {
            return MqttMessage.of(new Topic(topic), new byte[] {1}, qos, false);
        }
    }

    record Definition(String name, boolean virtualThread, boolean afterBuilder) {}

    /// Defines the classes of the client, and the scenario that uses them, itself instead of
    /// asking its parent. Records each class that it defines, whether a virtual thread asked for
    /// it, and whether `MqttClient.builder()` had returned.
    private static final class RecordingLoader extends URLClassLoader {

        private final List<Definition> definitions = new CopyOnWriteArrayList<>();

        private volatile boolean builderReturned;

        RecordingLoader() {
            super(
                    new URL[] {codeSource(MqttClient.class), codeSource(ClassPreloadTest.class)},
                    ClassPreloadTest.class.getClassLoader()
            );
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            // The client's public package, and its internal packages, where the scenario is too.
            boolean client = name.startsWith("org.mqtt4j.hato.client.")
                    || (name.startsWith("org.mqtt4j.hato.")
                            && name.indexOf('.', "org.mqtt4j.hato.".length()) < 0);
            if (!client) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> type = findLoadedClass(name);
                if (type == null) {
                    type = findClass(name);
                    definitions.add(new Definition(
                            name,
                            Thread.currentThread().isVirtual(),
                            builderReturned
                    ));
                }
                return type;
            }
        }

        void builderReturned() {
            builderReturned = true;
        }

        List<Definition> definitions() {
            return definitions;
        }

        private static URL codeSource(Class<?> type) {
            return type.getProtectionDomain().getCodeSource().getLocation();
        }
    }
}
