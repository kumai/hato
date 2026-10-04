plugins {
    id("mqtj.embedded-common")
    id("mqtj.standalone-jar")
}

description = "MQTT 3.1.1 and 5.0 client for Java. One jar, no runtime dependencies."

// Version and QoS have one source definition in common. In the client they are the public
// API values themselves, so there is no second internal enum and no converting wrapper.
// common.value therefore has no package-info: the client's own package-info null-marks the mapped
// package, and the internal copies in broker and testkit stay unmarked (they hold no nullable member).
tasks.named<EmbedCommonSources>("embedCommonSources") {
    packageMappings.put("org.mqtt4j.mqtj.common.value", "org.mqtt4j.mqtj")
}
tasks.named<VerifyDisjointPackages>("verifyEmbeddedPackagesAreDisjoint") {
    allowedSharedPackages.add("org.mqtt4j.mqtj")
}

testing {
    suites {
        withType<JvmTestSuite>().configureEach {
            dependencies {
                implementation(project(":mqtj-testkit"))
                implementation(libs.awaitility)
            }
        }
        named<JvmTestSuite>("integrationTest") {
            dependencies {
                implementation(project(":test-support"))
            }
            // Virtual threads are not preempted, so on a single carrier, as in a one-CPU
            // container, one busy virtual thread holds up all others. Run the suite that way too.
            targets.register("integrationTestSingleCarrier") {
                testTask.configure {
                    jvmArgs("-Djdk.virtualThreadScheduler.parallelism=1")
                }
            }
        }
    }
}
