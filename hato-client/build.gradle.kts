plugins {
    id("hato.embedded-common")
    id("hato.standalone-jar")
}

// Published in every version's POM, which cannot change after release: describe no planned feature.
description = "MQTT client for Java. One jar, no runtime dependencies."

// Version and QoS have one source definition in common. In the client they are the public
// API values themselves, so there is no second internal enum and no converting wrapper.
// common.value therefore has no package-info: the client's own package-info null-marks the mapped
// package, and the internal copies in broker and testkit stay unmarked (they hold no nullable member).
tasks.named<EmbedCommonSources>("embedCommonSources") {
    packageMappings.put("org.mqtt4j.hato.common.value", "org.mqtt4j.hato")
}
tasks.named<VerifyDisjointPackages>("verifyEmbeddedPackagesAreDisjoint") {
    allowedSharedPackages.add("org.mqtt4j.hato")
}

testing {
    suites {
        withType<JvmTestSuite>().configureEach {
            dependencies {
                implementation(project(":hato-testkit"))
                implementation(libs.awaitility)
            }
        }
        named<JvmTestSuite>("integrationTest") {
            dependencies {
                implementation(project(":test-support"))
            }
            // Measurements run only when asked for, with -Phato.measure=true, and print their
            // results instead of asserting limits; -Phato.measure.minutes sets the length of the
            // endurance runs. Other runs leave them out rather than skip them, so a build still
            // skips no test.
            targets.configureEach {
                testTask.configure {
                    val measure = providers.gradleProperty("hato.measure").orElse("false").get()
                    systemProperty("hato.measure", measure)
                    systemProperty(
                        "hato.measure.minutes",
                        providers.gradleProperty("hato.measure.minutes").orElse("30").get()
                    )
                    if (measure != "true") {
                        filter {
                            excludeTestsMatching("*MeasurementTest")
                        }
                    }
                }
            }
            // Virtual threads are not preempted, so on a single carrier, as in a one-CPU
            // container, one busy virtual thread holds up all others. Run the suite that way too.
            // The I/O pollers run as virtual threads, as they do by default on Linux, where they
            // compete for that carrier; macOS polls on platform threads by default.
            // Brokers configured with smaller limits check the client's decisions against what
            // they announce, which a single carrier does not change, so they run once.
            targets.register("integrationTestSingleCarrier") {
                testTask.configure {
                    jvmArgs(
                        "-Djdk.virtualThreadScheduler.parallelism=1",
                        "-Djdk.pollerMode=2"
                    )
                    filter {
                        excludeTestsMatching("*5RestrictedTest")
                    }
                }
            }
        }
    }
}
