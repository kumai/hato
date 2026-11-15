plugins {
    id("hato.java-library")
    id("hato.standalone-jar")
}

// Published in every version's POM, which cannot change after release: describe no planned feature.
description = "MQTT client and embedded MQTT broker for Java. One jar, no runtime dependencies."

// The client and the broker each load their classes before they run on virtual threads (see
// ClassPreload). Each reads an index of its own, so that an application that runs only clients
// loads no class of the broker: the client's leaves out the broker's packages, and the broker's
// leaves out the client's internals.
val writeClassIndex = tasks.register<WriteClassIndex>("writeClassIndex") {
    classDirectories.from(sourceSets.main.map { it.output.classesDirs })
    resourceDirectory = "org/mqtt4j/hato/internal/concurrent"
    indexes.put("client-classes.txt", listOf("org.mqtt4j.hato.broker."))
    indexes.put("broker-classes.txt", listOf("org.mqtt4j.hato.client."))
    outputDirectory = layout.buildDirectory.dir("generated/resources/classIndex/main")
}
sourceSets.main {
    resources.srcDir(writeClassIndex.flatMap { it.outputDirectory })
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
            // compete for that carrier; macOS polls on platform threads by default. Every client
            // also runs its core on virtual threads, and every connection reads on one, as they
            // do once the JVM's places for platform cores and its few platform readers are taken.
            // Brokers configured with smaller limits check the client's decisions against what
            // they announce, which a single carrier does not change, so they run once.
            targets.register("integrationTestSingleCarrier") {
                testTask.configure {
                    jvmArgs(
                        "-Djdk.virtualThreadScheduler.parallelism=1",
                        "-Djdk.pollerMode=2",
                        "-Dhato.client.platformReaders=0",
                        "-Dhato.client.platformCores=0"
                    )
                    filter {
                        excludeTestsMatching("*5RestrictedTest")
                    }
                }
            }
        }
        // The broker's integration tests run in a JVM of their own, with settings of their own,
        // apart from the client's.
        register<JvmTestSuite>("brokerIntegrationTest") {
            dependencies {
                implementation(project())
                implementation(project(":test-support"))
            }
            targets.register("brokerIntegrationTestSingleCarrier") {
                testTask.configure {
                    jvmArgs("-Djdk.virtualThreadScheduler.parallelism=1")
                }
            }
        }
    }
}
