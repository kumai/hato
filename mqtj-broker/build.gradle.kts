plugins {
    id("mqtj.embedded-common")
    id("mqtj.standalone-jar")
}

description = "In-memory, single-node MQTT broker. One independent jar."

testing {
    suites {
        named<JvmTestSuite>("integrationTest") {
            dependencies {
                implementation(project(":test-support"))
                implementation(project(":mqtj-testkit"))
                implementation(libs.awaitility)
            }
            targets.register("integrationTestSingleCarrier") {
                testTask.configure {
                    jvmArgs("-Djdk.virtualThreadScheduler.parallelism=1")
                }
            }
        }
    }
}
