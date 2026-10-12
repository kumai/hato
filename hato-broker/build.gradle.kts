plugins {
    id("hato.embedded-common")
    id("hato.standalone-jar")
}

description = "In-memory, single-node MQTT broker. One independent jar."

testing {
    suites {
        named<JvmTestSuite>("integrationTest") {
            dependencies {
                implementation(project(":test-support"))
                implementation(project(":hato-testkit"))
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
