plugins {
    id("mqtj.java-conventions")
    `java-library`
}

description = "Values and implementation shared by client, broker, and testkit: protocol, logging, concurrency, I/O. Not published; embedded at build time."

java {
    withSourcesJar()
}

// Consumers compile a copy of these sources into their own named module.
// This configuration is deliberately separate from Java API/runtime dependencies.
val commonSourcesElements = configurations.consumable("commonSourcesElements")
artifacts {
    add(commonSourcesElements.name, tasks.named("sourcesJar"))
}

testing {
    suites {
        named<JvmTestSuite>("integrationTest") {
            dependencies {
                // Starts Mosquitto in Docker for the raw wire exchange that closes version 0.1.
                implementation(libs.testcontainers)
            }
        }
    }
}
