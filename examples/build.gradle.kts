import java.time.Duration

plugins {
    id("hato.java-conventions")
    application
}

description = "Usage examples. Not published."

dependencies {
    implementation(project(":hato-client"))
    implementation(project(":hato-broker"))
    implementation(libs.micrometer.core)
}

application {
    mainClass = "org.mqtt4j.hato.examples.Hello"
}

// The examples that start their own broker run in the build, so that one that stops working, or
// whose shutdown never ends, fails it.
val runExamples = listOf(
    "Hello", "HandOffToMailbox", "MailboxLanes", "MicrometerMonitoring"
).map { example ->
    tasks.register<JavaExec>("run$example") {
        group = LifecycleBasePlugin.VERIFICATION_GROUP
        description = "Runs the $example example against its own embedded broker."
        classpath = sourceSets.main.get().runtimeClasspath
        mainClass = "org.mqtt4j.hato.examples.$example"
        timeout = Duration.ofMinutes(1)
    }
}

tasks.named("check") {
    dependsOn(runExamples)
}
