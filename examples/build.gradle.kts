plugins {
    id("mqtj.java-conventions")
    application
}

description = "Usage examples. Not published."

dependencies {
    implementation(project(":mqtj-client"))
    implementation(project(":mqtj-broker"))
}

application {
    mainClass = "org.mqtt4j.mqtj.examples.Hello"
}
