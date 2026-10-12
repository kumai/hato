plugins {
    id("hato.java-conventions")
    application
}

description = "Usage examples. Not published."

dependencies {
    implementation(project(":hato-client"))
    implementation(project(":hato-broker"))
}

application {
    mainClass = "org.mqtt4j.hato.examples.Hello"
}
