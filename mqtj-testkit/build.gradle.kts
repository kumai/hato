plugins {
    id("mqtj.embedded-common")
}

description = "Test support for applications that use MQTJ: embedded broker, fake client, fault injection, and manual scheduler."

dependencies {
    api(project(":mqtj-client"))
    api(project(":mqtj-broker"))
}
