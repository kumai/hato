plugins {
    id("hato.java-conventions")
    application
}

description = "Measures Hato against other Java MQTT clients on the subscribing side. Not published."

dependencies {
    implementation(project(":hato-client"))
    implementation(libs.paho.mqttv3)
    implementation(libs.hivemq.mqtt.client)
}

// The comparison runs only when asked for, with `run`: it takes minutes and prints its results
// instead of asserting limits. The build compiles it and nothing more. Each subscriber under
// measurement runs in a JVM of its own, which the driver starts with the same options for every
// client.
application {
    mainClass = "org.mqtt4j.hato.comparison.Driver"
}
