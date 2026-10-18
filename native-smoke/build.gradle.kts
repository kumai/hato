plugins {
    id("hato.java-conventions")
    application
    alias(libs.plugins.graalvm.native)
}

description = "GraalVM native-image smoke test: exercises the packaged client and broker. Not published."

dependencies {
    implementation(project(":hato-client"))
    implementation(project(":hato-broker"))
}

application {
    mainClass = "org.mqtt4j.hato.nativesmoke.NativeSmoke"
}

// The smoke reads the public test identities at run time: embedding them in the image would take
// native-image resource metadata, which Hato itself does not need. CI passes the same directory
// to the executable.
val testIdentities = layout.settingsDirectory.dir("test-support/src/main/resources/tls").asFile.path

tasks.named<JavaExec>("run") {
    args(testIdentities)
}

graalvmNative {
    // native-image comes from GRAALVM_HOME or JAVA_HOME; the Java toolchain used for compiling is not a GraalVM.
    toolchainDetection = false
    binaries {
        named("main") {
            imageName = "hato-native-smoke"
            mainClass = application.mainClass
            // Fail instead of silently producing an image that still needs a JVM.
            buildArgs.add("--no-fallback")
            runtimeArgs.add(testIdentities)
        }
    }
}
