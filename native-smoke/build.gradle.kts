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

graalvmNative {
    // native-image comes from GRAALVM_HOME or JAVA_HOME; the Java toolchain used for compiling is not a GraalVM.
    toolchainDetection = false
    binaries {
        named("main") {
            imageName = "hato-native-smoke"
            mainClass = application.mainClass
            // Fail instead of silently producing an image that still needs a JVM.
            buildArgs.add("--no-fallback")
        }
    }
}
