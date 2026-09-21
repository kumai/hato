import org.gradle.api.GradleException
import org.gradle.testfixtures.ProjectBuilder
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class VerifyDisjointPackagesTest {
    @TempDir
    lateinit var root: Path

    @Test
    fun only_the_explicit_public_package_may_mix_generated_and_own_sources() {
        val project = ProjectBuilder.builder().withProjectDir(root.toFile()).build()
        val generated = Files.createDirectory(root.resolve("generated"))
        val own = Files.createDirectory(root.resolve("own"))
        Files.writeString(generated.resolve("Version.java"), "package example.api; class Version {}")
        Files.writeString(own.resolve("Client.java"), "package example.api; class Client {}")
        val task = project.tasks.register("verify", VerifyDisjointPackages::class.java).get().apply {
            embeddedSources.set(generated.toFile())
            ownSources.from(project.fileTree(own))
        }

        assertThrows(GradleException::class.java) { task.verify() }
        task.allowedSharedPackages.add("example.api")
        assertDoesNotThrow { task.verify() }

        Files.writeString(generated.resolve("Log.java"), "package example.internal; class Log {}")
        Files.writeString(own.resolve("Other.java"), "package example.internal; class Other {}")
        assertThrows(GradleException::class.java) { task.verify() }
    }
}
