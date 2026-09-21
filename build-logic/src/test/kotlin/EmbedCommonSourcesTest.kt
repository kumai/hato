import org.gradle.api.GradleException
import org.gradle.testfixtures.ProjectBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

class EmbedCommonSourcesTest {
    @TempDir
    lateinit var root: Path

    @Test
    fun public_values_and_internal_references_use_the_same_definition() {
        val task = task()
        writeArchive(mapOf(
            "value/Version.java" to "package example.common.value; public enum Version { V5 }",
            "protocol/Packet.java" to """
                package example.common.protocol;
                import example.common.value.Version;
                public record Packet(Version version) { }
            """.trimIndent(),
        ))
        task.packageMappings.put("example.common.value", "example")

        task.embed()

        val output = task.outputDirectory.get().asFile
        assertTrue(output.resolve("example/Version.java").readText().startsWith("package example;"))
        val packet = output.resolve("example/client/internal/common/protocol/Packet.java").readText()
        assertTrue(packet.contains("package example.client.internal.common.protocol;"))
        assertTrue(packet.contains("import example.Version;"))
        assertFalse(output.resolve("example/client/internal/common/value/Version.java").exists())
    }

    @Test
    fun default_embedding_keeps_values_internal_and_removes_deleted_sources() {
        val task = task()
        writeArchive(mapOf("value/Old.java" to "package example.common.value; class Old {}"))
        task.embed()
        writeArchive(mapOf("value/New.java" to "package example.common.value; class New {}"))

        task.embed()

        val output = task.outputDirectory.get().asFile
        assertFalse(output.resolve("example/client/internal/common/value/Old.java").exists())
        assertEquals(
            "package example.client.internal.common.value; class New {}",
            output.resolve("example/client/internal/common/value/New.java").readText(),
        )
    }

    @Test
    fun conflicting_package_mappings_cannot_overwrite_a_source() {
        val task = task()
        writeArchive(mapOf(
            "value/Version.java" to "package example.common.value; class Version {}",
            "protocol/Version.java" to "package example.common.protocol; class Version {}",
        ))
        task.packageMappings.put("example.common.value", "example.api")
        task.packageMappings.put("example.common.protocol", "example.api")

        val failure = assertThrows(GradleException::class.java) { task.embed() }

        assertTrue(failure.message!!.contains("Multiple shared sources map to example/api/Version.java"))
    }

    private fun task(): EmbedCommonSources {
        val project = ProjectBuilder.builder().withProjectDir(root.toFile()).build()
        return project.tasks.register("embed", EmbedCommonSources::class.java).get().apply {
            sourceArchives.from(root.resolve("common-sources.jar").toFile())
            sourcePackage.set("example.common")
            destinationPackage.set("example.client.internal.common")
            outputDirectory.set(root.resolve("generated").toFile())
        }
    }

    private fun writeArchive(sources: Map<String, String>) {
        ZipOutputStream(root.resolve("common-sources.jar").toFile().outputStream()).use { zip ->
            for ((path, source) in sources) {
                zip.putNextEntry(ZipEntry("example/common/$path"))
                zip.write(source.toByteArray(Charsets.UTF_8))
                zip.closeEntry()
            }
        }
    }
}
