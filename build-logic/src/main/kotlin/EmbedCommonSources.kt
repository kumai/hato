import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import java.util.zip.ZipFile

/** Copies the shared Java sources under a consumer-specific package, rewriting every reference. */
@CacheableTask
abstract class EmbedCommonSources : DefaultTask() {
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val sourceArchives: ConfigurableFileCollection

    /** The package the shared sources are written in, for example `org.mqtt4j.hato.common`. */
    @get:Input
    abstract val sourcePackage: Property<String>

    /** The package the sources are moved to, for example `org.mqtt4j.hato.client.internal.common`. */
    @get:Input
    abstract val destinationPackage: Property<String>

    /** More specific source-package mappings, for example shared values used in a public API. */
    @get:Input
    abstract val packageMappings: MapProperty<String, String>

    init {
        packageMappings.convention(emptyMap())
    }

    @get:OutputDirectory
    abstract val outputDirectory: DirectoryProperty

    @TaskAction
    fun embed() {
        val sourcePackage = sourcePackage.get()
        val sourcePrefix = sourcePackage.replace('.', '/') + "/"
        val mappings = (mapOf(sourcePackage to destinationPackage.get()) + packageMappings.get())
            .entries.sortedByDescending { it.key.length }
        for ((source, target) in mappings) {
            if (source != sourcePackage && !source.startsWith("$sourcePackage.")) {
                throw GradleException("Package mapping is outside $sourcePackage: $source")
            }
            if (!JAVA_PACKAGE.matches(target)) {
                throw GradleException("Invalid destination package: $target")
            }
        }
        // One pass with the most specific prefixes first. A public package may itself be a prefix
        // of the source namespace, so applying successive replacements could move a type twice.
        val packageReferences = Regex(
            "(?<![\\w$])(?:" + mappings.joinToString("|") { Regex.escape(it.key) } + ")(?![\\w$])"
        )
        val destinations = mappings.associate { it.key to it.value }
        val outputRoot = outputDirectory.get().asFile.toPath()
        val sources = mutableMapOf<String, String>()
        ZipFile(sourceArchives.singleFile).use { archive ->
            for (entry in archive.entries()) {
                if (entry.isDirectory || entry.name == "META-INF/MANIFEST.MF") continue
                // Resources and module descriptors need explicit handling if introduced later.
                // Failing here prevents silently dropping them from a published jar.
                if (!entry.name.startsWith(sourcePrefix) || !entry.name.endsWith(".java")) {
                    throw GradleException("Unsupported shared source archive entry: ${entry.name}")
                }
                val mapping = mappings.first {
                    entry.name.startsWith(it.key.replace('.', '/') + "/")
                }
                val relativePath = mapping.value.replace('.', '/') + "/" +
                    entry.name.removePrefix(mapping.key.replace('.', '/') + "/")
                if (!outputRoot.resolve(relativePath).normalize().startsWith(outputRoot)) {
                    throw GradleException("Invalid shared source archive path: ${entry.name}")
                }
                val source = archive.getInputStream(entry).bufferedReader(Charsets.UTF_8).use {
                    it.readText()
                }
                val relocated = source.replace(packageReferences) { destinations.getValue(it.value) }
                if (sources.put(relativePath, relocated) != null) {
                    throw GradleException("Multiple shared sources map to $relativePath")
                }
            }
        }
        // Sync semantics: a deleted or renamed shared source must not survive in a consumer.
        outputRoot.toFile().deleteRecursively()
        for ((relativePath, source) in sources) {
            val target = outputRoot.resolve(relativePath).toFile()
            target.parentFile.mkdirs()
            target.writeText(source, Charsets.UTF_8)
        }
    }

    private companion object {
        val JAVA_PACKAGE = Regex("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)*")
    }
}
