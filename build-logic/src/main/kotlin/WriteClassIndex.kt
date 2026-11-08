import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

/**
 * Writes the binary name of every class in the compiled classes, one per line in sorted order, as
 * a resource that the library reads at run time. Module and package descriptors are left out.
 */
@CacheableTask
abstract class WriteClassIndex : DefaultTask() {
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val classDirectories: ConfigurableFileCollection

    /** The index's resource path, for example `org/mqtt4j/hato/client/internal/classes.txt`. */
    @get:Input
    abstract val resourcePath: Property<String>

    @get:OutputDirectory
    abstract val outputDirectory: DirectoryProperty

    @TaskAction
    fun write() {
        val names = sortedSetOf<String>()
        classDirectories.asFileTree.visit {
            if (!isDirectory && name.endsWith(".class") && name != "module-info.class" &&
                name != "package-info.class"
            ) {
                names.add(relativePath.pathString.removeSuffix(".class").replace('/', '.'))
            }
        }
        val root = outputDirectory.get().asFile
        root.deleteRecursively()
        val index = root.resolve(resourcePath.get())
        index.parentFile.mkdirs()
        index.writeText(names.joinToString("") { "$it\n" })
    }
}
