import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.process.CommandLineArgumentProvider

abstract class DistributionTestArguments : CommandLineArgumentProvider {
    // Byte-for-byte jar contents matter here, including metadata, sources, and entry names.
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val hatoJar: ConfigurableFileCollection

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val clientAliasJar: ConfigurableFileCollection

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val testkitJars: ConfigurableFileCollection

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val sourceJars: ConfigurableFileCollection

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val javadocJars: ConfigurableFileCollection

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val repositoryLicense: ConfigurableFileCollection

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val publicationMetadata: ConfigurableFileCollection

    override fun asArguments(): Iterable<String> = listOf(
        "-Dhato.jar=${hatoJar.singleFile.absolutePath}",
        "-Dclient.alias.jar=${clientAliasJar.singleFile.absolutePath}",
        "-Dtestkit.jars=${testkitJars.asPath}",
        "-Dsource.jars=${sourceJars.asPath}",
        "-Djavadoc.jars=${javadocJars.asPath}",
        "-Drepository.license=${repositoryLicense.singleFile.absolutePath}",
        "-Dpublication.metadata=${publicationMetadata.asPath}",
    )
}
