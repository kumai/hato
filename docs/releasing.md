# Releasing MQTJ

Releases are uploaded to Maven Central for review. The release workflow never
publishes or drops a deployment; the maintainer does that in the
[Central Portal](https://central.sonatype.com/publishing).

## Prepare the release environment

Before the first upload, verify the `org.mqtt4j` namespace in the Portal, create a
Portal user token, and publish the signing key's public key as described in
[Sonatype's signing requirements](https://central.sonatype.org/publish/requirements/gpg/).
Keep the private key and its revocation certificate outside the repository.

Create a GitHub environment named `release`, restricted to tags matching `v*`,
with these secrets:

| Secret | Value |
| --- | --- |
| `MQTJ_SIGNING_KEY` | ASCII-armored private PGP signing key |
| `MQTJ_SIGNING_PASSWORD` | Passphrase for that key; empty for an unprotected key |
| `CENTRAL_USERNAME` | Username from the Portal user token |
| `CENTRAL_PASSWORD` | Password from the Portal user token |

Environment availability for private repositories depends on the GitHub plan;
see [GitHub's environment documentation](https://docs.github.com/en/actions/deployment/targeting-different-environments/managing-environments-for-deployment).
The Portal credentials reach only the upload step, which runs the release tool
outside Gradle. Signing runs with Gradle's configuration cache and the workflow's
Gradle cache disabled.

## Inspect a bundle locally

```shell
./gradlew --no-configuration-cache :distribution-tests:releaseBundle -Pversion=0.3.0
```

The bundle is `distribution-tests/build/distributions/mqtj-0.3.0.zip`. It contains
the client, broker, and testkit publications under `org/mqtt4j/`. SNAPSHOT versions
are rejected. A local bundle can be built without a signing key for inspection.
When the signing environment variables are set, build and verify the signed bundle
with:

```shell
./gradlew --no-configuration-cache :distribution-tests:verifyReleaseBundle -Pversion=0.3.0
```

This checks the complete file list, nonempty signatures, and checksum contents.
The workflow requires a signing key and runs the same task before upload.

## Trial upload

The workflow must be present on the default branch for manual dispatch. Select
a pre-release trial version such as `0.3.0-rc.1` and create its `v0.3.0-rc.1` tag on
the commit to test. Manual trials reject final versions such as `0.3.0`, so a trial
never takes a release number. If the commit still has `0.3.0-SNAPSHOT` in
`gradle.properties`, pushing that tag starts a release run which rejects the version
mismatch in its first job, before the full build. This is the normal release guard;
it does not upload anything.

Manually dispatch **Release** with that same tag as the ref and `0.3.0-rc.1` as
the `version` input. For example, using GitHub CLI:

```shell
gh workflow run release.yml --ref v0.3.0-rc.1 -f version=0.3.0-rc.1
```

Manual trials override the committed version for the bundle, but the input must
still match the selected tag. Branch refs are rejected. The workflow runs the
full build and native smoke test for that commit, signs the bundle, uploads it as
`USER_MANAGED`, and waits for `VALIDATED`. Review the deployment in the Portal,
then **Drop** the trial instead of publishing it.

## Publish a release

Set `version` in `gradle.properties` to the release version, update README and
CHANGELOG, and finish the release checks before creating the matching `v` tag.
Push the tag to start **Release**. A tag/version mismatch or SNAPSHOT version stops
the run before the full build. Otherwise, its reusable CI workflow checks the same
commit before the upload job can run.

The workflow retains the signed ZIP as an Actions artifact and writes the
deployment ID to its summary. It makes one upload attempt and at most thirty
status requests. If upload times out, inspect the Portal before rerunning: the
deployment may already exist. If validation fails or takes longer than the wait,
inspect that deployment's status and errors in the Portal.

After `VALIDATED`, inspect the coordinates and files, then choose **Publish** in
the Portal. Use **Drop** if the contents are wrong. Finally, verify that a fresh
consumer resolves the published version and compare the published artifacts'
SHA-256 checksums with a rebuild from the tag using the same JDK.

The upload follows the
[Central Publisher API](https://central.sonatype.org/publish/publish-portal-api/).
The version check, bundle check, and upload are Java programs in `release-tools`.
After changing them or the workflow, run their tests, which upload to a local
stand-in for the Portal:

```shell
./gradlew :release-tools:check
```
