# Releasing Hato

Publishing a GitHub Release publishes Hato to Maven Central. The release's tag, such
as `v0.3.0`, names the version. The release workflow checks the tagged commit, signs
the bundle, and uploads it to the
[Central Portal](https://central.sonatype.com/publishing), which publishes it once it
passes validation. A manual trial runs the same workflow but stops after validation,
so the maintainer can review the deployment and drop it.

## Versions

`version` in `gradle.properties` is the next version planned, such as
`0.8.0-SNAPSHOT`, and labels development builds. A release may name that version or
an earlier one, such as `0.7.1`, but nothing above it, so a mistyped tag cannot
publish a number that Central never removes. Only the three numbers count:
`0.8.0-SNAPSHOT` allows `0.8.0` and `0.8.0-rc.1`.

After a release of the version on the default branch, such as `0.8.0` while it has
`0.8.0-SNAPSHOT`, the workflow moves the branch to the next minor version,
`0.9.0-SNAPSHOT`, which still allows `0.8.1`. The commit comes from
`github-actions[bot]`, so pull before pushing again; GitHub does not run CI for it.
Pre-releases and releases of earlier versions leave the branch alone. Raise the
version by hand for a major version, such as `1.0.0-SNAPSHOT` before `1.0.0`, or when
the workflow cannot push because the branch moved on or branch protection refuses
it.

## Prepare the release environment

Before the first upload, verify the `org.mqtt4j` namespace in the Portal, create a
Portal user token, and publish the signing key's public key as described in
[Sonatype's signing requirements](https://central.sonatype.org/publish/requirements/gpg/).
Keep the private key and its revocation certificate outside the repository.

Create a GitHub environment named `release`, restricted to tags matching `v*`,
with these secrets:

| Secret | Value |
| --- | --- |
| `HATO_SIGNING_KEY` | ASCII-armored private PGP signing key |
| `HATO_SIGNING_PASSWORD` | Passphrase for that key; empty for an unprotected key |
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

The bundle is `distribution-tests/build/distributions/hato-0.3.0.zip`. It contains
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

Maven Central never removes a published version, so try the release first. The
workflow must be present on the default branch for manual dispatch. Create and push
a pre-release tag such as `v0.3.0-rc.1` on the commit to test, then manually
dispatch **Release** with that tag as the ref. For example, using GitHub CLI:

```shell
gh workflow run release.yml --ref v0.3.0-rc.1
```

Manual trials reject final versions such as `0.3.0`, so a trial never takes a
release number, and branch refs are rejected. The workflow runs the full build and
native smoke test for that commit, signs the bundle, uploads it as `USER_MANAGED`,
and waits for `VALIDATED`. Review the deployment in the Portal, then **Drop** the
trial instead of publishing it.

## Publish a release

Update the coordinates in the READMEs and on the landing page, `site/src/index.html`,
and finish the release checks. Then publish a GitHub Release with a new tag, such as
`v0.3.0`, on the commit to release, and the release notes in its description.
Publishing creates the tag; pushing a tag alone starts nothing. For example:

```shell
gh release create v0.3.0 --target main --title 0.3.0 --notes-file release-notes.md
```

A draft release starts nothing until it is published, so the notes can also be
prepared in the GitHub UI. Publishing the release starts **Release**. A tag above
the committed version, or a SNAPSHOT tag, stops the run before the full build.
Otherwise, its reusable CI workflow checks the same commit before the upload job can
run. The upload job uploads the bundle as `AUTOMATIC` and waits until the Portal
starts publishing it.

The workflow retains the signed ZIP as an Actions artifact and writes the
deployment ID to its summary. It makes one upload attempt and at most thirty
status requests. If the run fails, the GitHub Release stays, but Maven Central has
nothing until a deployment passes validation:

- If upload times out, inspect the Portal before rerunning: the deployment may
  already exist and still be published.
- If validation fails, the Portal publishes nothing; its errors are on the
  deployment in the Portal.
- Rerun the failed jobs when the cause lies outside the commit, such as a missing
  secret. Otherwise, delete the release and its tag with
  `gh release delete v0.3.0 --cleanup-tag`, fix the cause, and publish again.

Once the Portal shows the deployment as published, verify that a fresh consumer
resolves the published version and compare the published artifacts' SHA-256
checksums with a rebuild from the tag using the same JDK. The tagged commit has the
SNAPSHOT version, so pass the released version to the rebuild, as in
`-Pversion=0.3.0`.

The upload follows the
[Central Publisher API](https://central.sonatype.org/publish/publish-portal-api/).
The version check, bundle check, and upload are Java programs in `release-tools`.
After changing them or the workflow, run their tests, which upload to a local
stand-in for the Portal:

```shell
./gradlew :release-tools:check
```

## Publish the site

After the upload, **Release** runs **Pages**, which builds the landing page in `site/src` and
the API documentation of the client, the test kit, and the embedded broker from the release
tag, and publishes them to GitHub Pages. A pre-release leaves the published site alone. To
publish a release's site again, for example after a failed run, run **Pages** by hand from
that release's tag; a run from a branch stops before the build.

GitHub Pages takes its source from GitHub Actions, and the `github-pages` environment allows
deployments from tags matching `v*` as well as from the default branch: Pages deploys from the
release tag, which the environment refuses by default.

To look at the site before a release, build it and serve it locally:

```shell
./gradlew :site:assembleSite
jwebserver -d "$PWD/site/build/site"
```
