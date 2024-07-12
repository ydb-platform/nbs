# OpenTelemetry Collector Release Procedure

Collector build and testing is currently fully automated. However there are still certain operations that need to be performed manually in order to make a release.

We release both core and contrib collectors with the same versions where the contrib release uses the core release as a dependency. We’ve divided this process into four sections. A release engineer must release:
1. The [Core](#releasing-opentelemetry-collector) collector, including the collector builder CLI tool.
1. The [Contrib](#releasing-opentelemetry-collector-contrib) collector.
1. The [artifacts](#producing-the-artifacts)

**Important Note:** You’ll need to be able to sign git commits/tags in order to be able to release a collector version. Follow [this guide](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits) to setup it up.

**Important Note:** You’ll need to be an approver for both the repos in order to be able to make the release. This is required as you’ll need to push tags and commits directly to the following repositories:

* [open-telemetry/opentelemetry-collector](https://github.com/open-telemetry/opentelemetry-collector)
* [open-telemetry/opentelemetry-collector-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib)
* [open-telemetry/opentelemetry-collector-releases](https://github.com/open-telemetry/opentelemetry-collector-releases)

## Release manager

A release manager is the person responsible for a specific release. While the manager might request help from other folks, they are ultimately responsible for the success of a release.

In order to have more people comfortable with the release process, and in order to decrease the burden on a small number of volunteers, all core approvers are release managers from time to time, listed under the [Release Schedule](#release-schedule) section. That table is updated at every release, with the current manager adding themselves to the bottom of the table, removing themselves from the top of the table.

It is possible that a core approver isn't a contrib approver. In that case, the release manager should coordinate with a contrib approver for the steps requiring such role, like the publishing of tags.

## Releasing opentelemetry-collector

1. Update Contrib to use the latest in development version of Core. Run `make update-otel` in Contrib root directory and if it results in any changes submit a draft PR to Contrib. Ensure the CI passes before proceeding. This is to ensure that the latest core does not break contrib in any way. We’ll update it once more to the final release number later.

2. Determine the version number that will be assigned to the release. During the beta phase, we increment the minor version number and set the patch number to 0. In this document, we are using `v0.85.0` as the version to be released, following `v0.54.0`.
   Check if stable modules have any changes since the last release by running `make check-changes PREVIOUS_VERSION=v1.0.0-rcv0014 MODSET=stable`. If there are no changes, there is no need to release new version for stable modules.

3. Manually run the action [Automation - Prepare Release](https://github.com/open-telemetry/opentelemetry-collector/actions/workflows/prepare-release.yml). This action will create an issue to track the progress of the release and a pull request to update the changelog and version numbers in the repo. **While this PR is open all merging in Core should be haulted**.
   - When prompted, enter the version numbers determined in Step 1, but do not include a leading `v`.
   - If not intending to release stable modeles, do not specify a version for `Release candidate version stable`.
   - If the PR needs updated in any way you can make the changes in a fork and PR those changes into the `prepare-release-prs/x` branch. You do not need to wait for the CI to pass in this prep-to-prep PR.
   -  🛑 **Do not move forward until this PR is merged.** 🛑

4. Check out the commit created by merging the PR created by `Automation - Prepare Release` (e.g. `prepare-release-prs/0.85.0`) and create a branch named `release/<release-series>` (e.g. `release/v0.85.x`). Push the new branch to `open-telemetry/opentelemetry-collector`.

5. Make sure you are on `release/<release-series>`. Tag the module groups with the new release version by running:
   - `make push-tags MODSET=beta` for beta modules group,
   - `make push-tags MODSET=stable` beta stable modules group, only if there were changes since the last release.

   If you set your remote using `https` you need to include `REMOTE=https://github.com/open-telemetry/opentelemetry-collector.git` in each command. Wait for the new tag build to pass successfully.

6. The release script for the collector builder should create a new GitHub release for the builder. This is a separate release from the core, but we might join them in the future if it makes sense.

7. A new `v0.85.0` release should be automatically created on Github by now. Edit it and use the contents from the CHANGELOG.md and CHANGELOG-API.md as the release's description.

8. If you created a draft PR to Contrib in step 3, update the PR to use the newly released Core version and set it to Ready for Review.
   - Run `make update-otel OTEL_VERSION=v0.85.0 OTEL_RC_VERSION=v1.0.0-rcv0014`
   - Manually update `cmd/otelcontribcol/builder-config.yaml`
   - Manually update `cmd/oteltestbedcol/builder-config.yaml`
   - Run `make genotelcontribcol`
   - Push updates to the PR
   -  🛑 **Do not move forward until this PR is merged.** 🛑

## Releasing opentelemetry-collector-contrib

1. Manually run the action [Automation - Prepare Release](https://github.com/open-telemetry/opentelemetry-collector-contrib/actions/workflows/prepare-release.yml). When prompted, enter the version numbers determined in Step 1, but do not include a leading `v`. This action will a pull request to update the changelog and version numbers in the repo. **While this PR is open all merging in Contrib should be haulted**.
   - If the PR needs updated in any way you can make the changes in a fork and PR those changes into the `prepare-release-prs/x` branch. You do not need to wait for the CI to pass in this prep-to-prep PR.
   -  🛑 **Do not move forward until this PR is merged.** 🛑

2. Check out the commit created by merging the PR created by `Automation - Prepare Release` (e.g. `prepare-release-prs/0.85.0`) and create a branch named `release/<release-series>` (e.g. `release/v0.85.x`). Push the new branch to `open-telemetry/opentelemetry-collector-contrib`.

3. Make sure you are on `release/<release-series>`. Tag all the module groups (`contrib-base`) with the new release version by running the `make push-tags MODSET=contrib-base` command. If you set your remote using `https` you need to include `REMOTE=https://github.com/open-telemetry/opentelemetry-collector-contrib.git` in each command. Wait for the new tag build to pass successfully.

4. A new `v0.85.0` release should be automatically created on Github by now. Edit it and use the contents from the CHANGELOG.md as the release's description.

## Producing the artifacts

The last step of the release process creates artifacts for the new version of the collector and publishes images to Dockerhub. The steps in this portion of the release are done in the [opentelemetry-collector-releases](https://github.com/open-telemetry/opentelemetry-collector-releases) repo.

1. Update the `./distribution/**/manifest.yaml` files to include the new release version.

2. Update the builder version in `OTELCOL_BUILDER_VERSION` to the new release in the `Makefile`. While this might not be strictly necessary for every release, this is a good practice.

3. Create a pull request with the change and ensure the build completes successfully. See [example](https://github.com/open-telemetry/opentelemetry-collector-releases/pull/71).
   -  🛑 **Do not move forward until this PR is merged.** 🛑

4. Check out the commit created by merging the PR and tag with the new release version by running the `make push-tags TAG=v0.85.0` command. If you set your remote using `https` you need to include `REMOTE=https://github.com/open-telemetry/opentelemetry-collector-releases.git` in each command. Wait for the new tag build to pass successfully.

5. Ensure the "Release" action passes, this will

    1. push new container images to `https://hub.docker.com/repository/docker/otel/opentelemetry-collector` and `https://hub.docker.com/repository/docker/otel/opentelemetry-collector-contrib`

    2. create a Github release for the tag and push all the build artifacts to the Github release. See [example](https://github.com/open-telemetry/opentelemetry-collector-releases/actions/runs/5869052077).

## Troubleshooting

1. `unknown revision internal/coreinternal/v0.85.0` -- This is typically an indication that there's a dependency on a new module. You can fix it by adding a new `replaces` entry to the `go.mod` for the affected module.
2. `commitChangesToNewBranch failed: invalid merge` -- This is a [known issue](https://github.com/open-telemetry/opentelemetry-go-build-tools/issues/47) with our release tooling. The current workaround is to clone a fresh copy of the repository and try again. Note that you may need to set up a `fork` remote pointing to your own fork for the release tooling to work properly.
3. `could not run Go Mod Tidy: go mod tidy failed` when running `multimod` -- This is a [known issue](https://github.com/open-telemetry/opentelemetry-go-build-tools/issues/46) with our release tooling. The current workaround is to run `make gotidy` manually after the multimod tool fails and commit the result.
4. `Incorrect version "X" of "go.opentelemetry.io/collector/component" is included in "X"` in CI after `make update-otel` -- It could be because the make target was run too soon after updating Core and the goproxy hasn't updated yet.  Try running `export GOPROXY=direct` and then `make update-otel`.
5. `error: failed to push some refs to 'https://github.com/open-telemetry/opentelemetry-collector-contrib.git'` during `make push-tags` -- If you encounter this error the `make push-tags` target will terminate without pushing all the tags. Using the output of the `make push-tags` target, save all the un-pushed the tags in `tags.txt` and then use this make target to complete the push:

   ```bash
   .PHONY: temp-push-tags
   temp-push-tags:
       for tag in `cat tags.txt`; do \
           echo "pushing tag $${tag}"; \
           git push ${REMOTE} $${tag}; \
       done;
   ```

## Bugfix releases

### Bugfix release criteria

Both `opentelemetry-collector` and `opentelemetry-collector-contrib` have very short 2 week release cycles. Because of this, we put a high bar when considering making a patch release, to avoid wasting engineering time unnecessarily.

When considering making a bugfix release on the `v0.N.x` release cycle, the bug in question needs to fulfill the following criteria:

1. The bug has no workaround or the workaround is significantly harder to put in place than updating the version. Examples of simple workarounds are:
    - Reverting a feature gate.
    - Changing the configuration to an easy to find value.
2. The bug happens in common setups. To gauge this, maintainers can consider the following:
    - The bug is not specific to an uncommon platform
    - The bug happens with the default configuration or with a commonly used one (e.g. has been reported by multiple people)
3. The bug is sufficiently severe. For example (non-exhaustive list):
    - The bug makes the Collector crash reliably
    - The bug makes the Collector fail to start under an accepted configuration
    - The bug produces significant data loss
    - The bug makes the Collector negatively affect its environment (e.g. significantly affects its host machine)

We aim to provide a release that fixes security-related issues in at most 30 days since they are publicly announced; with the current release schedule this means security issues will typically not warrant a bugfix release. An exception is critical vulnerabilities (CVSSv3 score >= 9.0), which will warrant a release within five business days.

The OpenTelemetry Collector maintainers will ultimately have the responsibility to assess if a given bug or security issue fulfills all the necessary criteria and may grant exceptions in a case-by-case basis.

### Bugfix release procedure

The following documents the procedure to release a bugfix

1. Create a pull request against the `release/<release-series>` (e.g. `release/v0.45.x`) branch to apply the fix.
2. Create a pull request to update version number against the `release/<release-series>` branch.
3. Once those changes have been merged, create a pull request to the `main` branch from the `release/<release-series>` branch.
4. Enable the **Merge pull request** setting in the repository's **Settings** tab.
5. Tag all the modules with the new release version by running the `make add-tag` command (e.g. `make add-tag TAG=v0.85.0`). Push them to `open-telemetry/opentelemetry-collector` with `make push-tag TAG=v0.85.0`. Wait for the new tag build to pass successfully.
6. **IMPORTANT**: The pull request to bring the changes from the release branch *MUST* be merged using the **Merge pull request** method, and *NOT* squashed using “**Squash and merge**”. This is important as it allows us to ensure the commit SHA from the release branch is also on the main branch. **Not following this step will cause much go dependency sadness.**
7. Once the branch has been merged, it will be auto-deleted. Restore the release branch via GitHub.
8. Once the patch is release, disable the **Merge pull request** setting.

## Release schedule

| Date       | Version | Release manager |
|------------|---------|-----------------|
| 2023-09-11 | v0.85.0 | @codeboten      |
| 2023-09-25 | v0.86.0 | @bogdandrutu    |
| 2023-10-09 | v0.87.0 | @Aneurysm9      |
| 2023-10-23 | v0.88.0 | @mx-psi         |
| 2023-11-06 | v0.89.0 | @jpkrohling     |
| 2023-11-20 | v0.90.0 | @djaglowski     |
| 2023-12-04 | v0.84.0 | @dmitryax       |