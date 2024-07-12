# Contributing Notes

## Releasing

Releases are made on a reasonably regular basis by the maintainers (HashiCorp staff), using our internal tooling. The following notes are only relevant to maintainers.

Release process:

 1. Update [`version/VERSION`](https://github.com/hashicorp/hc-install/blob/main/version/VERSION) to remove `-dev` suffix and set it to the intended version to be released
 1. Wait for [`build` workflow](https://github.com/hashicorp/hc-install/actions/workflows/build.yml) to finish
 1. Ensure you have the appropriate GitHub PAT set in `BOB_GITHUB_TOKEN` variable
 1. Set `SHA` to the corresponding (long) last commit SHA (after updating `VERSION` file) & `VERSION` to the same version
 1. Use `bob` to promote artifacts to **staging**
 ```
bob trigger-promotion \
    --product-name=hc-install \
    --environment=hc-install-oss \
    --org=hashicorp \
    --repo=hc-install \
    --slack-channel=C01QDH3Q37W \
    --product-version=$VERSION \
    --sha=$SHA \
    --branch=main \
    staging
 ```
 6. Use `bob` to promote artifacts to **production**
 ```
bob trigger-promotion \
    --product-name=hc-install \
    --environment=hc-install-oss \
    --org=hashicorp \
    --repo=hc-install \
    --slack-channel=C01QDH3Q37W \
    --product-version=$VERSION \
    --sha=$SHA \
    --branch=main \
    production
 ```
7. Wait for a message in the Slack channel saying that authorisation is needed to promote artifacts to production. Click on the link and approve.
8. Once notified that promotion is successful, go to https://github.com/hashicorp/crt-workflows-common/actions/workflows/promote-production-packaging.yml, locate the hc-install promote-production-packaging workflow, and approve.
