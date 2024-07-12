# Test data

This directory contains test data to mock `releases.hashicorp.com` in tests.

## Whoa, what's that private key here?

The attached private key is not used anywhere except in tests (ie. nothing was leaked)
and is provided merely for convenience of future developers who can generate
new test data without having to generate a new key.

## How to generate new mock Terraform builds?

```
cd ./terraform
export VERSION=0.14.11
gpg --import ../2FCA0A85.private.asc
goreleaser release --snapshot --rm-dist
mkdir -p ../mock_terraform_builds/${VERSION}
mv ./dist/terraform_${VERSION}_* ../mock_terraform_builds/${VERSION}/
```
