// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package build

import (
    "context"
    "fmt"
    "testing"

    "github.com/hashicorp/go-version"
    "github.com/hashicorp/hc-install/internal/testutil"
    "github.com/hashicorp/hc-install/product"
    "github.com/hashicorp/hc-install/src"
)

var (
    _ src.Buildable      = &GitRevision{}
    _ src.Removable      = &GitRevision{}
    _ src.LoggerSettable = &GitRevision{}
)

func TestGitRevision_terraform(t *testing.T) {
    testutil.EndToEndTest(t)

    gr := &GitRevision{Product: product.Terraform}
    gr.SetLogger(testutil.TestLogger())

    ctx := context.Background()

    execPath, err := gr.Build(ctx)
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { gr.Remove(ctx) })

    v, err := product.Terraform.GetVersion(ctx, execPath)
    if err != nil {
        t.Fatal(err)
    }

    latestConstraint, err := version.NewConstraint(">= 1.0")
    if err != nil {
        t.Fatal(err)
    }
    if !latestConstraint.Check(v.Core()) {
        t.Fatalf("versions don't match (expected: %s, installed: %s)",
            latestConstraint, v)
    }
}

func TestGitRevision_consul(t *testing.T) {
    testutil.EndToEndTest(t)

    gr := &GitRevision{Product: product.Consul}
    gr.SetLogger(testutil.TestLogger())

    ctx := context.Background()

    execPath, err := gr.Build(ctx)
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { gr.Remove(ctx) })

    v, err := product.Consul.GetVersion(ctx, execPath)
    if err != nil {
        t.Fatal(err)
    }

    latestConstraint, err := version.NewConstraint(">= 1.0")
    if err != nil {
        t.Fatal(err)
    }
    if !latestConstraint.Check(v.Core()) {
        t.Fatalf("versions don't match (expected: %s, installed: %s)",
            latestConstraint, v)
    }
}

func TestGitRevision_vault(t *testing.T) {
    testutil.EndToEndTest(t)

    gr := &GitRevision{Product: product.Vault}
    gr.SetLogger(testutil.TestLogger())

    ctx := context.Background()

    execPath, err := gr.Build(ctx)
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { gr.Remove(ctx) })

    v, err := product.Vault.GetVersion(ctx, execPath)
    if err != nil {
        t.Fatal(err)
    }

    latestConstraint, err := version.NewConstraint(">= 1.0")
    if err != nil {
        t.Fatal(err)
    }
    if !latestConstraint.Check(v.Core()) {
        t.Fatalf("versions don't match (expected: %s, installed: %s)",
            latestConstraint, v)
    }
}

func TestGitRevisionValidate(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        gr          GitRevision
        expectedErr error
    }{
        "Product-incorrect-binary-name": {
            gr: GitRevision{
                Product: product.Product{
                    BinaryName: func() string { return "invalid!" },
                    Name:       product.Terraform.Name,
                },
            },
            expectedErr: fmt.Errorf("invalid binary name: \"invalid!\""),
        },
        "Product-incorrect-name": {
            gr: GitRevision{
                Product: product.Product{
                    BinaryName: product.Terraform.BinaryName,
                    Name:       "invalid!",
                },
            },
            expectedErr: fmt.Errorf("invalid product name: \"invalid!\""),
        },
        "Product-missing-build-instructions": {
            gr: GitRevision{
                Product: product.Product{
                    BinaryName: product.Terraform.BinaryName,
                    Name:       product.Terraform.Name,
                },
            },
            expectedErr: fmt.Errorf("no build instructions"),
        },
        "Product-missing-build-instructions-build": {
            gr: GitRevision{
                Product: product.Product{
                    BinaryName: product.Terraform.BinaryName,
                    BuildInstructions: &product.BuildInstructions{
                        GitRepoURL: product.Terraform.BuildInstructions.GitRepoURL,
                    },
                    Name: product.Terraform.Name,
                },
            },
            expectedErr: fmt.Errorf("missing build instructions"),
        },
        "Product-missing-build-instructions-gitrepourl": {
            gr: GitRevision{
                Product: product.Product{
                    BinaryName: product.Terraform.BinaryName,
                    BuildInstructions: &product.BuildInstructions{
                        Build: product.Terraform.BuildInstructions.Build,
                    },
                    Name: product.Terraform.Name,
                },
            },
            expectedErr: fmt.Errorf("missing repository URL"),
        },
        "Product-valid": {
            gr: GitRevision{
                Product: product.Terraform,
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            err := testCase.gr.Validate()

            if err == nil && testCase.expectedErr != nil {
                t.Fatalf("expected error: %s, got no error", testCase.expectedErr)
            }

            if err != nil && testCase.expectedErr == nil {
                t.Fatalf("expected no error, got error: %s", err)
            }

            if err != nil && testCase.expectedErr != nil && err.Error() != testCase.expectedErr.Error() {
                t.Fatalf("expected error: %s, got error: %s", testCase.expectedErr, err)
            }
        })
    }
}
