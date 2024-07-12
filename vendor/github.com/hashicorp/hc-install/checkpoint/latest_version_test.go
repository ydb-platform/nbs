// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package checkpoint

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
    _ src.Installable    = &LatestVersion{}
    _ src.Removable      = &LatestVersion{}
    _ src.LoggerSettable = &LatestVersion{}
)

func TestLatestVersion(t *testing.T) {
    testutil.EndToEndTest(t)

    lv := &LatestVersion{
        Product: product.Terraform,
    }
    lv.SetLogger(testutil.TestLogger())

    ctx := context.Background()

    execPath, err := lv.Install(ctx)
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { lv.Remove(ctx) })

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

func TestLatestVersionValidate(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        lv          LatestVersion
        expectedErr error
    }{
        "Product-incorrect-binary-name": {
            lv: LatestVersion{
                Product: product.Product{
                    BinaryName: func() string { return "invalid!" },
                    Name:       product.Terraform.Name,
                },
            },
            expectedErr: fmt.Errorf("invalid binary name: \"invalid!\""),
        },
        "Product-incorrect-name": {
            lv: LatestVersion{
                Product: product.Product{
                    BinaryName: product.Terraform.BinaryName,
                    Name:       "invalid!",
                },
            },
            expectedErr: fmt.Errorf("invalid product name: \"invalid!\""),
        },
        "Product-valid": {
            lv: LatestVersion{
                Product: product.Terraform,
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            err := testCase.lv.Validate()

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
